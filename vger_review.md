# pangalactic.vger review — `vger.py` (2026-07-19)

Scope: `pangalactic/vger/vger.py` (2945 lines), reviewed in full. Per the
current review activity's constraints, this pass **assumes WAMP-cryptosign
is the authentication method in use** — i.e. `cb_details.caller_authid` is
treated as a trustworthy claim about caller identity (verified against
`authenticator.py`'s dynamic authenticator, which binds `authid` to a
public key looked up in the crossbar `principals.db`; parameterized SQL,
no injection risk there). Findings below are therefore about how `vger.py`
*uses* (or fails to use) that identity for authorization — not about
whether the identity itself can be forged. Requirements-management-scoped
code is excluded per the same policy as the `pangalactic.core` pass (e.g.
`get_caches()`'s handling of `rqt_allocz`/`allocz` was skipped, including a
dead `oids` parameter noticed there — it's requirements-scoped, so out of
scope here). `pangalactic.node`, `gargleblaster`, and vger's other modules
(`authenticator_gssapi.py`, `authenticator_ticket.py`, `userdir.py`,
`transform.py`) were not reviewed — deferred to the later session per the
stated activity plan.

---

## Findings (most severe first)

### 1. `upload_chunk()` — arbitrary file write via unsanitized `fname` (path traversal)
`pangalactic/vger/vger.py:653-677`
```python
def upload_chunk(fname=None, seq=0, data=b'', cb_details=None):
    ...
    vault_fpath = os.path.join(orb.vault, fname)
    with open(vault_fpath, 'ab') as f:
        f.write(data)
    return seq
```
`fname` is taken directly from the RPC caller with **no sanitization and no
authorization check of any kind** (the handler doesn't even read
`cb_details`). Two independent ways to escape `orb.vault`:
- `os.path.join(orb.vault, fname)` **discards the first argument entirely**
  if `fname` is an absolute path (standard `os.path.join` behavior) — e.g.
  `fname='/etc/cron.d/evil'` writes exactly there.
- A relative `fname` containing `../` segments (e.g.
  `'../../../home/vger/.ssh/authorized_keys'`) resolves outside `orb.vault`
  the same way any unsanitized path-join does.

Any client that can complete a WAMP-cryptosign handshake (i.e. any
registered user, no elevated role required) can therefore append
attacker-controlled bytes to **any file the vger OS process can write to**
— repeated calls with different `seq` values and `'ab'` (append) mode make
this a practical primitive for planting files, corrupting application data,
or worse depending on the vger process's filesystem permissions. This is
the most severe finding in the file.

Contrast with `download_chunk()` (line 699+) and the `add_update_model`/
`add_update_doc` flow, which derive their vault paths from
`orb.get_vault_fname()`/`get_vault_fpath()` (`rep_file.oid + '_' +
rep_file.user_file_name`, in `pangalactic.core/uberorb.py:2039-2050`) tied
to a server-side object — not a bulletproof design either (`user_file_name`
itself comes from client-submitted `parms.get('file name')` with no
sanitization, so the same class of traversal is reachable one step removed
through that path too), but `upload_chunk` needs no prior object creation
at all — a bare RPC call is enough.

### 2. `save()` — authorization bypass via spoofed `creator` field
`pangalactic/vger/vger.py:824-835`
```python
# objects created by the user
authorized = {oid:so for oid, so in sobjs_unique.items()
              if so.get('creator') == user_oid}
# existing objects for which the user has 'modify' permission
for oid, so in sobjs_unique.items():
    obj_in_repo = orb.get(so.get('oid'))
    if obj_in_repo:
        obj_id = obj_in_repo.id
        perms = get_perms(obj_in_repo, user=user_obj)
        if 'modify' in perms:
            authorized[oid] = so
```
The first line grants `authorized` status to any submitted object whose
**client-supplied** `creator` field equals the caller's own oid — with no
check that the object is actually new (`obj_in_repo is None`). The second
loop only *adds* more entries via a real `get_perms()` check; it never
removes or re-validates what the first line already added. Net effect,
traced end to end: a user who knows the oid of an existing object they do
not own (discoverable via `search_exact`/`sync_objects`/`get_objects` for
any public object) can submit
`vger.save([{'oid': <target oid>, '_cname': <cname>, 'creator': <their own
oid>, 'mod_datetime': <now>, <field>: <new value>, ...}])` and have
arbitrary fields applied to the existing object — completely bypassing
`get_perms()`. `deserialize()` (`pangalactic.core/serializers.py`) trusts
its caller for authorization and applies submitted fields via `setattr`
once the object is in `authorized`; it does not independently re-check
ownership. The only other gate the attacker must clear is supplying a
`mod_datetime` later than the object's current one, which is trivial
(current timestamp).

### 3. `save()` — `sobjs.remove(so)` on a `dict_values` view crashes the entire batch
`pangalactic/vger/vger.py:786-802`
```python
sobjs_unique = {so.get('oid'): so for so in serialized_objs
                if so.get('oid')}
sobjs = sobjs_unique.values()
...
for oid, so in sobjs_unique.items():
    if oid in deleted and so in sobjs:
        unauth_ids.append(so.get('id') or 'unknown_id')
        sobjs.remove(so)
```
`sobjs_unique.values()` is a `dict_values` view object, which has no
`.remove()` method — `AttributeError: 'dict_values' object has no
attribute 'remove'`. This fires whenever **any** object in a submitted
`save()` batch has an oid present in the server's `deleted` cache — a
realistic scenario (a client syncing stale local edits to an object another
user has since deleted, or any offline-edit/delete race). The exception is
unhandled at this level, so the entire RPC call fails for the whole batch,
not just the one stale object — every other valid object in the same
`save()` call is rejected as a side effect.

### 4. `sync_project()` — `UnboundLocalError` when the caller has no `caller_authid`
`pangalactic/vger/vger.py:1563-1572`
```python
userid = getattr(cb_details, 'caller_authid', '')
if userid:
    user = orb.select('Person', id=userid)
result = [[], [], [], [], [], {}, {}]
if not project_oid or project_oid == 'pgefobjects:SANDBOX':
    orb.log.info('   no project oid or SANDBOX -- no result.')
    return result
if not user:
    orb.log.info('   no user found -- cannot authorize.')
    return result
```
`user` is only assigned inside `if userid:`. When `caller_authid` is
missing (defaults to `''`, which is falsy), `user` is never bound, and
`if not user:` raises `UnboundLocalError` instead of taking the intended
"no user found" early return — the same shape of bug as the
`get_next_rqt_seq` `UnboundLocalError` found in the `pangalactic.core`
pass. Every other RPC in this file that reads `caller_authid` either
defaults it to `'unknown'`/`None` and calls `orb.select('Person',
id=userid)` unconditionally (safe — returns `None` for an unresolvable id),
or (like here) guards the lookup but then forgets the guard was
conditional.

### 5. Inconsistent authorization on user-directory RPCs
`pangalactic/vger/vger.py:2621-2634` (`get_user_object`) and `2825-2864`
(`get_people`)
```python
def get_user_object(userid):
    orb.log.info('* [rpc] vger.get_user_object()')
    return serialize(orb, [orb.select('Person', id=userid)])[0]

yield self.register(get_user_object, 'vger.get_user_object')
```
`get_user_object` takes a **caller-supplied** `userid` with no
`cb_details`/`RegisterOptions(details_arg=...)` at all, and returns that
person's full serialized record — no `is_cloaked`/`get_perms` check, no
derivation from the caller's own authenticated identity. `get_people()`
(no args, no `cb_details`) does the same for the entire roster, plus
whether each person has an active public key in the crossbar principals
db. Compare with `get_object`/`get_objects` (lines 2359-2456), which
carefully check `is_cloaked`/`get_perms` per object, and with
`get_user_roles`'s docstring, which explicitly notes caller identity should
come from `cb_details.caller_authid`, not a client-supplied argument.
This may be intentional (an org-wide user directory is a defensible design
for a collaboration tool), but as written any authenticated user can query
any other specific user's full Person record and see who currently has
repository access (`get_people`'s active-key flag) with zero authorization
logic — worth a deliberate decision either way rather than the current
inconsistency with the object-access RPCs.

### 6. `RepositoryService.__init__` depends on bare module globals set only in `__main__`
`pangalactic/vger/vger.py:144-158` (reading `cb_host`, `cb_port`, `cb_url`,
`realm`) vs. `2905-2912` (where they're actually assigned, inside
`if __name__ == '__main__':`)
Every other `__init__` parameter (`home`, `local_user`, `db_url`, `debug`,
`console`, `test`, `ldap_url`, `base_dn`) follows the `kw.get(x) or
config.get(x, default)` pattern, giving `RepositoryService` a well-defined
value regardless of how/when it's constructed. `cb_host`/`cb_port` instead
read bare global names with no `kw`/`config` fallback, relying entirely on
`__main__` having already executed those assignments before
`Component(session_factory=RepositoryService, ...)` triggers construction.
That happens to hold today (the script instantiates the reactor Component
after setting the globals, and `run([comp])` is what actually triggers
`RepositoryService.__init__`), so this isn't currently reachable as a
crash — but it's a real inconsistency with the rest of the method, and
would raise `NameError` if `RepositoryService` were ever constructed from a
test harness or alternate entry point that imports `vger` without running
its `__main__` block (the package does have a `test/` directory with its
own crossbar config, suggesting that's a plausible future use).

### 7. Broad bare `except:` around entire per-item loops masks partial writes
`pangalactic/vger/vger.py:1682-1708` (`set_parameters`), `1786-1819`
(`set_data_elements`), `1896-1928` (`set_properties`)
Each of these wraps its *whole* per-oid loop in one `try: ... except:
return 'failure'` (or `'failure: exception'`). A single bad item partway
through a batch (unknown oid, malformed value, etc.) causes the entire
handler to report a single opaque failure — but for `set_parameters` and
`set_properties`, the mutations already applied to the shared in-memory
caches (`set_pval`/`orb.set_prop_val`) for the *earlier* items in the same
loop are **not rolled back**: the function returns `'failure: exception'`
before reaching `recompute_parmz()`/the publish step, so those already-made
changes are left in the cache, uncommitted-looking to the caller (who was
told the whole call failed) but live in server memory for the next
recompute or restart to pick up. `set_parameters` and `set_properties` also
don't guard `obj = orb.get(oid)` against `None` before calling
`get_perms(obj, ...)` the way `set_data_elements` does (`if not obj:
continue` at line ~1789) — an unknown oid there falls through to
`get_perms(None, ...)` rather than being skipped explicitly, relying on
`access.py`'s permissive defaults for a non-`Product` "object" (`getattr(None,
'frozen', False)` etc. don't raise, but the resulting permission decision
for `None` is incidental, not intentional).

### 8. Missing null-checks on caller-supplied FK oids in `add_update_model`/`add_update_doc`
`pangalactic/vger/vger.py:536-537` (`add_update_model`) and `606-610`
(`add_update_doc`)
```python
thing = orb.get(parms.get('of_thing_oid', ''))
orb.log.info(f'        model of thing: {thing.id}')   # crashes if thing is None
...
owner = orb.get(parms.get('owner_oid'))
if not owner:
    owner = orb.get(parms.get('project_oid'))
orb.log.info(f'        doc owner: {owner.id}')          # crashes if owner is None
```
Both functions already demonstrate the right pattern elsewhere in the same
handler (`add_update_doc`'s `rel_obj` check at line 601-604 returns a clean
error tuple if the lookup fails), but `thing` in `add_update_model` is never
null-checked before `.id`, and `owner` in both functions can end up `None`
(if `owner_oid`/`project_oid` are both missing or invalid) before being
dereferenced. A malformed or stale client payload crashes the handler with
an unhandled `AttributeError` instead of the clean `(error message, [])`
response the sibling check already establishes as the intended contract.

---

## Cross-reference: a `pangalactic.core` defect that is most severe here

`RepositoryService.shutdown()` (`vger.py:298-314`) calls
`orb.dump_all()`/`orb.save_caches()`, which reach the three `save_*`
functions in `pangalactic.core/pangalactic/core/parametrics.py`. Those
functions open their JSON file in `'w'` mode (truncating it) *before*
calling `json.dumps`, so a serialization failure leaves the file at zero
bytes, swallows the error into a `log.debug`, and lets `save_caches()`
report success at `log.info`. Because `save_caches()` writes to both
`orb.home` and the dated backup dir in the same call, one bad value zeroes
both; only previous days' backups survive.

This matters far more on vger than on a client: `parameterz`,
`data_elementz`, and `mode_defz` are the only caches not derivable from the
database, and the server's copies are the authoritative ones that clients
re-sync from. See finding #1 in
`pangalactic.core/pangalactic_core_review_scoped.md` for the verified
reproduction and the fix.

## Verified assumption

`authenticator.py`'s dynamic authenticator (`pangalactic/vger/
authenticator.py:38-74`) binds `caller_authid` to a public key via a
parameterized SQL lookup (`SELECT authid, role FROM users WHERE pubkey =
?`) against the crossbar `principals.db` — no injection risk, and
`caller_authid` is a trustworthy claim of identity under cryptosign as long
as the corresponding private key is kept secret client-side. This
underpins why finding #2 above is a real authorization bug rather than a
non-issue: the caller's *identity* can be trusted, but `save()`'s
*authorization logic* substitutes a client-supplied payload field
(`creator`) for the identity check it should be doing instead.

## Suggested fix order

1. **#1 (`upload_chunk` path traversal)** — fix immediately; validate
   `fname` is a bare filename (no path separators, no leading `/`) and/or
   require it to match a filename already registered as a vault target
   before opening for write.
2. **#2 (`save()` creator-spoofing bypass)** — gate the "creator" fast-path
   on `obj_in_repo is None` (i.e. only trust the submitted `creator` field
   for genuinely new objects); existing objects should go through
   `get_perms()` only.
3. **#3 (`dict_values.remove()` crash)** — trivial fix: use a `list()`
   instead of `.values()`, or filter with a list/dict comprehension instead
   of mutating in place.
4. **#4 (`sync_project` UnboundLocalError)** — initialize `user = None`
   before the `if userid:` guard.
5. **#5 (user-directory auth)** — deliberate decision needed: either
   document that the Person roster is intentionally org-wide readable, or
   add the same `is_cloaked`/`get_perms` treatment used in `get_object`.
6. **#6 (global coupling)** — pass `cb_host`/`cb_port`/`cb_url`/`realm`
   through `kw`/`config` like every other `__init__` parameter.
7. **#7/#8** — narrow the broad `except:` blocks to per-item try/except so
   one bad item doesn't swallow an entire batch's results or error
   messages, and add the missing null-checks with clean error returns.
