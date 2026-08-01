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

**STATUS: FIXED.** `upload_chunk` now rejects the call unless (a) the
caller's `caller_authid` resolves to a known `Person` and (b) `fname` passes
a new module-level `valid_vault_fname()` — a bare file name, no `/` or `\`,
not absolute, no drive letter, not `.`/`..`, no embedded null. Rejections
raise `ApplicationError`, so the client's existing errback fires instead of
the call silently appearing to succeed. **Verified by execution**, against a
temp vault with a victim file outside it:

| attack `fname` | pre-fix | post-fix |
|---|---|---|
| `../victim/authorized_keys` | wrote, no error — **file breached** | blocked, vault untouched |
| `/abs/path/victim/authorized_keys` | wrote, no error — **file breached** | blocked, vault untouched |

Both legitimate client-generated shapes (`report.pdf`,
`<oid>_<name with spaces>.step`) still pass.

*(Not addressed, and worth a separate decision: `'ab'` mode means a repeated
upload of the same `fname` appends to the existing vault file rather than
replacing it, so an interrupted-and-retried upload silently corrupts the
file. Opening `'wb'` for `seq == 0` and `'ab'` thereafter would fix it, but
that is a behaviour change beyond this finding.)*

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

**STATUS: FIXED.** The two loops are now one, and the submitted `creator`
field is only consulted when `obj_in_repo is None` — i.e. for a genuinely new
object, where there is nothing else to check it against. An object that
already exists is authorized purely by `get_perms()`. **Verified by
execution** against a real orb with the standard test fixtures, running the
pre-fix and post-fix authorization blocks (transcribed verbatim) over the
same payload, as user `zaphod` targeting `FDValve-0000866` (creator `admin`,
`zaphod` perms `['add docs', 'add models', 'view']`):

| case | pre-fix | post-fix |
|---|---|---|
| spoofed `creator` on an existing object the user cannot modify | **authorized — bypass** | refused |
| genuinely new object created by the user | authorized | authorized |
| existing object the user really created | authorized | authorized |

The two regression cases confirm the fix does not cost the legitimate
creator anything: a real creator still gets `modify` from `get_perms()`'s own
creator branch, so the fast path was never load-bearing for them.

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

**STATUS: FIXED.** The in-place mutation is gone; entries whose oid is in the
`deleted` cache are now `pop`ped from `sobjs_unique` itself. **Verified by
execution** on a three-object batch with one oid in `deleted`: pre-fix raises
`AttributeError: 'dict_values' object has no attribute 'remove'` and the
whole batch fails; post-fix the two good objects survive and the stale one is
reported in `unauth`.

Note this also closes a **second bug in the same block**: the removal was
being applied to `sobjs`, but every subsequent step — the ownerless check and
the whole authorization section — operates on `sobjs_unique`, which was never
touched. So even had `sobjs` been a real list, an object in the `deleted`
cache would have been named in `unauth_ids` *and saved anyway*. Popping from
`sobjs_unique` is what the block was evidently meant to do; `sobjs` is a view
of it, so it now reflects the removals and the `if not sobjs:` early return
still behaves as before.

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

**STATUS: FIXED.** `user = None` is now initialized before the `if userid:`
guard. **Verified by execution**: pre-fix, a call with no `caller_authid`
raises `UnboundLocalError`; post-fix it takes the intended "no user found"
early return, and the normal-caller path is unchanged.

A sweep of all 27 `caller_authid` sites in the file confirms this was the
**only** remaining instance of the pattern — `get_object` (line ~2646) also
guards the lookup with `if userid:` but uses `user` exclusively inside that
block, and `get_objects` (~2703) already initializes `user_obj = None` first.

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

**STATUS: NOT A DEFECT — intentional, confirmed by the author.** The
org-wide readable Person roster is a structural requirement of the sync
model, not an oversight. Each user holds a complete local database of the
objects belonging to the projects they have a role on, and those objects
carry FK references to `Person` instances — `creator` and `modifier` on every
`HardwareProduct`, and so on. Those `Person` records are populated into the
client's database during the initial sync, which uses `get_people()`, and
refreshed when a new user is added. Restricting the roster would therefore
break FK resolution for objects the user is already entitled to see.

Given that, the asymmetry with `get_object`/`get_objects` is principled
rather than accidental: those RPCs gate *project data*, which is what
`is_cloaked`/`get_perms` exist to protect, while the Person roster is
reference data the client cannot function without. No code change.

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

**STATUS: FIXED.** `cb_host`, `cb_port`, `cb_url`, and `realm` now follow the
same `kw.get(x) or config.get(x, default)` pattern as every other `__init__`
parameter, with defaults matching what the `__main__` block uses. `__main__`
additionally writes `config['realm']` now, alongside the `cb_host`/`cb_port`/
`cb_url` entries it already wrote, so the values `__init__` logs are the ones
actually in use.

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

**STATUS: FIXED.** All three now handle each oid in its own `try/except
Exception`, logging the failure at `error` with the oid and skipping to the
next item, then report the count of failed oids. The publish/commit phase
keeps a separate `try/except` so the existing return contracts
(`parmz_dts`/`dez_dts` on success, `'failure: not authorized'`,
`'failure: exception'`, `'failure'`) are unchanged. `set_parameters` and
`set_properties` also gained the explicit `if not obj: continue` guard that
`set_data_elements` already had, so an unknown oid is skipped rather than
falling through to `get_perms(None, ...)`.

Two consequences beyond the finding as written:
- A bad item no longer strands earlier items' cache mutations behind a
  whole-call failure report — the good items complete and are published.
- `set_properties` previously created `prop_mods[oid] = {}` *before* testing
  `'modify' in perms`, so an unauthorized oid stayed in `prop_mods` as an
  empty entry, and the `oids = list(prop_mods)` step then bumped and
  committed `mod_datetime` on objects the caller had no right to touch. The
  entry is now created only after the permission check passes.

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

**STATUS: FIXED.** `add_update_model` now returns
`('model has no "of_thing" object', [])` when `thing` is missing and
`('model has no owner', [])` when neither `owner_oid` nor `project_oid`
resolves; `add_update_doc` returns `('doc has no owner', [])` for the same
owner case. All follow the error-tuple contract already set by
`add_update_doc`'s `rel_obj` check. The `thing.owner.id` log line was also
made defensive (`getattr(thing.owner, 'id', '[none]')`) — a `ManagedObject`
with no owner is possible (`save()` has an explicit `no_owners` path for
exactly that), and it should not crash a log statement.

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

## Found by live testing against marvin (2026-08-01)

Surfaced while testing the new non-LDAP "New User" flow
(`pangalactic.node/admin_tool_review.md` #2) end to end against the live test
server. Neither would have been found by reading, and neither was found by the
unit suite — the first is a database constraint, the second only bites without
LDAP.

### A. `add_person()` could not create a Person at all — the oid was never generated

`add_person` built the object as
`Person(create_datetime=dts, mod_datetime=dts, **data)` with no `oid`, and
`identifiable_.oid` is **not nullable**, so a genuinely new person failed with:

```
IntegrityError: (psycopg2.errors.NotNullViolation) null value in column "oid"
of relation "identifiable_" violates not-null constraint
DETAIL: Failing row contains (null, Person, reno, null, Reno Nevada, ...)
```

This is the *server-side* half of "add user is broken unless LDAP is being
used": a user could only ever be added when an oid arrived with the data —
i.e. from an LDAP directory record (`gargleblaster`'s schema maps `OID` →
`oid`). The same function already generates an oid this way for a new
`Organization` a few lines above, so it was an internal inconsistency as much
as an omission.

**STATUS: FIXED.** The oid is generated server-side with `uuid4()` when the
caller does not supply one. Per the author, that is the right layer: a user
has no need to know their own oid — the userid identifies them, and the oid is
what it maps to.

### B. Nothing enforced that the userid is unique

`add_person` decided create-vs-update purely by `orb.get(data.get('oid'))`, so
a caller supplying a new oid with an already-taken `id` would have created a
**second** Person with the same userid — and since the userid becomes the
`authid` in the authenticator's principals db, the userid → oid mapping would
have become ambiguous.

Per the author, production use assumed **LDAP** was responsible for userid
uniqueness, which is not a safe assumption once users can be created without
it.

**STATUS: FIXED.** When no oid is supplied (or it matches nothing), the lookup
now falls back to the userid, so such a call updates the existing person
instead of duplicating them. The update branch also no longer reassigns
`oid` — it is the primary key, and a person matched by userid will not have
the submitted one.

**Verified by execution** (real orb, standard fixtures, handler driven through
the capture harness):

| case | result |
|---|---|
| new person, **no oid supplied** | created; oid generated server-side |
| same userid again, still no oid | **1** Person with that id — updated, not duplicated; oid unchanged |
| non-admin caller | refused |

*Still to verify live:* `pk_added` was `False` in the local run because there
is no `principals.db` at the configured `auth_db_path` — which is precisely
what the new startup warning reports. Confirming that the public key lands in
the db, and that the new user can then authenticate, needs a redeployed vger.

*(Considered and rejected, for the record: using the userid as the oid, since
it is unique. It would break federation across separate vger environments,
and it contradicts the established invariant that an oid is non-semantic and
never caller-derivable — see the `clone()` discussion in
`pangalactic.core/pangalactic_core_review_scoped.md`.)*

## Missed by this review — found by the test suite

- **`get_object` was registered under the name `vger.get_mod_dts`**
  (`yield self.register(get_object, 'vger.get_mod_dts')`). So that RPC took
  the wrong arguments, and `get_mod_dts` — defined immediately above it — was
  never registered at all. Latent, because clients use their own local
  `orb.get_mod_dts`. Found and fixed on the optional-LDAP branch by a test
  asserting that each registered *name* corresponds to the expected
  *function*; **this pass reviewed `vger.py` in full and did not catch it.**
  Worth recording as a lesson about what manual review is bad at: the
  registration block is 46 near-identical `yield self.register(...)` lines,
  and a copy-paste substitution in one of them reads as correct.

  (A second bug was found at the same time in `userdir.py` —
  `search_ldap_directory()` raised `UnboundLocalError` when no `ldap_schema`
  was configured, since `f` was only initialized inside the `if schema:`
  branch but used unconditionally. That module was explicitly out of scope
  for this pass, per the scope note at the top.)

## Carried forward to the next `vger` pass

- **`--cert` is a dead command-line option.** `__main__` defines it
  (`parser.add_argument('--cert', dest='cert', type=str,
  default='server_cert.pem', ...)`) but **never reads `options.cert`**: the
  self-signed-cert branch hardcodes `cert_fname = 'server_cert.pem'` and joins
  it to `home`. So `--cert /path/to/other_cert.pem` is accepted and silently
  ignored, and the certificate must be in the home directory under exactly
  that name.

  This is not merely cosmetic: `pangalactic/vger/test/run_vger_test.sh`
  documents `--cert ~/remote_server_cert.pem` as the way to point vger at a
  **remote** crossbar host, so the documented workflow for the remote case
  does not work. Same shape as the `--key` option bug in
  `pangalaxian.py` (see `node_startup_review.md` #1) — an option that parses
  but is never honoured.

  **STATUS: FIXED.** `cert_fname` is now resolved with the same
  option/config/default precedence as every other setting (argparse default
  changed to `''` so config can win), stored in `config['cert']` before
  `write_config()`, and a **bare name is resolved in the home directory while
  a full path is used as given** — so the remote-host workflow
  `run_vger_test.sh` documents now works. Verified across all five
  precedence/shape combinations.

  Two further defects in the same ten lines, fixed with it:
  - **the `try` was in the wrong place.** `open()` and `load_certificate()`
    sat *outside* it, so a missing or malformed cert raised an uncaught
    `FileNotFoundError` and the message that names exactly that case
    (`"Could not find self-signed cert -- exiting."`) was unreachable.
    Verified: pre-fix a missing cert gives a traceback, post-fix a clean
    message and `sys.exit(1)`.
  - **the cert file was never closed** — `str(open(cert_fpath, 'r').read())`,
    now a `with` block.

  Also: `__init__` logged `server cert: 'server_cert.pem'` as a hardcoded
  string regardless of configuration; it now logs the cert actually in use.

- **vger and the authenticator disagree about where `principals.db` is.**
  `vger.add_person()` writes a new user's public key to
  `config.get('auth_db_path', os.path.join(orb.home, 'crossbar',
  'principals.db'))`, while `authenticator.py` reads the **hardcoded absolute
  path** `/node/principals.db` (and `/node/principals.json`), which its
  docstring documents as the directory mapped into the crossbar *docker*
  service. In the docker deployment the two coincide; anywhere else they do
  not, so a person added through the admin tool gets a public key written to a
  db the router never reads — and that user then cannot authenticate.

  *Correction to an earlier draft of this entry, which said there was "nothing
  in either log to say why".* That was wrong on both counts, and the real
  behaviour is worth stating precisely:
  - **vger does log it.** `add_person` has an `else:` branch — `path
    "{auth_db_path}" not found -- could not add public key` — at `info`.
  - **The client is told, but weakly.** `add_person` returns
    `(pk_added, ser_objs)` and `admin.on_person_added_success` builds
    `'Person "X" has been added'` and appends `' with public key'` **only
    when `pk_added` is True**. So the administrator gets a cheerful "Person
    Added" popup either way, differing only by four missing words. The
    failure is visible in principle and easy to miss in practice.

  Found while writing the interactive-testing recipe
  (`NOTES_ON_TESTING.md` §8.4), not during the original pass —
  `authenticator.py` was in scope only as far as the "Verified assumption"
  note below.

  **STATUS: FIXED.** `authenticator.py` now derives its paths from
  `PGEF_PRINCIPALS_DIR`, **defaulting to `/node`** so docker deployments are
  unaffected, and logs which principals db it is using when it starts. The
  authenticator runs inside crossbar and cannot read vger's config, so an
  environment variable set where crossbar is started is the available channel.
  Verified: with the variable unset the paths are exactly as before; with it
  set they follow it, and can be pointed at the same file as vger's
  `auth_db_path`.

  Paired with a **startup check in vger**: `auth_db_path` is now logged with
  the other settings, and if it does not exist vger logs at `error` that
  adding a user will not register their public key. Previously the mismatch
  only surfaced at `info` level the first time somebody was added — which is
  precisely why it went unnoticed.

- **`get_parmz`'s `oids` branch can emit `None` values.**
  `{oid: parameterz.get(oid) for oid in oids}` yields `None` for any oid the
  server does not know about. The client's
  `on_vger_get_parmz_result` does `parameterz.update(parmz_data)`, so a
  `None` would be written straight into the cache where every consumer
  expects a dict. **Unreachable today** — the client only ever calls
  `get_parmz()` with no arguments, which takes the other branch and returns
  the whole cache — but it is a trap waiting for the first caller that passes
  `oids`. Fix on the server side: skip unknown oids rather than emitting
  `None` for them.

  Noted while reviewing the client's parameter handling; see
  `pangalactic.node/pangalaxian_handlers_review.md` #2, which also records
  why the client's wholesale replacement of `parameterz` is correct and must
  not be softened into a merge.

## Status summary (2026-07-31)

Findings **#1, #2, #3, #4, #6, #7, #8 are fixed** in `vger.py`; each is
annotated inline above with what changed and how it was verified. **#5 is
closed as intentional** — the author confirmed the org-wide Person roster is
required by the sync model (clients need `Person` records to resolve
`creator`/`modifier` FKs on project objects they already hold); no code
change.

The one deliberately deferred item is noted under #1: `upload_chunk` opens
the vault file in `'ab'` mode regardless of `seq`, so a retried upload
appends to the previous attempt instead of replacing it. That is a
pre-existing data-corruption risk, independent of the traversal fix, and
needs its own decision.

Verification note: `vger.py` has **no test suite**, and cannot currently be
imported in an environment without `python-ldap`, because
`pangalactic/vger/userdir.py` does a bare top-level `import ldap` which
`vger.py` imports at module level. The checks above were therefore run with
`sys.modules['ldap']` stubbed, and the RPC handlers — which are closures
inside `RepositoryService.onJoin` — were exercised by transcribing the
pre-fix and post-fix blocks verbatim and running both against a real orb with
the standard test fixtures.

**Correction to that last point (2026-07-31).** I claimed the handlers were
"not reachable without a WAMP session" and that lifting them out of `onJoin`
was a prerequisite for testing. That was wrong. `onJoin` is an
`@inlineCallbacks` generator whose only session interactions are `register`,
`subscribe`, `publish` and `log`; with a stand-in session returning
already-fired Deferreds it runs synchronously and **all 42 RPCs can be
captured and called directly, with no production change**. Finding #2's
creator-spoofing case has since been re-run against the *real* `vger.save`
handler rather than a transcription, with the same result (refused, reported
in `unauth`, object unchanged, nothing published). See `NOTES_ON_TESTING.md`.
Only the `python-ldap` import is a genuine blocker, and it is being addressed
separately.

## Suggested fix order (as originally written; all now addressed)

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
