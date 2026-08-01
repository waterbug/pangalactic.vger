# Design note: a test suite for `pangalactic.vger`

Written 2026-07-31, at the close of the `vger.py` review. `vger.py` (3200+
lines, 42 registered RPCs) currently has **no tests at all**, and every
verification done during the review was by transcribing handler bodies into a
scratch script — which tests the logic but not the wiring.

**UPDATE (2026-08-01): partly superseded — a suite now exists.** The
optional-LDAP work landed on `claude/peaceful-jackson-495cbb` and brought
`pangalactic/vger/test/test_vger.py` and `test_userdir.py` with it: **31
tests, passing in ~2s**, needing neither crossbar nor a database. That branch
arrived independently at the same handler-capture technique described in §2
(its `register_rpcs()`), which is good converging evidence that the approach
is sound. §1(a)'s `sys.modules['ldap']` stub is **no longer needed and should
not be used** — see §7 for what that suite covers and what is left to build.

The rest of this note stands as written, with §4 now describing work still to
do rather than work to start from scratch.

---

## 1. The two things that were blocking it

**(a) `vger.py` cannot be imported without `python-ldap`.**
`pangalactic/vger/userdir.py:4` does a bare top-level `import ldap`, and
`vger.py` imports `search_ldap_directory` from it at module scope. This is
being addressed separately (making LDAP a conditional dependency). Until it
lands, tests can stub it:

```python
import sys, types
sys.modules.setdefault('ldap', types.ModuleType('ldap'))   # before importing vger
```
That stub should be **removed** once LDAP is optional — it is a workaround,
not part of the design.

**(b) The RPC handlers are closures inside `RepositoryService.onJoin`.**
This was assumed to require refactoring them out into methods before anything
could call them. **It does not.** See §2 — this turned out to be the
cheap part, and it changes the cost of the whole suite.

## 2. The harness: capturing the real handlers, no refactor, no router

`onJoin` is an `@inlineCallbacks` generator whose only interactions with the
WAMP session are `self.register` (46 calls), `self.publish` (28),
`self.subscribe` (1), `self.log`, and two of its own methods. If every
`yield`ed Deferred is already fired, `onJoin` runs to completion
**synchronously**. So a stand-in session can capture every handler:

```python
from twisted.internet.defer import succeed
from pangalactic.vger.vger import RepositoryService

class SessionHarness:
    """Stands in for the WAMP session: captures registrations and publishes."""
    def __init__(self):
        self.rpcs = {}        # uri -> handler function
        self.options = {}     # uri -> RegisterOptions
        self.published = []   # [(topic, payload), ...]
        self.log = _Log()     # no-op info/debug
    def register(self, fn, uri, options=None):
        self.rpcs[uri] = fn
        self.options[uri] = options
        return succeed(None)
    def subscribe(self, fn, topic):
        return succeed(None)
    def publish(self, topic, payload):
        self.published.append((topic, payload))
    def on_vger_msg(self, *a, **kw): pass
    def audit_deletions(self, *a, **kw): pass


def make_service():
    """Build a RepositoryService with its RPCs captured.

    NOTE: __init__ is deliberately bypassed (object.__new__) -- it would
    start the orb and connect to crossbar. The orb is started separately by
    the test module, exactly as p.core's tests do.
    """
    svc = object.__new__(RepositoryService)
    h = SessionHarness()
    for name in ('register', 'subscribe', 'publish', 'log', 'on_vger_msg',
                 'audit_deletions'):
        setattr(svc, name, getattr(h, name))
    RepositoryService.onJoin(svc, details=None)
    return svc, h
```

**Verified working.** It captures all **42** RPCs, exposes their
`RegisterOptions`, and the handlers are directly callable:

```
RPCs captured:   42
vger.save handler: <function RepositoryService.onJoin.<locals>.save at ...>
vger.save([]) -> {'new_obj_dts': {}, 'mod_obj_dts': {}, 'unauth': [], 'no_owners': []}
```

Caller identity is supplied with a plain object, since every handler reads it
via `getattr(cb_details, 'caller_authid', ...)`:

```python
from types import SimpleNamespace
cb = SimpleNamespace(caller_authid='zaphod')
result = rpcs['vger.save']([payload], cb_details=cb)
```

**This approach is better than refactoring for two reasons**, not just
cheaper:
- it exercises the **real registration wiring**, so a test can assert that
  `vger.save` is registered *and* that it has `details_arg='cb_details'` — a
  missing `RegisterOptions` is exactly the defect class behind
  `vger_review.md` #5, and no amount of testing the function body would catch
  it;
- it requires **no production change**, so the tests can be written and
  trusted before any restructuring, rather than after.

Hoisting the handlers to methods on `RepositoryService` remains worth doing
eventually for readability, but it is now an independent decision rather than
a prerequisite.

## 3. Layout

Following `pangalactic.core`'s conventions (`unittest.TestCase`, module-level
orb start, `test_NN_name` methods with a `CASE:` docstring, a `runtests`
shell script):

```
pangalactic/vger/test/
    __init__.py
    README                  # what needs to be running (nothing) and how to run
    runtests                # same prompt-per-module shape as p.core's
    harness.py              # SessionHarness + make_service() from sec. 2
    utils.py                # scratch-home fixture, test-user helpers
    test_vault.py           # valid_vault_fname, upload/download paths
    test_save.py            # vger.save: authorization, deleted-cache, batching
    test_sync.py            # sync_project / sync_objects / sync_library_objects
    test_parms.py           # set_parameters / set_data_elements / set_properties
    test_checkout.py        # check_out / check_in / release / get_checkouts
    test_registration.py    # every expected uri registered, with correct options
```

**Fixture strategy**, following `test_orb.py` but with one change: p.core's
tests use a fixed `HOME = 'pangalaxian_test'` directory in the cwd, which
couples the modules together (`test_registry.py` must create it before
`test_orb.py` uses it — the ordering hazard noted in the core review). For a
new suite, prefer a per-module scratch home:

```python
HOME = tempfile.mkdtemp(prefix='vger_test_')
orb.start(home=os.path.join(HOME, 'home'), debug=False, console=False)
deserialize(orb, create_test_users())
deserialize(orb, create_test_project())
```
Note `orb.start()` cannot create a home that does not exist — it writes the
ref db *as* a file at that path and then raises (the deferred `orb.start()`
finding in `pangalactic_core_review_scoped.md`). Create the directory first.

## 4. What to test, in priority order

**(a) Regression tests for the findings just fixed.** Highest value: these are
known-real defects with known-real reproductions, several of which were
security-relevant. Each already has a verified reproduction from the review
that can be lifted directly:

| test | asserts |
|---|---|
| `test_vault` | `valid_vault_fname` rejects absolute paths, `../`, both separators, `.`/`..`, nulls; accepts `<oid>_<name>` |
| `test_vault` | `upload_chunk` with a traversal `fname` raises and writes nothing outside the vault |
| `test_save` | a spoofed `creator` on an existing object the caller cannot modify is refused and reported in `unauth` |
| `test_save` | a genuinely new object with `creator == caller` is accepted (the regression that guards the fix) |
| `test_save` | an oid in the `deleted` cache does not crash the batch, and the *other* objects in it still save |
| `test_sync` | `sync_project` with no `caller_authid` returns the empty result rather than raising |
| `test_parms` | one bad item in a `set_parameters` batch does not discard the good ones |
| `test_registration` | all 42 uris registered; each expected one has `details_arg='cb_details'` |

The `vger.save` spoofing case has already been run against the real handler
end to end, as a proof that this is more than a plan:

```
target FDValve-0000866 (creator=admin), attacker=zaphod
vger.save result: {'new_obj_dts': {}, 'mod_obj_dts': {},
                   'unauth': ['FDValve-0000866'], 'no_owners': []}
description in repo now: 'Gas Fill & Drain Valve'   (unchanged)
publishes triggered: 0
```

**(b) Authorization matrix.** The review's severest findings were all
authorization, and `get_perms` interacts with role assignments, ownership,
frozen state and cloaking. A table-driven test across
(user × object × operation) → expected outcome would be the single most
valuable *new* coverage, as distinct from regression coverage.

**(c) Publish assertions.** `h.published` makes it cheap to assert that a
successful mutation publishes on the right channel with the right payload
shape, and — equally important — that a *refused* one publishes nothing. The
spoofing test above already asserts `len(published) == 0`.

**(d) Serialization round-trips** through `save` → `get_objects`, including
the `parameters` / `data_elements` sections that ride along with objects.

## 5. Known limitations of this approach

State them plainly so the suite is not over-trusted:

- **It does not test the WAMP layer.** Authentication (`authenticator.py`,
  cryptosign), the crossbar router, serialization over the wire, and
  `RegisterOptions` semantics as *enforced by crossbar* are all out of scope.
  The harness asserts what vger asks for, not what crossbar does with it.
- **`cb_details` is a stand-in.** Tests assert how vger *uses* caller
  identity; that the identity is trustworthy is a property of cryptosign and
  the authenticator, verified separately (see `vger_review.md`, "Verified
  assumption").
- **`RepositoryService.__init__` is bypassed**, so anything it sets up is not
  covered — including the `cb_host`/`cb_port`/`cb_url`/`realm` handling fixed
  as finding #6. That one wants its own small test constructing the object
  properly with `kw`/`config`.
- **Requirements-management RPCs are out of the review's scope** and would be
  the natural next area to cover once the rest is in place.

## 6. Suggested first step

*(Written before the suite below existed; kept for the reasoning. The actual
first step was taken by the LDAP branch — see §7.)*

`harness.py` plus `test_registration.py` and `test_vault.py`. Together they
are small, need no fixtures beyond a started orb, and immediately prove the
harness in CI. `test_save.py` next, since its reproductions already exist.

## 7. What now exists, and what is left

**Landed** (`pangalactic/vger/test/`, 31 tests):

- `test_vger.py` — RPC registration (uniqueness, `vger.` namespace, the names
  clients actually call, **name/function correspondence**, channel
  subscription); `vger.search_ldap` in all four states and the 2-element
  result contract the client's `on_search_result()` requires; `get_version`,
  `get_mod_dts`, `get_parmz`, `search_exact`; `audit_deletions`.
- `test_userdir.py` — search-string construction, the `test` modes, the live
  search path, and the mapping of LDAP entries to `Person` attributes.

Its handling of the LDAP dependency is better than the stub this note
originally proposed: nothing touches `sys.modules`. The "not available" tests
patch the `LDAP_AVAILABLE` flag, the live-search tests inject a fake `ldap`
module into `userdir`, and one test verifies that `vger` imports with no
python-ldap at all by doing it in a subprocess where `import ldap` fails.

**The name/function correspondence test earned its keep immediately**, which
is the point argued in §2: it found that `get_object` was registered under
`vger.get_mod_dts` (so that RPC took the wrong arguments, and `get_mod_dts`
was never registered at all). Note that a test which merely called each
registered handler would *not* have caught this, and neither did a full
manual read of `vger.py` — it needs an assertion that the registered name
matches the function.

**Also landed (2026-08-01)** — the regression modules from §4, built on
`register_rpcs()` plus a new `fixtures.py`:

- `test_vault.py` (10) — `valid_vault_fname` across the accept/reject cases,
  and `upload_chunk` refusing relative traversal, absolute paths, unknown
  callers and missing `cb_details`, each asserting the victim file outside the
  vault is untouched (finding #1).
- `test_save.py` (8) — spoofed-`creator` refusal (object unchanged, named in
  `unauth`, nothing published), the two acceptance regressions that guard the
  fix, and the `deleted`-cache batch cases (findings #2, #3).
- `test_sync.py` (7) — `sync_project` with no `cb_details`, no `caller_authid`,
  empty authid, unknown user, no project oid and SANDBOX, plus the authorized
  path as a regression guard (finding #4).
- `test_parms.py` (12) — per-item isolation in `set_parameters` /
  `set_data_elements` / `set_properties`, including that an unauthorized oid
  no longer has its `mod_datetime` bumped (finding #7 and the side bug found
  with it).

**Suite total: 68 tests, all passing** — `test_vger` 19, `test_userdir` 12,
`test_vault` 10, `test_save` 8, `test_sync` 7, `test_parms` 12. The mocked
half and the real-orb half coexist without interference: the landed tests
patch `orb` per test, so a started orb does not affect them.

The structural difference between the two halves is now explicit rather than
accidental: the landed tests mock `orb` and run in ~2s; these need a real orb
(`fixtures.start_test_orb()`, idempotent, scratch home removed at exit) and
take ~2-3 minutes, dominated by `recompute_parmz()` on each `save()`. See the
test README for how to run just the fast half while iterating.

**Still to build:**

- The authorization matrix (§4b) — still the most valuable *new* coverage, as
  distinct from regression coverage.
- A test constructing `RepositoryService` properly via `kw`/`config` to cover
  the `cb_host`/`cb_port`/`cb_url`/`realm` handling (finding #6), which the
  handler-capture harness deliberately bypasses.
- `get_parmz(oids=[...])` emitting `None` for unknown oids (the carried-forward
  item in `vger_review.md`) — worth a test with the fix.
