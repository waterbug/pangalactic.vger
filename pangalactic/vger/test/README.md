# Unit tests

The unit tests (`test_vger.py`, `test_userdir.py`) need neither a crossbar
router nor a database -- run them with pytest, either from this directory or
from the top of the source tree:

    pytest -v pangalactic/vger/test

`test_vger.py` gets at the rpcs by running `RepositoryService.onJoin()` against
a fake WAMP session (`register_rpcs()`), which collects the functions it
registers; `orb` is replaced by a mock for the duration of each test that calls
one, since `orb.log` does not exist until `orb.start()` has connected to a
database.

The tests run whether or not python-ldap (an optional dependency) is installed:
the tests of the "LDAP is not available" paths patch the `LDAP_AVAILABLE` flag,
and those of the LDAP search itself patch `userdir.ldap` with a fake ldap
module.  One test spawns a subprocess in which `import ldap` is made to fail,
to verify that vger imports in an environment with no python-ldap at all.

## The two halves of the suite

`test_vger.py` and `test_userdir.py` **mock `orb`**.  They cover rpc
registration, argument handling and the LDAP paths -- none of which need real
data -- and they are fast (about 2 seconds).

`test_vault.py`, `test_save.py`, `test_sync.py` and `test_parms.py` are
regression tests for the findings in `vger_review.md`, and they are about
authorization and persistence, so they need **a real orb**:  real objects,
real RoleAssignments and a real `get_perms()`.  `fixtures.py` starts one orb,
once, on a scratch home (removed at exit) populated with the standard test
data from `pangalactic.core.test.utils`, and shares it across those modules --
`orb.start()` is effectively a singleton, so `start_test_orb()` is idempotent
and every module calls it rather than starting its own.

That makes them much slower -- roughly 2-3 minutes for the four together,
dominated by `recompute_parmz()` on each `save()` and by the one-time orb
start.  Run just the fast half while iterating:

    pytest -q pangalactic/vger/test/test_vger.py pangalactic/vger/test/test_userdir.py

## Note for anyone adding tests

The rpcs harvested by `register_rpcs()` are **plain functions**.  Do not stash
one as a class attribute (`cls.save = cls.rpcs['vger.save']`) -- it becomes a
bound method and the test instance is passed as its first positional argument,
producing a confusing "got multiple values for argument" TypeError.  Look them
up at the point of use instead:

    save = self.rpcs['vger.save']

Also note that `import pangalactic.core.set_uberorb` must come before any
`from pangalactic.core... import` that pulls in `orb` (e.g. `access.py`), since
that name does not exist until `set_uberorb` has run.

# Interactive testing against a live crossbar router

The unit tests here deliberately do not exercise the WAMP layer:  no router,
no cryptosign authentication, and `RepositoryService.__init__` is bypassed.
Running vger for real against crossbar is what covers those, and it is the
complement to this suite rather than a part of it.

The full recipe -- home directory, private key generation, getting vger's
public key into the authenticator's `principals.db`, crossbar's certificates,
and the path gotchas -- is in **`NOTES_ON_TESTING.md`, section 8**, at the top
of this package.

`crossbar_for_test_vger.sh` and `principals.json` in this directory are part
of that setup.

