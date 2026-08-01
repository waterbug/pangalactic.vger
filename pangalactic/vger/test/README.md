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

# Interactive testing for pangalactic.vger (repository server)

[0] get or create a private key and certificate for localhost and copy them
    into the `.crossbar_for_test_vger` directory and name them server_key.pem
    and server_cert.pem, respectively (crossbar's config file,
    .crossbar_for_test_vger/config.json, specifies those names).

[1] start crossbar message server:

    ./crossbar_for_test_vger.sh

[2] start vger:

    python ~/pangalactic.vger/pangalactic/vger/vger.py \
        --home ~/vger_home \
        --db_url postgresql://user@localhost:5432/vgerdb \
        --debug \
        --test

