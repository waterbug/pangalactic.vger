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

