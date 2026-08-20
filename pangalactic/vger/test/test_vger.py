# -*- coding: utf-8 -*-
"""
Unit tests for vger (the repository service).

These tests run without a crossbar router and without a database:  the rpc
functions are harvested by running RepositoryService.onJoin() against a fake
WAMP session (see register_rpcs()), and "orb" is replaced by a mock for the
duration of each test that calls one.

NOTE:  nothing here stubs sys.modules['ldap'] -- vger imports cleanly whether
or not python-ldap is installed, and the tests that exercise the "LDAP is not
available" paths simply patch the userdir.LDAP_AVAILABLE flag.  The test that
verifies vger can be imported with no python-ldap at all does it honestly, in a
subprocess in which "import ldap" is made to fail.
"""
import os
import subprocess
import sys
import unittest
from unittest import mock

from twisted.python.failure import Failure

# set the orb
import pangalactic.core.set_uberorb

from pangalactic.vger import userdir
from pangalactic.vger import vger


# ---------------------------------------------------------------------------
# test harness
# ---------------------------------------------------------------------------

def register_rpcs():
    """
    Run RepositoryService.onJoin() against a fake WAMP session, returning the
    rpcs it registers.

    onJoin() is an inlineCallbacks generator whose only interactions with the
    session are self.log, self.subscribe() and self.register(), so a mock
    session drives it to completion synchronously and the rpc functions
    (defined as closures inside onJoin) become accessible for unit testing.

    Returns:
        tuple:  (rpcs (dict): rpc name -> function,
                 session (Mock): the fake session it was run against)
    """
    session = mock.MagicMock()
    # "orb" is used at the end of onJoin (and orb.log does not exist until
    # orb.start() has been called, which requires a database)
    with mock.patch.object(vger, 'orb'):
        d = vger.RepositoryService.onJoin(session, details=None)
    outcome = []
    d.addBoth(outcome.append)
    if outcome and isinstance(outcome[0], Failure):
        outcome[0].raiseException()
    rpcs = {}
    for call in session.register.call_args_list:
        fn, name = call.args[0], call.args[1]
        rpcs[name] = fn
    return rpcs, session


class FakeObj:
    """
    Stand-in for a db object:  any attributes given as keyword args.
    """
    def __init__(self, **kw):
        self.__dict__.update(kw)


def fake_person(oid='', pid='', first_name='', last_name='', mi_or_name='',
                email='', org_id='', employer_id=''):
    return FakeObj(oid=oid, id=pid, first_name=first_name,
                   last_name=last_name, mi_or_name=mi_or_name, email=email,
                   org=FakeObj(id=org_id), employer=FakeObj(id=employer_id))


class RpcRegistrationTests(unittest.TestCase):
    """
    Tests of the rpcs that onJoin() registers with the router.
    """

    @classmethod
    def setUpClass(cls):
        cls.rpcs, cls.session = register_rpcs()

    def test_01_onjoin_registers_rpcs(self):
        """CASE: onJoin() registers a set of uniquely named vger rpcs"""
        # onJoin() ran to completion (register_rpcs() re-raises if it did not)
        self.assertTrue(self.rpcs)
        registered_names = [call.args[1]
                            for call in self.session.register.call_args_list]
        # no rpc name is registered twice (a duplicate would silently shadow
        # the earlier registration)
        self.assertEqual(sorted(set(registered_names)),
                         sorted(registered_names))
        for name, fn in self.rpcs.items():
            self.assertTrue(name.startswith('vger.'),
                            'rpc name "{}" is not in the "vger." namespace'
                            .format(name))
            self.assertTrue(callable(fn))

    def test_02_expected_rpcs_are_registered(self):
        """CASE: the rpcs the client calls are all registered"""
        # this is the set of rpcs that pangalactic.node clients call -- any of
        # them going missing breaks a client
        expected = {'vger.assign_role', 'vger.add_person', 'vger.backup',
                    'vger.add_update_model', 'vger.add_update_doc',
                    'vger.upload_chunk', 'vger.save_uploaded_file',
                    'vger.download_chunk', 'vger.save', 'vger.delete',
                    'vger.freeze', 'vger.thaw', 'vger.check_out',
                    'vger.check_in', 'vger.release', 'vger.get_checkouts',
                    'vger.sync_objects', 'vger.sync_library_objects',
                    'vger.sync_project', 'vger.set_parameters',
                    'vger.set_data_elements', 'vger.set_properties',
                    'vger.get_project_parameters', 'vger.get_mode_defs',
                    'vger.update_mode_defs', 'vger.search_exact',
                    'vger.get_version', 'vger.get_object', 'vger.get_objects',
                    'vger.get_mod_dts', 'vger.get_caches', 'vger.get_parmz',
                    'vger.get_user_roles',
                    'vger.get_user_object', 'vger.search_ldap',
                    'vger.get_people'}
        self.assertEqual(set(), expected - set(self.rpcs))

    def test_03_rpc_names_match_function_names(self):
        """CASE: each rpc name corresponds to the function registered for it"""
        # NOTE:  'vger.get_mod_dts' was registered with the "get_object"
        # function until this test caught it
        for name, fn in self.rpcs.items():
            self.assertEqual(name.split('.', 1)[1], fn.__name__)

    def test_04_onjoin_subscribes_to_public_channel(self):
        """CASE: onJoin() subscribes to the public vger channel"""
        subscribed = [call.args[1]
                      for call in self.session.subscribe.call_args_list]
        self.assertIn('vger.channel.public', subscribed)


class SearchLdapRpcTests(unittest.TestCase):
    """
    Tests of the "vger.search_ldap" rpc, which must degrade gracefully when
    python-ldap is not installed or LDAP is not configured.
    """

    @classmethod
    def setUpClass(cls):
        cls.rpcs, cls.session = register_rpcs()

    def setUp(self):
        self.search_ldap = self.rpcs['vger.search_ldap']

    def test_01_known_users_search_does_not_use_ldap(self):
        """CASE: the "known_users" search works without python-ldap"""
        people = [fake_person(oid='test:buckaroo', pid='buckaroo',
                              first_name='Buckaroo', last_name='Banzai',
                              email='buckaroo@banzai.earth.milkyway.univ',
                              org_id='Banzai Institute', employer_id='Banzai')]
        with mock.patch.object(vger, 'LDAP_AVAILABLE', False), \
                mock.patch.dict(vger.config, {'ldap_url': '', 'base_dn': ''}), \
                mock.patch.object(vger, 'orb') as fake_orb:
            fake_orb.get_by_type.return_value = people
            res = self.search_ldap(known_users='result', id='x')
        label, records = res
        self.assertEqual('known users', label)
        self.assertEqual(1, len(records))
        self.assertEqual('Buckaroo Banzai', records[0]['name'])
        self.assertEqual('buckaroo', records[0]['id'])
        self.assertEqual('Banzai Institute', records[0]['org_code'])
        self.assertEqual('Banzai', records[0]['employer_name'])

    def test_02_reports_python_ldap_not_installed(self):
        """CASE: rpc reports "not available" if python-ldap is not installed"""
        with mock.patch.object(vger, 'LDAP_AVAILABLE', False), \
                mock.patch.dict(vger.config, {'ldap_url': '', 'base_dn': ''}), \
                mock.patch.object(vger, 'orb'):
            res = self.search_ldap(id='buckaroo')
        self.assertEqual([vger.LDAP_NOT_AVAILABLE, []], res)

    def test_03_reports_ldap_not_configured(self):
        """CASE: rpc reports "not available" if LDAP is not configured"""
        with mock.patch.object(vger, 'LDAP_AVAILABLE', True), \
                mock.patch.dict(vger.config, {'ldap_url': '', 'base_dn': ''}), \
                mock.patch.object(vger, 'orb'):
            res = self.search_ldap(id='buckaroo')
        msg, records = res
        self.assertIn('LDAP not available', msg)
        self.assertEqual([], records)

    def test_04_not_available_results_match_the_return_contract(self):
        """CASE: "not available" results have the (message, records) shape"""
        # the client (PersonSearchDialog.on_search_result) only renders a
        # result if len(res) == 2, so the "not available" results must have
        # the same shape as a successful search
        cases = [(False, '', ''), (True, '', ''), (False, 'ldap://x', 'dc=y')]
        for available, ldap_url, base_dn in cases:
            with mock.patch.object(vger, 'LDAP_AVAILABLE', available), \
                    mock.patch.object(userdir, 'LDAP_AVAILABLE', available), \
                    mock.patch.dict(vger.config, {'ldap_url': ldap_url,
                                                  'base_dn': base_dn}), \
                    mock.patch.object(vger, 'orb'), \
                    mock.patch.object(userdir, 'orb'):
                res = self.search_ldap(id='buckaroo')
            self.assertEqual(2, len(res),
                             'bad result shape for (available={}, url="{}")'
                             .format(available, ldap_url))
            self.assertEqual([], list(res[1]))

    def test_05_configured_search_delegates_to_userdir(self):
        """CASE: a configured search calls search_ldap_directory()"""
        expected = ('(&(agencyUID=buckaroo))', [{'id': 'buckaroo'}])
        with mock.patch.object(vger, 'LDAP_AVAILABLE', True), \
                mock.patch.dict(vger.config, {'ldap_url': 'ldap://ldap.x.com',
                                              'base_dn': 'dc=x,dc=com'}), \
                mock.patch.object(vger, 'orb'), \
                mock.patch.object(vger, 'search_ldap_directory') as fake_sld:
            fake_sld.return_value = expected
            res = self.search_ldap(id='buckaroo')
        self.assertEqual(expected, res)
        fake_sld.assert_called_once_with('ldap://ldap.x.com', 'dc=x,dc=com',
                                         id='buckaroo')

    def test_06_configured_search_without_python_ldap(self):
        """CASE: a configured search reports "not available" with no python-ldap"""
        # LDAP is configured but python-ldap is not installed:  the rpc goes
        # through to userdir.search_ldap_directory(), which reports that LDAP
        # is not available instead of raising
        with mock.patch.object(vger, 'LDAP_AVAILABLE', False), \
                mock.patch.object(userdir, 'LDAP_AVAILABLE', False), \
                mock.patch.dict(vger.config,
                                {'ldap_url': 'ldap://ldap.x.com',
                                 'base_dn': 'dc=x,dc=com',
                                 'ldap_schema': {'agencyUID': 'id'}}), \
                mock.patch.object(vger, 'orb'), \
                mock.patch.object(userdir, 'orb'):
            res = self.search_ldap(id='buckaroo')
        self.assertEqual((userdir.LDAP_NOT_AVAILABLE, []), res)


class SimpleRpcTests(unittest.TestCase):
    """
    Tests of rpcs whose logic does not depend on the state of the db.
    """

    @classmethod
    def setUpClass(cls):
        cls.rpcs, cls.session = register_rpcs()

    def test_01_get_version(self):
        """CASE: get_version() returns the version and schema change flag"""
        get_version = self.rpcs['vger.get_version']
        with mock.patch.object(vger, 'orb'), \
                mock.patch.dict(vger.schema_maps, {}, clear=True):
            version, schema_change = get_version()
        self.assertEqual(vger.__version__, version)
        self.assertFalse(schema_change)
        # a schema map keyed by this version means a schema change
        with mock.patch.object(vger, 'orb'), \
                mock.patch.dict(vger.schema_maps,
                                {vger.__version__: {}}, clear=True):
            version, schema_change = get_version()
        self.assertTrue(schema_change)

    def test_02_get_parmz(self):
        """CASE: get_parmz() returns all parameters, or those for some oids"""
        get_parmz = self.rpcs['vger.get_parmz']
        parms = {'oid-0': {'m': 1.0}, 'oid-1': {'m': 2.0}}
        with mock.patch.object(vger, 'orb'), \
                mock.patch.dict(vger.parameterz, parms, clear=True):
            self.assertEqual(parms, get_parmz())
            self.assertEqual({'oid-1': {'m': 2.0}}, get_parmz(oids=['oid-1']))

    def test_03_get_mod_dts(self):
        """CASE: get_mod_dts() passes its keyword args to the orb"""
        get_mod_dts = self.rpcs['vger.get_mod_dts']
        mod_dts = {'oid-0': '2026-07-31 12:00:00'}
        with mock.patch.object(vger, 'orb') as fake_orb:
            fake_orb.get_mod_dts.return_value = mod_dts
            res = get_mod_dts(cnames=['HardwareProduct'], oids=['oid-0'])
        fake_orb.get_mod_dts.assert_called_once_with(
                                        cnames=['HardwareProduct'],
                                        oids=['oid-0'])
        self.assertEqual(mod_dts, res)

    def test_03a_add_update_model_sets_mime_type(self):
        """
        CASE: add_update_model() puts the caller's mime_type on the
        RepresentationFile it creates.

        NOTE: mime_type was never set here, so every RepresentationFile in
        the repository had a null one.  The STEP importer needs it, and any
        caller that knows the type of file it is sending should be able to
        record it.
        """
        add_update_model = self.rpcs['vger.add_update_model']
        parms = {'file name': 'rover.stp', 'file size': '1234',
                 'mime_type': 'application/step', 'name': 'Rover',
                 'of_thing_oid': 'thing-0', 'owner_oid': 'org-0'}
        with mock.patch.object(vger, 'orb') as fake_orb, \
                mock.patch.object(vger, 'clone') as fake_clone, \
                mock.patch.object(vger, 'serialize') as fake_serialize:
            fake_serialize.return_value = []
            fake_orb.get_vault_fname.return_value = 'vault-name'
            add_update_model(mtype_oid='mt-0', fpath='/tmp/rover.stp',
                             parms=parms, cb_details=None)
        rep_file_calls = [c for c in fake_clone.call_args_list
                          if c.args and c.args[0] == 'RepresentationFile']
        self.assertEqual(1, len(rep_file_calls))
        kw = rep_file_calls[0].kwargs
        self.assertEqual('application/step', kw.get('mime_type'))
        self.assertEqual('rover.stp', kw.get('user_file_name'))

    def test_03b_add_update_model_without_mime_type(self):
        """
        CASE: a caller that does not supply a mime_type still works, getting
        an empty one rather than a KeyError.
        """
        add_update_model = self.rpcs['vger.add_update_model']
        parms = {'file name': 'thing.stp', 'file size': '10',
                 'name': 'Thing', 'of_thing_oid': 'thing-0',
                 'owner_oid': 'org-0'}
        with mock.patch.object(vger, 'orb') as fake_orb, \
                mock.patch.object(vger, 'clone') as fake_clone, \
                mock.patch.object(vger, 'serialize') as fake_serialize:
            fake_serialize.return_value = []
            fake_orb.get_vault_fname.return_value = 'vault-name'
            add_update_model(mtype_oid='mt-0', fpath='/tmp/thing.stp',
                             parms=parms, cb_details=None)
        rep_file_calls = [c for c in fake_clone.call_args_list
                          if c.args and c.args[0] == 'RepresentationFile']
        self.assertEqual('', rep_file_calls[0].kwargs.get('mime_type'))

    def test_04_search_exact(self):
        """CASE: search_exact() passes its keyword args to the orb"""
        search_exact = self.rpcs['vger.search_exact']
        found = [FakeObj(oid='oid-0')]
        with mock.patch.object(vger, 'orb') as fake_orb, \
                mock.patch.object(vger, 'serialize') as fake_serialize:
            fake_orb.search_exact.return_value = found
            fake_serialize.return_value = [{'oid': 'oid-0'}]
            res = search_exact(cname='HardwareProduct', id='HOG')
        fake_orb.search_exact.assert_called_once_with(
                                        cname='HardwareProduct', id='HOG')
        self.assertEqual([{'oid': 'oid-0'}], res)


class RepositoryServiceTests(unittest.TestCase):
    """
    Tests of RepositoryService methods that do not require a session.
    """

    def test_01_audit_deletions_deletes_leftovers(self):
        """CASE: audit_deletions() deletes objects still in the db"""
        leftovers = [FakeObj(oid='oid-1')]
        with mock.patch.object(vger, 'orb') as fake_orb, \
                mock.patch.dict(vger.deleted,
                                {'oid-1': 'dts', 'oid-2': 'dts'}, clear=True):
            fake_orb.get_oids.return_value = ['oid-0', 'oid-1']
            fake_orb.get.return_value = leftovers
            vger.RepositoryService.audit_deletions(mock.MagicMock())
        fake_orb.get.assert_called_once_with(oids=['oid-1'])
        fake_orb.delete.assert_called_once_with(leftovers)

    def test_02_audit_deletions_with_nothing_to_do(self):
        """CASE: audit_deletions() deletes nothing if the db is consistent"""
        with mock.patch.object(vger, 'orb') as fake_orb, \
                mock.patch.dict(vger.deleted, {'oid-1': 'dts'}, clear=True):
            fake_orb.get_oids.return_value = ['oid-0']
            vger.RepositoryService.audit_deletions(mock.MagicMock())
        fake_orb.delete.assert_not_called()


# script run in a subprocess by ImportTests, in which "import ldap" fails as
# it would in an environment with no python-ldap installed
NO_PYTHON_LDAP_SCRIPT = """
import sys

class BlockLdap:
    def find_spec(self, name, path=None, target=None):
        if name == 'ldap' or name.startswith('ldap.'):
            raise ImportError('no module named ldap [blocked by test]')
        return None

sys.meta_path.insert(0, BlockLdap())

import pangalactic.vger.vger as vger

assert vger.LDAP_AVAILABLE is False, 'LDAP_AVAILABLE should be False'
assert 'ldap' not in sys.modules, 'python-ldap was imported after all'
assert callable(vger.RepositoryService.onJoin)
print('imported ok')
"""


class ImportTests(unittest.TestCase):
    """
    Tests that vger imports in an environment without python-ldap, which is an
    optional dependency.
    """

    def test_01_imports_without_python_ldap(self):
        """CASE: vger can be imported when python-ldap is not installed"""
        # the subprocess must import the same pangalactic packages as this
        # process (which may be a source checkout rather than the installed
        # package), so hand it this process's sys.path
        env = dict(os.environ,
                   PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
        result = subprocess.run([sys.executable, '-c',
                                 NO_PYTHON_LDAP_SCRIPT],
                                capture_output=True, text=True, env=env)
        self.assertEqual(0, result.returncode,
                         'importing vger without python-ldap failed:\n'
                         + result.stderr)
        self.assertIn('imported ok', result.stdout)

    def test_02_ldap_availability_flags(self):
        """CASE: vger reports the same LDAP availability as userdir"""
        self.assertIsInstance(userdir.LDAP_AVAILABLE, bool)
        self.assertIs(userdir.LDAP_AVAILABLE, vger.LDAP_AVAILABLE)
        self.assertIs(userdir.LDAP_NOT_AVAILABLE, vger.LDAP_NOT_AVAILABLE)

    def test_03_minimum_client_version(self):
        """CASE: minimum client version defaults to the current version"""
        self.assertEqual(vger.config.get('min_version') or vger.__version__,
                         vger.MINIMUM_CLIENT_VERSION)
