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
import shutil
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from twisted.python.failure import Failure

# set the orb
import pangalactic.core.set_uberorb

from pangalactic.core import access

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


class FakeActivity:
    """Stands in for an Activity: identity is what the rule tests."""
    def __init__(self, oid='an-oid'):
        self.oid = oid
        self.id = 'an-activity'
        self.frozen = False


class FakeMission(FakeActivity):
    """An Activity subclass, as Mission and Test both are."""


class FakeActivityControl:
    """Stands in for an ActivityControl -- a Decision or a Merge."""
    def __init__(self, oid='an-oid'):
        self.oid = oid
        self.id = 'a-control'
        self.frozen = False


class FakeDecision(FakeActivityControl):
    """An ActivityControl subclass, as Decision and Merge both are."""


class FakeRepFile:
    """Stands in for a RepresentationFile that references others."""
    def __init__(self, of_object='a-model'):
        self.oid = 'rf-oid'
        self.id = 'a-rep-file'
        self.user_file_name = 'asm.stp'
        self.of_object = of_object
        self.component_files = []


class FakeProduct:
    def __init__(self, oid='an-oid'):
        self.oid = oid
        self.id = 'a-product'
        self.frozen = False


class FakePerson:
    def __init__(self, id=''):
        self.id = id


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
                    'vger.get_people', 'vger.missing_vault_files'}
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


class VaultFileRpcTests(unittest.TestCase):
    """
    The rpcs that move a file's bytes, and the rule they exist to keep:  a
    RepresentationFile in the repository has its file in the vault.

    These use a real temporary directory as the vault, because what is being
    tested is what ends up on disk -- a mock would assert the calls and miss
    the bytes.
    """

    @classmethod
    def setUpClass(cls):
        cls.rpcs, cls.session = register_rpcs()

    def setUp(self):
        self.vault = tempfile.mkdtemp(prefix='vger_vault_')
        self.caller = SimpleNamespace(caller_authid='zaphod')

    def tearDown(self):
        shutil.rmtree(self.vault, ignore_errors=True)

    def fake_orb(self, rep_files=None, known_user=True):
        """
        An orb whose vault is the temp directory and whose get() answers with
        the given RepresentationFile stand-ins.
        """
        rep_files = rep_files or {}
        orb = mock.MagicMock()
        orb.vault = self.vault
        orb.get.side_effect = lambda oid: rep_files.get(oid)
        orb.select.return_value = FakePerson(id='zaphod') if known_user \
            else None
        orb.get_vault_fname.side_effect = (
                        lambda rf: rf.oid + '_' + rf.user_file_name)
        orb.get_vault_fpath.side_effect = (
            lambda rf: os.path.join(self.vault,
                                    rf.oid + '_' + rf.user_file_name))
        return orb

    def rep_file(self, oid='rf-1', fname='asm.stp', size=0, of_object=None):
        if of_object is None:
            of_object = FakeObj(oid='model-1', id='a-model')
        return FakeObj(oid=oid, id=oid, user_file_name=fname, file_size=size,
                       of_object=of_object)

    def allowing(self, perms=('view',)):
        """
        Stand in for access.get_perms().  Patched rather than exercised:  the
        policy itself is tested where it lives, in
        pangalactic.core/test/test_digital_files.py, and rules in
        pangalactic.core.access resolve orb.classes through *that* module's
        orb, which this harness does not patch.
        """
        return mock.patch.object(vger, 'get_perms',
                                 lambda obj, user=None, **kw: list(perms))

    def vault_write(self, rep_file, data):
        path = os.path.join(self.vault,
                            rep_file.oid + '_' + rep_file.user_file_name)
        with open(path, 'wb') as f:
            f.write(data)
        return path

    # ---- upload_chunk ----------------------------------------------------

    def test_01_chunks_are_written_in_order(self):
        """CASE: an ordinary upload assembles the file from its chunks"""
        upload_chunk = self.rpcs['vger.upload_chunk']
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            for seq, data in enumerate([b'aaa', b'bbb', b'ccc']):
                upload_chunk(fname='rf-1_asm.stp', seq=seq, data=data,
                             cb_details=self.caller)
        with open(os.path.join(self.vault, 'rf-1_asm.stp'), 'rb') as f:
            self.assertEqual(b'aaabbbccc', f.read())

    def test_02_a_retried_upload_replaces_what_arrived_before(self):
        """
        CASE: an upload that failed part way through is sent again.

        The file used to be opened for append on every chunk, so the retry
        doubled the bytes that had already arrived.  Retrying is normal now
        that a file goes up whenever its RepresentationFile is synced.
        """
        upload_chunk = self.rpcs['vger.upload_chunk']
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            # a transfer that died after the first of three chunks
            upload_chunk(fname='rf-1_asm.stp', seq=0, data=b'aaa',
                         cb_details=self.caller)
            # ... and the whole thing sent again
            for seq, data in enumerate([b'aaa', b'bbb', b'ccc']):
                upload_chunk(fname='rf-1_asm.stp', seq=seq, data=data,
                             cb_details=self.caller)
        with open(os.path.join(self.vault, 'rf-1_asm.stp'), 'rb') as f:
            self.assertEqual(b'aaabbbccc', f.read())

    def test_03_an_unknown_user_may_not_upload(self):
        """CASE: the caller must be a known Person"""
        upload_chunk = self.rpcs['vger.upload_chunk']
        orb = self.fake_orb()
        orb.select.return_value = None
        with mock.patch.object(vger, 'orb', orb):
            self.assertRaises(vger.ApplicationError, upload_chunk,
                              fname='rf-1_asm.stp', seq=0, data=b'x',
                              cb_details=SimpleNamespace(
                                                caller_authid='nobody'))
        self.assertEqual([], os.listdir(self.vault))

    # ---- download_chunk --------------------------------------------------

    def test_04_absent_bytes_are_reported_not_raised(self):
        """
        CASE: the object exists and its file does not.

        This used to open the vault path unguarded, so a request for a file
        whose bytes had not arrived raised out of the rpc.  The caller now
        gets the same empty answer it gets for an unknown file.
        """
        download_chunk = self.rpcs['vger.download_chunk']
        rf = self.rep_file(size=9)
        with mock.patch.object(vger, 'orb', self.fake_orb({'rf-1': rf})), \
                self.allowing():
            result = download_chunk(digital_file_oid='rf-1', seq=0,
                                    cb_details=self.caller)
        self.assertEqual(('rf-1', 0, b''), result)

    def test_05_present_bytes_are_served(self):
        """
        CASE: the ordinary one.  The guard must not have broken the path it
        guards -- a neighbouring case that cannot pass vacuously.
        """
        download_chunk = self.rpcs['vger.download_chunk']
        rf = self.rep_file(size=9)
        self.vault_write(rf, b'aaabbbccc')
        with mock.patch.object(vger, 'orb', self.fake_orb({'rf-1': rf})), \
                self.allowing():
            oid, seq, chunk = download_chunk(digital_file_oid='rf-1', seq=0,
                                             cb_details=self.caller)
        self.assertEqual(('rf-1', 0, b'aaabbbccc'), (oid, seq, chunk))

    # ---- download_chunk authorization ------------------------------------
    #
    # This rpc used to check nothing at all:  an oid was enough to fetch the
    # bytes it named.

    def test_05a_an_unknown_user_may_not_download(self):
        """
        CASE: a caller the repository has never heard of.  upload_chunk() has
        always refused one; download_chunk() did not.
        """
        download_chunk = self.rpcs['vger.download_chunk']
        rf = self.rep_file(size=9)
        self.vault_write(rf, b'aaabbbccc')
        orb = self.fake_orb({'rf-1': rf}, known_user=False)
        with mock.patch.object(vger, 'orb', orb), self.allowing():
            self.assertRaises(vger.ApplicationError, download_chunk,
                              digital_file_oid='rf-1', seq=0,
                              cb_details=SimpleNamespace(
                                                caller_authid='nobody'))

    def test_05b_a_user_who_may_not_view_the_subject_is_refused(self):
        """
        CASE: a known user with no permission on what the file represents --
        someone with no role in the project owning a cloaked assembly.
        """
        download_chunk = self.rpcs['vger.download_chunk']
        rf = self.rep_file(size=9)
        self.vault_write(rf, b'aaabbbccc')
        with mock.patch.object(vger, 'orb', self.fake_orb({'rf-1': rf})), \
                self.allowing(perms=[]):
            self.assertRaises(vger.ApplicationError, download_chunk,
                              digital_file_oid='rf-1', seq=0,
                              cb_details=self.caller)

    def test_05c_the_subject_is_what_is_asked_about(self):
        """
        CASE: the permission consulted is the one on `of_object`, not on the
        file.

        This is the whole point:  RepresentationFile is in
        access.modifiables, which grants every user view/modify/delete on it,
        so a gate on the file itself would authorize everybody.
        """
        download_chunk = self.rpcs['vger.download_chunk']
        model = FakeObj(oid='model-9', id='the-model')
        rf = self.rep_file(size=9, of_object=model)
        self.vault_write(rf, b'aaabbbccc')
        asked = []
        def fake_perms(obj, user=None, **kw):
            asked.append(getattr(obj, 'id', None))
            return ['view']
        with mock.patch.object(vger, 'orb', self.fake_orb({'rf-1': rf})), \
                mock.patch.object(vger, 'get_perms', fake_perms):
            download_chunk(digital_file_oid='rf-1', seq=0,
                           cb_details=self.caller)
        self.assertEqual(['the-model'], asked)

    def test_05d_a_file_representing_nothing_is_refused(self):
        """
        CASE: a file with no `of_object`.  It has no permissions to inherit,
        and nothing in the application makes one -- so it is refused rather
        than served to anyone who names it.
        """
        download_chunk = self.rpcs['vger.download_chunk']
        rf = self.rep_file(size=9, of_object=False)
        rf.of_object = None
        self.vault_write(rf, b'aaabbbccc')
        with mock.patch.object(vger, 'orb', self.fake_orb({'rf-1': rf})), \
                self.allowing():
            self.assertRaises(vger.ApplicationError, download_chunk,
                              digital_file_oid='rf-1', seq=0,
                              cb_details=self.caller)

    # ---- missing_vault_files ---------------------------------------------

    def test_06_files_with_no_bytes_are_reported_missing(self):
        """CASE: a file the vault does not hold"""
        missing_vault_files = self.rpcs['vger.missing_vault_files']
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            result = missing_vault_files(files={'rf-1_asm.stp': 9},
                                         cb_details=self.caller)
        self.assertEqual(['rf-1_asm.stp'], result)

    def test_07_a_complete_file_is_not_reported(self):
        """CASE: bytes present and the right length -- nothing to send"""
        missing_vault_files = self.rpcs['vger.missing_vault_files']
        self.vault_write(self.rep_file(), b'aaabbbccc')
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            result = missing_vault_files(files={'rf-1_asm.stp': 9},
                                         cb_details=self.caller)
        self.assertEqual([], result)

    def test_08_a_short_file_is_reported_missing(self):
        """
        CASE: an interrupted upload left a partial file.

        Reporting it missing is what makes the transfer retried rather than
        served:  a short file is a failed transfer, not a file.
        """
        missing_vault_files = self.rpcs['vger.missing_vault_files']
        self.vault_write(self.rep_file(), b'aaa')
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            result = missing_vault_files(files={'rf-1_asm.stp': 9},
                                         cb_details=self.caller)
        self.assertEqual(['rf-1_asm.stp'], result)

    def test_09_a_file_the_repository_never_heard_of_is_reported(self):
        """
        CASE: the first upload of a new file, asked about before its object
        exists.

        This is the ordinary case, not an odd one:  the bytes are sent first,
        so the repository cannot know the object yet.  An answer keyed on the
        object would skip every new file -- which is exactly the bug an
        earlier draft of this rpc had.
        """
        missing_vault_files = self.rpcs['vger.missing_vault_files']
        orb = self.fake_orb()
        orb.get.side_effect = lambda oid: None      # no such object, at all
        with mock.patch.object(vger, 'orb', orb):
            result = missing_vault_files(files={'brand-new_asm.stp': 12},
                                         cb_details=self.caller)
        self.assertEqual(['brand-new_asm.stp'], result)

    def test_10_an_unsafe_name_is_not_examined(self):
        """
        CASE: a name that would escape the vault.  Refused the way
        upload_chunk() refuses it, rather than stat-ing whatever it points
        at.
        """
        missing_vault_files = self.rpcs['vger.missing_vault_files']
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            result = missing_vault_files(
                            files={'../../etc/passwd': 1, '/etc/passwd': 1},
                            cb_details=self.caller)
        self.assertEqual(['../../etc/passwd', '/etc/passwd'], sorted(result))

    def test_11_size_is_optional(self):
        """
        CASE: an expected size of 0 -- a file whose size was never recorded.
        Presence is all there is to go on, so a non-empty file passes.
        """
        missing_vault_files = self.rpcs['vger.missing_vault_files']
        self.vault_write(self.rep_file(), b'x')
        with mock.patch.object(vger, 'orb', self.fake_orb()):
            result = missing_vault_files(files={'rf-1_asm.stp': 0},
                                         cb_details=self.caller)
        self.assertEqual([], result)


class CheckOutRpcTests(unittest.TestCase):
    """
    Tests of vger.check_out's refusals.

    Only the early ones are reachable without a database -- which is where
    the Activity rule sits, deliberately:  an Activity is refused before any
    question about claims or permissions is asked.
    """

    @classmethod
    def setUpClass(cls):
        cls.rpcs, cls.session = register_rpcs()

    def _check_out(self, obj, classes):
        """
        Call vger.check_out for one object against a stubbed orb.

        Args:
            obj:  the object orb.get() should return
            classes (dict):  stands in for orb.classes

        Returns:
            dict:  the rpc's result
        """
        check_out = self.rpcs['vger.check_out']
        details = SimpleNamespace(caller_authid='zaphod')
        # NOTE: access.is_offline_excluded() resolves the excluded class
        # names through the *access* module's orb, not vger's -- the rule has
        # one definition and it lives there -- so both have to be stubbed.
        with mock.patch.object(vger, 'orb') as orb, \
                mock.patch.object(access, 'orb') as access_orb:
            access_orb.classes = classes
            orb.select.return_value = FakePerson(id='zaphod')
            orb.get.return_value = obj
            orb.classes = classes
            # the claim is expanded to the object's directly related items
            # before anything is decided;  here that is just the object
            # itself.  NOTE: this has to be set -- a bare MagicMock iterates
            # as empty, which silently expands the request to nothing and
            # makes every assertion below pass for the wrong reason.
            orb.get_checkout_set.return_value = [obj]
            # no existing claims: get_active_checkout() searches for them
            orb.search_exact.return_value = []
            return check_out(['an-oid'], cb_details=details)

    # is_offline_excluded() resolves these names through orb.classes, so a
    # stubbed orb has to carry both entries or the rule matches nothing
    CLASSES = {'Activity': FakeActivity,
               'ActivityControl': FakeActivityControl}

    def test_01_activity_is_refused(self):
        """
        CASE: an Activity.  Refused -- editing one adjusts the times of the
        others in its timeline, so a claim on one does not cover the work,
        and access.py will not write it offline whatever is claimed.
        """
        result = self._check_out(FakeActivity(), self.CLASSES)
        self.assertEqual([], result['granted'])
        self.assertEqual({'an-oid': 'not_offline_editable'}, result['denied'])

    def test_02_activity_subclass_is_refused(self):
        """
        CASE: a Mission, which is an Activity subclass.  The test is by
        isinstance, so Mission and Test go with Activity rather than needing
        to be named.
        """
        result = self._check_out(FakeMission(), self.CLASSES)
        self.assertEqual([], result['granted'])
        self.assertEqual({'an-oid': 'not_offline_editable'}, result['denied'])

    def test_03_activity_control_is_refused(self):
        """
        CASE: a Decision, which is an ActivityControl.  Refused too -- it is
        not an Activity, but it sequences the activities in a timeline, so
        the same reasoning covers it.
        """
        result = self._check_out(FakeDecision(), self.CLASSES)
        self.assertEqual([], result['granted'])
        self.assertEqual({'an-oid': 'not_offline_editable'}, result['denied'])

    def test_04_frozen_is_refused_first(self):
        """
        CASE: a frozen object.  Still refused as frozen -- the Activity rule
        is added after that test, not in place of it.
        """
        frozen = FakeActivity()
        frozen.frozen = True
        result = self._check_out(frozen, self.CLASSES)
        self.assertEqual({'an-oid': 'frozen'}, result['denied'])

    def test_05_a_product_is_not_refused_by_this_rule(self):
        """
        CASE: something that is not an Activity.  It gets past the rule and
        on to the permission test, which is the next thing check_out asks.

        get_perms is stubbed to withhold 'modify', so the refusal that comes
        back is "no_permission" -- proving the object reached that test
        rather than being turned away earlier for being an activity.
        """
        with mock.patch.object(vger, 'get_perms', return_value=['view']):
            result = self._check_out(FakeProduct(), self.CLASSES)
        self.assertEqual([], result['granted'])
        self.assertEqual({'an-oid': 'no_permission'}, result['denied'])


class AddComponentFileRpcTests(unittest.TestCase):
    """
    Tests of vger.add_component_file's refusals.

    A CAD assembly exported as a set of files needs every file transferred,
    not just the one the user chose.  This rpc records each referenced file
    against the file that references it.  Only its guards are reachable
    without a database, which is where the cases below stop.
    """

    @classmethod
    def setUpClass(cls):
        cls.rpcs, cls.session = register_rpcs()

    def _call(self, referencing, parms=None, perms=('modify',)):
        add_component_file = self.rpcs['vger.add_component_file']
        details = SimpleNamespace(caller_authid='zaphod')
        with mock.patch.object(vger, 'orb') as orb, \
                mock.patch.object(vger, 'get_perms', return_value=list(perms)):
            orb.select.return_value = FakePerson(id='zaphod')
            orb.get.return_value = referencing
            return add_component_file(rep_file_oid='rf-oid',
                                      fpath='/tmp/part.stp',
                                      parms=parms or {'file name': 'part.stp',
                                                      'file size': '10'},
                                      cb_details=details)

    def test_01_unknown_referencing_file_is_refused(self):
        """
        CASE: the file said to reference this one does not exist.  Nothing to
        attach it to.
        """
        fpath, sobjs = self._call(None)
        self.assertEqual('/tmp/part.stp', fpath)
        self.assertEqual([], sobjs)

    def test_02_a_file_belonging_to_no_model_is_refused(self):
        """
        CASE: the referencing file has no model.  A component file joins the
        model of the file that references it, so there is nowhere to put it.
        """
        referencing = FakeRepFile(of_object=None)
        fpath, sobjs = self._call(referencing)
        self.assertEqual([], sobjs)

    def test_03_no_permission_on_the_model_is_refused(self):
        """
        CASE: the caller may not modify the model.  Authorization is the
        model's, since that is what gains a file -- checked with the same
        get_perms() as everything else, so this cannot grant access the user
        would not otherwise have.
        """
        referencing = FakeRepFile()
        fpath, sobjs = self._call(referencing, perms=('view',))
        self.assertEqual([], sobjs)

    def test_04_a_file_with_no_name_is_refused(self):
        """
        CASE: no file name.  The name is what a reference is made under, so
        a nameless component file could never be resolved.
        """
        referencing = FakeRepFile()
        fpath, sobjs = self._call(referencing, parms={'file size': '10'})
        self.assertEqual([], sobjs)

    def test_05_an_already_recorded_file_is_not_duplicated(self):
        """
        CASE: this referencing file already names a file called part.stp.

        Returned rather than duplicated:  an import can legitimately be
        repeated, and a part shared by two subassemblies is named more than
        once in the same set.
        """
        existing = FakeRepFile()
        existing.user_file_name = 'part.stp'
        referencing = FakeRepFile()
        referencing.component_files = [existing]
        with mock.patch.object(vger, 'serialize',
                               return_value=['serialized']) as ser:
            fpath, sobjs = self._call(referencing)
        self.assertEqual(['serialized'], sobjs)
        ser.assert_called_once()
        self.assertEqual([existing], ser.call_args.args[1])


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
