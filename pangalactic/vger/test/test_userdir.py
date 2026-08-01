# -*- coding: utf-8 -*-
"""
Unit tests for userdir (the LDAP user directory interface).

python-ldap is an optional dependency, so these tests run either way:  the
"LDAP is not installed" cases patch userdir.LDAP_AVAILABLE to False, and the
"LDAP is installed" cases patch the userdir.ldap module reference with a fake
ldap module (rather than stubbing sys.modules['ldap'], which would affect
every module that imports it).
"""
import unittest
from unittest import mock

# set the orb
import pangalactic.core.set_uberorb

from pangalactic.vger import userdir


# as documented in docker/config_template, "ldap_schema" maps LDAP attribute
# names to Person attribute names (searches are specified using the Person
# attribute names and are translated into LDAP attribute names here)
TEST_SCHEMA = {'agencyUID': 'id', 'employeeNumber': 'oid',
               'givenName': 'first_name', 'sn': 'last_name',
               'nasaPrimaryEmail': 'email'}

TEST_REQ_FIELDS = '(objectClass=person)'

# an LDAP entry as python-ldap returns it: (dn, attributes), where the
# attribute values are lists of bytes
TEST_ENTRY = ('uid=buckaroo,dc=yoyodyne,dc=com',
              {'agencyUID': [b'buckaroo'],
               'employeeNumber': [b'42'],
               'givenName': [b'Buckaroo'],
               'initials': [b'B'],
               'sn': [b'Banzai'],
               'nasaEmployer': [b'Banzai Institute'],
               'nasaPrimaryEmail': [b'buckaroo@banzai.earth.milkyway.univ'],
               'nasaorgCode': [b'8900']})


def fake_ldap_module(entries=None):
    """
    A stand-in for the python-ldap module that returns the specified entries.

    Keyword Args:
        entries (list): LDAP entries, in python-ldap's (dn, attrs) form
    """
    entries = entries if entries is not None else [TEST_ENTRY]
    fake = mock.MagicMock(name='ldap')
    fake.VERSION3 = 3
    fake.SCOPE_SUBTREE = 2
    fake.RES_SEARCH_ENTRY = 100
    conn = mock.MagicMock(name='ldap_connection')
    conn.search.return_value = 1
    # each result() call returns one entry; an empty result ends the loop
    conn.result.side_effect = ([(fake.RES_SEARCH_ENTRY, [entry])
                                for entry in entries] + [(None, [])])
    fake.initialize.return_value = conn
    fake.connection = conn
    return fake


class LdapNotInstalledTests(unittest.TestCase):
    """
    Tests of the behavior of userdir without python-ldap installed.
    """

    def setUp(self):
        # orb.log is not available until orb.start() has been called
        orb_patch = mock.patch.object(userdir, 'orb')
        self.orb = orb_patch.start()
        self.addCleanup(orb_patch.stop)
        available_patch = mock.patch.object(userdir, 'LDAP_AVAILABLE', False)
        available_patch.start()
        self.addCleanup(available_patch.stop)
        config_patch = mock.patch.dict(userdir.config,
                                       {'ldap_schema': TEST_SCHEMA,
                                        'ldap_req_fields': TEST_REQ_FIELDS})
        config_patch.start()
        self.addCleanup(config_patch.stop)

    def test_01_search_reports_ldap_not_available(self):
        """CASE: a live search reports "not available" instead of raising"""
        res = userdir.search_ldap_directory('ldap://ldap.x.com', 'dc=x,dc=com',
                                            id='buckaroo')
        self.assertEqual((userdir.LDAP_NOT_AVAILABLE, []), res)

    def test_02_search_by_filterstring_raises(self):
        """CASE: the low-level search raises a clear error"""
        with self.assertRaises(RuntimeError) as ctx:
            userdir.search_by_filterstring('ldap://ldap.x.com', 'dc=x,dc=com',
                                           '(&(agencyUID=buckaroo))')
        self.assertIn('python-ldap', str(ctx.exception))

    def test_03_search_test_mode_still_works(self):
        """CASE: test="search" needs no python-ldap"""
        f, records = userdir.search_ldap_directory(
                                    'ldap://ldap.x.com', 'dc=x,dc=com',
                                    test='search', id='buckaroo')
        self.assertEqual('(&(agencyUID=buckaroo)(objectClass=person))', f)
        self.assertEqual([], records)

    def test_04_result_test_mode_still_works(self):
        """CASE: test="result" needs no python-ldap"""
        f, records = userdir.search_ldap_directory(
                                    'ldap://ldap.x.com', 'dc=x,dc=com',
                                    test='result', id='buckaroo')
        self.assertTrue(f.startswith('(&'))
        self.assertIn('buckaroo', [rec['id'] for rec in records])
        for rec in records:
            self.assertEqual({'oid', 'id', 'first_name', 'mi_or_name',
                              'last_name', 'name', 'org_code',
                              'employer_name', 'email'}, set(rec))

    def test_05_search_with_no_schema_configured(self):
        """CASE: a search with no configured ldap_schema returns a search"""
        # with no schema, the search falls back to test="search" mode
        with mock.patch.dict(userdir.config, {'ldap_schema': {}}):
            res = userdir.search_ldap_directory('ldap://ldap.x.com',
                                                'dc=x,dc=com',
                                                id='buckaroo')
        self.assertEqual(('(&)', []), res)


class LdapInstalledTests(unittest.TestCase):
    """
    Tests of the behavior of userdir with python-ldap installed (real or, if
    it is not installed in this environment, faked).
    """

    def setUp(self):
        orb_patch = mock.patch.object(userdir, 'orb')
        self.orb = orb_patch.start()
        self.addCleanup(orb_patch.stop)
        available_patch = mock.patch.object(userdir, 'LDAP_AVAILABLE', True)
        available_patch.start()
        self.addCleanup(available_patch.stop)
        config_patch = mock.patch.dict(userdir.config,
                                       {'ldap_schema': TEST_SCHEMA,
                                        'ldap_req_fields': TEST_REQ_FIELDS})
        config_patch.start()
        self.addCleanup(config_patch.stop)
        self.ldap = fake_ldap_module()
        ldap_patch = mock.patch.object(userdir, 'ldap', self.ldap)
        ldap_patch.start()
        self.addCleanup(ldap_patch.stop)

    def test_01_search_ldap_directory(self):
        """CASE: a live search binds, searches and returns dir_info records"""
        f, records = userdir.search_ldap_directory('ldap://ldap.x.com',
                                                   'dc=x,dc=com',
                                                   id='buckaroo')
        self.assertEqual('(&(agencyUID=buckaroo)(objectClass=person))', f)
        self.ldap.initialize.assert_called_once_with('ldap://ldap.x.com')
        self.ldap.connection.simple_bind_s.assert_called_once_with('', '')
        self.ldap.connection.search.assert_called_once_with(
                                    'dc=x,dc=com', self.ldap.SCOPE_SUBTREE, f,
                                    None)
        self.assertEqual(
            [{'id': 'buckaroo', 'oid': '42', 'first_name': 'Buckaroo',
              'mi_or_name': 'B', 'last_name': 'Banzai',
              'employer_name': 'Banzai Institute',
              'email': 'buckaroo@banzai.earth.milkyway.univ',
              'org_code': '890.0'}], records)

    def test_02_search_with_no_matching_entries(self):
        """CASE: a search that matches nothing returns no records"""
        with mock.patch.object(userdir, 'ldap', fake_ldap_module(entries=[])):
            f, records = userdir.search_ldap_directory('ldap://ldap.x.com',
                                                       'dc=x,dc=com',
                                                       id='nobody')
        self.assertEqual([], records)

    def test_03_search_by_filterstring_with_sizelimit(self):
        """CASE: a size-limited search uses the extended search"""
        userdir.search_by_filterstring('ldap://ldap.x.com', 'dc=x,dc=com',
                                       '(&(agencyUID=buckaroo))', sizelimit=5)
        self.ldap.connection.search_ext_s.assert_called_once_with(
                                    'dc=x,dc=com', self.ldap.SCOPE_SUBTREE,
                                    filterstr='(&(agencyUID=buckaroo))',
                                    sizelimit=5)
        self.ldap.connection.search.assert_not_called()

    def test_04_test_modes_do_not_hit_the_directory(self):
        """CASE: the test modes do not contact the LDAP service"""
        userdir.search_ldap_directory('ldap://ldap.x.com', 'dc=x,dc=com',
                                      test='search', id='buckaroo')
        userdir.search_ldap_directory('ldap://ldap.x.com', 'dc=x,dc=com',
                                      test='result', id='buckaroo')
        self.ldap.initialize.assert_not_called()


class GetDirInfoTests(unittest.TestCase):
    """
    Tests of the mapping of LDAP entries to Person attributes.
    """

    def test_01_field_values_are_decoded(self):
        """CASE: the (bytes) field values of an entry are decoded"""
        dir_info = userdir._get_dir_info(TEST_ENTRY)
        for name, value in dir_info.items():
            self.assertIsInstance(value, str, 'value of "{}" is not a str'
                                              .format(name))
        self.assertEqual('buckaroo', dir_info['id'])
        self.assertEqual('Banzai', dir_info['last_name'])

    def test_02_org_code_formats(self):
        """CASE: a 4 digit org code gets a dot; other codes are unchanged"""
        cases = [(b'8900', '890.0'), (b'890.0', '890.0'), (b'12345', '12345')]
        for raw, expected in cases:
            dn, attrs = TEST_ENTRY
            entry = (dn, dict(attrs, nasaorgCode=[raw]))
            self.assertEqual(expected, userdir._get_dir_info(entry)['org_code'])

    def test_03_optional_fields_may_be_absent(self):
        """CASE: an entry without the optional fields is handled"""
        dn, attrs = TEST_ENTRY
        required = ['agencyUID', 'givenName', 'sn', 'nasaEmployer']
        entry = (dn, {name: attrs[name] for name in required})
        dir_info = userdir._get_dir_info(entry)
        self.assertEqual('', dir_info['oid'])
        self.assertEqual('', dir_info['mi_or_name'])
        self.assertEqual('', dir_info['email'])
        self.assertEqual('', dir_info['org_code'])
