# -*- coding: utf-8 -*-
"""
Authorization tests for the single-item parameter / data element rpcs.

vger.add_parm(), vger.del_parm(), vger.add_de() and vger.del_de() previously
mutated the caches and published the result without looking at the caller at
all:  any user could add or remove any parameter or data element on any
object, and the call always reported success.  Their batch equivalents
set_parameters() and set_data_elements() had always checked "modify" perms --
these four had simply been missed.

Since check-out phase 2, get_perms() consults is_writable_now(), so routing
these through it also brings them under the check-out model:  a claim held by
another user blocks them exactly as it blocks an attribute edit.  Parameters
are the bulk of engineering content, so a claim that did not cover them would
not be worth much.

The tests assert on the *caches*, not on the return message:  a handler that
refused in its reply while still mutating the cache would be the same bug
wearing a disguise.
"""
import unittest

# set the orb (see the note in test_save.py)
import pangalactic.core.set_uberorb

from pangalactic.core.parametrics import (parameterz, data_elementz,
                                          add_parameter, add_data_element)

from pangalactic.vger.test.fixtures import (start_test_orb, get_test_user,
                                            find_unmodifiable_by,
                                            find_modifiable_by, FakeDetails)
from pangalactic.vger.test.test_vger import register_rpcs


class ParmAuthTests(unittest.TestCase):
    """
    Tests that the single-item parm/de rpcs authorize their caller.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')
        cls.mine = find_modifiable_by(cls.zaphod)
        cls.theirs = find_unmodifiable_by(cls.zaphod)

    def setUp(self):
        if self.theirs is None:
            self.skipTest('test data has no object off limits to zaphod')

    # -- refusals ---------------------------------------------------------

    def test_01_add_parm_refuses_unauthorized_caller(self):
        """CASE: add_parm on an object the caller cannot modify"""
        add_parm = self.rpcs['vger.add_parm']
        oid = self.theirs.oid
        before = dict(parameterz.get(oid) or {})
        result = add_parm(oid=oid, pid='P', cb_details=FakeDetails('zaphod'))
        self.assertTrue(result.startswith('failure:'), result)
        self.assertIn('not authorized', result)
        self.assertEqual(before, parameterz.get(oid) or {},
                         'add_parm mutated the cache despite refusing')

    def test_02_del_parm_refuses_unauthorized_caller(self):
        """CASE: del_parm on an object the caller cannot modify"""
        del_parm = self.rpcs['vger.del_parm']
        oid = self.theirs.oid
        add_parameter(oid, 'm')
        before = dict(parameterz.get(oid) or {})
        self.assertIn('m', before)
        result = del_parm(oid=oid, pid='m', cb_details=FakeDetails('zaphod'))
        self.assertTrue(result.startswith('failure:'), result)
        self.assertIn('not authorized', result)
        self.assertIn('m', parameterz.get(oid) or {},
                      'del_parm removed the parameter despite refusing')

    def test_03_add_de_refuses_unauthorized_caller(self):
        """CASE: add_de on an object the caller cannot modify"""
        add_de = self.rpcs['vger.add_de']
        oid = self.theirs.oid
        before = dict(data_elementz.get(oid) or {})
        result = add_de(oid=oid, deid='Vendor',
                        cb_details=FakeDetails('zaphod'))
        self.assertTrue(result.startswith('failure:'), result)
        self.assertIn('not authorized', result)
        self.assertEqual(before, data_elementz.get(oid) or {},
                         'add_de mutated the cache despite refusing')

    def test_04_del_de_refuses_unauthorized_caller(self):
        """CASE: del_de on an object the caller cannot modify"""
        del_de = self.rpcs['vger.del_de']
        oid = self.theirs.oid
        add_data_element(oid, 'Vendor')
        self.assertIn('Vendor', data_elementz.get(oid) or {})
        result = del_de(oid=oid, deid='Vendor',
                        cb_details=FakeDetails('zaphod'))
        self.assertTrue(result.startswith('failure:'), result)
        self.assertIn('not authorized', result)
        self.assertIn('Vendor', data_elementz.get(oid) or {},
                      'del_de removed the data element despite refusing')

    # -- the authorized path still works ----------------------------------

    def test_05_add_and_del_parm_still_work_when_authorized(self):
        """CASE: the owner can still add and remove a parameter"""
        if self.mine is None:
            self.skipTest('test data has no object zaphod can modify')
        add_parm = self.rpcs['vger.add_parm']
        del_parm = self.rpcs['vger.del_parm']
        oid = self.mine.oid
        result = add_parm(oid=oid, pid='P', cb_details=FakeDetails('zaphod'))
        self.assertFalse(result.startswith('failure:'), result)
        self.assertIn('P', parameterz.get(oid) or {})
        result = del_parm(oid=oid, pid='P', cb_details=FakeDetails('zaphod'))
        self.assertFalse(result.startswith('failure:'), result)
        self.assertNotIn('P', parameterz.get(oid) or {})

    def test_06_add_and_del_de_still_work_when_authorized(self):
        """CASE: the owner can still add and remove a data element"""
        if self.mine is None:
            self.skipTest('test data has no object zaphod can modify')
        add_de = self.rpcs['vger.add_de']
        del_de = self.rpcs['vger.del_de']
        oid = self.mine.oid
        result = add_de(oid=oid, deid='Vendor',
                        cb_details=FakeDetails('zaphod'))
        self.assertFalse(result.startswith('failure:'), result)
        self.assertIn('Vendor', data_elementz.get(oid) or {})
        result = del_de(oid=oid, deid='Vendor',
                        cb_details=FakeDetails('zaphod'))
        self.assertFalse(result.startswith('failure:'), result)
        self.assertNotIn('Vendor', data_elementz.get(oid) or {})

    # -- bad input --------------------------------------------------------

    def test_07_unknown_oid_is_refused_not_crashed(self):
        """CASE: an unknown oid does not reach get_perms(None, ...)"""
        for name, kw in [('vger.add_parm', {'pid': 'P'}),
                         ('vger.del_parm', {'pid': 'P'}),
                         ('vger.add_de', {'deid': 'Vendor'}),
                         ('vger.del_de', {'deid': 'Vendor'})]:
            result = self.rpcs[name](oid='no-such-oid-at-all',
                                     cb_details=FakeDetails('zaphod'), **kw)
            self.assertTrue(result.startswith('failure:'), f'{name}: {result}')
            self.assertIn('not found', result, f'{name}: {result}')

    def test_08_missing_oid_is_refused(self):
        """CASE: no oid at all"""
        for name, kw in [('vger.add_parm', {'pid': 'P'}),
                         ('vger.del_parm', {'pid': 'P'}),
                         ('vger.add_de', {'deid': 'Vendor'}),
                         ('vger.del_de', {'deid': 'Vendor'})]:
            result = self.rpcs[name](cb_details=FakeDetails('zaphod'), **kw)
            self.assertTrue(result.startswith('failure:'), f'{name}: {result}')


if __name__ == '__main__':
    unittest.main()
