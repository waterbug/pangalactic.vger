# -*- coding: utf-8 -*-
"""
Regression tests for the batch property setters (vger_review.md finding #7).

set_parameters(), set_data_elements() and set_properties() each wrapped their
*whole* per-oid loop in one bare "except:", so a single bad item partway
through reported one opaque failure for the entire call -- while leaving the
mutations already applied to the shared in-memory caches for the earlier items
in place, uncommitted-looking to the caller but live in server memory.

set_parameters() and set_properties() also lacked the "if not obj: continue"
guard that set_data_elements() already had, so an unknown oid fell through to
get_perms(None, ...).

set_properties() additionally created its prop_mods[oid] entry *before*
testing 'modify' permission, so an unauthorized oid stayed in the dict as an
empty entry and the "oids = list(prop_mods)" step then bumped and committed
mod_datetime on objects the caller had no right to touch.
"""
import unittest

# set the orb (see the note in test_save.py)
import pangalactic.core.set_uberorb

from pangalactic.core.parametrics import get_pval, parameterz

from pangalactic.vger.test.fixtures import (start_test_orb, get_test_user,
                                            find_unmodifiable_by,
                                            find_modifiable_by, FakeDetails)
from pangalactic.vger.test.test_vger import register_rpcs


class SetParametersTests(unittest.TestCase):
    """
    Tests of vger.set_parameters batch behaviour.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')
        cls.mine = find_modifiable_by(cls.zaphod)
        cls.theirs = find_unmodifiable_by(cls.zaphod)

    def setUp(self):
        if self.mine is None:
            self.skipTest('test data has no object zaphod can modify')

    def test_01_unknown_oid_does_not_sink_the_batch(self):
        """CASE: an unknown oid in the batch is skipped, and the good items
        in the same call are still applied"""
        set_parameters = self.rpcs['vger.set_parameters']
        before = get_pval(self.mine.oid, 'm')
        target = (before or 0) + 17.0

        result = set_parameters(parms={'no-such-oid-at-all': {'m': 1.0},
                                       self.mine.oid: {'m': target}},
                                cb_details=FakeDetails('zaphod'))

        self.assertFalse(str(result).startswith('failure'),
                         f'batch should not have failed wholesale: {result}')
        self.assertEqual(target, get_pval(self.mine.oid, 'm'))
        self.assertNotIn('no-such-oid-at-all', parameterz,
                         'an unknown oid must not be added to parameterz')

    def test_02_unknown_pid_is_skipped(self):
        """CASE: an unrecognized parameter id is skipped, not fatal"""
        set_parameters = self.rpcs['vger.set_parameters']
        before = get_pval(self.mine.oid, 'm')
        target = (before or 0) + 3.0

        result = set_parameters(parms={self.mine.oid: {'not_a_real_pid': 1.0,
                                                       'm': target}},
                                cb_details=FakeDetails('zaphod'))

        self.assertFalse(str(result).startswith('failure'))
        self.assertEqual(target, get_pval(self.mine.oid, 'm'))

    def test_03_unauthorized_oid_is_not_applied(self):
        """CASE: a parameter set on an object the caller cannot modify is
        refused, and does not change the value"""
        if self.theirs is None:
            self.skipTest('test data has no off-limits object')
        set_parameters = self.rpcs['vger.set_parameters']
        before = get_pval(self.theirs.oid, 'm')

        set_parameters(parms={self.theirs.oid: {'m': (before or 0) + 99.0}},
                       cb_details=FakeDetails('zaphod'))

        self.assertEqual(before, get_pval(self.theirs.oid, 'm'))

    def test_04_nothing_authorized_reports_failure(self):
        """CASE: a batch in which nothing could be applied says so"""
        set_parameters = self.rpcs['vger.set_parameters']
        result = set_parameters(parms={'no-such-oid': {'m': 1.0}},
                                cb_details=FakeDetails('zaphod'))
        self.assertEqual('failure: not authorized', result)

    def test_05_bad_argument_shape_is_rejected(self):
        """CASE: a non-dict "parms" is rejected without raising"""
        set_parameters = self.rpcs['vger.set_parameters']
        for bad in (None, [], 'nope', 42):
            result = set_parameters(parms=bad,
                                    cb_details=FakeDetails('zaphod'))
            self.assertEqual('failure: bad data format', result)


class SetPropertiesTests(unittest.TestCase):
    """
    Tests of vger.set_properties, including the mod_datetime side effect.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')
        cls.mine = find_modifiable_by(cls.zaphod)
        cls.theirs = find_unmodifiable_by(cls.zaphod)

    def setUp(self):
        if self.mine is None or self.theirs is None:
            self.skipTest('test data lacks a modifiable/unmodifiable pair')

    def test_01_unauthorized_object_keeps_its_mod_datetime(self):
        """CASE: an object the caller cannot modify is not stamped

        set_properties() used to create its prop_mods[oid] entry before the
        permission check, so an unauthorized oid was still collected into
        "oids" and had mod_datetime bumped and committed.
        """
        set_properties = self.rpcs['vger.set_properties']
        before_dts = self.theirs.mod_datetime
        before_val = get_pval(self.theirs.oid, 'm')

        set_properties(props={self.theirs.oid: {'m': (before_val or 0) + 5.0}},
                       cb_details=FakeDetails('zaphod'))

        self.orb.db.refresh(self.theirs)
        self.assertEqual(before_dts, self.theirs.mod_datetime,
                         'an unauthorized object must not be stamped')
        self.assertEqual(before_val, get_pval(self.theirs.oid, 'm'))

    def test_02_unknown_oid_is_skipped(self):
        """CASE: an unknown oid does not reach get_perms(None, ...)"""
        set_properties = self.rpcs['vger.set_properties']
        result = set_properties(props={'no-such-oid-here': {'m': 1.0}},
                                cb_details=FakeDetails('zaphod'))
        self.assertEqual('success', result)
        self.assertNotIn('no-such-oid-here', parameterz)

    def test_03_authorized_object_is_applied_and_stamped(self):
        """CASE: an authorized property set is applied and the object stamped

        Regression guard for the fix above:  skipping unauthorized oids must
        not stop authorized ones being handled normally.
        """
        set_properties = self.rpcs['vger.set_properties']
        before_dts = self.mine.mod_datetime
        target = (get_pval(self.mine.oid, 'm') or 0) + 11.0

        result = set_properties(props={self.mine.oid: {'m': target}},
                                cb_details=FakeDetails('zaphod'))

        self.assertEqual('success', result)
        self.assertEqual(target, get_pval(self.mine.oid, 'm'))
        self.orb.db.refresh(self.mine)
        self.assertNotEqual(before_dts, self.mine.mod_datetime)

    def test_04_mixed_batch_applies_only_the_authorized_part(self):
        """CASE: in a mixed batch the authorized item lands and the
        unauthorized one is left alone"""
        set_properties = self.rpcs['vger.set_properties']
        theirs_before = get_pval(self.theirs.oid, 'm')
        theirs_dts = self.theirs.mod_datetime
        mine_target = (get_pval(self.mine.oid, 'm') or 0) + 13.0

        result = set_properties(
                    props={self.theirs.oid: {'m': (theirs_before or 0) + 7.0},
                           self.mine.oid: {'m': mine_target}},
                    cb_details=FakeDetails('zaphod'))

        self.assertEqual('success', result)
        self.assertEqual(mine_target, get_pval(self.mine.oid, 'm'))
        self.assertEqual(theirs_before, get_pval(self.theirs.oid, 'm'))
        self.orb.db.refresh(self.theirs)
        self.assertEqual(theirs_dts, self.theirs.mod_datetime)


class SetDataElementsTests(unittest.TestCase):
    """
    Tests of vger.set_data_elements batch behaviour.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')
        cls.mine = find_modifiable_by(cls.zaphod)

    def test_01_bad_argument_shape_is_rejected(self):
        """CASE: a non-dict "des" is rejected without raising"""
        set_data_elements = self.rpcs['vger.set_data_elements']
        for bad in (None, [], 'nope'):
            self.assertEqual('failure',
                             set_data_elements(des=bad,
                                        cb_details=FakeDetails('zaphod')))

    def test_02_unknown_oid_is_skipped(self):
        """CASE: an unknown oid is skipped rather than failing the batch"""
        set_data_elements = self.rpcs['vger.set_data_elements']
        result = set_data_elements(des={'no-such-oid': {'TRL': 5}},
                                   cb_details=FakeDetails('zaphod'))
        # nothing was modified, so the documented 'failure' is returned --
        # the point is that it returns rather than raising
        self.assertEqual('failure', result)

    def test_03_authorized_data_element_is_applied(self):
        """CASE: a data element on an authorized object is applied"""
        if self.mine is None:
            self.skipTest('test data has no object zaphod can modify')
        set_data_elements = self.rpcs['vger.set_data_elements']
        result = set_data_elements(des={self.mine.oid: {'TRL': 6}},
                                   cb_details=FakeDetails('zaphod'))
        self.assertNotEqual('failure', result)


if __name__ == '__main__':
    unittest.main()
