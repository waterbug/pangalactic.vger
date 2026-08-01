# -*- coding: utf-8 -*-
"""
Regression tests for vger.sync_project() (vger_review.md finding #4).

"user" was only assigned inside "if userid:", but the guard below it ("if not
user:") read it unconditionally -- so a call whose caller_authid was missing
raised UnboundLocalError instead of taking the intended "no user found" early
return.  A sweep of all 27 caller_authid sites confirmed this was the only
remaining instance of the pattern; get_object guards the lookup but uses
"user" only inside the guarded block, and get_objects initializes to None.
"""
import unittest

# set the orb (see the note in test_save.py)
import pangalactic.core.set_uberorb

from pangalactic.vger.test.fixtures import (start_test_orb, get_test_user,
                                            all_of_class, FakeDetails)
from pangalactic.vger.test.test_vger import register_rpcs


EMPTY_RESULT = [[], [], [], [], [], {}, {}]


class SyncProjectCallerTests(unittest.TestCase):
    """
    Tests of how sync_project() resolves (or fails to resolve) the caller.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')
        projects = [p for p in all_of_class('Project')
                    if p.oid != 'pgefobjects:SANDBOX']
        cls.project = projects[0] if projects else None

    def test_01_no_cb_details_returns_empty_result(self):
        """CASE: a call with no cb_details returns the empty result

        This is finding #4:  it used to raise UnboundLocalError.
        """
        sync_project = self.rpcs['vger.sync_project']
        self.assertIsNotNone(self.project, 'test data has no non-SANDBOX project')
        result = sync_project(self.project.oid, {}, cb_details=None)
        self.assertEqual(EMPTY_RESULT, result)

    def test_02_cb_details_without_authid_returns_empty_result(self):
        """CASE: cb_details lacking caller_authid returns the empty result"""
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, {}, cb_details=FakeDetails())
        self.assertEqual(EMPTY_RESULT, result)

    def test_03_empty_authid_returns_empty_result(self):
        """CASE: an empty caller_authid returns the empty result"""
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, {},
                              cb_details=FakeDetails(''))
        self.assertEqual(EMPTY_RESULT, result)

    def test_04_unknown_user_returns_empty_result(self):
        """CASE: an authid that resolves to no Person returns the empty result

        This is the path the guard was always meant to take; before the fix it
        was only reachable when caller_authid was present but unresolvable.
        """
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, {},
                              cb_details=FakeDetails('no-such-user'))
        self.assertEqual(EMPTY_RESULT, result)

    def test_05_no_project_oid_returns_empty_result(self):
        """CASE: a missing project oid returns the empty result"""
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project('', {}, cb_details=FakeDetails('zaphod'))
        self.assertEqual(EMPTY_RESULT, result)

    def test_06_sandbox_returns_empty_result(self):
        """CASE: the SANDBOX project is excluded from project sync"""
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project('pgefobjects:SANDBOX', {},
                              cb_details=FakeDetails('zaphod'))
        self.assertEqual(EMPTY_RESULT, result)

    def test_07_authorized_caller_gets_project_objects(self):
        """CASE: a user with a role on the project gets a real result

        Regression guard:  the early returns above must not have made the
        normal path unreachable.  A client reporting nothing should be told
        about the project's objects.
        """
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, {},
                              cb_details=FakeDetails('zaphod'))
        self.assertNotEqual(EMPTY_RESULT, result,
                            'zaphod has a role on this project, so the '
                            'server should have returned its objects')
        self.assertTrue(result[0], 'expected server objects in result[0]')


if __name__ == '__main__':
    unittest.main()
