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

from pangalactic.core.access import is_global_admin
from pangalactic.core.parametrics import mode_defz

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


class SyncProjectClassificationTests(unittest.TestCase):
    """
    Tests of how sync_project() classifies the oids the client sends.

    The three buckets it returns -- "same", "newer" (as serialized objects)
    and "older" -- were not exhaustive over what a client can hold.  "same"
    and "newer" are both derived from `server_objs`, and "older" used to be
    everything the client sent *minus* those two, so any oid the server did
    not enumerate fell through to "older" -- which the client reads as "your
    copy is newer, push it".

    A non-admin's own RoleAssignment is exactly such an oid: it reaches the
    client from get_user_roles(), but RoleAssignments are not owned by the
    project, so get_objects_for_project() does not return them, and the
    "project_ras" list is empty unless the caller is a project admin or a
    global admin.  So every sync told the client to push an object identical
    to the server's, which it then withheld for lack of "modify" permission.

    Reported from live multi-client testing, 2026-08-04.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.user = get_test_user('zaphod')
        # NOTE: the rpc is deliberately NOT stored as a class attribute --
        # that makes it a bound method, so "self" arrives as its first
        # positional argument and collides with cb_details.  See the note in
        # test/README.md.

    def setUp(self):
        from pangalactic.core import orb
        self.ra = None
        self.project = None
        for r in orb.search_exact(cname='RoleAssignment',
                                  assigned_to=self.user):
            ctx = r.role_assignment_context
            if ctx is not None and ctx.__class__.__name__ == 'Project':
                self.ra, self.project = r, ctx
                break
        if self.ra is None:
            self.skipTest('test data has no project-scoped RoleAssignment '
                          'for this user')

    def test_08_own_role_assignment_is_not_classified_as_older(self):
        """CASE: a non-admin's own RoleAssignment, identical to the server's,
        is not returned as something to push"""
        data = {self.ra.oid: str(self.ra.mod_datetime)}
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, data,
                              cb_details=FakeDetails('zaphod'))
        older_oids = result[2]
        self.assertNotIn(self.ra.oid, older_oids,
                         'server told the client to push an identical object')

    def test_09_unenumerated_oid_is_in_no_bucket_at_all(self):
        """CASE: an oid the server did not enumerate is simply not mentioned

        Neither pushed nor pulled.  "unknown_oids" is for oids the server has
        no object for at all, which is not this case -- the RoleAssignment
        exists, it is just not part of what this caller syncs by project.
        """
        data = {self.ra.oid: str(self.ra.mod_datetime)}
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, data,
                              cb_details=FakeDetails('zaphod'))
        newer_sobjs, same_oids, older_oids, unknown_oids = result[:4]
        newer_oids = [so.get('oid') for so in newer_sobjs]
        for bucket, name in ((same_oids, 'same'), (older_oids, 'older'),
                             (unknown_oids, 'unknown'), (newer_oids, 'newer')):
            self.assertNotIn(self.ra.oid, bucket,
                             f'RoleAssignment turned up in "{name}"')

    def test_10_a_genuinely_older_server_copy_is_still_reported(self):
        """CASE: the positive formulation still finds real work to do

        Guards against "fixing" the false positive by never reporting
        anything: a project object whose server copy really is older than the
        client's must still come back in "older_oids".
        """
        from pangalactic.core import orb
        objs = [o for o in orb.get_objects_for_project(self.project)
                if getattr(o, 'mod_datetime', None)]
        if not objs:
            self.skipTest('no project objects with a mod_datetime')
        obj = objs[0]
        # client claims a copy modified a day later than the server's
        later = obj.mod_datetime.replace(year=obj.mod_datetime.year + 1)
        data = {obj.oid: str(later)}
        sync_project = self.rpcs['vger.sync_project']
        result = sync_project(self.project.oid, data,
                              cb_details=FakeDetails('zaphod'))
        self.assertIn(obj.oid, result[2],
                      'a genuinely newer client copy was not requested')


class UpdateModeDefsAuthTests(unittest.TestCase):
    """
    Tests that vger.update_mode_defs() authorizes its caller.

    It replaces a project's mode definitions wholesale, and used to do so for
    *any* authenticated caller: "userid" was read and then used only in the
    published message, so nothing tested for access to the project.  The
    comment in the handler asserted that "all users with access to the project
    are authorized", which described an intent rather than the code.

    The client was already written for the refusal -- it tests the result
    against 'unauthorized' -- so only the server half was missing.

    Any role in the project authorizes, deliberately: discipline engineers add
    subsystems to mode_defz when defining modes at component level.  See
    NOTES_ON_CHECKOUT_MODEL.md section 9, decision 4.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()

    def setUp(self):
        from pangalactic.core import orb
        self.project = orb.get('H2G2')
        if self.project is None:
            self.skipTest('test data has no H2G2 project')
        self.data = {'modes': {'1': 'Nominal'}}

    def _call(self, userid):
        update_mode_defs = self.rpcs['vger.update_mode_defs']
        return update_mode_defs(project_oid=self.project.oid, data=self.data,
                                cb_details=FakeDetails(userid))

    def test_11_user_with_a_project_role_is_authorized(self):
        """CASE: zaphod holds Systems Engineer on H2G2"""
        result = self._call('zaphod')
        self.assertNotEqual('unauthorized', result)

    def test_12_user_with_no_role_on_the_project_is_refused(self):
        """CASE: a real user holding no role on this project"""
        from pangalactic.core import orb
        outsider = None
        for p in orb.get_by_type('Person'):
            if p.id in ('admin',):
                continue
            ras = orb.search_exact(cname='RoleAssignment', assigned_to=p,
                                   role_assignment_context=self.project)
            if not ras and not is_global_admin(p):
                outsider = p
                break
        if outsider is None:
            self.skipTest('test data has no user without a role on H2G2')
        before = dict(mode_defz)
        result = self._call(outsider.id)
        self.assertEqual('unauthorized', result)
        self.assertEqual(before.get(self.project.oid),
                         mode_defz.get(self.project.oid),
                         'mode_defz was modified despite the refusal')

    def test_13_unknown_user_is_refused(self):
        """CASE: an authid with no Person record"""
        result = self._call('no-such-user-at-all')
        self.assertEqual('unauthorized', result)
