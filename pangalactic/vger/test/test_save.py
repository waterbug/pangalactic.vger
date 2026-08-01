# -*- coding: utf-8 -*-
"""
Regression tests for vger.save() (vger_review.md findings #2 and #3).

Finding #2 -- authorization bypass via a spoofed "creator" field.  save()
granted "authorized" status to any submitted object whose client-supplied
"creator" equalled the caller's oid, with no check that the object was
actually new.  A user who knew the oid of an existing object they did not own
could therefore submit it with their own oid as "creator" and have arbitrary
fields applied, bypassing get_perms() entirely.

Finding #3 -- a batch containing an oid in the server's "deleted" cache raised
AttributeError ("'dict_values' object has no attribute 'remove'"), failing the
whole rpc, so every other valid object in the same call was rejected as a side
effect.  The same block also never actually removed the stale object from the
dict the authorization logic uses, so it would have been saved anyway.
"""
import unittest

# set the orb -- MUST precede any import of a pangalactic.core module that
# does "from pangalactic.core import orb" (e.g. access.py), since that name
# does not exist until set_uberorb has run
import pangalactic.core.set_uberorb

from pangalactic.core import deleted
from pangalactic.core.access import get_perms
from pangalactic.core.serializers import serialize
from pangalactic.core.utils.datetimes import dtstamp

from pangalactic.vger.test.fixtures import (start_test_orb, get_test_user,
                                            find_unmodifiable_by,
                                            find_modifiable_by, FakeDetails)
from pangalactic.vger.test.test_vger import register_rpcs


class SaveAuthorizationTests(unittest.TestCase):
    """
    Tests of who vger.save() will and will not accept a write from.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')

    def setUp(self):
        # the session is shared across the class, so clear recorded publishes
        self.session.publish.reset_mock()

    def test_01_spoofed_creator_on_existing_object_is_refused(self):
        """CASE: a spoofed "creator" does not authorize writing to an object
        the caller has no 'modify' permission on

        This is finding #2.  The submitted "creator" is only meaningful for an
        object that does not yet exist; for one that does, authorization has
        to come from get_perms().
        """
        save = self.rpcs['vger.save']
        target = find_unmodifiable_by(self.zaphod)
        self.assertIsNotNone(target, 'test data has no off-limits object')
        self.assertNotIn('modify', get_perms(target, user=self.zaphod))
        original = target.description

        payload = serialize(self.orb, [target])[0]
        payload['creator'] = self.zaphod.oid          # the spoof
        payload['description'] = 'PWNED'
        payload['mod_datetime'] = str(dtstamp())      # newer than the server's

        result = save([payload], cb_details=FakeDetails('zaphod'))

        self.assertEqual({}, result['mod_obj_dts'])
        self.assertEqual({}, result['new_obj_dts'])
        self.assertIn(target.id, result['unauth'])
        self.orb.db.refresh(target)
        self.assertEqual(original, target.description,
                         'the repository copy must be untouched')

    def test_02_refusal_publishes_nothing(self):
        """CASE: a refused save does not announce anything to other clients"""
        save = self.rpcs['vger.save']
        target = find_unmodifiable_by(self.zaphod)
        payload = serialize(self.orb, [target])[0]
        payload['creator'] = self.zaphod.oid
        payload['description'] = 'PWNED AGAIN'
        payload['mod_datetime'] = str(dtstamp())

        save([payload], cb_details=FakeDetails('zaphod'))

        self.assertEqual([], self.session.publish.call_args_list)

    def test_03_new_object_created_by_caller_is_accepted(self):
        """CASE: a genuinely new object whose creator is the caller is saved

        Regression guard for the fix to #2:  the "creator" fast path is still
        the thing that lets a user save work the repository has never seen.
        """
        save = self.rpcs['vger.save']
        template = find_modifiable_by(self.zaphod) or find_unmodifiable_by(
                                                                self.zaphod)
        payload = serialize(self.orb, [template])[0]
        payload['oid'] = 'test-brand-new-oid-0001'
        payload['id'] = 'BrandNewPart-0001'
        payload['name'] = 'Brand New Part'
        payload['creator'] = self.zaphod.oid
        payload['mod_datetime'] = str(dtstamp())
        self.assertIsNone(self.orb.get(payload['oid']),
                          'oid must not already exist')

        result = save([payload], cb_details=FakeDetails('zaphod'))

        self.assertIn(payload['oid'], result['new_obj_dts'])
        self.assertEqual([], result['unauth'])
        saved = self.orb.get(payload['oid'])
        self.assertIsNotNone(saved)
        self.orb.delete([saved])

    def test_04_existing_object_the_caller_created_is_accepted(self):
        """CASE: the real creator of an existing object can still modify it

        Regression guard for the fix to #2:  removing the creator fast path
        for existing objects must not cost a legitimate creator anything --
        get_perms() grants 'modify' to the creator on its own.
        """
        save = self.rpcs['vger.save']
        owned = None
        for obj in self.orb.db.query(
                            self.orb.classes['HardwareProduct']).all():
            if (obj.creator is self.zaphod
                    and 'modify' in get_perms(obj, user=self.zaphod)):
                owned = obj
                break
        if owned is None:
            self.skipTest('test data has no HardwareProduct created by zaphod')
        original = owned.description

        payload = serialize(self.orb, [owned])[0]
        payload['description'] = 'edited by the real creator'
        payload['mod_datetime'] = str(dtstamp())

        result = save([payload], cb_details=FakeDetails('zaphod'))

        self.assertEqual([], result['unauth'])
        self.assertIn(owned.oid, result['mod_obj_dts'])
        # restore
        payload['description'] = original
        payload['mod_datetime'] = str(dtstamp())
        save([payload], cb_details=FakeDetails('zaphod'))

    def test_05_unknown_caller_saves_nothing(self):
        """CASE: a caller who is not a known Person cannot write"""
        save = self.rpcs['vger.save']
        target = find_unmodifiable_by(self.zaphod)
        original = target.description
        payload = serialize(self.orb, [target])[0]
        payload['creator'] = 'no-such-person-oid'
        payload['description'] = 'PWNED BY NOBODY'
        payload['mod_datetime'] = str(dtstamp())

        result = save([payload], cb_details=FakeDetails('no-such-user'))

        self.assertIn(target.id, result['unauth'])
        self.orb.db.refresh(target)
        self.assertEqual(original, target.description)


class SaveDeletedCacheTests(unittest.TestCase):
    """
    Tests of a save() batch that contains an oid in the "deleted" cache.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        cls.zaphod = get_test_user('zaphod')

    def setUp(self):
        self.stale_oids = []

    def tearDown(self):
        for oid in self.stale_oids:
            deleted.pop(oid, None)

    def _mark_deleted(self, oid):
        deleted[oid] = str(dtstamp())
        self.stale_oids.append(oid)

    def test_01_stale_oid_does_not_fail_the_whole_batch(self):
        """CASE: one oid in the "deleted" cache does not sink the other
        objects in the same save() call

        This is finding #3:  the batch used to raise AttributeError before it
        reached the authorization step, so every valid object went down with
        the stale one.
        """
        save = self.rpcs['vger.save']
        template = find_unmodifiable_by(self.zaphod)
        good, stale = [], None
        for i in (1, 2):
            p = serialize(self.orb, [template])[0]
            p['oid'] = f'test-batch-good-{i}'
            p['id'] = f'BatchGood-{i}'
            p['name'] = f'Batch Good {i}'
            p['creator'] = self.zaphod.oid
            p['mod_datetime'] = str(dtstamp())
            good.append(p)
        stale = serialize(self.orb, [template])[0]
        stale['oid'] = 'test-batch-stale-1'
        stale['id'] = 'BatchStale-1'
        stale['name'] = 'Batch Stale 1'
        stale['creator'] = self.zaphod.oid
        stale['mod_datetime'] = str(dtstamp())
        self._mark_deleted(stale['oid'])

        # stale one deliberately first, so an early failure would take the
        # good ones with it
        result = save([stale] + good, cb_details=FakeDetails('zaphod'))

        for p in good:
            self.assertIn(p['oid'], result['new_obj_dts'],
                          f"{p['id']} should have been saved")
        self.assertIn('BatchStale-1', result['unauth'])
        self.assertIsNone(self.orb.get(stale['oid']),
                          'an object in the "deleted" cache must not be saved')
        for p in good:
            obj = self.orb.get(p['oid'])
            if obj:
                self.orb.delete([obj])

    def test_02_all_oids_stale_returns_cleanly(self):
        """CASE: a batch in which every oid is in the "deleted" cache returns
        the empty result rather than raising"""
        save = self.rpcs['vger.save']
        template = find_unmodifiable_by(self.zaphod)
        p = serialize(self.orb, [template])[0]
        p['oid'] = 'test-batch-all-stale'
        p['id'] = 'BatchAllStale'
        p['name'] = 'Batch All Stale'
        p['creator'] = self.zaphod.oid
        p['mod_datetime'] = str(dtstamp())
        self._mark_deleted(p['oid'])

        result = save([p], cb_details=FakeDetails('zaphod'))

        self.assertEqual({}, result['new_obj_dts'])
        self.assertEqual({}, result['mod_obj_dts'])
        self.assertIn('BatchAllStale', result['unauth'])
        self.assertIsNone(self.orb.get(p['oid']))

    def test_03_empty_call_returns_the_empty_result(self):
        """CASE: save() with nothing returns the documented empty result"""
        save = self.rpcs['vger.save']
        result = save([], cb_details=FakeDetails('zaphod'))
        self.assertEqual({'new_obj_dts': {}, 'mod_obj_dts': {},
                          'unauth': [], 'no_owners': []}, result)


if __name__ == '__main__':
    unittest.main()
