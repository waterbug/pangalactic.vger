# -*- coding: utf-8 -*-
"""
Tests for what vger.save() reports as refused.

An object that carries no change is not an attempt to modify anything, so
there is nothing to authorize and nothing to refuse.

A batch carries more than the object that was edited:  a client serializing a
modified Product uses include_components=True, so the assembly's whole white
box travels with it -- every Acu and every component, exactly as the
repository already holds them.  Whether the caller could modify those is the
wrong question about the wrong objects;  answering it told the user their
work had been rejected when nothing of theirs had been touched.

"Refused" has to mean "your change was rejected".  A frozen part is used
below only because it is a convenient way to make an object the caller
cannot modify -- the rule under test has nothing to do with freezing.

These run against a real orb rather than the mock in test_vger.py -- the
rule is about the mod_datetime of an object that is actually in the
repository, so there has to be one.
"""
import unittest
from types import SimpleNamespace

# set the orb
import pangalactic.core.set_uberorb

from pangalactic.core             import orb
from pangalactic.core.serializers import deserialize
from pangalactic.core.test.utils  import (create_test_users,
                                          create_test_project)

HOME = 'vger_save_test'
orb.start(home=HOME)
deserialize(orb, create_test_users() + create_test_project())

from pangalactic.vger.test.test_vger import register_rpcs

NOW = '2026-09-05 12:00:00'
LATER = '2026-09-06 12:00:00'
ASSEMBLY = 'test:spacecraft0'


def a_frozen_library_part(oid='frozen-part', mod_datetime=NOW):
    """
    A HardwareProduct nobody may modify:  frozen, and created by someone
    other than the caller.  This is what a shipped library part looks like.
    """
    return dict(_cname='HardwareProduct', oid=oid, id=oid,
                name='A Frozen Part', owner='H2G2',
                creator='test:steve', modifier='test:steve',
                public=True, frozen=True,
                create_datetime=NOW, mod_datetime=mod_datetime)


def an_acu_by_zaphod(oid='new-acu', component='frozen-part'):
    """
    The object the user actually made:  new, and theirs.
    """
    return dict(_cname='Acu', oid=oid, id=oid, name='a new usage',
                assembly=ASSEMBLY, component=component,
                creator='test:zaphod', modifier='test:zaphod',
                create_datetime=NOW, mod_datetime=NOW)


class SaveReportTest(unittest.TestCase):

    def setUp(self):
        self.rpcs, self.session = register_rpcs()
        self.zaphod = SimpleNamespace(caller_authid='zaphod')

    def save(self, sobjs, cb_details=None):
        return self.rpcs['vger.save'](sobjs,
                                      cb_details=cb_details or self.zaphod)

    def test_01_an_unchanged_object_is_not_reported_as_refused(self):
        """
        CASE:  the batch carries a frozen library part the caller cannot
        modify, exactly as the repository already holds it.  There is
        nothing in it to save and nothing to complain about.
        """
        deserialize(orb, [a_frozen_library_part(oid='unchanged-part')])
        orb.db.commit()
        result = self.save([a_frozen_library_part(oid='unchanged-part')])
        self.assertEqual([], result['unauth'])

    def test_02_a_changed_object_is_still_reported(self):
        """
        CASE:  the caller changed an object they may not modify.  That IS
        their work being rejected, and it must still be reported.
        """
        deserialize(orb, [a_frozen_library_part(oid='changed-part')])
        orb.db.commit()
        result = self.save([a_frozen_library_part(oid='changed-part',
                                                  mod_datetime=LATER)])
        self.assertEqual(['changed-part'], result['unauth'])

    def test_03_a_new_object_refused_is_still_reported(self):
        """
        CASE:  a new object whose creator is not the caller.  Nothing to
        compare it against, and it is a save that did not happen.
        """
        someone_elses = a_frozen_library_part(oid='not-mine')
        result = self.save([someone_elses])
        self.assertEqual(['not-mine'], result['unauth'])

    def test_04_the_users_own_new_object_is_saved(self):
        """
        CASE:  the whole point.  Dropping a library part onto an assembly
        makes an Acu, which is new and is the caller's;  it is saved, and the
        unchanged part that travelled with it is not complained about.

        This is the shape of the FireSat report:  the warning named the
        subsystem's existing components and never named the Acu, because the
        Acu was accepted.
        """
        deserialize(orb, [a_frozen_library_part(oid='travelling-part')])
        orb.db.commit()
        result = self.save([a_frozen_library_part(oid='travelling-part'),
                            an_acu_by_zaphod(oid='zaphods-acu',
                                             component='travelling-part')])
        expected = [[], True]
        value = [result['unauth'], 'zaphods-acu' in result['new_obj_dts']]
        self.assertEqual(expected, value)

    def test_05_a_mix_reports_only_what_changed(self):
        """
        CASE:  one unchanged part and one the caller edited.  Only the edited
        one is the user's rejected work.
        """
        deserialize(orb, [a_frozen_library_part(oid='quiet-part'),
                          a_frozen_library_part(oid='edited-part')])
        orb.db.commit()
        result = self.save([a_frozen_library_part(oid='quiet-part'),
                            a_frozen_library_part(oid='edited-part',
                                                  mod_datetime=LATER)])
        self.assertEqual(['edited-part'], result['unauth'])


if __name__ == '__main__':
    unittest.main()
