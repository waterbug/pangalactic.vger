# -*- coding: utf-8 -*-
"""
Tests for what vger.delete() tells the world about a cascading deletion.

Deleting a Product takes its Models, their RepresentationFiles and the bytes
of those.  That happens in the orb, on whichever machine runs it -- but the
repository has two more jobs the orb cannot do for it:  tell the other
clients, so they do not keep a Model of a product that no longer exists, and
record the whole set as deleted, so none of it can be pushed back.

Run against a real orb:  the subject is what a cascade actually removes.
"""
import os
import unittest
from types import SimpleNamespace

# set the orb
import pangalactic.core.set_uberorb

from pangalactic.core             import orb, deleted, state
from pangalactic.core.serializers import deserialize
from pangalactic.core.test.utils  import (create_test_users,
                                          create_test_project)

HOME = 'vger_delete_test'
orb.start(home=HOME)
deserialize(orb, create_test_users() + create_test_project())

from pangalactic.core.digital_files import (new_model_with_file,
                                            stage_in_vault, vault_path)
from pangalactic.vger.test.test_vger import register_rpcs

MCAD = 'pgefobjects:ModelType.MCAD'


class DeleteCascadeReportingTest(unittest.TestCase):

    def setUp(self):
        self.rpcs, self.session = register_rpcs()
        self.zaphod = SimpleNamespace(caller_authid='zaphod')
        self.was_user = state.get('local_user_oid')
        state['local_user_oid'] = 'test:zaphod'
        self.tmpdir = os.path.join(orb.home, 'delete_files')
        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)

    def tearDown(self):
        state['local_user_oid'] = self.was_user

    def a_product_with_a_model(self, name):
        from pangalactic.core.clone import clone
        product = clone('HardwareProduct', id=f'{name}-id', name=name,
                        owner=orb.get('H2G2'), save_hw=False)
        orb.db.commit()
        fpath = os.path.join(self.tmpdir, f'{name}.stp')
        with open(fpath, 'wb') as f:
            f.write(b'ISO-10303-21;\n' * 50)
        model, rep_file = new_model_with_file(
                MCAD, fpath,
                {'file name': f'{name}.stp',
                 'file size': str(os.path.getsize(fpath)),
                 'mime_type': 'application/step', 'name': name,
                 'of_thing_oid': product.oid, 'owner_oid': 'H2G2',
                 'project_oid': 'H2G2'})
        orb.save([model, rep_file])
        orb.db.commit()
        stage_in_vault(rep_file, fpath)
        return product, model, rep_file

    def published_deletions(self):
        out = []
        for call in self.session.publish.call_args_list:
            payload = call.args[1]
            if 'deleted' in payload:
                out.append(payload['deleted'])
        return out

    def test_01_everything_that_went_is_reported(self):
        """
        CASE:  the caller asked about one product and three objects went.
        It cannot work out the other two from what it asked for, so they are
        reported.
        """
        product, model, rep_file = self.a_product_with_a_model('Reported')
        result = self.rpcs['vger.delete']([product.oid],
                                          cb_details=self.zaphod)
        expected = sorted([product.oid, model.oid, rep_file.oid])
        self.assertEqual(expected, sorted(result['deleted']))

    def test_02_everything_that_went_is_published(self):
        """
        CASE:  the other clients.  Publishing only the named oid left every
        one of them holding a Model of a product that no longer exists.
        """
        product, model, rep_file = self.a_product_with_a_model('Published')
        self.rpcs['vger.delete']([product.oid], cb_details=self.zaphod)
        published = self.published_deletions()
        for oid in (product.oid, model.oid, rep_file.oid):
            self.assertIn(oid, published)

    def test_03_everything_that_went_is_recorded_as_deleted(self):
        """
        CASE:  the "deleted" cache.  vger.save() refuses an oid only if the
        cache names it, so recording just the product left the door open for
        a client to push the Model back.
        """
        product, model, rep_file = self.a_product_with_a_model('Recorded')
        self.rpcs['vger.delete']([product.oid], cb_details=self.zaphod)
        for oid in (product.oid, model.oid, rep_file.oid):
            self.assertIn(oid, deleted)

    def test_04_the_bytes_go_from_the_repository_vault(self):
        """
        CASE:  the repository's own vault.  The same rule as a client's --
        a vault file is named for the oid of the object that describes it.
        """
        product, model, rep_file = self.a_product_with_a_model('Bytes')
        path = vault_path(rep_file)
        was_there = os.path.exists(path)
        self.rpcs['vger.delete']([product.oid], cb_details=self.zaphod)
        self.assertEqual([True, False], [was_there, os.path.exists(path)])

    def test_05_a_refusal_still_reports_nothing_deleted(self):
        """
        CASE:  an object the caller may not delete.  Nothing goes, so
        nothing is published and nothing is recorded.
        """
        product, model, rep_file = self.a_product_with_a_model('Refused')
        product.creator = orb.get('test:steve')
        orb.db.commit()
        result = self.rpcs['vger.delete'](
                        [product.oid],
                        cb_details=SimpleNamespace(caller_authid='buckaroo'))
        expected = [[], [product.oid]]
        self.assertEqual(expected, [result['deleted'], result['unauth']])


if __name__ == '__main__':
    unittest.main()
