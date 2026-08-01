# -*- coding: utf-8 -*-
"""
Regression tests for the vault file-name handling (vger_review.md finding #1).

Before the fix, vger.upload_chunk() joined a caller-supplied "fname" straight
onto orb.vault and opened it for append, with no sanitization and no
authorization check at all -- so any client that could complete the WAMP
handshake could append attacker-controlled bytes to any file the vger process
could write to.  Two independent escapes:  an absolute "fname" (os.path.join
discards its left operand), and "../" segments.
"""
import os
import unittest

from pangalactic.vger.test.fixtures import (start_test_orb, FakeDetails)
from pangalactic.vger.test.test_vger import register_rpcs
from pangalactic.vger import vger
from pangalactic.vger.vger import valid_vault_fname


class ValidVaultFnameTests(unittest.TestCase):
    """
    Tests of the valid_vault_fname() guard itself.
    """

    def test_01_accepts_names_the_client_generates(self):
        """CASE: names the client actually produces are accepted

        upload_file() sends either the base name of the file being uploaded or
        "[RepresentationFile.oid]_[base name]", so both must pass.
        """
        for fname in ('report.pdf',
                      'thruster_spec.step',
                      'abc-123-def_thruster spec.step',
                      'no-extension'):
            self.assertTrue(valid_vault_fname(fname),
                            f'should have accepted {fname!r}')

    def test_02_rejects_absolute_paths(self):
        """CASE: absolute paths are rejected

        os.path.join(orb.vault, '/etc/cron.d/evil') discards orb.vault
        entirely, so an absolute name writes exactly where it says.
        """
        for fname in ('/etc/cron.d/evil', '/tmp/x', '/'):
            self.assertFalse(valid_vault_fname(fname),
                             f'should have rejected {fname!r}')

    def test_03_rejects_traversal_and_separators(self):
        """CASE: path separators and ".." segments are rejected

        Both separators are checked, so a windows-style path is refused when
        vger runs on posix and vice versa.
        """
        for fname in ('../../../home/vger/.ssh/authorized_keys',
                      'sub/dir/f.txt',
                      '..\\..\\windows\\evil',
                      'a\\b',
                      '..',
                      '.'):
            self.assertFalse(valid_vault_fname(fname),
                             f'should have rejected {fname!r}')

    def test_04_rejects_empty_non_str_and_nulls(self):
        """CASE: empty, non-str and null-bearing names are rejected"""
        for fname in ('', None, b'bytes.txt', 42, 'ok\x00.txt'):
            self.assertFalse(valid_vault_fname(fname),
                             f'should have rejected {fname!r}')


class UploadChunkTests(unittest.TestCase):
    """
    Tests of the vger.upload_chunk rpc, driven through the registered handler.
    """

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()
        # NOTE: do NOT stash an rpc as a class attribute (e.g.
        # "cls.upload_chunk = cls.rpcs[...]") -- the rpcs are plain functions,
        # so as a class attribute one becomes a *bound method* and the test
        # instance is passed as its first positional argument.  Look them up
        # from cls.rpcs / self.rpcs at the point of use instead.
        # a file outside the vault, standing in for anything the vger process
        # can write -- an authorized_keys, a crontab, application data
        cls.victim_dir = os.path.join(os.path.dirname(cls.orb.vault), 'victim')
        os.makedirs(cls.victim_dir, exist_ok=True)
        cls.victim = os.path.join(cls.victim_dir, 'authorized_keys')

    def setUp(self):
        with open(self.victim, 'w') as f:
            f.write('# original contents\n')

    def _victim_contents(self):
        with open(self.victim) as f:
            return f.read()

    def test_01_registered_with_caller_details(self):
        """CASE: upload_chunk is registered so crossbar supplies cb_details

        Without details_arg the handler cannot see who is calling, and the
        authorization check below could never fire.
        """
        options = self.session.register.call_args_list
        found = [c for c in options if c.args[1] == 'vger.upload_chunk']
        self.assertEqual(1, len(found))
        opts = found[0].args[2] if len(found[0].args) > 2 else None
        opts = opts or found[0].kwargs.get('options')
        self.assertEqual('cb_details', getattr(opts, 'details_arg', None))

    def test_02_accepts_a_legitimate_upload(self):
        """CASE: a normal upload from a known user writes into the vault"""
        cb = FakeDetails(caller_authid='zaphod')
        upload_chunk = self.rpcs['vger.upload_chunk']
        seq = upload_chunk(fname='legit_upload.bin', seq=0,
                           data=b'hello', cb_details=cb)
        self.assertEqual(0, seq)
        written = os.path.join(self.orb.vault, 'legit_upload.bin')
        self.assertTrue(os.path.exists(written))
        with open(written, 'rb') as f:
            self.assertEqual(b'hello', f.read())
        os.remove(written)

    def test_03_relative_traversal_is_refused(self):
        """CASE: a "../" fname cannot write outside the vault"""
        cb = FakeDetails(caller_authid='zaphod')
        upload_chunk = self.rpcs['vger.upload_chunk']
        fname = os.path.join('..', 'victim', 'authorized_keys')
        with self.assertRaises(Exception):
            upload_chunk(fname=fname, seq=0,
                         data=b'ssh-rsa ATTACKER\n', cb_details=cb)
        self.assertEqual('# original contents\n', self._victim_contents())

    def test_04_absolute_path_is_refused(self):
        """CASE: an absolute fname cannot write outside the vault"""
        cb = FakeDetails(caller_authid='zaphod')
        upload_chunk = self.rpcs['vger.upload_chunk']
        with self.assertRaises(Exception):
            upload_chunk(fname=self.victim, seq=0,
                         data=b'ssh-rsa ATTACKER\n', cb_details=cb)
        self.assertEqual('# original contents\n', self._victim_contents())

    def test_05_unknown_caller_is_refused(self):
        """CASE: a caller whose authid is not a known Person is refused

        The handler previously did not read cb_details at all, so it applied
        no authorization of any kind.
        """
        upload_chunk = self.rpcs['vger.upload_chunk']
        for authid in ('no-such-user', '', None):
            cb = FakeDetails(caller_authid=authid)
            with self.assertRaises(Exception):
                upload_chunk(fname='anything.bin', seq=0, data=b'x',
                             cb_details=cb)
        self.assertFalse(os.path.exists(
                            os.path.join(self.orb.vault, 'anything.bin')))

    def test_06_missing_cb_details_is_refused(self):
        """CASE: a call with no cb_details at all is refused"""
        upload_chunk = self.rpcs['vger.upload_chunk']
        with self.assertRaises(Exception):
            upload_chunk(fname='anything2.bin', seq=0, data=b'x',
                         cb_details=None)
        self.assertFalse(os.path.exists(
                            os.path.join(self.orb.vault, 'anything2.bin')))


if __name__ == '__main__':
    unittest.main()
