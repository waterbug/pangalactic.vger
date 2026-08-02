# -*- coding: utf-8 -*-
"""
Tests for vger.get_parmz().

The "oids" branch previously used parameterz.get(oid), which mapped an oid the
server has no parameters for to None.  The client applies the result with
parameterz.update(), so those None values landed directly in the client's
cache, where anything iterating parameterz[oid] would hit them.

Note there is no production caller that passes "oids" today -- the client
calls vger.get_parmz() with no arguments -- so this branch is latent.  That is
precisely why it is worth pinning: the next caller to use it would inherit the
trap silently.

Deliberately NOT tested here: access filtering.  get_parmz returns the whole
cache to any caller by design; see "Read access is NOT applied to the
parameter caches" in p.core/NOTES_FOR_DEVELOPERS.md.
"""
import unittest

# set the orb (see the note in test_save.py)
import pangalactic.core.set_uberorb

from pangalactic.core.parametrics import parameterz

from pangalactic.vger.test.fixtures import start_test_orb
from pangalactic.vger.test.test_vger import register_rpcs


class GetParmzTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.orb = start_test_orb()
        cls.rpcs, cls.session = register_rpcs()

    def setUp(self):
        self.get_parmz = self.rpcs['vger.get_parmz']
        self.known = next((o for o in parameterz if parameterz[o]), None)
        if self.known is None:
            self.skipTest('test data has no object with parameters')

    def test_01_unknown_oid_is_omitted_not_mapped_to_none(self):
        """CASE: an oid the server has no parameters for is left out"""
        result = self.get_parmz(oids=['no-such-oid-at-all'])
        self.assertEqual({}, result)

    def test_02_no_none_values_in_a_mixed_request(self):
        """CASE: a good oid and an unknown one in the same request

        The good one comes back; the unknown one does not appear at all, so a
        client doing parameterz.update(result) cannot acquire a None entry.
        """
        result = self.get_parmz(oids=[self.known, 'no-such-oid-at-all'])
        self.assertIn(self.known, result)
        self.assertNotIn('no-such-oid-at-all', result)
        self.assertFalse([k for k, v in result.items() if v is None],
                         f'None values present: {result}')

    def test_03_known_oid_returns_its_parameters(self):
        """CASE: the ordinary case still works"""
        result = self.get_parmz(oids=[self.known])
        self.assertEqual({self.known: parameterz[self.known]}, result)

    def test_04_no_oids_returns_the_whole_cache(self):
        """CASE: the no-argument form used by the client is unchanged"""
        result = self.get_parmz()
        self.assertIs(parameterz, result)


if __name__ == '__main__':
    unittest.main()
