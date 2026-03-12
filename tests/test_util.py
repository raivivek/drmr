# -*- coding: utf-8 -*-

import unittest
import copy
import collections.abc
import drmr.util

class TestMergeMappings(unittest.TestCase):

    def test_shallow_merge(self):
        m1 = {'a': 1, 'b': 2}
        m2 = {'b': 3, 'c': 4}
        expected = {'a': 1, 'b': 3, 'c': 4}
        self.assertEqual(drmr.util.merge_mappings(m1, m2), expected)

    def test_recursive_merge(self):
        m1 = {'a': {'x': 1, 'y': 2}, 'b': 2}
        m2 = {'a': {'y': 3, 'z': 4}, 'c': 5}
        expected = {
            'a': {'x': 1, 'y': 3, 'z': 4},
            'b': 2,
            'c': 5
        }
        self.assertEqual(drmr.util.merge_mappings(m1, m2), expected)

    def test_multiple_mappings(self):
        m1 = {'a': 1}
        m2 = {'b': 2}
        m3 = {'c': 3}
        expected = {'a': 1, 'b': 2, 'c': 3}
        self.assertEqual(drmr.util.merge_mappings(m1, m2, m3), expected)

    def test_deep_recursive_merge(self):
        m1 = {'a': {'b': {'c': 1}}}
        m2 = {'a': {'b': {'d': 2}}}
        expected = {'a': {'b': {'c': 1, 'd': 2}}}
        self.assertEqual(drmr.util.merge_mappings(m1, m2), expected)

    def test_overwrite_non_mapping_with_mapping(self):
        m1 = {'a': 1}
        m2 = {'a': {'b': 2}}
        expected = {'a': {'b': 2}}
        self.assertEqual(drmr.util.merge_mappings(m1, m2), expected)

    def test_overwrite_mapping_with_non_mapping(self):
        m1 = {'a': {'b': 1}}
        m2 = {'a': 2}
        expected = {'a': 2}
        self.assertEqual(drmr.util.merge_mappings(m1, m2), expected)

    def test_empty_input(self):
        self.assertEqual(drmr.util.merge_mappings(), {})
        self.assertEqual(drmr.util.merge_mappings({}), {})

    def test_non_mapping_input_ignored(self):
        # Based on implementation, non-mapping inputs should be ignored
        self.assertEqual(drmr.util.merge_mappings({'a': 1}, None, 123), {'a': 1})

    def test_deep_copy_behavior(self):
        m1 = {'a': {'b': 1}}
        m2 = {'c': 2}
        merged = drmr.util.merge_mappings(m1, m2)

        # Modify the merged result
        merged['a']['b'] = 99

        # Original should remain unchanged
        self.assertEqual(m1['a']['b'], 1)

        # Verify nested structures are also copied
        m3 = {'a': [1, 2, 3]}
        merged2 = drmr.util.merge_mappings(m3)
        merged2['a'].append(4)
        self.assertEqual(m3['a'], [1, 2, 3])

if __name__ == '__main__':
    unittest.main()
