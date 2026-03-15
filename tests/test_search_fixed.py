"""Tests for searchFixed (named searches) support — DataTables 2.0+."""
import unittest
from unittest.mock import patch, MagicMock
from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestSearchFixed(BaseDataTablesTest):

    def _make_dt(self, extra_args):
        args = dict(self.request_args)
        args.update(extra_args)
        with patch.object(DataTables, 'has_text_index', new_callable=lambda: property(lambda self: False)):
            return DataTables(self.mongo, 'test_collection', args,
                              [DataField('name', 'string'),
                               DataField('email', 'string'),
                               DataField('status', 'string')])

    def test_no_search_fixed_returns_empty(self):
        dt = self._make_dt({})
        self.assertEqual(dt._parse_search_fixed(), {})

    def test_empty_dict_returns_empty(self):
        dt = self._make_dt({'searchFixed': {}})
        self.assertEqual(dt._parse_search_fixed(), {})

    def test_single_fixed_search_produces_or_across_columns(self):
        dt = self._make_dt({'searchFixed': {'role': 'admin'}})
        result = dt._parse_search_fixed()
        self.assertIn('$or', result)
        # Should search across all searchable columns
        fields = [list(cond.keys())[0] for cond in result['$or']]
        self.assertIn('name', fields)
        self.assertIn('email', fields)
        self.assertIn('status', fields)

    def test_multiple_fixed_searches_are_anded(self):
        dt = self._make_dt({'searchFixed': {'role': 'admin', 'dept': 'eng'}})
        result = dt._parse_search_fixed()
        self.assertIn('$and', result)
        self.assertEqual(len(result['$and']), 2)

    def test_empty_value_is_skipped(self):
        dt = self._make_dt({'searchFixed': {'role': '', 'dept': 'eng'}})
        result = dt._parse_search_fixed()
        # Only 'eng' produces a condition — no $and wrapper needed
        self.assertNotIn('$and', result)
        self.assertIn('$or', result)

    def test_search_fixed_included_in_filter(self):
        dt = self._make_dt({'searchFixed': {'role': 'admin'}})
        f = dt.filter
        # filter should contain the searchFixed condition
        self.assertNotEqual(f, {})

    def test_search_fixed_combined_with_global_search(self):
        args = {'searchFixed': {'role': 'admin'}}
        args['search'] = {'value': 'john', 'regex': 'false'}
        dt = self._make_dt(args)
        f = dt.filter
        # Both conditions present — must be $and at top level
        self.assertIn('$and', f)

    def test_non_dict_search_fixed_ignored(self):
        dt = self._make_dt({'searchFixed': 'invalid'})
        self.assertEqual(dt._parse_search_fixed(), {})


if __name__ == '__main__':
    unittest.main()
