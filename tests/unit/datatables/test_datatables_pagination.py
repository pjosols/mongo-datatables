"""Tests for DataTables pagination functionality."""
import unittest
from unittest.mock import patch

from mongo_datatables import DataTables
from tests.base_test import BaseDataTablesTest


class TestPagination(BaseDataTablesTest):
    """Test cases for DataTables pagination functionality."""

    def _get_pipeline(self, datatables):
        with patch.object(datatables.collection, 'aggregate', return_value=[]) as mock_agg:
            datatables.results()
            args, _ = mock_agg.call_args
            return args[0]

    def test_pagination_in_pipeline(self):
        self.request_args["start"] = 10
        self.request_args["length"] = 20
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 10)
        self.assertEqual(datatables.limit, 20)
        pipeline = self._get_pipeline(datatables)
        skip_stage = next((s for s in pipeline if '$skip' in s), None)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(skip_stage)
        self.assertEqual(skip_stage['$skip'], 10)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], 20)

    def test_pagination_with_all_records(self):
        self.request_args["start"] = 0
        self.request_args["length"] = -1
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 0)
        self.assertEqual(datatables.limit, -1)
        pipeline = self._get_pipeline(datatables)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNone(limit_stage)

    def test_pagination_with_zero_length(self):
        self.request_args["start"] = 0
        self.request_args["length"] = 0
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 0)
        self.assertEqual(datatables.limit, 0)
        pipeline = self._get_pipeline(datatables)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNone(limit_stage)

    def test_pagination_with_string_values(self):
        self.request_args["start"] = "5"
        self.request_args["length"] = "15"
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 5)
        self.assertEqual(datatables.limit, 15)
        pipeline = self._get_pipeline(datatables)
        skip_stage = next((s for s in pipeline if '$skip' in s), None)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(skip_stage)
        self.assertEqual(skip_stage['$skip'], 5)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], 15)

    def test_length_minus_one_omits_limit_stage(self):
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        self.assertIsNone(next((s for s in pipeline if '$limit' in s), None))

    def test_length_positive_includes_limit_stage(self):
        self.request_args["length"] = 25
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], 25)

    def test_get_rows_with_length_minus_one(self):
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.collection.aggregate.return_value = []
        self.collection.estimated_document_count.return_value = 0
        self.collection.count_documents.return_value = 0
        response = dt.get_rows()
        self.assertIn('data', response)
        self.assertIn('recordsTotal', response)


if __name__ == '__main__':
    unittest.main()
