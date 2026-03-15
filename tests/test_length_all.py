from unittest.mock import patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestLengthAll(BaseDataTablesTest):
    """Tests for DataTables length=-1 (Show All) handling."""

    def _get_pipeline(self, datatables: DataTables) -> list:
        with patch.object(datatables.collection, 'aggregate', return_value=[]) as mock_agg:
            datatables.results()
            args, _ = mock_agg.call_args
            return args[0]

    def test_length_minus_one_omits_limit_stage(self):
        """length=-1 should produce no $limit stage in the pipeline."""
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNone(limit_stage)

    def test_length_minus_one_limit_property(self):
        """limit property should still return -1 for backward compatibility."""
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, -1)

    def test_length_positive_includes_limit_stage(self):
        """Positive length should produce a $limit stage with the correct value."""
        self.request_args["length"] = 25
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], 25)

    def test_length_zero_omits_limit_stage(self):
        """length=0 should produce no $limit stage in the pipeline."""
        self.request_args["length"] = 0
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNone(limit_stage)

    def test_get_rows_with_length_minus_one(self):
        """get_rows() should complete without error when length=-1."""
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.collection.aggregate.return_value = []
        self.collection.estimated_document_count.return_value = 0
        self.collection.count_documents.return_value = 0
        response = dt.get_rows()
        self.assertIn('data', response)
        self.assertIn('recordsTotal', response)
