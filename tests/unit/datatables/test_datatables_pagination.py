"""Test DataTables pagination: skip, limit, and CWE-400 clamping."""
import unittest
from unittest.mock import patch

from mongo_datatables import DataTables
from mongo_datatables.datatables._limits import MAX_PAGE_SIZE, DEFAULT_PAGE_SIZE
from tests.unit.base_test import BaseDataTablesTest


class TestPagination(BaseDataTablesTest):
    """Test pagination in aggregation pipeline.
    
    Validates skip/limit stages, boundary conditions, and CWE-400 DoS prevention.
    """

    def _get_pipeline(self, datatables):
        with patch.object(datatables.collection, 'aggregate', return_value=[]) as mock_agg:
            datatables.results()
            args, _ = mock_agg.call_args
            return args[0]

    def test_normal_pagination_in_pipeline(self):
        """Verify skip and limit stages in pipeline for normal pagination."""
        self.request_args["start"] = 10
        self.request_args["length"] = 20
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.start, 10)
        self.assertEqual(dt.limit, 20)
        pipeline = self._get_pipeline(dt)
        skip_stage = next((s for s in pipeline if '$skip' in s), None)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(skip_stage)
        self.assertEqual(skip_stage['$skip'], 10)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], 20)

    def test_pagination_with_string_values(self):
        """Coerce string start/length to integers."""
        self.request_args["start"] = "5"
        self.request_args["length"] = "15"
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.start, 5)
        self.assertEqual(dt.limit, 15)

    # --- CWE-400: unbounded pagination clamping ---

    def test_negative_length_returns_default_page_size(self):
        """length=-1 must not bypass pagination; returns DEFAULT_PAGE_SIZE."""
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, DEFAULT_PAGE_SIZE)

    def test_zero_length_returns_default_page_size(self):
        """length=0 is invalid; returns DEFAULT_PAGE_SIZE."""
        self.request_args["length"] = 0
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, DEFAULT_PAGE_SIZE)

    def test_large_length_clamped_to_max(self):
        """Arbitrarily large length is clamped to MAX_PAGE_SIZE."""
        self.request_args["length"] = 10_000_000
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, MAX_PAGE_SIZE)

    def test_length_at_max_boundary_accepted(self):
        """length == MAX_PAGE_SIZE is accepted as-is."""
        self.request_args["length"] = MAX_PAGE_SIZE
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, MAX_PAGE_SIZE)

    def test_length_one_above_max_clamped(self):
        """length == MAX_PAGE_SIZE + 1 is clamped to MAX_PAGE_SIZE."""
        self.request_args["length"] = MAX_PAGE_SIZE + 1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, MAX_PAGE_SIZE)

    def test_negative_length_pipeline_still_has_limit_stage(self):
        """With negative length, pipeline must include a $limit stage (no unbounded scan)."""
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], DEFAULT_PAGE_SIZE)

    def test_large_length_pipeline_limit_clamped(self):
        """Pipeline $limit stage must not exceed MAX_PAGE_SIZE."""
        self.request_args["length"] = 10_000_000
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(limit_stage)
        self.assertLessEqual(limit_stage['$limit'], MAX_PAGE_SIZE)

    def test_invalid_string_length_returns_default(self):
        """Non-numeric length string raises InvalidDataError."""
        from mongo_datatables.exceptions import InvalidDataError
        self.request_args["length"] = "abc"
        with self.assertRaises(InvalidDataError):
            DataTables(self.mongo, 'users', self.request_args)

    def test_none_length_returns_default(self):
        """Missing length returns DEFAULT_PAGE_SIZE."""
        self.request_args.pop("length", None)
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, DEFAULT_PAGE_SIZE)


if __name__ == '__main__':
    unittest.main()
