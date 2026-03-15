"""Tests for order[i][name] ColReorder support in get_sort_specification."""
import unittest
from unittest.mock import MagicMock, patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestColReorderNameBasedSort(BaseDataTablesTest):
    """Tests for order[i][name] (ColReorder) support."""

    def _make_dt(self, order, columns=None):
        if columns is None:
            columns = [
                {"data": "name", "name": "name", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                {"data": "email", "name": "email", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                {"data": "status", "name": "status", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
            ]
        args = dict(self.request_args)
        args["columns"] = columns
        args["order"] = order
        return DataTables(self.mongo, "test", args)

    def test_name_based_sort_asc(self):
        """order[i][name] resolves column by name, ignoring index."""
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "asc"}])
        spec = dt.get_sort_specification()
        self.assertEqual(spec["email"], 1)

    def test_name_based_sort_desc(self):
        """order[i][name] with dir=desc."""
        dt = self._make_dt([{"column": "99", "name": "status", "dir": "desc"}])
        spec = dt.get_sort_specification()
        self.assertEqual(spec["status"], -1)

    def test_name_overrides_invalid_index(self):
        """When name is present and valid, out-of-range index is ignored."""
        dt = self._make_dt([{"column": "50", "name": "name", "dir": "asc"}])
        spec = dt.get_sort_specification()
        self.assertIn("name", spec)
        self.assertEqual(spec["name"], 1)

    def test_name_match_by_data_field(self):
        """order[i][name] matches column by data field when name attr not set."""
        columns = [
            {"data": "name", "name": "", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
            {"data": "email", "name": "", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
        ]
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "desc"}], columns=columns)
        spec = dt.get_sort_specification()
        self.assertEqual(spec["email"], -1)

    def test_index_fallback_when_no_name(self):
        """When order[i][name] is absent, falls back to index-based lookup."""
        dt = self._make_dt([{"column": "0", "dir": "desc"}])
        spec = dt.get_sort_specification()
        self.assertEqual(spec["name"], -1)

    def test_index_fallback_when_name_empty(self):
        """When order[i][name] is empty string, falls back to index."""
        dt = self._make_dt([{"column": "1", "name": "", "dir": "asc"}])
        spec = dt.get_sort_specification()
        self.assertEqual(spec["email"], 1)

    def test_multi_column_name_based(self):
        """Multiple order entries using name-based lookup."""
        dt = self._make_dt([
            {"column": "99", "name": "status", "dir": "asc"},
            {"column": "99", "name": "name", "dir": "desc"},
        ])
        spec = dt.get_sort_specification()
        self.assertEqual(spec["status"], 1)
        self.assertEqual(spec["name"], -1)

    def test_name_not_found_falls_back_to_index(self):
        """When name doesn't match any column, falls back to index."""
        dt = self._make_dt([{"column": "0", "name": "nonexistent", "dir": "asc"}])
        spec = dt.get_sort_specification()
        # nonexistent name → no match → falls back to index 0 → "name" column
        self.assertIn("name", spec)

    def test_name_non_orderable_skipped(self):
        """Column found by name but orderable=false is skipped."""
        columns = [
            {"data": "name", "name": "name", "searchable": "true", "orderable": "false", "search": {"value": "", "regex": "false"}},
        ]
        dt = self._make_dt([{"column": "99", "name": "name", "dir": "asc"}], columns=columns)
        spec = dt.get_sort_specification()
        self.assertNotIn("name", spec)
        self.assertEqual(spec, {"_id": 1})

    def test_id_tiebreaker_always_appended(self):
        """_id tiebreaker is always present."""
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "asc"}])
        spec = dt.get_sort_specification()
        self.assertIn("_id", spec)


if __name__ == "__main__":
    unittest.main()
