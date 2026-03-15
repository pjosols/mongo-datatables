"""Tests for multi-column sort support in get_sort_specification."""
import copy
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestMultiColumnSort(BaseDataTablesTest):

    def _make_dt(self):
        return DataTables(self.mongo, 'users', self.request_args)

    def test_single_column_sort_asc(self):
        """Single sort still works (backward compat)."""
        self.request_args["order"] = [{"column": "0", "dir": "asc"}]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec["name"], 1)
        self.assertIn("_id", spec)

    def test_single_column_sort_desc(self):
        self.request_args["order"] = [{"column": "1", "dir": "desc"}]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec["email"], -1)

    def test_multi_column_sort_two_columns(self):
        """Two-column sort produces both fields in correct order."""
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec["name"], 1)
        self.assertEqual(spec["email"], -1)
        keys = list(spec.keys())
        self.assertLess(keys.index("name"), keys.index("email"))

    def test_multi_column_sort_three_columns(self):
        """Three-column sort includes all three fields."""
        self.request_args["order"] = [
            {"column": "2", "dir": "asc"},
            {"column": "0", "dir": "desc"},
            {"column": "1", "dir": "asc"},
        ]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec["status"], 1)
        self.assertEqual(spec["name"], -1)
        self.assertEqual(spec["email"], 1)

    def test_non_orderable_column_skipped(self):
        """Columns with orderable=false are excluded from sort."""
        self.request_args["columns"][0]["orderable"] = "false"
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertNotIn("name", spec)
        self.assertEqual(spec["email"], -1)

    def test_duplicate_field_first_wins(self):
        """If same column appears twice, first direction wins."""
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "0", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec["name"], 1)

    def test_empty_order_falls_back_to_id(self):
        """Empty order list returns only _id tiebreaker."""
        self.request_args["order"] = []
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec, {"_id": 1})

    def test_out_of_range_column_index_skipped(self):
        """Column index beyond columns list is silently skipped."""
        self.request_args["order"] = [{"column": "99", "dir": "asc"}]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec, {"_id": 1})

    def test_id_not_duplicated_when_sorting_by_id_column(self):
        """If _id is already in sort spec, tiebreaker is not added again."""
        self.request_args["columns"].append(
            {"data": "_id", "name": "", "searchable": "true", "orderable": "true",
             "search": {"value": "", "regex": "false"}}
        )
        self.request_args["order"] = [{"column": "3", "dir": "desc"}]
        dt = self._make_dt()
        spec = dt.get_sort_specification()
        self.assertEqual(spec["_id"], -1)
        self.assertEqual(list(spec.keys()).count("_id"), 1)

    def test_sort_specification_property_alias(self):
        """sort_specification property returns same result as get_sort_specification()."""
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        self.assertEqual(dt.sort_specification, dt.get_sort_specification())
