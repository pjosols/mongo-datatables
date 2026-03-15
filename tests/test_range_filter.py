"""Tests for pipe-delimited range filtering in build_column_search."""

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField


def _make_datatables(mongo, columns, data_fields):
    """Build a DataTables instance with column search values set."""
    request_args = {
        "draw": "1",
        "start": 0,
        "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": columns,
    }
    return DataTables(mongo, "test_col", request_args, data_fields=data_fields, use_text_index=False)


def _col(name, search_value, field_type=None):
    """Return a column dict with the given search value."""
    return {"data": name, "name": name, "searchable": True, "search": {"value": search_value, "regex": False}}


class TestNumericRangeFilter(unittest.TestCase):

    def setUp(self):
        self.mongo = MagicMock()

    def _run(self, search_value):
        dt = _make_datatables(
            self.mongo,
            [_col("price", search_value)],
            [DataField("price", "number")],
        )
        return dt.column_search_conditions

    def test_both_bounds(self):
        result = self._run("10|50")
        cond = result["$and"][0]["price"]
        self.assertEqual(cond["$gte"], 10)
        self.assertEqual(cond["$lte"], 50)

    def test_lower_bound_only(self):
        result = self._run("10|")
        cond = result["$and"][0]["price"]
        self.assertIn("$gte", cond)
        self.assertEqual(cond["$gte"], 10)
        self.assertNotIn("$lte", cond)

    def test_upper_bound_only(self):
        result = self._run("|50")
        cond = result["$and"][0]["price"]
        self.assertIn("$lte", cond)
        self.assertEqual(cond["$lte"], 50)
        self.assertNotIn("$gte", cond)

    def test_exact_value_no_pipe(self):
        result = self._run("42")
        cond = result["$and"][0]["price"]
        self.assertEqual(cond, 42)

    def test_float_bounds(self):
        result = self._run("1.5|9.9")
        cond = result["$and"][0]["price"]
        self.assertAlmostEqual(cond["$gte"], 1.5)
        self.assertAlmostEqual(cond["$lte"], 9.9)

    def test_invalid_range_no_condition(self):
        result = self._run("abc|xyz")
        self.assertEqual(result, {})


class TestDateRangeFilter(unittest.TestCase):

    def setUp(self):
        self.mongo = MagicMock()

    def _run(self, search_value):
        dt = _make_datatables(
            self.mongo,
            [_col("created_at", search_value)],
            [DataField("created_at", "date")],
        )
        return dt.column_search_conditions

    def test_both_bounds(self):
        result = self._run("2024-01-01|2024-12-31")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$gte", cond)
        self.assertIn("$lte", cond)
        self.assertIsInstance(cond["$gte"], datetime)
        self.assertIsInstance(cond["$lte"], datetime)
        self.assertEqual(cond["$gte"], datetime(2024, 1, 1))
        # $lte from get_date_range_for_comparison('2024-12-31', '<=') → $lt next_day
        self.assertEqual(cond["$lte"], datetime(2025, 1, 1))

    def test_lower_bound_only(self):
        result = self._run("2024-06-01|")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$gte", cond)
        self.assertNotIn("$lte", cond)
        self.assertEqual(cond["$gte"], datetime(2024, 6, 1))

    def test_upper_bound_only(self):
        result = self._run("|2024-06-30")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$lte", cond)
        self.assertNotIn("$gte", cond)

    def test_non_range_uses_date_condition_not_regex(self):
        result = self._run("2024-03-15")
        cond = result["$and"][0]["created_at"]
        # Should use date-aware condition, not regex
        self.assertNotIn("$regex", cond)
        self.assertIn("$gte", cond)
        self.assertIn("$lt", cond)

    def test_invalid_date_range_no_condition(self):
        result = self._run("not-a-date|also-not")
        self.assertEqual(result, {})


class TestRangeFilterCombined(unittest.TestCase):
    """Range filter combined with global search."""

    def setUp(self):
        self.mongo = MagicMock()

    def test_range_plus_global_search(self):
        request_args = {
            "draw": "1",
            "start": 0,
            "length": 10,
            "search": {"value": "widget", "regex": False},
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [
                {"data": "name", "name": "name", "searchable": True, "search": {"value": "", "regex": False}},
                {"data": "price", "name": "price", "searchable": True, "search": {"value": "5|100", "regex": False}},
            ],
        }
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(
                self.mongo, "products", request_args,
                data_fields=[DataField("price", "number")],
                use_text_index=False,
            )
            col_cond = dt.column_search_conditions
            self.assertIn("$and", col_cond)
            price_cond = col_cond["$and"][0]["price"]
            self.assertEqual(price_cond["$gte"], 5)
            self.assertEqual(price_cond["$lte"], 100)


if __name__ == "__main__":
    unittest.main()
