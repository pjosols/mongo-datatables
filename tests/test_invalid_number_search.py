"""Tests for invalid number search handling in _build_number_condition."""
import unittest
from unittest.mock import MagicMock

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField


def _make_dt(search_value, column_search_value=""):
    mongo = MagicMock()
    request_args = {
        "draw": "1",
        "start": 0,
        "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [
            {"data": "price", "name": "price", "searchable": True,
             "search": {"value": column_search_value, "regex": False}},
        ],
    }
    return DataTables(
        mongo, "products", request_args,
        data_fields=[DataField("price", "number")],
        use_text_index=False,
    )


class TestInvalidNumberSearch(unittest.TestCase):

    def test_invalid_colon_search_returns_no_condition(self):
        """Invalid value for number field via colon syntax returns empty filter."""
        dt = _make_dt("price:abc")
        result = dt.column_specific_search_condition
        self.assertEqual(result, {})

    def test_invalid_colon_search_no_regex_on_number_field(self):
        """Invalid number search must not produce a $regex condition on a number field."""
        dt = _make_dt("price:notanumber")
        result = dt.column_specific_search_condition
        # Must not contain a regex condition
        if "$and" in result:
            for cond in result["$and"]:
                price_cond = cond.get("price", {})
                self.assertNotIn("$regex", price_cond)

    def test_invalid_operator_colon_search_no_condition(self):
        """Invalid value with operator for number field returns empty filter."""
        dt = _make_dt("price:>abc")
        result = dt.column_specific_search_condition
        self.assertEqual(result, {})

    def test_valid_number_colon_search_still_works(self):
        """Valid number value via colon syntax still produces correct condition."""
        dt = _make_dt("price:42")
        result = dt.column_specific_search_condition
        self.assertIn("$and", result)
        price_cond = result["$and"][0]["price"]
        self.assertEqual(price_cond, 42)

    def test_valid_operator_colon_search_still_works(self):
        """Valid number with operator via colon syntax still produces correct condition."""
        dt = _make_dt("price:>10")
        result = dt.column_specific_search_condition
        self.assertIn("$and", result)
        price_cond = result["$and"][0]["price"]
        self.assertIn("$gt", price_cond)
        self.assertEqual(price_cond["$gt"], 10)

    def test_invalid_column_search_returns_no_condition(self):
        """Invalid value in column search for number field returns empty filter."""
        dt = _make_dt("", column_search_value="notanumber")
        result = dt.column_search_conditions
        self.assertEqual(result, {})

    def test_invalid_column_search_no_regex_on_number_field(self):
        """Invalid column search for number field must not produce $regex condition."""
        dt = _make_dt("", column_search_value="xyz")
        result = dt.column_search_conditions
        if "$and" in result:
            for cond in result["$and"]:
                price_cond = cond.get("price", {})
                self.assertNotIn("$regex", price_cond)


if __name__ == "__main__":
    unittest.main()
