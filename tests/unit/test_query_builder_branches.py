"""Query builder branch coverage: keyword search, CC conditions, number/date helpers."""
import unittest
from unittest.mock import MagicMock, patch

from mongo_datatables.datatables import DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import DateHandler, FieldMapper

from mongo_datatables import DataTables


def _col(name, search_value=""):
    return {"data": name, "name": name, "searchable": True, "orderable": True,
            "search": {"value": search_value, "regex": False}}


def _build(columns, data_fields=None):
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": columns,
    }
    dt = DataTables(mock_db, "test", args, data_fields=data_fields or [])
    return dt.query_builder.build_column_search(columns)


class TestQueryBuilderCoverageGaps(unittest.TestCase):
    """Cover uncovered branches in query_builder.py."""

    def _qb(self, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    def test_build_column_search_keyword_field_exact_match(self):
        """keyword field in per-column search → exact match (no regex)."""
        cols = [_col("status", "active")]
        result = _build(cols, [DataField("status", "keyword")])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["status"], "active")

    def test_cc_list_empty_dict_no_condition(self):
        """list_data is an empty dict (falsy) → no condition"""
        qb = self._qb()
        result = qb._build_column_control_condition("field", "string", {"list": {}})
        self.assertEqual(result, [])

    def test_cc_list_number_all_fail_conversion(self):
        """number list where all values fail to_number → no $in condition"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "price", "number", {"list": {"0": "notanumber", "1": "alsonot"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_empty_value_non_empty_logic_skips(self):
        """search dict value is empty, logic not empty/notEmpty → no condition"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "name", "string",
            {"search": {"value": "", "logic": "contains", "type": "text"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_num_unknown_logic(self):
        """num type with unknown logic → no condition appended"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "price", "number",
            {"search": {"value": "10", "logic": "bogusOp", "type": "num"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_date_unknown_logic(self):
        """date type with unknown logic → no condition appended"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "created", "date",
            {"search": {"value": "2024-01-01", "logic": "bogusOp", "type": "date"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_string_unknown_logic(self):
        """string type with unknown logic → no condition appended"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "name", "string",
            {"search": {"value": "foo", "logic": "bogusOp", "type": "text"}}
        )
        self.assertEqual(result, [])

    def test_number_condition_lt(self):
        """< operator"""
        qb = self._qb()
        result = qb._build_number_condition("price", "50", "<")
        self.assertEqual(result, {"price": {"$lt": 50}})

    def test_number_condition_lte(self):
        """<= operator"""
        qb = self._qb()
        result = qb._build_number_condition("price", "50", "<=")
        self.assertEqual(result, {"price": {"$lte": 50}})

    def test_number_condition_eq(self):
        """= operator"""
        qb = self._qb()
        result = qb._build_number_condition("price", "50", "=")
        self.assertEqual(result, {"price": 50})

    def test_date_condition_non_date_string_fallback(self):
        """value doesn't look like YYYY-MM-DD → regex fallback"""
        qb = self._qb()
        result = qb._build_date_condition("created", "january", None)
        self.assertIn("$regex", result["created"])

    def test_date_condition_parse_exception_fallback(self):
        """DateHandler raises → regex fallback"""
        qb = self._qb()
        with patch.object(DateHandler, "get_date_range_for_comparison", side_effect=ValueError("bad")):
            result = qb._build_date_condition("created", "2024-01-01", None)
        self.assertIn("$regex", result["created"])


if __name__ == "__main__":
    unittest.main()
