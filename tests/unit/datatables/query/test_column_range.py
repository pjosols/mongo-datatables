"""Range filter and operator syntax tests: numeric/date ranges, colon syntax, parity."""
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


def _col(name, search_value=""):
    return {"data": name, "name": name, "searchable": True, "orderable": True,
            "search": {"value": search_value, "regex": False}}


def _make_datatables(mongo, columns, data_fields):
    """Build a DataTables instance with column search values set."""
    request_args = {
        "draw": "1", "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": columns,
    }
    return DataTables(mongo, "test_col", request_args, data_fields=data_fields, use_text_index=False)


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
        self.assertIn("$lt", cond)
        self.assertIsInstance(cond["$gte"], datetime)
        self.assertIsInstance(cond["$lt"], datetime)
        self.assertEqual(cond["$gte"], datetime(2024, 1, 1))
        self.assertEqual(cond["$lt"], datetime(2025, 1, 1))

    def test_lower_bound_only(self):
        result = self._run("2024-06-01|")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$gte", cond)
        self.assertNotIn("$lte", cond)
        self.assertEqual(cond["$gte"], datetime(2024, 6, 1))

    def test_upper_bound_only(self):
        result = self._run("|2024-06-30")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$lt", cond)
        self.assertNotIn("$gte", cond)

    def test_non_range_uses_date_condition_not_regex(self):
        result = self._run("2024-03-15")
        cond = result["$and"][0]["created_at"]
        self.assertNotIn("$regex", cond)
        self.assertIn("$gte", cond)
        self.assertIn("$lt", cond)

    def test_invalid_date_range_no_condition(self):
        result = self._run("not-a-date|also-not")
        self.assertEqual(result, {})

    def test_operator_prefix_gte_in_column_search(self):
        """>=YYYY-MM-DD in a date column box uses $gte operator, not '='."""
        result = self._run(">=2024-06-01")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$gte", cond)
        self.assertNotIn("$lt", cond)
        self.assertEqual(cond["$gte"], datetime(2024, 6, 1))

    def test_operator_prefix_lt_in_column_search(self):
        """<YYYY-MM-DD in a date column box uses $lt operator."""
        result = self._run("<2024-06-01")
        cond = result["$and"][0]["created_at"]
        self.assertIn("$lt", cond)
        self.assertNotIn("$gte", cond)


class TestRangeFilterCombined(unittest.TestCase):
    """Range filter combined with global search."""

    def setUp(self):
        self.mongo = MagicMock()

    def test_range_plus_global_search(self):
        request_args = {
            "draw": "1", "start": 0, "length": 10,
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


class TestSearchPathParity(unittest.TestCase):
    """Assert column search and colon syntax produce equivalent conditions."""

    def _qb(self, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    def _column_cond(self, field, value, field_type):
        """Build condition via build_column_search (per-column input box)."""
        qb = self._qb([DataField(field, field_type)])
        cols = [_col(field, value)]
        result = qb.build_column_search(cols)
        return result["$and"][0] if "$and" in result else result

    def _colon_cond(self, field, value, field_type):
        """Build condition via build_column_specific_search (colon syntax)."""
        qb = self._qb([DataField(field, field_type)])
        result = qb.build_column_specific_search([f"{field}:{value}"], [field])
        return result["$and"][0] if "$and" in result else result

    def test_string_field_parity(self):
        col = self._column_cond("author", "orwell", "string")
        colon = self._colon_cond("author", "orwell", "string")
        self.assertEqual(col, colon)

    def test_keyword_field_parity(self):
        col = self._column_cond("status", "active", "keyword")
        colon = self._colon_cond("status", "active", "keyword")
        self.assertEqual(col, colon)

    def test_number_field_no_operator_parity(self):
        col = self._column_cond("price", "50", "number")
        colon = self._colon_cond("price", "50", "number")
        self.assertEqual(col, colon)

    def test_number_field_gte_operator_parity(self):
        col = self._column_cond("price", ">=50", "number")
        colon = self._colon_cond("price", ">=50", "number")
        self.assertEqual(col, colon)

    def test_number_field_lt_operator_parity(self):
        col = self._column_cond("price", "<100", "number")
        colon = self._colon_cond("price", "<100", "number")
        self.assertEqual(col, colon)

    def test_date_field_no_operator_parity(self):
        col = self._column_cond("created", "2024-01-01", "date")
        colon = self._colon_cond("created", "2024-01-01", "date")
        self.assertEqual(col, colon)

    def test_date_field_gte_operator_parity(self):
        col = self._column_cond("created", ">=2024-01-01", "date")
        colon = self._colon_cond("created", ">=2024-01-01", "date")
        self.assertEqual(col, colon)

    def test_date_field_lte_operator_parity(self):
        col = self._column_cond("created", "<=2024-12-31", "date")
        colon = self._colon_cond("created", "<=2024-12-31", "date")
        self.assertEqual(col, colon)


class TestColonSyntax(unittest.TestCase):
    """Colon syntax (build_column_specific_search) edge cases."""

    def _qb(self, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    def test_empty_field_skipped(self):
        qb = self._qb([DataField("name", "string")])
        result = qb.build_column_specific_search([":value"], ["name"])
        self.assertEqual(result, {})

    def test_empty_value_skipped(self):
        qb = self._qb([DataField("name", "string")])
        result = qb.build_column_specific_search(["name:"], ["name"])
        self.assertEqual(result, {})

    def test_field_not_searchable(self):
        qb = self._qb([DataField("name", "string")])
        result = qb.build_column_specific_search(["secret:value"], ["name"])
        self.assertEqual(result, {})

    def test_lte_operator(self):
        qb = self._qb([DataField("price", "number")])
        result = qb.build_column_specific_search(["price:<=50"], ["price"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], {"$lte": 50})

    def test_lt_operator(self):
        qb = self._qb([DataField("price", "number")])
        result = qb.build_column_specific_search(["price:<50"], ["price"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], {"$lt": 50})

    def test_eq_operator(self):
        qb = self._qb([DataField("price", "number")])
        result = qb.build_column_specific_search(["price:=50"], ["price"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], 50)

    def test_date_field(self):
        qb = self._qb([DataField("created", "date")])
        result = qb.build_column_specific_search(["created:2024-01-01"], ["created"])
        self.assertIn("$and", result)
        self.assertIn("created", result["$and"][0])

    def test_keyword_field(self):
        qb = self._qb([DataField("status", "keyword")])
        result = qb.build_column_specific_search(["status:active"], ["status"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["status"], "active")


if __name__ == "__main__":
    unittest.main()
