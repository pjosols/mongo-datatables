"""Consolidated column search tests: ColumnControl, field mapping, range filter."""
import json
import re
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import DateHandler, FieldMapper


# --- from tests/test_column_control.py ---
def _dt(columns, data_fields=None):
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": columns,
    }
    return DataTables(mock_db, "test", args, data_fields=data_fields or [])


def _col(name, cc=None, search_value="", searchable=True):
    return {"data": name, "name": name, "searchable": searchable, "orderable": True,
            "search": {"value": search_value, "regex": False}, **({"columnControl": cc} if cc else {})}


def _build(columns, data_fields=None):
    return _dt(columns, data_fields).query_builder.build_column_search(columns)


# --- Text logic ---

class TestColumnControlText:
    FIELDS = [DataField("name", "string")]

    def _q(self, logic, value="foo"):
        cols = [_col("name", {"search": {"value": value, "logic": logic, "type": "text"}})]
        return _build(cols, self.FIELDS)

    def test_contains(self):
        r = self._q("contains")
        assert r["$and"][0]["name"] == {"$regex": re.escape("foo"), "$options": "i"}

    def test_notContains(self):
        r = self._q("notContains")
        assert r["$and"][0]["name"] == {"$not": {"$regex": re.escape("foo"), "$options": "i"}}

    def test_equal(self):
        r = self._q("equal")
        assert r["$and"][0]["name"] == {"$regex": f"^{re.escape('foo')}$", "$options": "i"}

    def test_notEqual(self):
        r = self._q("notEqual")
        assert r["$and"][0]["name"] == {"$not": {"$regex": f"^{re.escape('foo')}$", "$options": "i"}}

    def test_starts(self):
        r = self._q("starts")
        assert r["$and"][0]["name"] == {"$regex": f"^{re.escape('foo')}", "$options": "i"}

    def test_ends(self):
        r = self._q("ends")
        assert r["$and"][0]["name"] == {"$regex": f"{re.escape('foo')}$", "$options": "i"}

    def test_empty(self):
        cols = [_col("name", {"search": {"value": "", "logic": "empty", "type": "text"}})]
        r = _build(cols, self.FIELDS)
        assert r["$and"][0]["name"] == {"$in": [None, ""]}

    def test_notEmpty(self):
        cols = [_col("name", {"search": {"value": "", "logic": "notEmpty", "type": "text"}})]
        r = _build(cols, self.FIELDS)
        assert r["$and"][0]["name"] == {"$nin": [None, ""]}


# --- Num logic ---

class TestColumnControlNum:
    FIELDS = [DataField("salary", "number")]

    def _q(self, logic, value="50000"):
        cols = [_col("salary", {"search": {"value": value, "logic": logic, "type": "num"}})]
        return _build(cols, self.FIELDS)

    def test_equal(self):
        assert self._q("equal")["$and"][0]["salary"] == 50000

    def test_notEqual(self):
        assert self._q("notEqual")["$and"][0]["salary"] == {"$ne": 50000}

    def test_greater(self):
        assert self._q("greater")["$and"][0]["salary"] == {"$gt": 50000}

    def test_greaterOrEqual(self):
        assert self._q("greaterOrEqual")["$and"][0]["salary"] == {"$gte": 50000}

    def test_less(self):
        assert self._q("less")["$and"][0]["salary"] == {"$lt": 50000}

    def test_lessOrEqual(self):
        assert self._q("lessOrEqual")["$and"][0]["salary"] == {"$lte": 50000}

    def test_empty(self):
        cols = [_col("salary", {"search": {"value": "", "logic": "empty", "type": "num"}})]
        r = _build(cols, self.FIELDS)
        assert r["$and"][0]["salary"] == {"$in": [None, ""]}

    def test_notEmpty(self):
        cols = [_col("salary", {"search": {"value": "", "logic": "notEmpty", "type": "num"}})]
        r = _build(cols, self.FIELDS)
        assert r["$and"][0]["salary"] == {"$nin": [None, ""]}


# --- Date logic ---

class TestColumnControlDate:
    FIELDS = [DataField("created", "date")]

    def _q(self, logic, value="2024-01-15"):
        cols = [_col("created", {"search": {"value": value, "logic": logic, "type": "date"}})]
        return _build(cols, self.FIELDS)

    def test_equal_is_day_range(self):
        r = self._q("equal")
        cond = r["$and"][0]["created"]
        assert "$gte" in cond and "$lt" in cond

    def test_notEqual(self):
        r = self._q("notEqual")
        assert "$or" in r["$and"][0]

    def test_greater(self):
        r = self._q("greater")
        assert "$gt" in r["$and"][0]["created"]

    def test_less(self):
        r = self._q("less")
        assert "$lt" in r["$and"][0]["created"]

    def test_empty(self):
        cols = [_col("created", {"search": {"value": "", "logic": "empty", "type": "date"}})]
        r = _build(cols, self.FIELDS)
        assert r["$and"][0]["created"] == {"$in": [None, ""]}

    def test_notEmpty(self):
        cols = [_col("created", {"search": {"value": "", "logic": "notEmpty", "type": "date"}})]
        r = _build(cols, self.FIELDS)
        assert r["$and"][0]["created"] == {"$nin": [None, ""]}

    def test_equal_iso_datetime_string(self):
        r = self._q("equal", "2024-01-15T00:00:00.000Z")
        cond = r["$and"][0]["created"]
        assert "$gte" in cond and "$lt" in cond

    def test_greater_iso_datetime_string(self):
        r = self._q("greater", "2024-01-15T00:00:00.000Z")
        assert "$gt" in r["$and"][0]["created"]

    def test_less_iso_datetime_string(self):
        r = self._q("less", "2024-01-15T00:00:00.000Z")
        assert "$lt" in r["$and"][0]["created"]


# --- List (multi-select) ---

class TestColumnControlList:
    def test_list_text(self):
        cols = [_col("status", {"list": {"0": "active", "1": "pending"}})]
        r = _build(cols, [DataField("status", "string")])
        assert r["$and"][0]["status"] == {"$in": ["active", "pending"]}

    def test_list_num(self):
        cols = [_col("score", {"list": {"0": "10", "1": "20"}})]
        r = _build(cols, [DataField("score", "number")])
        assert r["$and"][0]["score"] == {"$in": [10, 20]}

    def test_list_empty_dict_ignored(self):
        cols = [_col("status", {"list": {}})]
        r = _build(cols, [DataField("status", "string")])
        assert r == {}


# --- AND with standard column search ---

class TestColumnControlAndColumnSearch:
    def test_cc_anded_with_column_search(self):
        cols = [_col("name", {"search": {"value": "bar", "logic": "starts", "type": "text"}}, search_value="foo")]
        r = _build(cols, [DataField("name", "string")])
        assert len(r["$and"]) == 2


# --- Backward compat / edge cases ---

class TestColumnControlEdgeCases:
    def test_no_cc_no_search_returns_empty(self):
        assert _build([_col("name")]) == {}

    def test_null_cc_ignored(self):
        cols = [{"data": "name", "name": "name", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}, "columnControl": None}]
        assert _build(cols) == {}

    def test_invalid_num_value_ignored(self):
        cols = [_col("salary", {"search": {"value": "notanumber", "logic": "equal", "type": "num"}})]
        r = _build(cols, [DataField("salary", "number")])
        assert r == {}


# --- from tests/test_column_control_not_bson.py ---

def _builder():
    fm = FieldMapper([])
    return MongoQueryBuilder(fm)


def _cc(logic, value):
    return {"search": {"value": value, "logic": logic, "type": "text"}}


def _cond(logic, value):
    return _builder()._build_column_control_condition("name", "text", _cc(logic, value))


def test_not_contains_no_compiled_regex():
    result = _cond("notContains", "foo")
    not_val = result[0]["name"]["$not"]
    assert not isinstance(not_val, re.Pattern), "$not must not be a compiled regex"
    assert isinstance(not_val, dict), "$not must be a plain dict"


def test_not_contains_bson_serializable():
    result = _cond("notContains", "foo")
    json.dumps(result)


def test_not_contains_correct_pattern():
    result = _cond("notContains", "foo")
    not_val = result[0]["name"]["$not"]
    assert not_val == {"$regex": re.escape("foo"), "$options": "i"}


def test_not_equal_no_compiled_regex():
    result = _cond("notEqual", "bar")
    not_val = result[0]["name"]["$not"]
    assert not isinstance(not_val, re.Pattern), "$not must not be a compiled regex"
    assert isinstance(not_val, dict), "$not must be a plain dict"


def test_not_equal_bson_serializable():
    result = _cond("notEqual", "bar")
    json.dumps(result)


def test_not_equal_correct_pattern():
    result = _cond("notEqual", "bar")
    not_val = result[0]["name"]["$not"]
    assert not_val == {"$regex": f"^{re.escape('bar')}$", "$options": "i"}


@pytest.mark.parametrize("logic,expected_key", [
    ("contains", "$regex"),
    ("equal",    "$regex"),
    ("starts",   "$regex"),
    ("ends",     "$regex"),
])
def test_positive_logics_use_regex_dict(logic, expected_key):
    result = _cond(logic, "baz")
    cond = result[0]["name"]
    assert expected_key in cond
    assert "$options" in cond
    json.dumps(result)


# --- from tests/test_column_search_field_mapping.py ---
def _make_qb(*data_fields):
    fm = FieldMapper(list(data_fields))
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


class TestColumnSearchFieldMapping(unittest.TestCase):
    """Verify build_column_search uses db field name (not UI alias) in query keys."""

    def test_text_column_uses_db_field_name(self):
        """Text regex condition key must be the DB field path, not the UI alias."""
        qb = _make_qb(DataField("author.fullName", "string", "Author"))
        columns = [{"data": "Author", "searchable": True, "search": {"value": "smith", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("author.fullName", cond)
        self.assertNotIn("Author", cond)

    def test_text_column_no_alias_unchanged(self):
        """When no alias is set, column name equals db field — key is unchanged."""
        qb = _make_qb(DataField("title", "string"))
        columns = [{"data": "title", "searchable": True, "search": {"value": "mongo", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("title", cond)

    def test_text_column_regex_true_uses_db_field_name(self):
        """Raw-regex condition key must also be the DB field path."""
        qb = _make_qb(DataField("meta.tags", "string", "Tags"))
        columns = [{"data": "Tags", "searchable": True, "search": {"value": "^py", "regex": True}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("meta.tags", cond)
        self.assertNotIn("Tags", cond)

    def test_number_column_already_used_db_field(self):
        """Number branch was already correct — verify it still uses db field name."""
        qb = _make_qb(DataField("stats.score", "number", "Score"))
        columns = [{"data": "Score", "searchable": True, "search": {"value": "90", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("stats.score", cond)
        self.assertNotIn("Score", cond)


if __name__ == "__main__":
    unittest.main()


# --- from tests/test_global_search_field_mapping.py ---
def _make_dt(data_fields, search_value, searchable_columns, quoted=False):
    """Build a DataTables instance with no text index and given data_fields."""
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)

    columns = [
        {"data": col, "name": col, "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}}
        for col in searchable_columns
    ]
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": False},
        "order": [], "columns": columns,
    }
    return DataTables(mock_db, "test", args, data_fields=data_fields, use_text_index=False)


class TestGlobalSearchFieldMapping:
    """build_global_search() must use DB field names, not UI aliases."""

    def test_unquoted_term_uses_db_field(self):
        """Non-quoted global search should key on the DB field name."""
        data_fields = [DataField("author_name", "string", alias="Author")]
        dt = _make_dt(data_fields, "Smith", ["Author"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "author_name" in keys
        assert "Author" not in keys

    def test_quoted_phrase_uses_db_field(self):
        """Quoted global search (non-text-index path) should key on the DB field name."""
        data_fields = [DataField("author_name", "string", alias="Author")]
        dt = _make_dt(data_fields, '"Jonathan Kennedy"', ["Author"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "author_name" in keys
        assert "Author" not in keys

    def test_no_alias_field_unchanged(self):
        """When alias equals field name, the key should still be the DB field name."""
        data_fields = [DataField("status", "string")]
        dt = _make_dt(data_fields, "active", ["status"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "status" in keys

    def test_multiple_aliased_columns(self):
        """All columns in OR conditions should use DB field names."""
        data_fields = [
            DataField("first_name", "string", alias="FirstName"),
            DataField("last_name", "string", alias="LastName"),
        ]
        dt = _make_dt(data_fields, "Alice", ["FirstName", "LastName"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "first_name" in keys
        assert "last_name" in keys
        assert "FirstName" not in keys
        assert "LastName" not in keys


# --- from tests/test_range_filter.py ---
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


def _col_range(name, search_value, field_type=None):
    """Return a column dict with the given search value."""
    return {"data": name, "name": name, "searchable": True, "search": {"value": search_value, "regex": False}}


class TestNumericRangeFilter(unittest.TestCase):

    def setUp(self):
        self.mongo = MagicMock()

    def _run(self, search_value):
        dt = _make_datatables(
            self.mongo,
            [_col_range("price", search_value)],
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
            [_col_range("created_at", search_value)],
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
        # $lt from get_date_range_for_comparison('2024-12-31', '<=') → $lt next_day
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
        # Should use date-aware condition, not regex
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


class TestSearchPathParity(unittest.TestCase):
    """Assert that column search and colon syntax produce equivalent conditions for the same input."""

    def _qb(self, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    def _column_cond(self, field, value, field_type):
        """Build condition via build_column_search (per-column input box)."""
        qb = self._qb([DataField(field, field_type)])
        cols = [_col(field, search_value=value)]
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


class TestQueryBuilderCoverageGaps(unittest.TestCase):
    """Cover uncovered branches in query_builder.py."""

    def _qb(self, data_fields=None):
        field_mapper = FieldMapper(data_fields or [])
        return MongoQueryBuilder(field_mapper, use_text_index=False, has_text_index=False)

    def test_build_column_search_keyword_field_exact_match(self):
        """L92: keyword field in per-column search → exact match (no regex)."""
        cols = [_col("status", search_value="active")]
        result = _build(cols, [DataField("status", "keyword")])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["status"], "active")

    # --- build_global_search ---

    def test_global_search_no_searchable_columns_returns_empty(self):
        """L168: no searchable columns → {}"""
        qb = self._qb()
        result = qb.build_global_search(["hello"], [])
        self.assertEqual(result, {})

    def test_global_search_quoted_skips_date_number_cols(self):
        """L184: quoted phrase skips date/number columns; all skipped → {}"""
        qb = self._qb([DataField("created", "date"), DataField("amount", "number")])
        result = qb.build_global_search(
            ["exact"], ["created", "amount"],
            original_search='"exact"'
        )
        self.assertEqual(result, {})

    def test_global_search_keyword_col_skipped(self):
        """keyword fields are excluded from global search (exact-match fields shouldn't be regex-searched)."""
        qb = self._qb([DataField("status", "keyword"), DataField("name", "string")])
        result = qb.build_global_search(["active"], ["status", "name"])
        # status should not appear — only name gets the regex condition
        self.assertIn("$or", result)
        fields = [list(c.keys())[0] for c in result["$or"]]
        self.assertNotIn("status", fields)
        self.assertIn("name", fields)

    def test_global_search_quoted_with_string_col(self):
        """L183-192: quoted phrase, string col → $or with word-boundary regex"""
        qb = self._qb([DataField("name", "string")])
        result = qb.build_global_search(
            ["hello"], ["name"],
            original_search='"hello"'
        )
        self.assertIn("$or", result)
        self.assertIn("\\b", result["$or"][0]["name"]["$regex"])

    def test_global_search_smart_multi_term_all_number_cols_non_numeric(self):
        """L207-220: smart multi-term, all number cols, non-numeric term → {}"""
        qb = self._qb([DataField("price", "number")])
        result = qb.build_global_search(
            ["hello", "world"], ["price"],
            search_smart=True
        )
        # non-numeric terms fail to_number → no conditions → {}
        self.assertEqual(result, {})

    def test_global_search_smart_multi_term_numeric_values(self):
        """L207-220: smart multi-term with number col, numeric terms → $and"""
        qb = self._qb([DataField("price", "number")])
        result = qb.build_global_search(
            ["10", "20"], ["price"],
            search_smart=True
        )
        self.assertIn("$and", result)

    # --- build_column_specific_search ---

    def test_colon_search_empty_field_skipped(self):
        """L261: empty field portion → skip"""
        qb = self._qb([DataField("name", "string")])
        result = qb.build_column_specific_search([":value"], ["name"])
        self.assertEqual(result, {})

    def test_colon_search_empty_value_skipped(self):
        """L261: empty value portion → skip"""
        qb = self._qb([DataField("name", "string")])
        result = qb.build_column_specific_search(["name:"], ["name"])
        self.assertEqual(result, {})

    def test_colon_search_field_not_searchable(self):
        """L266: field not in searchable_columns → skip"""
        qb = self._qb([DataField("name", "string")])
        result = qb.build_column_specific_search(["secret:value"], ["name"])
        self.assertEqual(result, {})

    def test_colon_search_lte_operator(self):
        """L274: <= operator for number field"""
        qb = self._qb([DataField("price", "number")])
        result = qb.build_column_specific_search(["price:<=50"], ["price"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], {"$lte": 50})

    def test_colon_search_lt_operator(self):
        """L278: < operator for number field"""
        qb = self._qb([DataField("price", "number")])
        result = qb.build_column_specific_search(["price:<50"], ["price"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], {"$lt": 50})

    def test_colon_search_eq_operator(self):
        """L280-281: = operator for number field"""
        qb = self._qb([DataField("price", "number")])
        result = qb.build_column_specific_search(["price:=50"], ["price"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], 50)

    def test_colon_search_date_field(self):
        """L288-290: date field with colon syntax"""
        qb = self._qb([DataField("created", "date")])
        result = qb.build_column_specific_search(["created:2024-01-01"], ["created"])
        self.assertIn("$and", result)
        self.assertIn("created", result["$and"][0])

    def test_colon_search_keyword_field(self):
        """L292: keyword field → exact match"""
        qb = self._qb([DataField("status", "keyword")])
        result = qb.build_column_specific_search(["status:active"], ["status"])
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["status"], "active")

    # --- _build_column_control_condition ---

    def test_cc_list_empty_dict_no_condition(self):
        """list_data is an empty dict (falsy) → outer guard skips block → no condition"""
        qb = self._qb()
        result = qb._build_column_control_condition("field", "string", {"list": {}})
        self.assertEqual(result, [])

    def test_cc_list_number_all_fail_conversion(self):
        """L312+314: number list where all values fail to_number → no $in condition"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "price", "number", {"list": {"0": "notanumber", "1": "alsonot"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_empty_value_non_empty_logic_skips(self):
        """L329: search dict value is empty, logic not empty/notEmpty → no condition"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "name", "string",
            {"search": {"value": "", "logic": "contains", "type": "text"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_num_unknown_logic(self):
        """L343: num type with unknown logic → no condition appended"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "price", "number",
            {"search": {"value": "10", "logic": "bogusOp", "type": "num"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_date_unknown_logic(self):
        """L357: date type with unknown logic → no condition appended"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "created", "date",
            {"search": {"value": "2024-01-01", "logic": "bogusOp", "type": "date"}}
        )
        self.assertEqual(result, [])

    def test_cc_search_string_unknown_logic(self):
        """L373: string type with unknown logic → no condition appended"""
        qb = self._qb()
        result = qb._build_column_control_condition(
            "name", "string",
            {"search": {"value": "foo", "logic": "bogusOp", "type": "text"}}
        )
        self.assertEqual(result, [])

    # --- _build_number_condition ---

    def test_number_condition_lt(self):
        """L400: < operator"""
        qb = self._qb()
        result = qb._build_number_condition("price", "50", "<")
        self.assertEqual(result, {"price": {"$lt": 50}})

    def test_number_condition_lte(self):
        """L404: <= operator"""
        qb = self._qb()
        result = qb._build_number_condition("price", "50", "<=")
        self.assertEqual(result, {"price": {"$lte": 50}})

    def test_number_condition_eq(self):
        """L406: = operator"""
        qb = self._qb()
        result = qb._build_number_condition("price", "50", "=")
        self.assertEqual(result, {"price": 50})

    # --- _build_date_condition ---

    def test_date_condition_non_date_string_fallback(self):
        """L433: value doesn't look like YYYY-MM-DD → regex fallback"""
        qb = self._qb()
        result = qb._build_date_condition("created", "january", None)
        self.assertIn("$regex", result["created"])

    def test_date_condition_parse_exception_fallback(self):
        """L434-435: DateHandler raises → regex fallback"""
        qb = self._qb()
        with patch.object(DateHandler, "get_date_range_for_comparison", side_effect=ValueError("bad")):
            result = qb._build_date_condition("created", "2024-01-01", None)
        self.assertIn("$regex", result["created"])


if __name__ == "__main__":
    unittest.main()

