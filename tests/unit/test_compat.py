"""Tests for DataTablesMixin shims in datatables/compat.py."""
import pytest
from unittest.mock import MagicMock
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


def _make_dt(mongo, request_args):
    return DataTables(mongo, "test", request_args)


def _col(data, search_value="", regex="false", searchable="true"):
    return {
        "data": data, "name": "", "searchable": searchable,
        "orderable": "true", "search": {"value": search_value, "regex": regex},
    }


def _base_args(columns, search_value=""):
    return {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": "false"},
        "order": [{"column": "0", "dir": "asc"}],
        "columns": columns,
    }


class TestColumnSearchConditionsShim(BaseDataTablesTest):
    """column_search_conditions delegates to query_builder.build_column_search."""

    def test_returns_empty_when_no_column_search(self):
        dt = _make_dt(self.mongo, _base_args([_col("name")]))
        assert dt.column_search_conditions == {}

    def test_returns_condition_for_active_column_search(self):
        dt = _make_dt(self.mongo, _base_args([_col("name", search_value="Alice")]))
        result = dt.column_search_conditions
        assert "$and" in result
        assert result["$and"][0] == {"name": {"$regex": "Alice", "$options": "i"}}

    def test_multiple_columns_combined(self):
        cols = [_col("name", search_value="Alice"), _col("email", search_value="@test")]
        dt = _make_dt(self.mongo, _base_args(cols))
        result = dt.column_search_conditions
        assert "$and" in result
        fields = [list(c.keys())[0] for c in result["$and"]]
        assert "name" in fields
        assert "email" in fields

    def test_non_searchable_column_excluded(self):
        cols = [_col("name", search_value="Alice", searchable="false")]
        dt = _make_dt(self.mongo, _base_args(cols))
        assert dt.column_search_conditions == {}

    def test_regex_true_uses_raw_pattern(self):
        cols = [_col("name", search_value="a.b", regex="true")]
        dt = _make_dt(self.mongo, _base_args(cols))
        result = dt.column_search_conditions
        assert result["$and"][0]["name"]["$regex"] == "a.b"

    def test_regex_false_escapes_pattern(self):
        cols = [_col("name", search_value="a.b", regex="false")]
        dt = _make_dt(self.mongo, _base_args(cols))
        result = dt.column_search_conditions
        assert result["$and"][0]["name"]["$regex"] == r"a\.b"

    def test_delegates_to_query_builder(self):
        dt = _make_dt(self.mongo, _base_args([_col("name", search_value="x")]))
        dt.query_builder = MagicMock()
        dt.query_builder.build_column_search.return_value = {"mocked": True}
        result = dt.column_search_conditions
        dt.query_builder.build_column_search.assert_called_once_with(dt.columns)
        assert result == {"mocked": True}


class TestParseSearchFixedShim(BaseDataTablesTest):
    """_parse_search_fixed delegates to parse_search_fixed module function."""

    def test_returns_empty_when_no_fixed_search(self):
        dt = _make_dt(self.mongo, _base_args([_col("name")]))
        assert dt._parse_search_fixed() == {}

    def test_legacy_searchFixed_dict(self):
        args = _base_args([_col("name"), _col("email")])
        args["searchFixed"] = {"active": "Alice"}
        dt = _make_dt(self.mongo, args)
        result = dt._parse_search_fixed()
        assert result  # non-empty filter produced
        assert "$or" in result or "$and" in result or "$regex" in str(result)

    def test_dt2_wire_format_search_fixed_array(self):
        args = _base_args([_col("name"), _col("email")])
        args["search"]["fixed"] = [{"name": "myFilter", "term": "Bob"}]
        dt = _make_dt(self.mongo, args)
        result = dt._parse_search_fixed()
        assert result

    def test_function_term_skipped_in_wire_format(self):
        # "function" terms are skipped only in the DT2 wire format (search.fixed array)
        args = _base_args([_col("name")])
        args["search"]["fixed"] = [{"name": "fn", "term": "function"}]
        dt = _make_dt(self.mongo, args)
        assert dt._parse_search_fixed() == {}

    def test_empty_term_skipped(self):
        args = _base_args([_col("name")])
        args["searchFixed"] = {"key": ""}
        dt = _make_dt(self.mongo, args)
        assert dt._parse_search_fixed() == {}

    def test_multiple_fixed_terms_combined(self):
        args = _base_args([_col("name"), _col("email")])
        args["searchFixed"] = {"f1": "Alice", "f2": "Bob"}
        dt = _make_dt(self.mongo, args)
        result = dt._parse_search_fixed()
        assert "$and" in result
        assert len(result["$and"]) == 2

    def test_delegates_to_module_function(self):
        args = _base_args([_col("name")])
        dt = _make_dt(self.mongo, args)
        with pytest.MonkeyPatch().context() as mp:
            mock_fn = MagicMock(return_value={"patched": True})
            mp.setattr("mongo_datatables.search_fixed.parse_search_fixed", mock_fn)
            # Re-import to pick up patch via the shim's import path
            from mongo_datatables import search_fixed as sf
            original = sf.parse_search_fixed
            sf.parse_search_fixed = mock_fn
            try:
                result = dt._parse_search_fixed()
            finally:
                sf.parse_search_fixed = original
