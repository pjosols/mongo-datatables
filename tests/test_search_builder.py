"""Tests for SearchBuilder server-side support."""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables


def _dt(sb_payload, extra_args=None):
    """Build a DataTables instance with a searchBuilder payload."""
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": [],
        "searchBuilder": sb_payload,
    }
    if extra_args:
        args.update(extra_args)
    return DataTables(mock_db, "test", args)


class TestSearchBuilderEmpty:
    def test_no_search_builder_returns_empty(self):
        dt = _dt(None)
        assert dt._parse_search_builder() == {}

    def test_empty_dict_returns_empty(self):
        dt = _dt({})
        assert dt._parse_search_builder() == {}

    def test_empty_criteria_returns_empty(self):
        dt = _dt({"criteria": [], "logic": "AND"})
        assert dt._parse_search_builder() == {}


class TestSearchBuilderString:
    def test_equals(self):
        dt = _dt({"criteria": [{"condition": "=", "origData": "name", "type": "string", "value": ["Alice"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"name": {"$regex": "^Alice$", "$options": "i"}}

    def test_not_equals(self):
        dt = _dt({"criteria": [{"condition": "!=", "origData": "name", "type": "string", "value": ["Alice"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        not_val = result["name"]["$not"]
        assert not_val == {"$regex": "^Alice$", "$options": "i"}

    def test_contains(self):
        dt = _dt({"criteria": [{"condition": "contains", "origData": "city", "type": "string", "value": ["York"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"city": {"$regex": "York", "$options": "i"}}

    def test_not_contains(self):
        dt = _dt({"criteria": [{"condition": "!contains", "origData": "city", "type": "string", "value": ["York"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        not_val = result["city"]["$not"]
        assert not_val == {"$regex": "York", "$options": "i"}

    def test_starts(self):
        dt = _dt({"criteria": [{"condition": "starts", "origData": "name", "type": "string", "value": ["Al"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"name": {"$regex": "^Al", "$options": "i"}}

    def test_not_starts(self):
        dt = _dt({"criteria": [{"condition": "!starts", "origData": "name", "type": "string", "value": ["Al"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        not_val = result["name"]["$not"]
        assert not_val == {"$regex": "^Al", "$options": "i"}

    def test_ends(self):
        dt = _dt({"criteria": [{"condition": "ends", "origData": "name", "type": "string", "value": ["ice"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"name": {"$regex": "ice$", "$options": "i"}}

    def test_not_ends(self):
        dt = _dt({"criteria": [{"condition": "!ends", "origData": "name", "type": "string", "value": ["ice"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        not_val = result["name"]["$not"]
        assert not_val == {"$regex": "ice$", "$options": "i"}

    def test_special_chars_escaped(self):
        dt = _dt({"criteria": [{"condition": "contains", "origData": "email", "type": "string", "value": ["user.name+tag"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert "$regex" in result["email"]
        assert "+" not in result["email"]["$regex"].replace("\\+", "")


class TestSearchBuilderNumber:
    def test_equals(self):
        dt = _dt({"criteria": [{"condition": "=", "origData": "age", "type": "num", "value": ["30"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"age": 30.0}

    def test_not_equals(self):
        dt = _dt({"criteria": [{"condition": "!=", "origData": "age", "type": "num", "value": ["30"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"age": {"$ne": 30.0}}

    def test_less_than(self):
        dt = _dt({"criteria": [{"condition": "<", "origData": "salary", "type": "num", "value": ["50000"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"salary": {"$lt": 50000.0}}

    def test_less_than_or_equal(self):
        dt = _dt({"criteria": [{"condition": "<=", "origData": "salary", "type": "num", "value": ["50000"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"salary": {"$lte": 50000.0}}

    def test_greater_than(self):
        dt = _dt({"criteria": [{"condition": ">", "origData": "salary", "type": "num", "value": ["30000"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"salary": {"$gt": 30000.0}}

    def test_greater_than_or_equal(self):
        dt = _dt({"criteria": [{"condition": ">=", "origData": "salary", "type": "num", "value": ["30000"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"salary": {"$gte": 30000.0}}

    def test_between(self):
        dt = _dt({"criteria": [{"condition": "between", "origData": "age", "type": "num", "value": ["20", "40"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert result == {"age": {"$gte": 20.0, "$lte": 40.0}}

    def test_not_between(self):
        dt = _dt({"criteria": [{"origData": "age", "condition": "!between", "type": "num", "value": ["20", "30"]}], "logic": "AND"})
        result = dt.filter
        assert result == {"$or": [{"age": {"$lt": 20}}, {"age": {"$gt": 30}}]}

    def test_not_between_date(self):
        from datetime import datetime
        dt = _dt({"criteria": [{"origData": "created", "condition": "!between", "type": "date", "value": ["2024-01-01", "2024-12-31"]}], "logic": "AND"})
        result = dt.filter
        assert "$or" in result
        assert len(result["$or"]) == 2
        assert "$lt" in result["$or"][0].get("created", {})
        assert "$gte" in result["$or"][1].get("created", {})


class TestSearchBuilderNullConditions:
    def test_null_string_type(self):
        """null on string type checks None and empty string."""
        result = _dt({"criteria": [{"condition": "null", "data": "manager", "origData": "manager", "type": "string", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"manager": {"$in": [None, ""]}}

    def test_not_null_string_type(self):
        """!null on string type excludes None and empty string."""
        result = _dt({"criteria": [{"condition": "!null", "data": "manager", "origData": "manager", "type": "string", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"manager": {"$nin": [None, ""]}}

    def test_null_num_type(self):
        """null on num type checks only None (not empty string)."""
        result = _dt({"criteria": [{"condition": "null", "data": "score", "origData": "score", "type": "num", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"score": None}

    def test_not_null_num_type(self):
        """!null on num type checks only None."""
        result = _dt({"criteria": [{"condition": "!null", "data": "score", "origData": "score", "type": "num", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"score": {"$ne": None}}

    def test_null_date_type(self):
        """null on date type checks only None."""
        result = _dt({"criteria": [{"condition": "null", "data": "created", "origData": "created", "type": "date", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"created": None}

    def test_not_null_date_type(self):
        """!null on date type checks only None."""
        result = _dt({"criteria": [{"condition": "!null", "data": "created", "origData": "created", "type": "date", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"created": {"$ne": None}}

    def test_null_html_num_type(self):
        """null on html-num type checks only None."""
        result = _dt({"criteria": [{"condition": "null", "data": "price", "origData": "price", "type": "html-num", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"price": None}

    def test_null_html_type(self):
        """null on html type (string-like) checks None and empty string."""
        result = _dt({"criteria": [{"condition": "null", "data": "bio", "origData": "bio", "type": "html", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"bio": {"$in": [None, ""]}}


class TestSearchBuilderLogic:
    def test_and_logic_two_criteria(self):
        dt = _dt({"criteria": [
            {"condition": "=", "origData": "dept", "type": "string", "value": ["Engineering"]},
            {"condition": ">", "origData": "age", "type": "num", "value": ["25"]}
        ], "logic": "AND"})
        result = dt._parse_search_builder()
        assert "$and" in result
        assert len(result["$and"]) == 2

    def test_or_logic_two_criteria(self):
        dt = _dt({"criteria": [
            {"condition": "=", "origData": "city", "type": "string", "value": ["London"]},
            {"condition": "=", "origData": "city", "type": "string", "value": ["Paris"]}
        ], "logic": "OR"})
        result = dt._parse_search_builder()
        assert "$or" in result
        assert len(result["$or"]) == 2

    def test_single_criterion_no_wrapper(self):
        """Single criterion should not be wrapped in $and."""
        dt = _dt({"criteria": [{"condition": "=", "origData": "name", "type": "string", "value": ["Bob"]}], "logic": "AND"})
        result = dt._parse_search_builder()
        assert "$and" not in result
        assert "name" in result

    def test_nested_group(self):
        """Nested criteria group (group within group)."""
        dt = _dt({
            "criteria": [
                {"condition": "=", "origData": "dept", "type": "string", "value": ["Engineering"]},
                {
                    "criteria": [
                        {"condition": ">", "origData": "age", "type": "num", "value": ["20"]},
                        {"condition": "<", "origData": "age", "type": "num", "value": ["40"]}
                    ],
                    "logic": "OR"
                }
            ],
            "logic": "AND"
        })
        result = dt._parse_search_builder()
        assert "$and" in result
        parts = result["$and"]
        assert len(parts) == 2
        # second part should be the nested OR group
        assert "$or" in parts[1]


class TestSearchBuilderFilterIntegration:
    def test_search_builder_included_in_filter(self):
        """SearchBuilder condition should appear in the combined filter."""
        mock_col = MagicMock()
        mock_col.list_indexes.return_value = []
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [], "columns": [],
            "searchBuilder": {
                "criteria": [{"condition": "=", "origData": "status", "type": "string", "value": ["active"]}],
                "logic": "AND"
            }
        }
        dt = DataTables(mock_db, "test", args)
        f = dt.filter
        # filter should contain the searchBuilder condition
        assert f  # non-empty
        import json
        filter_str = json.dumps(f)
        assert "status" in filter_str

    def test_search_builder_combined_with_custom_filter(self):
        """SearchBuilder + custom_filter should both appear in combined filter."""
        mock_col = MagicMock()
        mock_col.list_indexes.return_value = []
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [], "columns": [],
            "searchBuilder": {
                "criteria": [{"condition": ">", "origData": "score", "type": "num", "value": ["80"]}],
                "logic": "AND"
            }
        }
        dt = DataTables(mock_db, "test", args, tenant="acme")
        f = dt.filter
        assert "$and" in f
        parts = f["$and"]
        # custom_filter and searchBuilder should both be present
        assert any("tenant" in str(p) for p in parts)
        assert any("score" in str(p) for p in parts)

    def test_no_search_builder_no_impact_on_filter(self):
        """Absence of searchBuilder should not affect filter."""
        mock_col = MagicMock()
        mock_col.list_indexes.return_value = []
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [], "columns": []
        }
        dt = DataTables(mock_db, "test", args)
        assert dt.filter == {}
