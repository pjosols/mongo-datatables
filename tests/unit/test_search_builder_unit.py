"""SearchBuilder core tests: conditions, logic, filter integration, value2, coverage gaps."""
import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from mongo_datatables import DataTables
from mongo_datatables.search_builder import _sb_date, _sb_number, _sb_string


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _dt(sb_payload, extra_args=None):
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


def _sb_dt(sb_payload):
    return _dt(sb_payload)


def _criterion(condition, field, type_, value, value2=None):
    c = {"condition": condition, "origData": field, "type": type_, "value": value}
    if value2 is not None:
        c["value2"] = value2
    return c


# ---------------------------------------------------------------------------
# Empty / no-op
# ---------------------------------------------------------------------------

class TestSearchBuilderEmpty:
    def test_no_search_builder_returns_empty(self):
        assert _dt(None)._parse_search_builder() == {}

    def test_empty_dict_returns_empty(self):
        assert _dt({})._parse_search_builder() == {}

    def test_empty_criteria_returns_empty(self):
        assert _dt({"criteria": [], "logic": "AND"})._parse_search_builder() == {}


# ---------------------------------------------------------------------------
# String conditions
# ---------------------------------------------------------------------------

class TestSearchBuilderString:
    def test_equals(self):
        result = _dt({"criteria": [{"condition": "=", "origData": "name", "type": "string", "value": ["Alice"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"name": {"$regex": "^Alice$", "$options": "i"}}

    def test_not_equals(self):
        result = _dt({"criteria": [{"condition": "!=", "origData": "name", "type": "string", "value": ["Alice"]}], "logic": "AND"})._parse_search_builder()
        assert result["name"]["$not"] == {"$regex": "^Alice$", "$options": "i"}

    def test_contains(self):
        result = _dt({"criteria": [{"condition": "contains", "origData": "city", "type": "string", "value": ["York"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"city": {"$regex": "York", "$options": "i"}}

    def test_not_contains(self):
        result = _dt({"criteria": [{"condition": "!contains", "origData": "city", "type": "string", "value": ["York"]}], "logic": "AND"})._parse_search_builder()
        assert result["city"]["$not"] == {"$regex": "York", "$options": "i"}

    def test_starts(self):
        result = _dt({"criteria": [{"condition": "starts", "origData": "name", "type": "string", "value": ["Al"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"name": {"$regex": "^Al", "$options": "i"}}

    def test_not_starts(self):
        result = _dt({"criteria": [{"condition": "!starts", "origData": "name", "type": "string", "value": ["Al"]}], "logic": "AND"})._parse_search_builder()
        assert result["name"]["$not"] == {"$regex": "^Al", "$options": "i"}

    def test_ends(self):
        result = _dt({"criteria": [{"condition": "ends", "origData": "name", "type": "string", "value": ["ice"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"name": {"$regex": "ice$", "$options": "i"}}

    def test_not_ends(self):
        result = _dt({"criteria": [{"condition": "!ends", "origData": "name", "type": "string", "value": ["ice"]}], "logic": "AND"})._parse_search_builder()
        assert result["name"]["$not"] == {"$regex": "ice$", "$options": "i"}

    def test_special_chars_escaped(self):
        result = _dt({"criteria": [{"condition": "contains", "origData": "email", "type": "string", "value": ["user.name+tag"]}], "logic": "AND"})._parse_search_builder()
        assert "$regex" in result["email"]
        assert "+" not in result["email"]["$regex"].replace("\\+", "")


# ---------------------------------------------------------------------------
# Number conditions
# ---------------------------------------------------------------------------

class TestSearchBuilderNumber:
    def test_equals(self):
        result = _dt({"criteria": [{"condition": "=", "origData": "age", "type": "num", "value": ["30"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"age": 30.0}

    def test_not_equals(self):
        result = _dt({"criteria": [{"condition": "!=", "origData": "age", "type": "num", "value": ["30"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"age": {"$ne": 30.0}}

    def test_less_than(self):
        result = _dt({"criteria": [{"condition": "<", "origData": "salary", "type": "num", "value": ["50000"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"salary": {"$lt": 50000.0}}

    def test_less_than_or_equal(self):
        result = _dt({"criteria": [{"condition": "<=", "origData": "salary", "type": "num", "value": ["50000"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"salary": {"$lte": 50000.0}}

    def test_greater_than(self):
        result = _dt({"criteria": [{"condition": ">", "origData": "salary", "type": "num", "value": ["30000"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"salary": {"$gt": 30000.0}}

    def test_greater_than_or_equal(self):
        result = _dt({"criteria": [{"condition": ">=", "origData": "salary", "type": "num", "value": ["30000"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"salary": {"$gte": 30000.0}}

    def test_between(self):
        result = _dt({"criteria": [{"condition": "between", "origData": "age", "type": "num", "value": ["20", "40"]}], "logic": "AND"})._parse_search_builder()
        assert result == {"age": {"$gte": 20.0, "$lte": 40.0}}

    def test_not_between(self):
        result = _dt({"criteria": [{"origData": "age", "condition": "!between", "type": "num", "value": ["20", "30"]}], "logic": "AND"}).filter
        assert result == {"$or": [{"age": {"$lt": 20}}, {"age": {"$gt": 30}}]}

    def test_not_between_date(self):
        result = _dt({"criteria": [{"origData": "created", "condition": "!between", "type": "date", "value": ["2024-01-01", "2024-12-31"]}], "logic": "AND"}).filter
        assert "$or" in result
        assert len(result["$or"]) == 2
        assert "$lt" in result["$or"][0].get("created", {})
        assert "$gte" in result["$or"][1].get("created", {})


# ---------------------------------------------------------------------------
# Null conditions
# ---------------------------------------------------------------------------

class TestSearchBuilderNullConditions:
    def test_null_string_type(self):
        result = _dt({"criteria": [{"condition": "null", "data": "manager", "origData": "manager", "type": "string", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"manager": {"$in": [None, ""]}}

    def test_not_null_string_type(self):
        result = _dt({"criteria": [{"condition": "!null", "data": "manager", "origData": "manager", "type": "string", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"manager": {"$nin": [None, ""]}}

    def test_null_num_type(self):
        result = _dt({"criteria": [{"condition": "null", "data": "score", "origData": "score", "type": "num", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"score": None}

    def test_not_null_num_type(self):
        result = _dt({"criteria": [{"condition": "!null", "data": "score", "origData": "score", "type": "num", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"score": {"$ne": None}}

    def test_null_date_type(self):
        result = _dt({"criteria": [{"condition": "null", "data": "created", "origData": "created", "type": "date", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"created": None}

    def test_not_null_date_type(self):
        result = _dt({"criteria": [{"condition": "!null", "data": "created", "origData": "created", "type": "date", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"created": {"$ne": None}}

    def test_null_html_num_type(self):
        result = _dt({"criteria": [{"condition": "null", "data": "price", "origData": "price", "type": "html-num", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"price": None}

    def test_null_html_type(self):
        result = _dt({"criteria": [{"condition": "null", "data": "bio", "origData": "bio", "type": "html", "value": []}], "logic": "AND"})._parse_search_builder()
        assert result == {"bio": {"$in": [None, ""]}}


# ---------------------------------------------------------------------------
# Logic (AND / OR / nested)
# ---------------------------------------------------------------------------

class TestSearchBuilderLogic:
    def test_and_logic_two_criteria(self):
        result = _dt({"criteria": [
            {"condition": "=", "origData": "dept", "type": "string", "value": ["Engineering"]},
            {"condition": ">", "origData": "age", "type": "num", "value": ["25"]}
        ], "logic": "AND"})._parse_search_builder()
        assert "$and" in result
        assert len(result["$and"]) == 2

    def test_or_logic_two_criteria(self):
        result = _dt({"criteria": [
            {"condition": "=", "origData": "city", "type": "string", "value": ["London"]},
            {"condition": "=", "origData": "city", "type": "string", "value": ["Paris"]}
        ], "logic": "OR"})._parse_search_builder()
        assert "$or" in result
        assert len(result["$or"]) == 2

    def test_single_criterion_no_wrapper(self):
        result = _dt({"criteria": [{"condition": "=", "origData": "name", "type": "string", "value": ["Bob"]}], "logic": "AND"})._parse_search_builder()
        assert "$and" not in result
        assert "name" in result

    def test_nested_group(self):
        result = _dt({
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
        })._parse_search_builder()
        assert "$and" in result
        parts = result["$and"]
        assert len(parts) == 2
        assert "$or" in parts[1]


# ---------------------------------------------------------------------------
# Filter integration
# ---------------------------------------------------------------------------

class TestSearchBuilderFilterIntegration:
    def test_search_builder_included_in_filter(self):
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [], "columns": [],
            "searchBuilder": {
                "criteria": [{"condition": "=", "origData": "status", "type": "string", "value": ["active"]}],
                "logic": "AND"
            }
        }
        dt = DataTables(MagicMock(**{"__getitem__": MagicMock(return_value=MagicMock(list_indexes=MagicMock(return_value=[])))}), "test", args)
        assert "status" in json.dumps(dt.filter)

    def test_search_builder_combined_with_custom_filter(self):
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
        assert any("tenant" in str(p) for p in parts)
        assert any("score" in str(p) for p in parts)

    def test_no_search_builder_no_impact_on_filter(self):
        mock_col = MagicMock()
        mock_col.list_indexes.return_value = []
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [], "columns": []
        }
        assert DataTables(mock_db, "test", args).filter == {}


# ---------------------------------------------------------------------------
# value2 support
# ---------------------------------------------------------------------------

class TestSbValue2:
    def test_number_between_value2(self):
        result = _sb_dt({"criteria": [_criterion("between", "age", "num", ["20"], value2="40")],
                         "logic": "AND"})._parse_search_builder()
        assert result == {"age": {"$gte": 20.0, "$lte": 40.0}}

    def test_number_not_between_value2(self):
        result = _sb_dt({"criteria": [_criterion("!between", "age", "num", ["20"], value2="30")],
                         "logic": "AND"}).filter
        assert result == {"$or": [{"age": {"$lt": 20}}, {"age": {"$gt": 30}}]}

    def test_date_between_value2(self):
        result = _sb_dt({"criteria": [_criterion("between", "created", "date",
                                                  ["2024-01-01"], value2="2024-01-31")],
                         "logic": "AND"})._parse_search_builder()
        assert result == {"created": {"$gte": datetime(2024, 1, 1), "$lt": datetime(2024, 2, 1)}}

    def test_date_not_between_value2(self):
        result = _sb_dt({"criteria": [_criterion("!between", "created", "date",
                                                  ["2024-01-01"], value2="2024-12-31")],
                         "logic": "AND"}).filter
        assert "$or" in result
        assert len(result["$or"]) == 2
        assert "$lt" in result["$or"][0].get("created", {})
        assert "$gte" in result["$or"][1].get("created", {})

    def test_between_value_array_still_works(self):
        result = _sb_dt({"criteria": [_criterion("between", "age", "num", ["20", "40"])],
                         "logic": "AND"})._parse_search_builder()
        assert result == {"age": {"$gte": 20.0, "$lte": 40.0}}

    def test_between_value2_takes_precedence(self):
        result = _sb_dt({"criteria": [_criterion("between", "price", "num",
                                                  ["100"], value2="200")],
                         "logic": "AND"})._parse_search_builder()
        assert result == {"price": {"$gte": 100.0, "$lte": 200.0}}


# ---------------------------------------------------------------------------
# Coverage gaps
# ---------------------------------------------------------------------------

class TestSearchBuilderCoverageGaps:
    """Targets previously uncovered branches in search_builder.py."""

    def test_json_string_payload_parsed(self):
        """searchBuilder delivered as a JSON string is decoded."""
        sb = json.dumps({
            "criteria": [{"condition": "=", "origData": "status", "type": "string", "value": ["active"]}],
            "logic": "AND",
        })
        assert _dt(sb)._parse_search_builder() == {"status": {"$regex": "^active$", "$options": "i"}}

    def test_invalid_json_string_returns_empty(self):
        assert _dt("not-valid-json")._parse_search_builder() == {}

    def test_non_dict_json_string_returns_empty(self):
        assert _dt(json.dumps(["a", "b"]))._parse_search_builder() == {}

    def test_empty_nested_group_skipped(self):
        sb = {
            "logic": "AND",
            "criteria": [
                {"logic": "OR", "criteria": [{"condition": "=", "origData": "", "type": "string", "value": ["x"]}]},
                {"condition": "=", "origData": "name", "type": "string", "value": ["Alice"]},
            ],
        }
        assert _dt(sb)._parse_search_builder() == {"name": {"$regex": "^Alice$", "$options": "i"}}

    def test_number_unknown_condition_returns_empty(self):
        assert _sb_number("age", "unknown", "30", None) == {}

    def test_date_not_equals(self):
        result = _sb_date("created_at", "!=", "2024-06-01", None)
        d = datetime(2024, 6, 1)
        assert result == {"$or": [
            {"created_at": {"$lt": d}},
            {"created_at": {"$gte": d + timedelta(days=1)}},
        ]}

    def test_date_unknown_condition_returns_empty(self):
        assert _sb_date("created_at", "unknown", "2024-06-01", None) == {}

    def test_string_none_value_returns_empty(self):
        assert _sb_string("name", "contains", None) == {}

    def test_string_unknown_condition_returns_empty(self):
        assert _sb_string("name", "unknown", "Alice") == {}


# ---------------------------------------------------------------------------
# _sb_string negative conditions: dict $not (not compiled regex)
# ---------------------------------------------------------------------------

NEGATIVE_CONDITIONS = [
    ("!=",       "foo",  r"^foo$"),
    ("!contains","foo",  r"foo"),
    ("!starts",  "foo",  r"^foo"),
    ("!ends",    "foo",  r"foo$"),
]


@pytest.mark.parametrize("condition,value,expected_pattern", NEGATIVE_CONDITIONS)
def test_negative_condition_uses_dict_not_compiled_regex(condition, value, expected_pattern):
    result = _sb_string("name", condition, value)
    not_val = result["name"]["$not"]
    assert isinstance(not_val, dict), f"$not must be a dict, got {type(not_val)}"
    assert not_val["$regex"] == expected_pattern
    assert not_val["$options"] == "i"


@pytest.mark.parametrize("condition,value,_", NEGATIVE_CONDITIONS)
def test_negative_condition_is_json_serializable(condition, value, _):
    import json as _json
    _json.dumps(_sb_string("name", condition, value))  # must not raise
