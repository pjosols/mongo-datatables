"""Consolidated SearchBuilder tests (merged from test_search_builder.py, test_sb_*.py, test_qb_exception_narrowing.py)."""
import json
import pytest
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables, DataField
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.exceptions import FieldMappingError


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


def _make_sb_date_dt():
    collection = MagicMock()
    collection.list_indexes.return_value = []
    mongo = MagicMock()
    mongo.__getitem__ = MagicMock(return_value=collection)
    return DataTables(mongo, "col", {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "created_at", "searchable": True, "orderable": True,
                     "search": {"value": "", "regex": False}}],
    })


def _make_qb_dt():
    col = MagicMock()
    col.database = MagicMock()
    return DataTables(col, 'test', {}, [DataField('price', 'number'), DataField('created', 'date')])


def _make_qb():
    fm = MagicMock()
    fm.get_field_type.return_value = "string"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm)


# ---------------------------------------------------------------------------
# test_search_builder.py — TestSearchBuilderEmpty/String/Number/NullConditions/Logic/FilterIntegration
# ---------------------------------------------------------------------------

class TestSearchBuilderEmpty:
    def test_no_search_builder_returns_empty(self):
        assert _dt(None)._parse_search_builder() == {}

    def test_empty_dict_returns_empty(self):
        assert _dt({})._parse_search_builder() == {}

    def test_empty_criteria_returns_empty(self):
        assert _dt({"criteria": [], "logic": "AND"})._parse_search_builder() == {}


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


class TestSearchBuilderFilterIntegration:
    def test_search_builder_included_in_filter(self):
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
        assert f
        assert "status" in json.dumps(f)

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
        dt = DataTables(mock_db, "test", args)
        assert dt.filter == {}


# ---------------------------------------------------------------------------
# test_sb_date_iso_datetime.py — TestSbDateIsoDatetime
# ---------------------------------------------------------------------------

class TestSbDateIsoDatetime(unittest.TestCase):
    def setUp(self):
        self.dt = _make_sb_date_dt()
        self.field = "created_at"

    def test_equal_iso_datetime_string(self):
        result = self.dt._sb_date(self.field, "=", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 1, 15), "$lt": datetime(2024, 1, 16)}})

    def test_greater_iso_datetime_string(self):
        result = self.dt._sb_date(self.field, ">", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$gt": datetime(2024, 1, 15)}})

    def test_less_iso_datetime_string(self):
        result = self.dt._sb_date(self.field, "<", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$lt": datetime(2024, 1, 15)}})

    def test_plain_date_string_unchanged(self):
        result = self.dt._sb_date(self.field, "=", "2024-01-15", None)
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 1, 15), "$lt": datetime(2024, 1, 16)}})


# ---------------------------------------------------------------------------
# test_sb_date_operators.py — TestSbDateOperators
# ---------------------------------------------------------------------------

class TestSbDateOperators(unittest.TestCase):
    def setUp(self):
        collection = MagicMock()
        collection.list_indexes.return_value = []
        mongo = MagicMock()
        mongo.__getitem__ = MagicMock(return_value=collection)
        self.dt = DataTables(mongo, "col", {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "", "regex": False},
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [{"data": "date_field", "searchable": True, "orderable": True,
                         "search": {"value": "", "regex": False}}],
        })
        self.field = "created_at"
        self.date_str = "2024-03-15"
        self.day_start = datetime(2024, 3, 15)
        self.next_day = datetime(2024, 3, 16)

    def test_sb_date_lte_returns_lt_next_day(self):
        result = self.dt._sb_date(self.field, "<=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$lt": self.next_day}})

    def test_sb_date_gte_returns_gte_day_start(self):
        result = self.dt._sb_date(self.field, ">=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gte": self.day_start}})

    def test_sb_date_lt_still_works(self):
        result = self.dt._sb_date(self.field, "<", self.date_str, None)
        self.assertEqual(result, {self.field: {"$lt": self.day_start}})

    def test_sb_date_gt_still_works(self):
        result = self.dt._sb_date(self.field, ">", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gt": self.day_start}})

    def test_sb_date_eq_still_works(self):
        result = self.dt._sb_date(self.field, "=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gte": self.day_start, "$lt": self.next_day}})

    def test_sb_date_between_still_works(self):
        result = self.dt._sb_date(self.field, "between", "2024-03-01", "2024-03-31")
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 3, 1), "$lt": datetime(2024, 4, 1)}})

    def test_sb_date_invalid_date_returns_empty(self):
        result = self.dt._sb_date(self.field, "<=", "not-a-date", None)
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# test_sb_exception_narrowing.py — TestSbNumberExceptionNarrowing, TestSbDateExceptionNarrowing
# ---------------------------------------------------------------------------

class TestSbNumberExceptionNarrowing:
    def test_invalid_number_returns_empty(self):
        assert _make_qb_dt()._sb_number('price', '=', 'not-a-number', None) == {}

    def test_invalid_number_between_returns_empty(self):
        assert _make_qb_dt()._sb_number('price', 'between', 'abc', 'xyz') == {}

    def test_valid_number_works(self):
        assert _make_qb_dt()._sb_number('price', '=', '42', None) == {'price': 42}

    def test_valid_number_gt_works(self):
        assert _make_qb_dt()._sb_number('price', '>', '10', None) == {'price': {'$gt': 10}}


class TestSbDateExceptionNarrowing:
    def test_invalid_date_returns_empty(self):
        assert _make_qb_dt()._sb_date('created', '=', 'not-a-date', None) == {}

    def test_invalid_date_between_returns_empty(self):
        assert _make_qb_dt()._sb_date('created', 'between', 'bad', 'also-bad') == {}

    def test_valid_date_works(self):
        result = _make_qb_dt()._sb_date('created', '=', '2024-01-15', None)
        assert '$gte' in result['created']
        assert '$lt' in result['created']

    def test_valid_date_gt_works(self):
        result = _make_qb_dt()._sb_date('created', '>', '2024-01-15', None)
        assert '$gt' in result['created']


# ---------------------------------------------------------------------------
# test_sb_string_not_bson.py — parametrized negative condition tests
# ---------------------------------------------------------------------------

NEGATIVE_CONDITIONS = [
    ("!=",       "foo",  r"^foo$"),
    ("!contains","foo",  r"foo"),
    ("!starts",  "foo",  r"^foo"),
    ("!ends",    "foo",  r"foo$"),
]


@pytest.mark.parametrize("condition,value,expected_pattern", NEGATIVE_CONDITIONS)
def test_negative_condition_uses_dict_not_compiled_regex(condition, value, expected_pattern):
    dt = DataTables.__new__(DataTables)
    result = dt._sb_string("name", condition, value)
    not_val = result["name"]["$not"]
    assert isinstance(not_val, dict), f"$not must be a dict, got {type(not_val)}"
    assert not_val["$regex"] == expected_pattern
    assert not_val["$options"] == "i"


@pytest.mark.parametrize("condition,value,_", NEGATIVE_CONDITIONS)
def test_negative_condition_is_json_serializable(condition, value, _):
    dt = DataTables.__new__(DataTables)
    result = dt._sb_string("name", condition, value)
    json.dumps(result)  # must not raise


# ---------------------------------------------------------------------------
# test_qb_exception_narrowing.py — TestBuildColumnSearchExceptions, etc.
# ---------------------------------------------------------------------------

class TestBuildColumnSearchExceptions:
    def test_invalid_number_returns_empty(self):
        qb = _make_qb()
        qb.field_mapper.get_field_type.return_value = "number"
        col = {"data": "price", "search": {"value": "notanumber", "regex": False}, "searchable": True}
        assert qb.build_column_search([col]) == {}

    def test_invalid_date_range_returns_empty(self):
        qb = _make_qb()
        qb.field_mapper.get_field_type.return_value = "date"
        col = {"data": "created", "search": {"value": "notadate|alsonotadate", "regex": False}, "searchable": True}
        assert qb.build_column_search([col]) == {}

    def test_unexpected_exception_propagates_in_number_column(self):
        qb = _make_qb()
        qb.field_mapper.get_field_type.return_value = "number"
        col = {"data": "price", "search": {"value": "5", "regex": False}, "searchable": True}
        with patch("mongo_datatables.query_builder.TypeConverter.to_number", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb.build_column_search([col])


class TestBuildNumberConditionExceptions:
    def test_invalid_value_returns_none(self):
        assert _make_qb()._build_number_condition("price", "notanumber", None) is None

    def test_unexpected_exception_propagates(self):
        qb = _make_qb()
        with patch("mongo_datatables.query_builder.TypeConverter.to_number", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb._build_number_condition("price", "5", None)


class TestBuildDateConditionExceptions:
    def test_invalid_date_returns_regex_fallback(self):
        result = _make_qb()._build_date_condition("created", "notadate", None)
        assert result is not None
        assert "created" in result

    def test_unexpected_exception_propagates(self):
        qb = _make_qb()
        with patch("mongo_datatables.query_builder.DateHandler.get_date_range_for_comparison", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb._build_date_condition("created", "2024-01-15", None)


class TestColumnControlExceptions:
    def test_invalid_num_stype_returns_empty(self):
        qb = _make_qb()
        cc = {"search": {"value": "notanumber", "logic": "equal", "type": "num"}}
        assert qb._build_column_control_condition("price", "number", cc) == []

    def test_invalid_date_stype_returns_empty(self):
        qb = _make_qb()
        cc = {"search": {"value": "notadate", "logic": "equal", "type": "date"}}
        assert qb._build_column_control_condition("created", "date", cc) == []


# ---------------------------------------------------------------------------
# SearchBuilder value2 fix (merged from test_sb_value2.py)
# ---------------------------------------------------------------------------

def _sb_dt(sb_payload):
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    return DataTables(mock_db, "test", {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": [],
        "searchBuilder": sb_payload,
    })


def _criterion(condition, field, type_, value, value2=None):
    c = {"condition": condition, "origData": field, "type": type_, "value": value}
    if value2 is not None:
        c["value2"] = value2
    return c


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
        from datetime import datetime
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
