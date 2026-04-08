"""ColumnControl logic tests: text, num, date, list, edge cases."""
import json
import re
import unittest
from unittest.mock import MagicMock

import pytest

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


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


def _builder():
    fm = FieldMapper([])
    return MongoQueryBuilder(fm)


def _cc(logic, value):
    return {"search": {"value": value, "logic": logic, "type": "text"}}


def _cond(logic, value):
    return _builder()._build_column_control_condition("name", "text", _cc(logic, value))


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


class TestColumnControlAndColumnSearch:
    def test_cc_anded_with_column_search(self):
        cols = [_col("name", {"search": {"value": "bar", "logic": "starts", "type": "text"}}, search_value="foo")]
        r = _build(cols, [DataField("name", "string")])
        assert len(r["$and"]) == 2


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


if __name__ == "__main__":
    unittest.main()
