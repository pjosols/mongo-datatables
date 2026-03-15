"""Tests that _build_column_control_condition produces BSON-serializable dicts.

The bug: notContains and notEqual used re.compile() inside $not, which is not
JSON/BSON serializable when passed through non-PyMongo paths.

The fix: use {"$not": {"$regex": ..., "$options": "i"}} (pure dict form).
"""

import json
import re
import pytest
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


def _builder():
    fm = FieldMapper([])
    return MongoQueryBuilder(fm)


def _cc(logic, value):
    return {"search": {"value": value, "logic": logic, "type": "text"}}


def _cond(logic, value):
    return _builder()._build_column_control_condition("name", "text", _cc(logic, value))


# --- notContains: must be pure dict, no re.Pattern ---

def test_not_contains_no_compiled_regex():
    result = _cond("notContains", "foo")
    not_val = result[0]["name"]["$not"]
    assert not isinstance(not_val, re.Pattern), "$not must not be a compiled regex"
    assert isinstance(not_val, dict), "$not must be a plain dict"


def test_not_contains_bson_serializable():
    result = _cond("notContains", "foo")
    json.dumps(result)  # raises if not serializable


def test_not_contains_correct_pattern():
    result = _cond("notContains", "foo")
    not_val = result[0]["name"]["$not"]
    assert not_val == {"$regex": re.escape("foo"), "$options": "i"}


# --- notEqual: must be pure dict, no re.Pattern ---

def test_not_equal_no_compiled_regex():
    result = _cond("notEqual", "bar")
    not_val = result[0]["name"]["$not"]
    assert not isinstance(not_val, re.Pattern), "$not must not be a compiled regex"
    assert isinstance(not_val, dict), "$not must be a plain dict"


def test_not_equal_bson_serializable():
    result = _cond("notEqual", "bar")
    json.dumps(result)  # raises if not serializable


def test_not_equal_correct_pattern():
    result = _cond("notEqual", "bar")
    not_val = result[0]["name"]["$not"]
    assert not_val == {"$regex": f"^{re.escape('bar')}$", "$options": "i"}


# --- Regression: positive logics still produce plain $regex dicts ---

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
    json.dumps(result)  # must be serializable
