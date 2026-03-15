"""Tests verifying _sb_string negative conditions return BSON-serializable pure dicts."""
import json
import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables


def make_dt():
    dt = DataTables.__new__(DataTables)
    return dt


@pytest.mark.parametrize("condition,field,value", [
    ("!=", "name", "Alice"),
    ("!contains", "name", "bob"),
    ("!starts", "name", "C"),
    ("!ends", "name", "son"),
])
def test_negative_condition_is_pure_dict(condition, field, value):
    dt = make_dt()
    result = dt._sb_string(field, condition, value)
    not_val = result[field]["$not"]
    assert isinstance(not_val, dict), f"$not value should be dict, got {type(not_val)}"
    assert "$regex" in not_val
    assert "$options" in not_val
    assert not_val["$options"] == "i"


@pytest.mark.parametrize("condition,field,value", [
    ("!=", "name", "Alice"),
    ("!contains", "name", "bob"),
    ("!starts", "name", "C"),
    ("!ends", "name", "son"),
])
def test_negative_condition_json_serializable(condition, field, value):
    dt = make_dt()
    result = dt._sb_string(field, condition, value)
    # Should not raise
    json.dumps(result)
