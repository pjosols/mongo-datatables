import re
import pytest
from mongo_datatables import DataTables

NEGATIVE_CONDITIONS = [
    ("!=",       "foo",  re.compile(r"^foo$",  re.IGNORECASE)),
    ("!contains","foo",  re.compile(r"foo",    re.IGNORECASE)),
    ("!starts",  "foo",  re.compile(r"^foo",   re.IGNORECASE)),
    ("!ends",    "foo",  re.compile(r"foo$",   re.IGNORECASE)),
]

@pytest.mark.parametrize("condition,value,expected_regex", NEGATIVE_CONDITIONS)
def test_negative_condition_uses_compiled_regex(condition, value, expected_regex):
    """$not conditions must use compiled regex (not plain dict) for valid BSON."""
    dt = DataTables.__new__(DataTables)
    result = dt._sb_string("name", condition, value)
    not_val = result["name"]["$not"]
    assert isinstance(not_val, re.Pattern), (
        f"$not value must be a compiled regex, got {type(not_val)}"
    )
    assert not_val.pattern == expected_regex.pattern
    assert not_val.flags & re.IGNORECASE

@pytest.mark.parametrize("condition,value,_", NEGATIVE_CONDITIONS)
def test_negative_condition_no_options_key(condition, value, _):
    """$not value must not be a plain dict with $options (invalid BSON)."""
    dt = DataTables.__new__(DataTables)
    result = dt._sb_string("name", condition, value)
    not_val = result["name"]["$not"]
    assert not isinstance(not_val, dict), (
        f"$not must not be a plain dict — that produces invalid BSON"
    )
