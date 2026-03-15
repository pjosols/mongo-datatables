import json
import pytest
from mongo_datatables import DataTables

NEGATIVE_CONDITIONS = [
    ("!=",       "foo",  r"^foo$"),
    ("!contains","foo",  r"foo"),
    ("!starts",  "foo",  r"^foo"),
    ("!ends",    "foo",  r"foo$"),
]

@pytest.mark.parametrize("condition,value,expected_pattern", NEGATIVE_CONDITIONS)
def test_negative_condition_uses_dict_not_compiled_regex(condition, value, expected_pattern):
    """$not conditions must use pure dict form, not compiled regex, for BSON-serializability."""
    dt = DataTables.__new__(DataTables)
    result = dt._sb_string("name", condition, value)
    not_val = result["name"]["$not"]
    assert isinstance(not_val, dict), f"$not must be a dict, got {type(not_val)}"
    assert not_val["$regex"] == expected_pattern
    assert not_val["$options"] == "i"

@pytest.mark.parametrize("condition,value,_", NEGATIVE_CONDITIONS)
def test_negative_condition_is_json_serializable(condition, value, _):
    """$not conditions must be JSON/BSON-serializable (no re.Pattern objects)."""
    dt = DataTables.__new__(DataTables)
    result = dt._sb_string("name", condition, value)
    json.dumps(result)  # must not raise
