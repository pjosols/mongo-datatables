"""Tests for CWE-400: query DoS prevention in handle_search() — bounded search term and $in array."""
import pytest
from unittest.mock import MagicMock
from pymongo.collection import Collection

from mongo_datatables.editor.search import handle_search, MAX_SEARCH_VALUES, MAX_SEARCH_TERM_LEN
from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.utils import FieldMapper


def _col(docs=None):
    col = MagicMock(spec=Collection)
    col.find.return_value.limit.return_value = docs or []
    return col


def _mapper():
    return FieldMapper({})


def test_values_capped_at_max():
    """$in array must not exceed MAX_SEARCH_VALUES entries."""
    col = _col()
    oversized = [f"val{i}" for i in range(MAX_SEARCH_VALUES + 50)]
    handle_search({"field": "tag", "values": oversized}, col, _mapper(), {})
    call_query = col.find.call_args[0][0]
    assert len(call_query["tag"]["$in"]) <= MAX_SEARCH_VALUES


def test_values_exactly_at_limit_passes_through():
    """Exactly MAX_SEARCH_VALUES entries should all be included."""
    col = _col()
    values = [f"v{i}" for i in range(MAX_SEARCH_VALUES)]
    handle_search({"field": "tag", "values": values}, col, _mapper(), {})
    call_query = col.find.call_args[0][0]
    assert len(call_query["tag"]["$in"]) == MAX_SEARCH_VALUES


def test_non_scalar_dict_values_filtered_out():
    """Nested dicts must be stripped from the $in list."""
    col = _col()
    handle_search(
        {"field": "tag", "values": ["ok", {"$gt": ""}, "also_ok"]},
        col, _mapper(), {},
    )
    call_query = col.find.call_args[0][0]
    assert {"$gt": ""} not in call_query["tag"]["$in"]
    assert call_query["tag"]["$in"] == ["ok", "also_ok"]


def test_non_scalar_list_values_filtered_out():
    """Nested lists must be stripped from the $in list."""
    col = _col()
    handle_search(
        {"field": "tag", "values": ["good", ["nested"], 42]},
        col, _mapper(), {},
    )
    call_query = col.find.call_args[0][0]
    assert ["nested"] not in call_query["tag"]["$in"]
    assert call_query["tag"]["$in"] == ["good", 42]


def test_scalar_types_str_int_float_bool_accepted():
    """str, int, float, and bool values must all pass through."""
    col = _col()
    handle_search(
        {"field": "f", "values": ["s", 1, 3.14, True]},
        col, _mapper(), {},
    )
    call_query = col.find.call_args[0][0]
    assert call_query["f"]["$in"] == ["s", 1, 3.14, True]


def test_all_non_scalar_values_returns_empty_in():
    """If every value is non-scalar, $in should be empty (no query DoS)."""
    col = _col()
    handle_search(
        {"field": "tag", "values": [{"a": 1}, [1, 2], None]},
        col, _mapper(), {},
    )
    call_query = col.find.call_args[0][0]
    assert call_query["tag"]["$in"] == []


def test_oversized_list_truncated_before_scalar_filter():
    """Truncation happens before scalar filtering; combined cap is MAX_SEARCH_VALUES."""
    col = _col()
    # 200 dicts — all non-scalar, but truncation should still apply
    values = [{"x": i} for i in range(MAX_SEARCH_VALUES + 100)]
    handle_search({"field": "tag", "values": values}, col, _mapper(), {})
    call_query = col.find.call_args[0][0]
    assert call_query["tag"]["$in"] == []


def test_empty_values_list_returns_empty_data():
    """Empty values list should short-circuit without querying."""
    col = _col()
    result = handle_search({"field": "tag", "values": []}, col, _mapper(), {})
    col.find.assert_not_called()
    assert result == {"data": []}


def test_max_search_values_constant_is_reasonable():
    """MAX_SEARCH_VALUES must be a positive integer no greater than 1000."""
    assert isinstance(MAX_SEARCH_VALUES, int)
    assert 1 <= MAX_SEARCH_VALUES <= 1000


# --- search term length cap (CWE-400 regex DoS) ---

def test_search_term_at_max_length_is_accepted():
    """A search term exactly at MAX_SEARCH_TERM_LEN must not raise."""
    col = _col()
    term = "a" * MAX_SEARCH_TERM_LEN
    handle_search({"field": "name", "search": term}, col, _mapper(), {})
    col.find.assert_called_once()


def test_search_term_over_max_length_raises():
    """A search term one character over the limit must raise InvalidDataError."""
    col = _col()
    term = "a" * (MAX_SEARCH_TERM_LEN + 1)
    with pytest.raises(InvalidDataError, match="search term exceeds maximum length"):
        handle_search({"field": "name", "search": term}, col, _mapper(), {})
    col.find.assert_not_called()


def test_search_term_large_raises_without_querying():
    """A 1 MB search term must raise before any MongoDB query."""
    col = _col()
    term = "x" * 1_000_000
    with pytest.raises(InvalidDataError):
        handle_search({"field": "name", "search": term}, col, _mapper(), {})
    col.find.assert_not_called()


def test_search_term_none_skips_length_check():
    """None search term must not trigger the length check."""
    col = _col()
    result = handle_search({"field": "name"}, col, _mapper(), {})
    assert result == {"data": []}


def test_search_term_empty_string_is_accepted():
    """Empty string is within the limit and must not raise."""
    col = _col()
    handle_search({"field": "name", "search": ""}, col, _mapper(), {})
    col.find.assert_called_once()


def test_max_search_term_len_constant_is_reasonable():
    """MAX_SEARCH_TERM_LEN must be a positive integer."""
    assert isinstance(MAX_SEARCH_TERM_LEN, int)
    assert MAX_SEARCH_TERM_LEN > 0
