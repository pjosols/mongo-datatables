"""Tests for search_terms property caching."""
import pytest
from unittest.mock import patch, MagicMock
from mongo_datatables import DataTables


def make_dt(search_value=""):
    col = MagicMock()
    col.find.return_value = []
    col.count_documents.return_value = 0
    args = {"draw": "1", "start": "0", "length": "10",
            "search": {"value": search_value, "regex": "false"},
            "columns": [], "order": []}
    return DataTables(col, "test", args)


def test_search_terms_cache_initialized_none():
    dt = make_dt("hello world")
    assert dt._search_terms_cache is None


def test_search_terms_cache_populated_on_first_access():
    dt = make_dt("hello world")
    _ = dt.search_terms
    assert dt._search_terms_cache is not None


def test_search_terms_parse_called_once():
    dt = make_dt("foo bar")
    with patch("mongo_datatables.datatables.SearchTermParser.parse",
               wraps=lambda v: v.split()) as mock_parse:
        _ = dt.search_terms
        _ = dt.search_terms
        _ = dt.search_terms
    mock_parse.assert_called_once()


def test_search_terms_cache_returns_same_object():
    dt = make_dt("alpha beta")
    first = dt.search_terms
    second = dt.search_terms
    assert first is second


def test_search_terms_empty_string_cached():
    dt = make_dt("")
    result = dt.search_terms
    # Empty string parses to empty list; cache should hold it
    assert result == []
    assert dt._search_terms_cache == []
