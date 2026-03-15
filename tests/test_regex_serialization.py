"""Tests for bson.Regex serialization in _format_result_values."""
import json
import pytest
from unittest.mock import MagicMock
from bson import Regex
from mongo_datatables import DataTables


def _make_dt():
    col = MagicMock()
    col.list_indexes.return_value = []
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=col)
    return DataTables(db, "test", {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [], "columns": [],
    })


class TestRegexSerialization:
    def test_regex_with_flags_serialized(self):
        dt = _make_dt()
        doc = {"pattern": Regex("foo.*bar", "i")}
        dt._format_result_values(doc)
        assert doc["pattern"] == "/foo.*bar/i"

    def test_regex_no_flags_serialized(self):
        dt = _make_dt()
        doc = {"pattern": Regex("simple")}
        dt._format_result_values(doc)
        assert doc["pattern"] == "/simple/"

    def test_regex_multiple_flags(self):
        dt = _make_dt()
        doc = {"pattern": Regex("test", "im")}
        dt._format_result_values(doc)
        result = doc["pattern"]
        assert result.startswith("/test/")
        assert "i" in result
        assert "m" in result

    def test_regex_in_list(self):
        dt = _make_dt()
        doc = {"patterns": [Regex("alpha", "i"), Regex("beta")]}
        dt._format_result_values(doc)
        assert doc["patterns"][0] == "/alpha/i"
        assert doc["patterns"][1] == "/beta/"

    def test_regex_json_serializable(self):
        dt = _make_dt()
        doc = {"pattern": Regex("test", "i")}
        dt._format_result_values(doc)
        # Must not raise
        result = json.dumps(doc)
        assert "/test/i" in result

    def test_non_regex_fields_unaffected(self):
        dt = _make_dt()
        doc = {"name": "Alice", "age": 30, "pattern": Regex("x")}
        dt._format_result_values(doc)
        assert doc["name"] == "Alice"
        assert doc["age"] == 30
        assert doc["pattern"] == "/x/"

    def test_regex_in_list_json_serializable(self):
        dt = _make_dt()
        doc = {"patterns": [Regex("a", "i"), "plain_string"]}
        dt._format_result_values(doc)
        result = json.dumps(doc)
        assert "/a/i" in result
        assert "plain_string" in result
