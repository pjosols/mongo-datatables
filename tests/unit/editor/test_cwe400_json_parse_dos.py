"""Tests for CWE-400: unbounded JSON parse and missing recursion depth guard.

Verifies that preprocess_document() rejects JSON strings exceeding _MAX_JSON_PARSE_LEN
and that build_updates() raises InvalidDataError when nesting exceeds _MAX_DOC_NESTING.
"""
import json
import pytest
from unittest.mock import MagicMock

from mongo_datatables.datatables import DataField
from mongo_datatables.editor.document import (
    _MAX_JSON_PARSE_LEN,
    build_updates,
    preprocess_document,
)
from mongo_datatables.editor.validators.payload import _MAX_DOC_NESTING
from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.utils import FieldMapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fields(*names: str) -> tuple[dict, list]:
    df = [DataField(n, "string") for n in names]
    return {f.alias: f for f in df}, df


def _field_mapper(*names: str) -> FieldMapper:
    df = [DataField(n, "string") for n in names]
    return FieldMapper(df)


# ---------------------------------------------------------------------------
# preprocess_document — JSON parse size gate
# ---------------------------------------------------------------------------

class TestJsonParseSizeGate:
    """preprocess_document must not call json.loads on strings > _MAX_JSON_PARSE_LEN."""

    def test_json_string_at_limit_is_parsed(self):
        """JSON string exactly at the limit is parsed normally."""
        payload = json.dumps({"k": "v"})
        # pad to exactly _MAX_JSON_PARSE_LEN with a long string value
        long_val = "x" * (_MAX_JSON_PARSE_LEN - len('{"k": ""}') - 1)
        payload = json.dumps({"k": long_val})
        assert len(payload) <= _MAX_JSON_PARSE_LEN
        fields, df = _fields("data")
        processed, _ = preprocess_document({"data": payload}, fields, df)
        # parsed as dict
        assert isinstance(processed["data"], dict)

    def test_json_string_over_limit_is_stored_raw(self):
        """JSON string exceeding _MAX_JSON_PARSE_LEN is stored as-is, not parsed."""
        # Build a valid JSON string that is just over the limit
        inner = "x" * (_MAX_JSON_PARSE_LEN + 1)
        payload = json.dumps(inner)  # a JSON string value, valid JSON but > limit
        assert len(payload) > _MAX_JSON_PARSE_LEN
        fields, df = _fields("data")
        processed, _ = preprocess_document({"data": payload}, fields, df)
        # Must remain a str — not parsed into a Python object
        assert isinstance(processed["data"], str)

    def test_deeply_nested_json_over_limit_is_not_parsed(self):
        """A deeply-nested JSON object exceeding the size limit is stored raw."""
        # Build a wide dict with many keys so serialised size exceeds 64 KB
        obj = {f"key_{i}": "x" * 100 for i in range(700)}
        payload = json.dumps(obj)
        assert len(payload) > _MAX_JSON_PARSE_LEN
        fields, df = _fields("nested")
        processed, _ = preprocess_document({"nested": payload}, fields, df)
        assert isinstance(processed["nested"], str)

    def test_small_json_string_is_still_parsed(self):
        """Small JSON strings well under the limit continue to be parsed."""
        fields, df = _fields("tags")
        processed, _ = preprocess_document({"tags": '["a","b"]'}, fields, df)
        assert processed["tags"] == ["a", "b"]

    def test_max_json_parse_len_constant_is_64kb(self):
        """_MAX_JSON_PARSE_LEN must be 64 * 1024 bytes."""
        assert _MAX_JSON_PARSE_LEN == 64 * 1024


# ---------------------------------------------------------------------------
# build_updates — recursion depth guard
# ---------------------------------------------------------------------------

class TestBuildUpdatesDepthGuard:
    """build_updates must raise InvalidDataError when nesting exceeds _MAX_DOC_NESTING."""

    def _run(self, data: dict, *field_names: str) -> dict:
        fields, df = _fields(*field_names)
        fm = _field_mapper(*field_names)
        updates: dict = {}
        build_updates(data, fm, fields, df, updates)
        return updates

    def _nested_dict(self, depth: int) -> dict:
        """Build a dict nested `depth` levels deep."""
        root: dict = {}
        cur = root
        for i in range(depth):
            cur[f"l{i}"] = {}
            cur = cur[f"l{i}"]
        cur["leaf"] = "value"
        return root

    def test_flat_dict_processes_normally(self):
        """Flat dict with no nesting processes without error."""
        updates = self._run({"name": "Alice"}, "name")
        assert updates["name"] == "Alice"

    def test_nesting_at_limit_is_allowed(self):
        """Dict nested exactly at _MAX_DOC_NESTING is allowed."""
        data = self._nested_dict(_MAX_DOC_NESTING)
        # Should not raise
        updates: dict = {}
        fields, df = _fields("l0")
        fm = _field_mapper("l0")
        build_updates(data, fm, fields, df, updates)

    def test_nesting_one_over_limit_raises(self):
        """Dict nested one level beyond _MAX_DOC_NESTING raises InvalidDataError."""
        data = self._nested_dict(_MAX_DOC_NESTING + 1)
        fields, df = _fields("l0")
        fm = _field_mapper("l0")
        updates: dict = {}
        with pytest.raises(InvalidDataError, match="nesting exceeds"):
            build_updates(data, fm, fields, df, updates)

    def test_deeply_nested_dict_raises(self):
        """Deeply nested dict (50 levels) raises InvalidDataError."""
        data = self._nested_dict(50)
        fields, df = _fields("l0")
        fm = _field_mapper("l0")
        updates: dict = {}
        with pytest.raises(InvalidDataError, match="nesting exceeds"):
            build_updates(data, fm, fields, df, updates)

    def test_non_dict_data_is_ignored(self):
        """Non-dict input to build_updates returns without error."""
        fields, df = _fields("x")
        fm = _field_mapper("x")
        updates: dict = {}
        build_updates("not a dict", fm, fields, df, updates)
        assert updates == {}

    def test_max_doc_nesting_constant_value(self):
        """_MAX_DOC_NESTING must be 10."""
        assert _MAX_DOC_NESTING == 10
