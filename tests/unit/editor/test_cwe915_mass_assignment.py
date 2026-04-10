"""Tests for CWE-915: Mass assignment — field whitelist enforcement in preprocess_document.

Verifies that when data_fields is empty, all client-supplied fields pass through
(documented opt-out), a warning is emitted, and that when a whitelist is configured
only allowed fields are written to MongoDB.
"""
import logging
import pytest
from unittest.mock import MagicMock

from mongo_datatables.editor.document import preprocess_document
from mongo_datatables.datatables import DataField
from mongo_datatables.utils import FieldMapper


def _call(doc, fields=None, data_fields=None):
    df = data_fields or []
    return preprocess_document(doc, fields or {}, df, FieldMapper(df))


class TestNoWhitelist:
    """When data_fields is empty, all fields pass through (opt-out mode)."""

    def test_arbitrary_fields_pass_through(self):
        doc = {"name": "Alice", "__proto__": "evil", "admin": True, "role": "superuser"}
        processed, _ = _call(doc)
        assert "__proto__" in processed
        assert processed["admin"] is True
        assert processed["role"] == "superuser"

    def test_warning_emitted_when_no_whitelist(self, caplog):
        with caplog.at_level(logging.WARNING, logger="mongo_datatables.editor.document"):
            _call({"name": "Alice"})
        assert any("whitelist" in r.message.lower() or "data_fields" in r.message for r in caplog.records)

    def test_no_warning_when_whitelist_configured(self, caplog):
        df = [DataField("name", "string")]
        with caplog.at_level(logging.WARNING, logger="mongo_datatables.editor.document"):
            _call({"name": "Alice"}, fields={f.alias: f for f in df}, data_fields=df)
        assert not any("whitelist" in r.message.lower() or "data_fields" in r.message for r in caplog.records)

    def test_internal_metadata_field_passes_through(self):
        doc = {"_internal": "secret", "name": "Bob"}
        processed, _ = _call(doc)
        assert "_internal" in processed

    def test_privilege_escalation_field_passes_through(self):
        doc = {"role": "admin", "is_superuser": True}
        processed, _ = _call(doc)
        assert processed["role"] == "admin"
        assert processed["is_superuser"] is True


class TestWithWhitelist:
    """When data_fields is configured, only whitelisted fields are written."""

    def _fields(self, *names):
        df = [DataField(n, "string") for n in names]
        return {f.alias: f for f in df}, df

    def test_whitelisted_field_passes(self):
        fields, df = self._fields("name")
        processed, _ = _call({"name": "Alice"}, fields=fields, data_fields=df)
        assert processed["name"] == "Alice"

    def test_non_whitelisted_field_blocked(self):
        fields, df = self._fields("name")
        processed, _ = _call({"name": "Alice", "role": "admin"}, fields=fields, data_fields=df)
        assert "role" not in processed

    def test_proto_pollution_field_blocked(self):
        fields, df = self._fields("name")
        processed, _ = _call({"name": "Alice", "__proto__": "evil"}, fields=fields, data_fields=df)
        assert "__proto__" not in processed

    def test_multiple_injected_fields_all_blocked(self):
        fields, df = self._fields("title")
        doc = {"title": "Post", "admin": True, "is_superuser": True, "_internal": "x"}
        processed, _ = _call(doc, fields=fields, data_fields=df)
        assert set(processed.keys()) == {"title"}

    def test_only_configured_fields_survive(self):
        fields, df = self._fields("email", "username")
        doc = {"email": "a@b.com", "username": "alice", "password_hash": "abc", "role": "admin"}
        processed, _ = _call(doc, fields=fields, data_fields=df)
        assert "password_hash" not in processed
        assert "role" not in processed
        assert processed["email"] == "a@b.com"
        assert processed["username"] == "alice"

    def test_dot_notation_injected_field_blocked(self):
        fields, df = self._fields("profile")
        doc = {"profile": "ok", "profile.role": "admin"}
        processed, dot = _call(doc, fields=fields, data_fields=df)
        # dot-notation key root "profile" is allowed, but "profile.role" root is "profile" — allowed
        # injected unrelated dot key should be blocked
        doc2 = {"profile": "ok", "admin.role": "superuser"}
        processed2, dot2 = _call(doc2, fields=fields, data_fields=df)
        assert "admin.role" not in dot2
        assert "admin.role" not in processed2
