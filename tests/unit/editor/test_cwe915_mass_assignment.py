"""Tests for CWE-915 / CWE-20: Field whitelist enforcement in preprocess_document.

Verifies that:
- Missing data_fields whitelist raises InvalidDataError (no unrestricted writes).
- _id and __v are blocked even when whitelisted fields are configured.
- Only whitelisted fields are written when a whitelist is configured.
"""
import pytest

from mongo_datatables.editor.document import preprocess_document
from mongo_datatables.datatables import DataField
from mongo_datatables.exceptions import InvalidDataError


def _call(doc, fields=None, data_fields=None):
    df = data_fields or []
    return preprocess_document(doc, fields or {}, df)


def _wl(*names):
    df = [DataField(n, "string") for n in names]
    return {f.alias: f for f in df}, df


class TestNoWhitelist:
    """When data_fields is empty, preprocess_document must raise InvalidDataError."""

    def test_raises_when_no_whitelist(self):
        """Raise InvalidDataError when no whitelist is configured."""
        with pytest.raises(InvalidDataError, match="whitelist"):
            _call({"name": "Alice"})

    def test_raises_with_empty_fields_and_data_fields(self):
        """Raise InvalidDataError when fields and data_fields are both empty."""
        with pytest.raises(InvalidDataError):
            _call({"role": "admin"}, fields={}, data_fields=[])

    def test_raises_even_for_empty_doc(self):
        """Raise InvalidDataError even for empty document."""
        with pytest.raises(InvalidDataError):
            _call({})

    def test_raises_with_operator_injection_attempt(self):
        """Raise InvalidDataError when document contains MongoDB operators."""
        with pytest.raises(InvalidDataError):
            _call({"$where": "1==1", "name": "Alice"})


class TestProtectedFields:
    """_id and __v must be blocked even when a whitelist is configured."""

    def test_id_field_blocked(self):
        """Block _id field from being written."""
        fields, df = _wl("name")
        processed, _ = _call({"name": "Alice", "_id": "overwrite"}, fields=fields, data_fields=df)
        assert "_id" not in processed

    def test_dunder_v_field_blocked(self):
        """Block __v field from being written."""
        fields, df = _wl("name")
        processed, _ = _call({"name": "Alice", "__v": 99}, fields=fields, data_fields=df)
        assert "__v" not in processed

    def test_id_blocked_even_if_explicitly_in_whitelist(self):
        """Block _id even when explicitly in whitelist."""
        # _id should never be writable regardless of whitelist
        df = [DataField("_id", "string"), DataField("name", "string")]
        fields = {f.alias: f for f in df}
        processed, _ = preprocess_document({"_id": "hack", "name": "Bob"}, fields, df)
        assert "_id" not in processed


class TestWithWhitelist:
    """When data_fields is configured, only whitelisted fields are written."""

    def test_whitelisted_field_passes(self):
        """Allow whitelisted field to be written."""
        fields, df = _wl("name")
        processed, _ = _call({"name": "Alice"}, fields=fields, data_fields=df)
        assert processed["name"] == "Alice"

    def test_non_whitelisted_field_blocked(self):
        """Block non-whitelisted field from being written."""
        fields, df = _wl("name")
        processed, _ = _call({"name": "Alice", "role": "admin"}, fields=fields, data_fields=df)
        assert "role" not in processed

    def test_multiple_injected_fields_all_blocked(self):
        """Block all non-whitelisted fields when multiple are injected."""
        fields, df = _wl("title")
        doc = {"title": "Post", "admin": True, "is_superuser": True, "_internal": "x"}
        processed, _ = _call(doc, fields=fields, data_fields=df)
        assert set(processed.keys()) == {"title"}

    def test_only_configured_fields_survive(self):
        """Only configured fields survive; injected fields are blocked."""
        fields, df = _wl("email", "username")
        doc = {"email": "a@b.com", "username": "alice", "password_hash": "abc", "role": "admin"}
        processed, _ = _call(doc, fields=fields, data_fields=df)
        assert "password_hash" not in processed
        assert "role" not in processed
        assert processed["email"] == "a@b.com"
        assert processed["username"] == "alice"

    def test_unrelated_dot_notation_key_blocked(self):
        """Block unrelated dot-notation keys from being written."""
        fields, df = _wl("profile")
        doc = {"profile": "ok", "admin.role": "superuser"}
        processed, dot = _call(doc, fields=fields, data_fields=df)
        assert "admin.role" not in dot
        assert "admin.role" not in processed
