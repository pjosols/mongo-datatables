"""Verify CWE-915 / CWE-20: Field whitelist enforcement.

Confirms missing data_fields whitelist raises InvalidDataError; _id and __v
are blocked even when whitelisted; only whitelisted fields are written.
"""
import pytest
from unittest.mock import MagicMock
from bson.objectid import ObjectId

from mongo_datatables.editor.document import preprocess_document
from mongo_datatables.editor.crud import run_create, run_edit
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


def _wl(*names):
    df = [DataField(n, "string") for n in names]
    return {f.alias: f for f in df}, df


def _noop_hook(*_):
    return True


def _make_collection(doc):
    col = MagicMock()
    inserted = MagicMock()
    inserted.inserted_id = doc["_id"]
    col.insert_one.return_value = inserted
    col.find_one.return_value = doc
    col.update_one.return_value = MagicMock()
    return col


class TestRunCreateWhitelist:
    """run_create must enforce the whitelist when called directly."""

    def test_raises_for_non_whitelisted_field(self):
        """run_create raises InvalidDataError when data contains a non-whitelisted field."""
        fields, df = _wl("name")
        col = MagicMock()
        with pytest.raises(InvalidDataError):
            run_create(
                data={"0": {"name": "Alice", "role": "admin"}},
                collection=col,
                fields=fields,
                data_fields=df,
                file_fields=[],
                storage_adapter=None,
                row_class=None,
                row_data=None,
                row_attr=None,
                pre_hook=_noop_hook,
            )

    def test_allows_whitelisted_fields(self):
        """run_create succeeds when data contains only whitelisted fields."""
        fields, df = _wl("name")
        doc = {"_id": ObjectId(), "name": "Alice"}
        col = _make_collection(doc)
        result = run_create(
            data={"0": {"name": "Alice"}},
            collection=col,
            fields=fields,
            data_fields=df,
            file_fields=[],
            storage_adapter=None,
            row_class=None,
            row_data=None,
            row_attr=None,
            pre_hook=_noop_hook,
        )
        assert "data" in result

    def test_raises_without_whitelist_configured(self):
        """run_create raises InvalidDataError when no whitelist is configured."""
        col = MagicMock()
        with pytest.raises(InvalidDataError):
            run_create(
                data={"0": {"name": "Alice", "role": "admin"}},
                collection=col,
                fields={},
                data_fields=[DataField("name", "string")],
                file_fields=[],
                storage_adapter=None,
                row_class=None,
                row_data=None,
                row_attr=None,
                pre_hook=_noop_hook,
            )

    def test_no_db_write_on_whitelist_violation(self):
        """run_create must not write to the database when whitelist is violated."""
        fields, df = _wl("name")
        col = MagicMock()
        with pytest.raises(InvalidDataError):
            run_create(
                data={"0": {"name": "Alice", "is_admin": True}},
                collection=col,
                fields=fields,
                data_fields=df,
                file_fields=[],
                storage_adapter=None,
                row_class=None,
                row_data=None,
                row_attr=None,
                pre_hook=_noop_hook,
            )
        col.insert_one.assert_not_called()


class TestRunEditWhitelist:
    """run_edit must enforce the whitelist when called directly."""

    def test_raises_for_non_whitelisted_field(self):
        """run_edit raises InvalidDataError when data contains a non-whitelisted field."""
        fields, df = _wl("name")
        doc_id = str(ObjectId())
        col = MagicMock()
        with pytest.raises(InvalidDataError):
            run_edit(
                list_of_ids=[doc_id],
                data={doc_id: {"name": "Alice", "role": "admin"}},
                collection=col,
                fields=fields,
                data_fields=df,
                file_fields=[],
                storage_adapter=None,
                row_class=None,
                row_data=None,
                row_attr=None,
                pre_hook=_noop_hook,
            )

    def test_allows_whitelisted_fields(self):
        """run_edit succeeds when data contains only whitelisted fields."""
        fields, df = _wl("name")
        doc_id = str(ObjectId())
        doc = {"_id": ObjectId(doc_id), "name": "Alice"}
        col = _make_collection(doc)
        result = run_edit(
            list_of_ids=[doc_id],
            data={doc_id: {"name": "Alice"}},
            collection=col,
            fields=fields,
            data_fields=df,
            file_fields=[],
            storage_adapter=None,
            row_class=None,
            row_data=None,
            row_attr=None,
            pre_hook=_noop_hook,
        )
        assert "data" in result

    def test_no_db_write_on_whitelist_violation(self):
        """run_edit must not write to the database when whitelist is violated."""
        fields, df = _wl("name")
        doc_id = str(ObjectId())
        col = MagicMock()
        with pytest.raises(InvalidDataError):
            run_edit(
                list_of_ids=[doc_id],
                data={doc_id: {"name": "Alice", "password_hash": "x"}},
                collection=col,
                fields=fields,
                data_fields=df,
                file_fields=[],
                storage_adapter=None,
                row_class=None,
                row_data=None,
                row_attr=None,
                pre_hook=_noop_hook,
            )
        col.update_one.assert_not_called()

    def test_raises_for_multiple_ids_with_violation(self):
        """run_edit raises InvalidDataError when any row violates the whitelist."""
        fields, df = _wl("name")
        id1 = str(ObjectId())
        id2 = str(ObjectId())
        col = MagicMock()
        with pytest.raises(InvalidDataError):
            run_edit(
                list_of_ids=[id1, id2],
                data={
                    id1: {"name": "Alice"},
                    id2: {"name": "Bob", "role": "superuser"},
                },
                collection=col,
                fields=fields,
                data_fields=df,
                file_fields=[],
                storage_adapter=None,
                row_class=None,
                row_data=None,
                row_attr=None,
                pre_hook=_noop_hook,
            )
