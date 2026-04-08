"""Tests verifying that error messages returned to clients do not disclose
sensitive internal information (DB schema, connection strings, stack traces,
raw exception messages).
"""
import logging
import pytest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    PyMongoError,
    OperationFailure,
    ConnectionFailure,
    ServerSelectionTimeoutError,
)

from mongo_datatables import DataTables, DataField
from mongo_datatables.editor.core import Editor
from mongo_datatables.exceptions import DatabaseOperationError, InvalidDataError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SENSITIVE_PATTERNS = [
    "mongodb://",
    "password",
    "secret",
    "traceback",
    "Traceback",
    "File \"",
    "line ",
    "collection",
    "index not found",
    "connection refused",
    "authentication failed",
    "ServerSelectionTimeout",
    "OperationFailure",
    "PyMongoError",
]


def _contains_sensitive(text: str) -> bool:
    """Return True if text contains any known sensitive pattern."""
    lower = text.lower()
    for pat in _SENSITIVE_PATTERNS:
        if pat.lower() in lower:
            return True
    return False


def _base_request_args() -> dict:
    return {
        "draw": "1",
        "start": "0",
        "length": "10",
        "search": {"value": "", "regex": False},
        "columns": [],
        "order": [],
    }


def _make_datatables(side_effect=None) -> DataTables:
    mongo = MagicMock()
    col = MagicMock(spec=Collection)
    col.list_indexes.return_value = iter([])
    col.estimated_document_count.return_value = 0
    col.count_documents.return_value = 0
    col.aggregate.return_value = iter([])
    if side_effect:
        col.aggregate.side_effect = side_effect
        col.count_documents.side_effect = side_effect
    mongo.__getitem__ = MagicMock(return_value=col)
    dt = DataTables(mongo, "users", _base_request_args())
    dt._collection = col
    return dt


def _make_editor(action: str = "create") -> tuple:
    mongo = MagicMock()
    col = MagicMock(spec=Collection)
    mongo.db = MagicMock(spec=Database)
    mongo.db.__getitem__ = MagicMock(return_value=col)
    request_args = {"action": action, "data": {"0": {"name": "Alice"}}}
    editor = Editor(mongo, "users", request_args)
    editor._collection = col
    return editor, col


# ---------------------------------------------------------------------------
# DataTables.get_rows — error field must not leak raw exception details
# ---------------------------------------------------------------------------

class TestDataTablesErrorDisclosure:
    def test_connection_failure_error_is_generic(self):
        """ConnectionFailure message must not appear verbatim in client response."""
        dt = _make_datatables()
        with patch.object(dt, "results", side_effect=ConnectionFailure(
            "mongodb://admin:secret@db.internal:27017 connection refused"
        )):
            response = dt.get_rows()
        assert "error" in response
        assert "mongodb://" not in response["error"]
        assert "secret" not in response["error"]
        assert "db.internal" not in response["error"]

    def test_operation_failure_error_is_generic(self):
        """OperationFailure with schema details must not leak to client."""
        dt = _make_datatables()
        with patch.object(dt, "count_total", side_effect=OperationFailure(
            "index not found on collection users, field password_hash"
        )):
            response = dt.get_rows()
        assert "error" in response
        assert "password_hash" not in response["error"]
        assert "index not found" not in response["error"]

    def test_server_selection_timeout_error_is_generic(self):
        """ServerSelectionTimeoutError must not expose host/port in response."""
        dt = _make_datatables()
        with patch.object(dt, "results", side_effect=ServerSelectionTimeoutError(
            "No servers found yet, serverSelectionTimeoutMS=30000, "
            "Timeout connecting to ['db.internal:27017']"
        )):
            response = dt.get_rows()
        assert "error" in response
        assert "db.internal" not in response["error"]
        assert "27017" not in response["error"]

    def test_runtime_error_message_is_sanitized(self):
        """RuntimeError with internal path info must not leak to client."""
        dt = _make_datatables()
        with patch.object(dt, "results", side_effect=RuntimeError(
            "pipeline failed at /app/mongo_datatables/datatables/results.py line 42"
        )):
            response = dt.get_rows()
        assert "error" in response
        # The raw message with file path must not appear
        assert "/app/mongo_datatables" not in response["error"]
        assert "line 42" not in response["error"]

    def test_error_response_is_user_friendly_string(self):
        """Error message must be a non-empty human-readable string."""
        dt = _make_datatables()
        with patch.object(dt, "results", side_effect=PyMongoError("internal db error")):
            response = dt.get_rows()
        assert isinstance(response["error"], str)
        assert len(response["error"]) > 0

    def test_error_response_structure_is_complete(self):
        """Error response must include all required DataTables keys."""
        dt = _make_datatables()
        with patch.object(dt, "results", side_effect=PyMongoError("fail")):
            response = dt.get_rows()
        assert "draw" in response
        assert "recordsTotal" in response
        assert "recordsFiltered" in response
        assert "data" in response
        assert response["data"] == []

    def test_get_export_data_error_returns_empty_list_not_exception(self):
        """get_export_data must return [] on error, not raise or expose details."""
        dt = _make_datatables()
        with patch(
            "mongo_datatables.datatables.core.fetch_results",
            side_effect=OperationFailure("schema: field _internal_audit not indexed"),
        ):
            result = dt.get_export_data()
        assert result == []


# ---------------------------------------------------------------------------
# Editor.process — error field must not leak raw exception details
# ---------------------------------------------------------------------------

class TestEditorErrorDisclosure:
    def test_pymongo_connection_error_is_generic(self):
        """ConnectionFailure must not expose connection string in editor response."""
        editor, col = _make_editor("create")
        col.insert_one.side_effect = ConnectionFailure(
            "mongodb://admin:s3cr3t@db.internal:27017 refused"
        )
        result = editor.process()
        assert "error" in result
        assert "mongodb://" not in result["error"]
        assert "s3cr3t" not in result["error"]
        assert "db.internal" not in result["error"]

    def test_operation_failure_schema_not_leaked(self):
        """OperationFailure with field names must not expose them in response."""
        editor, col = _make_editor("create")
        col.insert_one.side_effect = OperationFailure(
            "Document failed validation: field ssn is required"
        )
        result = editor.process()
        assert "error" in result
        assert "ssn" not in result["error"]
        assert "Document failed validation" not in result["error"]

    def test_database_operation_error_is_generic(self):
        """DatabaseOperationError must return a generic message, not the raw cause."""
        editor, col = _make_editor("edit")
        doc_id = str(ObjectId())
        editor.request_args["data"] = {doc_id: {"name": "Bob"}}
        editor.doc_id = doc_id
        with patch.object(editor, "edit", side_effect=DatabaseOperationError(
            "Failed to update documents: write concern error on replica set rs0"
        )):
            result = editor.process()
        assert "error" in result
        assert "replica set" not in result["error"]
        assert "rs0" not in result["error"]

    def test_invalid_data_error_message_is_safe(self):
        """InvalidDataError message is user-supplied so it may appear, but must not
        contain internal path or schema details injected by the library."""
        editor, col = _make_editor("create")
        with patch.object(editor, "create", side_effect=InvalidDataError("bad data")):
            result = editor.process()
        assert "error" in result
        # The message itself is safe user-facing text
        assert "bad data" in result["error"]

    def test_unsupported_action_error_does_not_leak_internals(self):
        """Unsupported action error must be a safe string."""
        editor, col = _make_editor("__import__('os').system('id')")
        result = editor.process()
        assert "error" in result
        assert isinstance(result["error"], str)
        # Must not echo back executable code
        assert "os" not in result["error"] or "Unsupported" in result["error"]

    def test_key_error_does_not_expose_field_name(self):
        """KeyError from internal processing must not expose raw key in response."""
        editor, col = _make_editor("edit")
        doc_id = str(ObjectId())
        editor.request_args["data"] = {doc_id: {"name": "Bob"}}
        editor.doc_id = doc_id
        with patch.object(editor, "edit", side_effect=KeyError("_internal_secret_field")):
            result = editor.process()
        assert "error" in result
        assert "_internal_secret_field" not in result["error"]

    def test_type_error_does_not_expose_type_details(self):
        """TypeError must not expose internal type information."""
        editor, col = _make_editor("create")
        with patch.object(editor, "create", side_effect=TypeError(
            "unsupported operand type(s) for +: 'NoneType' and 'str'"
        )):
            result = editor.process()
        assert "error" in result
        assert "NoneType" not in result["error"]

    def test_value_error_does_not_expose_internal_details(self):
        """ValueError must not expose internal details."""
        editor, col = _make_editor("create")
        with patch.object(editor, "create", side_effect=ValueError(
            "invalid literal for int() with base 10: 'secret_token_abc123'"
        )):
            result = editor.process()
        assert "error" in result
        assert "secret_token_abc123" not in result["error"]


# ---------------------------------------------------------------------------
# Logging — sensitive details must be logged (for ops) but not returned
# ---------------------------------------------------------------------------

class TestErrorLogging:
    def test_pymongo_error_is_logged_with_details(self, caplog):
        """PyMongoError must be logged at ERROR level for operators."""
        dt = _make_datatables()
        sensitive_msg = "connection refused to db.internal:27017"
        with caplog.at_level(logging.ERROR, logger="mongo_datatables.datatables.core"):
            with patch.object(dt, "count_total", side_effect=PyMongoError(sensitive_msg)):
                dt.get_rows()
        # An ERROR-level record must exist (details logged for ops)
        assert any(r.levelno >= logging.ERROR for r in caplog.records)

    def test_editor_pymongo_error_is_logged(self, caplog):
        """Editor PyMongoError must be logged, not silently swallowed."""
        editor, col = _make_editor("create")
        col.insert_one.side_effect = PyMongoError("write concern timeout on rs0")
        with caplog.at_level(logging.ERROR, logger="mongo_datatables.editor"):
            editor.process()
        assert any(r.levelno >= logging.ERROR for r in caplog.records)
