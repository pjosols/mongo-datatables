"""Verify specific exception handling, not bare except Exception.

Confirms PyMongoError and domain errors are caught specifically; unrelated
exception types propagate rather than being silently swallowed.
"""
import pytest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError, OperationFailure

from mongo_datatables.datatables.results import (
    fetch_results,
    count_total,
    count_filtered,
    get_rowgroup_data,
)
from mongo_datatables.editor.crud import run_create, run_edit, run_remove
from mongo_datatables.editor.core import Editor
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collection():
    col = MagicMock(spec=Collection)
    col.estimated_document_count.return_value = 0
    return col


def _field_mapper():
    fm = MagicMock()
    fm.get_db_field.side_effect = lambda x: x
    return fm


def _crud_kwargs(**overrides):
    defaults = dict(
        fields={},
        data_fields=[],
        file_fields=[],
        storage_adapter=None,
        row_class=None,
        row_data=None,
        row_attr=None,
        pre_hook=lambda action, row_id, row_data: True,
    )
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# results.fetch_results — PyMongoError caught, other errors propagate
# ---------------------------------------------------------------------------

class TestFetchResultsExceptions:
    def test_pymongo_error_returns_empty_list(self):
        col = _collection()
        col.aggregate.side_effect = OperationFailure("index not found")
        result = fetch_results(col, [], None, _field_mapper(), None, None, None, False)
        assert result == []

    def test_non_pymongo_error_on_aggregate_propagates(self):
        col = _collection()
        col.aggregate.side_effect = RuntimeError("unexpected")
        with pytest.raises(RuntimeError):
            fetch_results(col, [], None, _field_mapper(), None, None, None, False)

    def test_value_error_in_formatting_returns_empty_list(self):
        col = _collection()
        col.aggregate.return_value = iter([{"_id": ObjectId(), "name": "x"}])
        with patch("mongo_datatables.datatables.results.process_cursor", side_effect=ValueError("bad")):
            result = fetch_results(col, [], None, _field_mapper(), None, None, None, False)
        assert result == []

    def test_type_error_in_formatting_returns_empty_list(self):
        col = _collection()
        col.aggregate.return_value = iter([])
        with patch("mongo_datatables.datatables.results.process_cursor", side_effect=TypeError("bad type")):
            result = fetch_results(col, [], None, _field_mapper(), None, None, None, False)
        assert result == []


# ---------------------------------------------------------------------------
# results.count_total — PyMongoError caught, other errors propagate
# ---------------------------------------------------------------------------

class TestCountTotalExceptions:
    def test_pymongo_error_returns_zero(self):
        col = _collection()
        col.estimated_document_count.side_effect = PyMongoError("timeout")
        col.count_documents.side_effect = PyMongoError("timeout")
        assert count_total(col, {}) == 0

    def test_non_pymongo_error_propagates(self):
        col = _collection()
        col.estimated_document_count.side_effect = RuntimeError("unexpected")
        with pytest.raises(RuntimeError):
            count_total(col, {})


# ---------------------------------------------------------------------------
# results.count_filtered — PyMongoError caught, other errors propagate
# ---------------------------------------------------------------------------

class TestCountFilteredExceptions:
    def test_pymongo_error_falls_back_to_count_documents(self):
        col = _collection()
        col.aggregate.side_effect = OperationFailure("facet error")
        col.count_documents.return_value = 5
        result = count_filtered(col, {"status": "active"}, [], 10, False)
        assert result == 5

    def test_both_pymongo_errors_returns_zero(self):
        col = _collection()
        col.aggregate.side_effect = PyMongoError("agg fail")
        col.count_documents.side_effect = PyMongoError("count fail")
        result = count_filtered(col, {"status": "active"}, [], 10, False)
        assert result == 0

    def test_non_pymongo_error_propagates(self):
        col = _collection()
        col.aggregate.side_effect = RuntimeError("unexpected")
        with pytest.raises(RuntimeError):
            count_filtered(col, {"status": "active"}, [], 10, False)

    def test_value_error_returns_zero(self):
        col = _collection()
        col.aggregate.return_value = iter([{"total": "not-an-int"}])
        # Simulate a TypeError when accessing result[0]["total"]
        with patch("mongo_datatables.datatables.results.list", side_effect=TypeError("bad")):
            result = count_filtered(col, {"x": 1}, [], 10, False)
        assert result == 0


# ---------------------------------------------------------------------------
# results.get_rowgroup_data — PyMongoError caught, returns None
# ---------------------------------------------------------------------------

class TestGetRowgroupDataExceptions:
    def test_pymongo_error_returns_none(self):
        col = _collection()
        col.aggregate.side_effect = PyMongoError("agg fail")
        fm = _field_mapper()
        result = get_rowgroup_data(
            col,
            [{"data": "status"}],
            fm,
            {},
            {"rowGroup": {"dataSrc": "status"}},
            False,
        )
        assert result is None

    def test_non_pymongo_error_propagates(self):
        col = _collection()
        col.aggregate.side_effect = RuntimeError("unexpected")
        fm = _field_mapper()
        with pytest.raises(RuntimeError):
            get_rowgroup_data(
                col,
                [{"data": "status"}],
                fm,
                {},
                {"rowGroup": {"dataSrc": "status"}},
                False,
            )


# ---------------------------------------------------------------------------
# editor.crud.run_create — PyMongoError raises DatabaseOperationError
# ---------------------------------------------------------------------------

class TestRunCreateExceptions:
    def test_pymongo_error_raises_database_operation_error(self):
        col = _collection()
        col.insert_one.side_effect = PyMongoError("write failed")
        data = {"0": {"name": "Alice"}}
        with pytest.raises(DatabaseOperationError):
            run_create(data, col, **_crud_kwargs())

    def test_non_pymongo_error_propagates(self):
        col = _collection()
        col.insert_one.side_effect = RuntimeError("unexpected")
        data = {"0": {"name": "Alice"}}
        with pytest.raises(RuntimeError):
            run_create(data, col, **_crud_kwargs())

    def test_empty_data_raises_invalid_data_error(self):
        col = _collection()
        with pytest.raises(InvalidDataError):
            run_create({}, col, **_crud_kwargs())


# ---------------------------------------------------------------------------
# editor.crud.run_edit — PyMongoError raises DatabaseOperationError
# ---------------------------------------------------------------------------

class TestRunEditExceptions:
    def test_pymongo_error_raises_database_operation_error(self):
        col = _collection()
        doc_id = str(ObjectId())
        col.update_one.side_effect = PyMongoError("write failed")
        col.find_one.return_value = {"_id": ObjectId(doc_id), "name": "Bob"}
        data = {doc_id: {"name": "Bob"}}
        with pytest.raises(DatabaseOperationError):
            run_edit([doc_id], data, col, **_crud_kwargs())

    def test_empty_ids_raises_invalid_data_error(self):
        col = _collection()
        with pytest.raises(InvalidDataError):
            run_edit([], {}, col, **_crud_kwargs())

    def test_non_pymongo_error_propagates(self):
        col = _collection()
        doc_id = str(ObjectId())
        col.update_one.side_effect = RuntimeError("unexpected")
        data = {doc_id: {"name": "Bob"}}
        with pytest.raises(RuntimeError):
            run_edit([doc_id], data, col, **_crud_kwargs())


# ---------------------------------------------------------------------------
# editor.crud.run_remove — PyMongoError raises DatabaseOperationError
# ---------------------------------------------------------------------------

class TestRunRemoveExceptions:
    def test_pymongo_error_raises_database_operation_error(self):
        col = _collection()
        col.delete_one.side_effect = PyMongoError("delete failed")
        doc_id = str(ObjectId())
        with pytest.raises(DatabaseOperationError):
            run_remove([doc_id], col, lambda a, r, d: True)

    def test_empty_ids_raises_invalid_data_error(self):
        col = _collection()
        with pytest.raises(InvalidDataError):
            run_remove([], col, lambda a, r, d: True)

    def test_non_pymongo_error_propagates(self):
        col = _collection()
        col.delete_one.side_effect = RuntimeError("unexpected")
        doc_id = str(ObjectId())
        with pytest.raises(RuntimeError):
            run_remove([doc_id], col, lambda a, r, d: True)


# ---------------------------------------------------------------------------
# editor.core.Editor.process — specific exceptions caught, returns error dict
# ---------------------------------------------------------------------------

class TestEditorProcessExceptions:
    def _editor(self, action: str, **kwargs):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        request_args = {"action": action, "data": {}}
        editor = Editor(mongo, "users", request_args, **kwargs)
        editor._collection = col
        return editor, col

    def test_pymongo_error_returns_error_dict(self):
        editor, col = self._editor("create")
        with patch.object(editor, "create", side_effect=PyMongoError("conn refused")):
            result = editor.process()
        assert "error" in result
        # Generic message — must not echo raw DB error to client
        assert "conn refused" not in result["error"]

    def test_invalid_data_error_returns_error_dict(self):
        editor, col = self._editor("create")
        with patch.object(editor, "create", side_effect=InvalidDataError("bad data")):
            result = editor.process()
        assert "error" in result
        assert "bad data" in result["error"]

    def test_database_operation_error_returns_error_dict(self):
        editor, col = self._editor("edit")
        with patch.object(editor, "edit", side_effect=DatabaseOperationError("db fail")):
            result = editor.process()
        assert "error" in result

    def test_key_error_returns_error_dict(self):
        editor, col = self._editor("edit")
        with patch.object(editor, "edit", side_effect=KeyError("missing_field")):
            result = editor.process()
        assert "error" in result

    def test_type_error_returns_error_dict(self):
        editor, col = self._editor("create")
        with patch.object(editor, "create", side_effect=TypeError("bad type")):
            result = editor.process()
        assert "error" in result

    def test_unsupported_action_returns_error_dict(self):
        editor, _ = self._editor("unknown_action")
        result = editor.process()
        assert "error" in result
        assert "Unsupported action" in result["error"]

    def test_runtime_error_propagates(self):
        """RuntimeError is NOT in the caught set — must propagate."""
        editor, col = self._editor("create")
        with patch.object(editor, "create", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                editor.process()
