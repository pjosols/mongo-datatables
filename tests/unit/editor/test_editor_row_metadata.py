"""Editor tests — row metadata (DT_RowClass, DT_RowData, DT_RowAttr)."""
import unittest
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult
from pymongo.errors import PyMongoError

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField
from mongo_datatables.editor import StorageAdapter
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError

def _make_row_metadata_editor(request_args, row_class=None, row_data=None, row_attr=None):
    mongo = MagicMock()
    doc_id = str(ObjectId())
    editor = Editor(mongo, "test", request_args, doc_id=doc_id,
                    row_class=row_class, row_data=row_data, row_attr=row_attr,
                    data_fields=[DataField("name", "string")])
    return editor


def _mock_row_metadata_doc(editor):
    oid = ObjectId()
    doc = {"_id": oid, "name": "Alice"}
    editor._collection.find_one.return_value = doc
    editor._collection.insert_one.return_value = MagicMock(inserted_id=oid)
    return oid


class TestEditorRowMetadataAbsent:
    def test_no_row_class_key_absent(self):
        editor = _make_row_metadata_editor({"action": "create", "data": {"0": {"name": "Alice"}}})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowClass" not in result["data"][0]

    def test_no_row_data_key_absent(self):
        editor = _make_row_metadata_editor({"action": "create", "data": {"0": {"name": "Alice"}}})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowData" not in result["data"][0]

    def test_no_row_attr_key_absent(self):
        editor = _make_row_metadata_editor({"action": "create", "data": {"0": {"name": "Alice"}}})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowAttr" not in result["data"][0]


class TestEditorRowClassStatic:
    def test_static_row_class_in_create(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}}, row_class="highlight")
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert result["data"][0]["DT_RowClass"] == "highlight"

    def test_static_row_class_in_edit(self):
        oid = ObjectId()
        doc_id = str(oid)
        mongo = MagicMock()
        editor = Editor(mongo, "test",
                        {"action": "edit", "data": {doc_id: {"name": "Bob"}}},
                        doc_id=doc_id, row_class="active",
                        data_fields=[DataField("name", "string")])
        editor._collection.find_one.return_value = {"_id": oid, "name": "Bob"}
        result = editor.edit()
        assert result["data"][0]["DT_RowClass"] == "active"


class TestEditorRowClassCallable:
    def test_callable_row_class_receives_dt_row_id(self):
        received = {}

        def cls_fn(row):
            received.update(row)
            return "computed"

        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}}, row_class=cls_fn)
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowId" in received
        assert result["data"][0]["DT_RowClass"] == "computed"

    def test_callable_row_class_return_value_used(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_class=lambda row: f"row-{row['DT_RowId'][:4]}")
        _mock_row_metadata_doc(editor)
        result = editor.create()
        dt_row_id = result["data"][0]["DT_RowId"]
        assert result["data"][0]["DT_RowClass"] == f"row-{dt_row_id[:4]}"


class TestEditorRowData:
    def test_static_row_data_dict(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_data={"source": "mongo", "version": 2})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert result["data"][0]["DT_RowData"] == {"source": "mongo", "version": 2}

    def test_callable_row_data(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_data=lambda row: {"id": row["DT_RowId"]})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        dt_row_id = result["data"][0]["DT_RowId"]
        assert result["data"][0]["DT_RowData"] == {"id": dt_row_id}


class TestEditorRowAttr:
    def test_static_row_attr_dict(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_attr={"data-type": "record", "tabindex": "0"})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert result["data"][0]["DT_RowAttr"] == {"data-type": "record", "tabindex": "0"}


class TestEditorRowMetadataAllThree:
    def test_all_three_combined(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_class="highlight", row_data={"pkey": 1}, row_attr={"data-id": "x"})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        row = result["data"][0]
        assert row["DT_RowClass"] == "highlight"
        assert row["DT_RowData"] == {"pkey": 1}
        assert row["DT_RowAttr"] == {"data-id": "x"}


