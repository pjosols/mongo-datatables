"""Tests for mongo_datatables.datatables.formatting."""
import inspect
from typing import get_type_hints

import pytest
from bson import ObjectId

from mongo_datatables.datatables.formatting import process_cursor
from mongo_datatables.field_utils import FieldMapper


def _make_mapper(mappings: dict | None = None) -> FieldMapper:
    """Build a FieldMapper with optional db_to_ui overrides."""
    fm = FieldMapper([])
    if mappings:
        fm.db_to_ui = mappings
    return fm


# --- type annotation tests ---

def test_process_cursor_has_return_annotation():
    hints = get_type_hints(process_cursor)
    assert "return" in hints


def test_process_cursor_cursor_annotated():
    hints = get_type_hints(process_cursor)
    assert "cursor" in hints


def test_process_cursor_field_mapper_annotated():
    hints = get_type_hints(process_cursor)
    assert "field_mapper" in hints


def test_process_cursor_row_class_annotated():
    hints = get_type_hints(process_cursor)
    assert "row_class" in hints


def test_process_cursor_row_data_annotated():
    hints = get_type_hints(process_cursor)
    assert "row_data" in hints


def test_process_cursor_row_attr_annotated():
    hints = get_type_hints(process_cursor)
    assert "row_attr" in hints


# --- docstring tests ---

def test_process_cursor_docstring_documents_cursor():
    assert "cursor" in process_cursor.__doc__


def test_process_cursor_docstring_documents_row_id():
    assert "row_id" in process_cursor.__doc__


def test_process_cursor_docstring_documents_field_mapper():
    assert "field_mapper" in process_cursor.__doc__


def test_process_cursor_docstring_documents_returns():
    doc = process_cursor.__doc__
    assert "Returns" in doc or "returns" in doc


# --- behaviour tests ---

def test_empty_cursor_returns_empty_list():
    result = process_cursor([], None, _make_mapper())
    assert result == []


def test_id_field_becomes_dt_row_id():
    oid = ObjectId()
    docs = [{"_id": oid, "name": "Alice"}]
    result = process_cursor(docs, None, _make_mapper())
    assert result[0]["DT_RowId"] == str(oid)
    assert "_id" not in result[0]


def test_explicit_row_id_field():
    docs = [{"_id": ObjectId(), "slug": "abc"}]
    result = process_cursor(docs, "slug", _make_mapper())
    assert result[0]["DT_RowId"] == "abc"


def test_row_class_callable():
    docs = [{"_id": ObjectId(), "status": "active"}]
    result = process_cursor(docs, None, _make_mapper(), row_class=lambda d: d["status"])
    assert result[0]["DT_RowClass"] == "active"


def test_row_class_static():
    docs = [{"_id": ObjectId()}]
    result = process_cursor(docs, None, _make_mapper(), row_class="highlight")
    assert result[0]["DT_RowClass"] == "highlight"


def test_row_data_callable():
    docs = [{"_id": ObjectId(), "x": 1}]
    result = process_cursor(docs, None, _make_mapper(), row_data=lambda d: {"val": d["x"]})
    assert result[0]["DT_RowData"] == {"val": 1}


def test_row_data_static():
    docs = [{"_id": ObjectId()}]
    result = process_cursor(docs, None, _make_mapper(), row_data={"key": "v"})
    assert result[0]["DT_RowData"] == {"key": "v"}


def test_row_attr_callable():
    docs = [{"_id": ObjectId()}]
    result = process_cursor(docs, None, _make_mapper(), row_attr=lambda d: {"data-id": "1"})
    assert result[0]["DT_RowAttr"] == {"data-id": "1"}


def test_row_attr_static():
    docs = [{"_id": ObjectId()}]
    result = process_cursor(docs, None, _make_mapper(), row_attr={"data-x": "y"})
    assert result[0]["DT_RowAttr"] == {"data-x": "y"}


def test_alias_remapping_applied():
    docs = [{"_id": ObjectId(), "db_name": "Bob"}]
    fm = _make_mapper({"db_name": "name"})
    result = process_cursor(docs, None, fm)
    assert "name" in result[0]
    assert "db_name" not in result[0]


def test_no_dt_row_metadata_when_not_provided():
    docs = [{"_id": ObjectId(), "v": 1}]
    result = process_cursor(docs, None, _make_mapper())
    assert "DT_RowClass" not in result[0]
    assert "DT_RowData" not in result[0]
    assert "DT_RowAttr" not in result[0]


def test_multiple_docs_all_processed():
    oids = [ObjectId(), ObjectId()]
    docs = [{"_id": oids[0]}, {"_id": oids[1]}]
    result = process_cursor(docs, None, _make_mapper())
    assert len(result) == 2
    assert result[0]["DT_RowId"] == str(oids[0])
    assert result[1]["DT_RowId"] == str(oids[1])
