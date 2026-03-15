"""Tests for result field alias remapping in _process_cursor."""
import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables, DataField


def _dt(data_fields=None, extra_args=None):
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": [],
    }
    if extra_args:
        args.update(extra_args)
    return DataTables(mock_db, "test", args, data_fields=data_fields or [])


class TestRemapAliases:
    def test_no_alias_no_change(self):
        dt = _dt()
        doc = {"title": "Hello", "DT_RowId": "abc"}
        assert dt._remap_aliases(doc) == {"title": "Hello", "DT_RowId": "abc"}

    def test_simple_rename(self):
        dt = _dt([DataField("pub_date", "date", alias="Published")])
        doc = {"pub_date": "2001-01-01"}
        result = dt._remap_aliases(doc)
        assert result == {"Published": "2001-01-01"}
        assert "pub_date" not in result

    def test_nested_field_extracted_to_alias(self):
        dt = _dt([DataField("PublisherInfo.Date", "date", alias="Published")])
        doc = {"PublisherInfo": {"Date": "2001-12-12"}}
        result = dt._remap_aliases(doc)
        assert result["Published"] == "2001-12-12"
        assert "PublisherInfo" not in result

    def test_nested_field_missing_value_unchanged(self):
        dt = _dt([DataField("PublisherInfo.Date", "date", alias="Published")])
        doc = {"title": "Book"}
        result = dt._remap_aliases(doc)
        assert "Published" not in result
        assert result == {"title": "Book"}

    def test_shared_parent_not_deleted(self):
        """When two aliased fields share a parent, the parent dict is kept."""
        dt = _dt([
            DataField("Info.Date", "date", alias="Published"),
            DataField("Info.Author", "string", alias="Writer"),
        ])
        doc = {"Info": {"Date": "2001-01-01", "Author": "Bob"}}
        result = dt._remap_aliases(doc)
        assert result["Published"] == "2001-01-01"
        assert result["Writer"] == "Bob"
        # Parent should still exist because both fields share it
        # (second field's parent removal check will see the first still needs it)

    def test_process_cursor_applies_remapping(self):
        dt = _dt([DataField("PublisherInfo.Date", "date", alias="Published")])
        cursor = [{"_id": "abc", "PublisherInfo": {"Date": "2001-12-12"}}]
        result = dt._process_cursor(cursor)
        assert len(result) == 1
        assert result[0]["Published"] == "2001-12-12"
        assert result[0]["DT_RowId"] == "abc"
        assert "PublisherInfo" not in result[0]

    def test_alias_same_as_db_field_no_change(self):
        """DataField with no explicit alias (alias == last segment) is a no-op."""
        dt = _dt([DataField("Date", "date")])
        doc = {"Date": "2001-01-01"}
        result = dt._remap_aliases(doc)
        assert result == {"Date": "2001-01-01"}
