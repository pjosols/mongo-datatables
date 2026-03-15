"""Tests that build_global_search() uses DB field names, not UI aliases."""
import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField


def _make_dt(data_fields, search_value, searchable_columns, quoted=False):
    """Build a DataTables instance with no text index and given data_fields."""
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)

    columns = [
        {"data": col, "name": col, "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}}
        for col in searchable_columns
    ]
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": False},
        "order": [], "columns": columns,
    }
    return DataTables(mock_db, "test", args, data_fields=data_fields, use_text_index=False)


class TestGlobalSearchFieldMapping:
    """build_global_search() must use DB field names, not UI aliases."""

    def test_unquoted_term_uses_db_field(self):
        """Non-quoted global search should key on the DB field name."""
        data_fields = [DataField("author_name", "string", alias="Author")]
        dt = _make_dt(data_fields, "Smith", ["Author"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "author_name" in keys
        assert "Author" not in keys

    def test_quoted_phrase_uses_db_field(self):
        """Quoted global search (non-text-index path) should key on the DB field name."""
        data_fields = [DataField("author_name", "string", alias="Author")]
        dt = _make_dt(data_fields, '"Jonathan Kennedy"', ["Author"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "author_name" in keys
        assert "Author" not in keys

    def test_no_alias_field_unchanged(self):
        """When alias equals field name, the key should still be the DB field name."""
        data_fields = [DataField("status", "string")]
        dt = _make_dt(data_fields, "active", ["status"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "status" in keys

    def test_multiple_aliased_columns(self):
        """All columns in OR conditions should use DB field names."""
        data_fields = [
            DataField("first_name", "string", alias="FirstName"),
            DataField("last_name", "string", alias="LastName"),
        ]
        dt = _make_dt(data_fields, "Alice", ["FirstName", "LastName"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "first_name" in keys
        assert "last_name" in keys
        assert "FirstName" not in keys
        assert "LastName" not in keys
