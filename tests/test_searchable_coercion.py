"""Tests for columns[i][searchable] string coercion.

DataTables sends searchable as the string "true"/"false" from HTTP form data.
Python bool True/False must also work for direct API usage.
"""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables
from mongo_datatables.query_builder import MongoQueryBuilder


def _make_column(data, searchable):
    return {"data": data, "name": "", "searchable": searchable,
            "orderable": "true", "search": {"value": "", "regex": "false"}}


def _make_request(columns, search_value=""):
    return {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": "false"},
        "order": [{"column": "0", "dir": "asc"}],
        "columns": columns,
    }


@pytest.fixture
def mock_collection():
    col = MagicMock()
    col.database.list_collection_names.return_value = ["test"]
    col.aggregate.return_value = iter([])
    col.count_documents.return_value = 0
    col.estimated_document_count.return_value = 0
    return col


class TestSearchableStringCoercion:
    """columns[i][searchable] must handle both bool and string values."""

    @pytest.mark.parametrize("searchable_val", [True, "true", "True", 1])
    def test_searchable_truthy_values_included(self, searchable_val, mock_collection):
        """Columns with truthy searchable values appear in searchable_columns."""
        cols = [_make_column("name", searchable_val), _make_column("age", "false")]
        dt = DataTables(mock_collection, "test", _make_request(cols))
        assert "name" in dt.searchable_columns
        assert "age" not in dt.searchable_columns

    @pytest.mark.parametrize("searchable_val", [False, "false", "False", 0, None])
    def test_searchable_falsy_values_excluded(self, searchable_val, mock_collection):
        """Columns with falsy searchable values are excluded from searchable_columns."""
        cols = [_make_column("name", searchable_val)]
        dt = DataTables(mock_collection, "test", _make_request(cols))
        assert "name" not in dt.searchable_columns

    def test_global_search_skips_non_searchable_string_false(self, mock_collection):
        """Global search does not include columns where searchable="false"."""
        cols = [
            _make_column("name", "true"),
            _make_column("internal", "false"),
        ]
        dt = DataTables(mock_collection, "test", _make_request(cols, search_value="alice"))
        assert "name" in dt.searchable_columns
        assert "internal" not in dt.searchable_columns

    def test_query_builder_respects_searchable_string_false(self):
        """MongoQueryBuilder.build_column_search skips columns with searchable="false"."""
        col = _make_column("secret", "false")
        col["search"] = {"value": "test", "regex": "false"}
        fm = MagicMock()
        fm.get_field_type.return_value = "string"
        fm.get_db_field.return_value = "secret"
        qb = MongoQueryBuilder(fm)
        result = qb.build_column_search([col])
        assert result == {}

    def test_query_builder_respects_searchable_string_true(self):
        """MongoQueryBuilder.build_column_search processes columns with searchable="true"."""
        col = _make_column("name", "true")
        col["search"] = {"value": "alice", "regex": "false"}
        fm = MagicMock()
        fm.get_field_type.return_value = "string"
        fm.get_db_field.return_value = "name"
        qb = MongoQueryBuilder(fm)
        result = qb.build_column_search([col])
        assert result != {}
