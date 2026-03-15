"""Tests for columns[i][orderable] string/bool coercion in get_sort_specification().

DataTables sends orderable as the string "true"/"false" from HTTP form data.
Python bool True/False must also work for direct API usage.
"""
import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables


def _make_column(data, orderable):
    return {"data": data, "name": "", "searchable": "true",
            "orderable": orderable, "search": {"value": "", "regex": "false"}}


def _make_request(columns, order_col=0):
    return {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": "false"},
        "order": [{"column": str(order_col), "dir": "asc"}],
        "columns": columns,
    }


@pytest.fixture
def mock_collection():
    col = MagicMock()
    col.list_indexes.return_value = []
    col.aggregate.return_value = iter([])
    col.count_documents.return_value = 0
    col.estimated_document_count.return_value = 0
    return col


class TestOrderableCoercion:
    """get_sort_specification() must handle both bool and string orderable values."""

    @pytest.mark.parametrize("orderable_val", ["false", False])
    def test_orderable_falsy_excluded_from_sort(self, orderable_val, mock_collection):
        """Columns with orderable=False/"false" are skipped in sort specification."""
        cols = [_make_column("name", orderable_val)]
        dt = DataTables(mock_collection, "test", _make_request(cols))
        sort_spec = dt.get_sort_specification()
        assert "name" not in sort_spec

    @pytest.mark.parametrize("orderable_val", ["true", True])
    def test_orderable_truthy_included_in_sort(self, orderable_val, mock_collection):
        """Columns with orderable=True/"true" appear in sort specification."""
        cols = [_make_column("name", orderable_val)]
        dt = DataTables(mock_collection, "test", _make_request(cols))
        sort_spec = dt.get_sort_specification()
        assert "name" in sort_spec

    def test_orderable_absent_defaults_to_sortable(self, mock_collection):
        """Columns without an orderable key are sortable by default."""
        cols = [{"data": "name", "name": "", "searchable": "true",
                 "search": {"value": "", "regex": "false"}}]
        dt = DataTables(mock_collection, "test", _make_request(cols))
        sort_spec = dt.get_sort_specification()
        assert "name" in sort_spec
