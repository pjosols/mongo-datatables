"""Tests for SearchPanes date value conversion in _parse_searchpanes_filters."""
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
from mongo_datatables.datatables import DataTables, DataField


def _make_dt(request_args):
    col = MagicMock()
    col.list_indexes.return_value = []
    col.estimated_document_count.return_value = 0
    with patch.object(DataTables, '_get_collection', return_value=col), \
         patch.object(DataTables, '_check_text_index'):
        dt = DataTables.__new__(DataTables)
        dt.collection = col
        dt.request_args = request_args
        dt.data_fields = [DataField('created_at', 'date')]
        from mongo_datatables.utils import FieldMapper
        dt.field_mapper = FieldMapper(dt.data_fields)
        dt.use_text_index = False
        dt.allow_disk_use = False
        dt.row_class = dt.row_data = dt.row_attr = dt.row_id = None
        dt.custom_filter = {}
        dt._results = dt._recordsTotal = dt._recordsFiltered = dt._filter_cache = None
        dt._has_text_index = False
        from mongo_datatables.query_builder import MongoQueryBuilder
        dt.query_builder = MongoQueryBuilder(dt.field_mapper, False, False)
        return dt


class TestSearchPanesDateFilter:
    def test_date_iso_date_string_converted_to_datetime(self):
        """SearchPanes date value as YYYY-MM-DD is converted to datetime."""
        dt = _make_dt({"searchPanes": {"created_at": ["2024-03-15"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 3, 15)]}}]}

    def test_date_iso_datetime_string_converted_to_datetime(self):
        """SearchPanes date value as ISO datetime string is converted to datetime."""
        dt = _make_dt({"searchPanes": {"created_at": ["2024-03-15T00:00:00.000Z"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 3, 15)]}}]}

    def test_date_invalid_falls_back_to_string(self):
        """SearchPanes date value that can't be parsed falls back to raw string."""
        dt = _make_dt({"searchPanes": {"created_at": ["not-a-date"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": ["not-a-date"]}}]}

    def test_date_multiple_values(self):
        """Multiple date values are all converted."""
        dt = _make_dt({"searchPanes": {"created_at": ["2024-01-01", "2024-06-15T12:00:00Z"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 1, 1), datetime(2024, 6, 15)]}}]}
