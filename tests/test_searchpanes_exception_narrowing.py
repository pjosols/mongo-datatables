"""Tests for narrowed exception handling in _parse_searchpanes_filters."""
from datetime import datetime
from unittest.mock import MagicMock, patch
from bson import ObjectId
import pytest
from mongo_datatables.datatables import DataTables, DataField
from mongo_datatables.utils import FieldMapper
from mongo_datatables.query_builder import MongoQueryBuilder


def _make_dt(data_fields, request_args):
    col = MagicMock()
    col.list_indexes.return_value = []
    col.estimated_document_count.return_value = 0
    with patch.object(DataTables, '_get_collection', return_value=col), \
         patch.object(DataTables, '_check_text_index'):
        dt = DataTables.__new__(DataTables)
        dt.collection = col
        dt.request_args = request_args
        dt.data_fields = data_fields
        dt.field_mapper = FieldMapper(data_fields)
        dt.use_text_index = False
        dt.allow_disk_use = False
        dt.row_class = dt.row_data = dt.row_attr = dt.row_id = None
        dt.custom_filter = {}
        dt._results = dt._recordsTotal = dt._recordsFiltered = dt._filter_cache = None
        dt._has_text_index = False
        dt.query_builder = MongoQueryBuilder(dt.field_mapper, False, False)
        return dt


class TestSearchPanesExceptionNarrowing:
    def test_invalid_objectid_falls_back_to_raw_string(self):
        """Invalid ObjectId string is caught and falls back to the raw string value."""
        dt = _make_dt([DataField('ref', 'objectid')], {"searchPanes": {"ref": ["not-an-objectid"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"ref": {"$in": ["not-an-objectid"]}}]}

    def test_valid_objectid_is_converted(self):
        """Valid ObjectId string is converted to an ObjectId object."""
        oid = ObjectId()
        dt = _make_dt([DataField('ref', 'objectid')], {"searchPanes": {"ref": [str(oid)]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"ref": {"$in": [oid]}}]}

    def test_invalid_date_falls_back_to_raw_string(self):
        """Invalid date string is caught and falls back to the raw string value."""
        dt = _make_dt([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["not-a-date"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": ["not-a-date"]}}]}

    def test_valid_date_is_converted_to_datetime(self):
        """Valid ISO date string is converted to a datetime object."""
        dt = _make_dt([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["2024-06-01"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 6, 1)]}}]}
