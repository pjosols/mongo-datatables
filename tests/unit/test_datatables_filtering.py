"""Test DataTables filtering functionality."""
from unittest.mock import MagicMock, patch

import pytest
from pymongo.collection import Collection

from mongo_datatables import DataTables


_REQUEST_ARGS = {
    "draw": "1",
    "start": 0,
    "length": 10,
    "search": {"value": "", "regex": False},
    "order": [{"column": 0, "dir": "asc"}],
    "columns": [
        {"data": "name", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "email", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "status", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ],
}

_BASE_ARGS = {
    "draw": 1, "start": 0, "length": 10,
    "search": {"value": "", "regex": False},
    "order": [{"column": 0, "dir": "asc"}],
    "columns": [{"data": "Title", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}}],
}


@pytest.fixture
def mongo():
    from unittest.mock import MagicMock
    from pymongo.database import Database

    m = MagicMock()
    m.db = MagicMock(spec=Database)
    col = MagicMock(spec=Collection)
    col.estimated_document_count.return_value = 0
    m.db.__getitem__.return_value = col
    return m


@pytest.fixture
def request_args():
    import copy
    return copy.deepcopy(_REQUEST_ARGS)


def _make_dt(request_args, data_fields=None, **custom_filter):
    col = MagicMock(spec=Collection)
    col.list_indexes = MagicMock(return_value=[])
    col.aggregate = MagicMock(return_value=iter([]))
    col.count_documents = MagicMock(return_value=0)
    col.estimated_document_count = MagicMock(return_value=0)
    db = {"test": col}
    return DataTables(db, "test", request_args, data_fields or [], **custom_filter), col


class TestFiltering:
    """Test cases for DataTables filtering functionality."""

    def test_filter_property_empty(self, mongo, request_args):
        dt = DataTables(mongo, 'users', request_args)
        assert dt.filter == {}

    def test_filter_property_with_custom_filter(self, mongo, request_args):
        custom_filter = {"department": "IT"}
        dt = DataTables(mongo, 'users', request_args, **custom_filter)
        assert dt.filter == custom_filter

    def test_filter_property_with_global_search(self, mongo, request_args):
        request_args["search"]["value"] = "John"
        with patch.object(DataTables, 'has_text_index', return_value=True):
            dt = DataTables(mongo, 'users', request_args)
            assert '$text' in dt.filter

    def test_filter_property_with_column_search(self, mongo, request_args):
        request_args["columns"][2]["search"]["value"] = "active"
        dt = DataTables(mongo, 'users', request_args)
        assert '$and' in dt.filter

    def test_filter_property_with_combined_searches(self, mongo, request_args):
        custom_filter = {"department": "IT"}
        request_args["search"]["value"] = "John"
        request_args["columns"][2]["search"]["value"] = "active"
        with patch.object(DataTables, 'has_text_index', return_value=True):
            dt = DataTables(mongo, 'users', request_args, **custom_filter)
            result = dt.filter
            assert '$and' in result
            assert any('department' in str(c) and 'IT' in str(c) for c in result['$and'])
            assert any('$text' in str(c) and 'John' in str(c) for c in result['$and'])
            assert any('$and' in str(c) and 'active' in str(c) for c in result['$and'])

    def test_filter_with_complex_custom_filter(self, mongo, request_args):
        complex_filter = {"$or": [{"status": "active"}, {"role": "admin"}]}
        dt = DataTables(mongo, 'users', request_args, **complex_filter)
        assert '$or' in dt.filter

    def test_filter_with_nested_fields(self, mongo):
        args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "address.city:New York", "regex": False},
            "columns": [
                {"data": "address.city", "name": "address.city", "searchable": True,
                 "orderable": True, "search": {"value": "", "regex": False}}
            ],
            "order": [],
        }
        with patch.object(DataTables, 'has_text_index', return_value=False):
            dt = DataTables(mongo, 'users', args, use_text_index=False)
            result = dt.column_specific_search_condition
            assert '$and' in result
            assert any('address.city' in str(c) for c in result['$and'])

    def test_filter_cache_returns_same_object(self, mongo, request_args):
        dt = DataTables(mongo, 'users', request_args)
        assert dt.filter is dt.filter

    def test_filter_cache_is_none_before_access(self, mongo, request_args):
        dt = DataTables(mongo, 'users', request_args)
        assert dt._filter_cache is None
        _ = dt.filter
        assert dt._filter_cache is not None


class TestBuildFilter:
    """Tests for the filter property."""

    def test_empty_returns_empty_dict(self):
        dt, _ = _make_dt(_BASE_ARGS)
        assert dt.filter == {}

    def test_custom_filter_included(self):
        dt, _ = _make_dt(_BASE_ARGS, status="active")
        assert dt.filter.get("status") == "active"

    def test_global_search_included(self):
        args = {**_BASE_ARGS, "search": {"value": "Orwell", "regex": False}}
        dt, _ = _make_dt(args)
        dt._has_text_index = False
        assert dt.filter != {}

    def test_column_search_included(self):
        args = {**_BASE_ARGS, "columns": [
            {"data": "Title", "searchable": True, "orderable": True,
             "search": {"value": "1984", "regex": False}}
        ]}
        dt, _ = _make_dt(args)
        assert dt.filter != {}

    def test_searchbuilder_included(self):
        args = {**_BASE_ARGS, "searchBuilder": {
            "logic": "AND",
            "criteria": [{"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}],
        }}
        dt, _ = _make_dt(args)
        assert dt.filter != {}

    def test_searchpanes_included(self):
        args = {**_BASE_ARGS, "searchPanes": {"Title": ["1984"]}}
        dt, _ = _make_dt(args)
        assert dt.filter != {}

    def test_multiple_sources_wrapped_in_and(self):
        args = {**_BASE_ARGS, "search": {"value": "Orwell", "regex": False}}
        dt, _ = _make_dt(args, status="active")
        dt._has_text_index = False
        result = dt.filter
        assert "$and" in result
        assert len(result["$and"]) >= 2

    def test_single_source_not_wrapped(self):
        dt, _ = _make_dt(_BASE_ARGS, status="active")
        result = dt.filter
        assert "$and" not in result
        assert result == {"status": "active"}

    def test_search_fixed_included(self):
        args = {**_BASE_ARGS, "search": {"value": "", "regex": False,
                                          "fixed": [{"name": "active", "term": "Orwell"}]}}
        dt, _ = _make_dt(args)
        assert dt.filter != {}
