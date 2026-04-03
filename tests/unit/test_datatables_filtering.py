"""Tests for DataTables filtering functionality."""
import unittest
from unittest.mock import patch

from mongo_datatables import DataTables
from tests.base_test import BaseDataTablesTest


class TestFiltering(BaseDataTablesTest):
    """Test cases for DataTables filtering functionality."""

    def test_filter_property_empty(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.filter, {})

    def test_filter_property_with_custom_filter(self):
        custom_filter = {"department": "IT"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
        self.assertEqual(datatables.filter, custom_filter)

    def test_filter_property_with_global_search(self):
        self.request_args["search"]["value"] = "John"
        with patch.object(DataTables, 'has_text_index', return_value=True):
            datatables = DataTables(self.mongo, 'users', self.request_args)
            result = datatables.filter
            self.assertIn('$text', result)

    def test_filter_property_with_column_search(self):
        self.request_args["columns"][2]["search"]["value"] = "active"
        datatables = DataTables(self.mongo, 'users', self.request_args)
        result = datatables.filter
        self.assertIn('$and', result)

    def test_filter_property_with_combined_searches(self):
        custom_filter = {"department": "IT"}
        self.request_args["search"]["value"] = "John"
        self.request_args["columns"][2]["search"]["value"] = "active"
        with patch.object(DataTables, 'has_text_index', return_value=True):
            datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
            result = datatables.filter
            self.assertIn('$and', result)
            custom_filter_included = any('department' in str(cond) and 'IT' in str(cond) for cond in result['$and'])
            self.assertTrue(custom_filter_included)
            text_search_included = any('$text' in str(cond) and 'John' in str(cond) for cond in result['$and'])
            self.assertTrue(text_search_included)
            column_search_included = any('$and' in str(cond) and 'active' in str(cond) for cond in result['$and'])
            self.assertTrue(column_search_included)

    def test_filter_with_complex_custom_filter(self):
        complex_filter = {"$or": [{"status": "active"}, {"role": "admin"}]}
        datatables = DataTables(self.mongo, 'users', self.request_args, **complex_filter)
        result = datatables.filter
        self.assertIn('$or', result)

    def test_filter_with_nested_fields(self):
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "address.city:New York", "regex": False},
            "columns": [
                {"data": "address.city", "name": "address.city", "searchable": True,
                 "orderable": True, "search": {"value": "", "regex": False}}
            ],
            "order": [],
        }
        with patch.object(DataTables, 'has_text_index', return_value=False):
            datatables = DataTables(self.mongo, 'users', request_args, use_text_index=False)
            result = datatables.column_specific_search_condition
            self.assertIn('$and', result)
            has_nested_field = any('address.city' in str(cond) for cond in result['$and'])
            self.assertTrue(has_nested_field)

    def test_filter_cache_returns_same_object(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        first = datatables.filter
        second = datatables.filter
        assert first is second

    def test_filter_cache_is_none_before_access(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        assert datatables._filter_cache is None
        _ = datatables.filter
        assert datatables._filter_cache is not None


class TestBuildFilter:
    """Tests for _build_filter method."""

    _BASE_ARGS = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "Title", "searchable": True, "orderable": True,
                      "search": {"value": "", "regex": False}}],
    }

    def _make_dt(self, request_args, data_fields=None, **custom_filter):
        from unittest.mock import MagicMock
        from pymongo.collection import Collection
        col = MagicMock(spec=Collection)
        col.list_indexes = MagicMock(return_value=[])
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)
        db = {"test": col}
        return DataTables(db, "test", request_args, data_fields or [], **custom_filter), col

    def test_empty_returns_empty_dict(self):
        dt, _ = self._make_dt(self._BASE_ARGS)
        assert dt._build_filter() == {}

    def test_custom_filter_included(self):
        dt, _ = self._make_dt(self._BASE_ARGS, status="active")
        result = dt._build_filter()
        assert result.get("status") == "active"

    def test_global_search_included(self):
        args = {**self._BASE_ARGS, "search": {"value": "Orwell", "regex": False}}
        dt, _ = self._make_dt(args)
        dt._has_text_index = False
        result = dt._build_filter()
        assert result != {}

    def test_column_search_included(self):
        args = {**self._BASE_ARGS, "columns": [
            {"data": "Title", "searchable": True, "orderable": True,
             "search": {"value": "1984", "regex": False}}
        ]}
        dt, _ = self._make_dt(args)
        result = dt._build_filter()
        assert result != {}

    def test_searchbuilder_included(self):
        args = {**self._BASE_ARGS, "searchBuilder": {
            "logic": "AND",
            "criteria": [{"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}]
        }}
        dt, _ = self._make_dt(args)
        result = dt._build_filter()
        assert result != {}

    def test_searchpanes_included(self):
        args = {**self._BASE_ARGS, "searchPanes": {"Title": ["1984"]}}
        dt, _ = self._make_dt(args)
        result = dt._build_filter()
        assert result != {}

    def test_multiple_sources_wrapped_in_and(self):
        args = {**self._BASE_ARGS, "search": {"value": "Orwell", "regex": False}}
        dt, _ = self._make_dt(args, status="active")
        dt._has_text_index = False
        result = dt._build_filter()
        assert "$and" in result
        assert len(result["$and"]) >= 2

    def test_single_source_not_wrapped(self):
        dt, _ = self._make_dt(self._BASE_ARGS, status="active")
        result = dt._build_filter()
        assert "$and" not in result
        assert result == {"status": "active"}

    def test_search_fixed_included(self):
        args = {**self._BASE_ARGS, "search": {"value": "", "regex": False, "fixed": [{"name": "active", "term": "Orwell"}]}}
        dt, _ = self._make_dt(args)
        result = dt._build_filter()
        assert result != {}


if __name__ == '__main__':
    unittest.main()
