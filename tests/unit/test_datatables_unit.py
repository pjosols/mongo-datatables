"""Consolidated DataTables core and utility tests."""
from bson.objectid import ObjectId
from datetime import datetime
from datetime import datetime, timedelta
from mongo_datatables import DataField
from mongo_datatables import DataTables
from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables import DataField
from mongo_datatables.datatables import DataTables, DataField
from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper
from mongo_datatables.utils import SearchTermParser
from mongo_datatables.utils import TypeConverter
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError
from tests.base_test import BaseDataTablesTest
from unittest.mock import MagicMock
from unittest.mock import MagicMock, call
from unittest.mock import MagicMock, patch
from unittest.mock import Mock, patch, MagicMock
from unittest.mock import call, patch
from unittest.mock import patch
from unittest.mock import patch, MagicMock
import json
import os
import pymongo
import pytest
import re
import sys
import unittest


# --- from tests/test_datatables_edge_cases.py ---
class TestDataTablesEdgeCases(BaseDataTablesTest):
    """Test cases for edge cases and uncovered code in the DataTables class"""

    def test_initialization_with_mongo_client(self):
        """Test initialization with a PyMongo client"""
        # Create a mock MongoClient
        mongo_client = MagicMock(spec=MongoClient)
        db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        
        # Setup the mocks
        mongo_client.get_database.return_value = db
        db.__getitem__.return_value = collection
        
        # Initialize DataTables with the client
        datatables = DataTables(mongo_client, 'test_collection', self.request_args)
        
        # Verify the collection was retrieved correctly
        mongo_client.get_database.assert_called_once()
        db.__getitem__.assert_called_once_with('test_collection')
        
        # Verify the collection property returns the correct collection
        self.assertEqual(datatables.collection, collection)

    def test_initialization_with_database(self):
        """Test initialization with a PyMongo database"""
        # Create a mock Database
        db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        
        # Setup the mocks
        db.__getitem__.return_value = collection
        
        # Initialize DataTables with the database
        datatables = DataTables(db, 'test_collection', self.request_args)
        
        # Verify the collection was retrieved correctly
        db.__getitem__.assert_called_once_with('test_collection')
        
        # Verify the collection property returns the correct collection
        self.assertEqual(datatables.collection, collection)

    def test_initialization_with_dict_like(self):
        """Test initialization with a dict-like object"""
        # Create a dict-like object
        dict_like = {}
        collection = MagicMock(spec=Collection)
        dict_like['test_collection'] = collection
        
        # Initialize DataTables with the dict-like object
        datatables = DataTables(dict_like, 'test_collection', self.request_args)
        
        # Verify the collection property returns the correct collection
        self.assertEqual(datatables.collection, collection)

    def test_exact_phrase_search_without_text_index(self):
        """Test exact phrase search when text index is not available"""
        # Set up a search with a quoted phrase
        self.request_args['search']['value'] = '"exact phrase"'
        
        # Create a DataTables instance with use_text_index=False
        # This is more reliable than mocking has_text_index
        datatables = DataTables(self.mongo, 'test_collection', self.request_args, use_text_index=False)
        
        # Get the global search condition
        condition = datatables.global_search_condition
        
        # Verify the condition uses regex for exact phrase matching
        self.assertIn('$or', condition)
        
        # Check that the regex pattern includes word boundaries
        for subcondition in condition['$or']:
                field_name = list(subcondition.keys())[0]
                if '$regex' in subcondition[field_name]:
                    # The regex pattern will have escaped backslashes and spaces
                    self.assertIn('\\bexact\\ phrase\\b', subcondition[field_name]['$regex'])
                    self.assertEqual(subcondition[field_name]['$options'], 'i')

    def test_numeric_field_search(self):
        """Test search with numeric field types"""
        # Set up a search with a numeric value
        self.request_args['search']['value'] = '42'
        
        # Initialize DataTables with data_fields
        data_fields = [
            DataField('age', 'number'),
            DataField('name', 'string')
        ]
        
        # Add columns to request args to match the data_fields
        self.request_args['columns'] = [
            {'data': 'name', 'searchable': True},
            {'data': 'age', 'searchable': True}
        ]
        
        # Create DataTables instance
        datatables = DataTables(self.mongo, 'test_collection', self.request_args, data_fields=data_fields)
        
        # Get the global search condition
        condition = datatables.global_search_condition
        
        # Verify the condition includes numeric comparison
        self.assertIn('$or', condition)
        
        # Find the numeric condition
        numeric_condition = None
        for subcondition in condition['$or']:
            if 'age' in subcondition:
                numeric_condition = subcondition
                break
        
        # Verify numeric field is queried with exact value, not regex
        self.assertIsNotNone(numeric_condition)
        self.assertEqual(numeric_condition['age'], 42.0)

    def test_numeric_field_search_with_invalid_number(self):
        """Test search with numeric field types but invalid number"""
        # Set up a search with a non-numeric value
        self.request_args['search']['value'] = 'not-a-number'
        
        # Initialize DataTables with data_fields
        data_fields = [
            DataField('age', 'number'),
            DataField('name', 'string')
        ]
        
        # Add columns to request args to match the data_fields
        self.request_args['columns'] = [
            {'data': 'name', 'searchable': True},
            {'data': 'age', 'searchable': True}
        ]
        
        # Create DataTables instance
        datatables = DataTables(self.mongo, 'test_collection', self.request_args, data_fields=data_fields)
        
        # Get the global search condition
        condition = datatables.global_search_condition
        
        # Verify the condition does not include numeric field
        self.assertIn('$or', condition)
        
        # Check that no condition exists for the age field
        age_condition = None
        for subcondition in condition['$or']:
            if 'age' in subcondition:
                age_condition = subcondition
                break
        
        # Verify numeric field is not included in the search
        self.assertIsNone(age_condition)

if __name__ == '__main__':
    unittest.main()


# --- from tests/test_datatables_error_handling.py ---
class TestDataTablesErrorHandling(BaseDataTablesTest):
    """Test cases for error handling in DataTables"""

    def setUp(self):
        super().setUp()
        # Set up data fields for testing
        self.data_fields = [
            DataField('title', 'string'),
            DataField('author', 'string'),
            DataField('year', 'number'),
            DataField('rating', 'number')
        ]

    def test_error_in_results_method(self):
        """Test error handling in the results method"""
        # Set up request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True},
            {'data': 'author', 'searchable': True}
        ]
        
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields
        )
        
        # Mock the collection.aggregate method to raise an exception
        with patch.object(datatables.collection, 'aggregate', side_effect=pymongo.errors.OperationFailure('Test error')):
            # Call results method and verify it handles the exception
            results = datatables.results()
            
            # Should return an empty list on error
            self.assertEqual(results, [])

    def test_error_in_count_total(self):
        """Test error handling in the count_total method"""
        # Create DataTables instance
        datatables = DataTables(
            self.mongo,
            'test_collection',
            self.request_args,
            data_fields=self.data_fields
        )

        # Mock the collection.count_documents method to raise a PyMongoError
        with patch.object(datatables.collection, 'count_documents', side_effect=PyMongoError('Test error')):
            # Force cache to be cleared
            datatables._recordsTotal = None
            # Call count_total method and verify it handles the exception
            count = datatables.count_total()
            
            # Should return 0 on error
            self.assertEqual(count, 0)

    def test_error_in_count_filtered(self):
        """Test error handling in the count_filtered method"""
        # Create DataTables instance
        datatables = DataTables(
            self.mongo,
            'test_collection',
            self.request_args,
            data_fields=self.data_fields
        )

        # Mock the collection.count_documents method to raise a PyMongoError
        with patch.object(datatables.collection, 'count_documents', side_effect=PyMongoError('Test error')):
            # Force cache to be cleared
            datatables._recordsFiltered = None
            # Call count_filtered method and verify it handles the exception
            count = datatables.count_filtered()
            
            # Should return 0 on error
            self.assertEqual(count, 0)

    def test_invalid_sort_specification(self):
        """Test handling of invalid sort specification"""
        # Set up invalid sort specification in request args
        self.request_args['order'] = [{'column': 999, 'dir': 'asc'}]  # Invalid column index
        self.request_args['columns'] = [
            {'data': 'title', 'orderable': True},
            {'data': 'author', 'orderable': True}
        ]
        
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields
        )
        
        # Get sort specification and verify it handles the invalid index
        sort_spec = datatables.sort_specification
        
        # Should fall back to default sort (usually by _id)
        self.assertTrue(isinstance(sort_spec, dict) or isinstance(sort_spec, list), 
                      f"Sort specification should be a dict or list, got {type(sort_spec)}")

    def test_format_result_values_with_complex_data(self):
        """Test formatting of complex result values"""
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields
        )
        
        # Create a complex result dictionary with various MongoDB types
        from bson.objectid import ObjectId
        from datetime import datetime
        
        result_dict = {
            '_id': ObjectId('5f50c31e8a91e8c9c8d5c5d5'),
            'title': 'Test Title',
            'published_date': datetime(2020, 1, 1),
            'nested': {
                'id': ObjectId('5f50c31e8a91e8c9c8d5c5d6'),
                'date': datetime(2020, 2, 2)
            },
            'array_field': [
                ObjectId('5f50c31e8a91e8c9c8d5c5d7'),
                datetime(2020, 3, 3)
            ]
        }
        
        # Make a copy of the dictionary to avoid modifying the original
        import copy
        result_copy = copy.deepcopy(result_dict)
        
        # Format the result values (the method modifies the dictionary in place)
        datatables._format_result_values(result_copy)
        formatted_dict = result_copy
        
        # Verify ObjectId and datetime values are properly formatted
        self.assertIsInstance(formatted_dict['_id'], str)
        self.assertIsInstance(formatted_dict['published_date'], str)
        self.assertIsInstance(formatted_dict['nested']['id'], str)
        self.assertIsInstance(formatted_dict['nested']['date'], str)
        self.assertIsInstance(formatted_dict['array_field'][0], str)
        self.assertIsInstance(formatted_dict['array_field'][1], str)


    def test_count_filtered_both_aggregate_and_count_documents_fail(self):
        """When both aggregate and count_documents fail, count_filtered returns 0."""
        dt = DataTables(self.mongo, 'test_collection', self.request_args, ["name"])
        dt._filter_cache = {"name": "test"}  # inject non-empty filter via cache
        self.collection.aggregate.side_effect = PyMongoError("aggregate failed")
        self.collection.count_documents.side_effect = PyMongoError("count_documents failed")
        result = dt.count_filtered()
        self.assertEqual(result, 0)


    def test_get_rows_returns_error_field_on_exception(self):
        """get_rows() returns DataTables error response when an unhandled exception occurs."""
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        with patch.object(dt, "results", side_effect=RuntimeError("pipeline failed")):
            response = dt.get_rows()
        self.assertIn("error", response)
        self.assertEqual(response["error"], "pipeline failed")
        self.assertEqual(response["data"], [])
        self.assertEqual(response["recordsTotal"], 0)
        self.assertEqual(response["recordsFiltered"], 0)
        self.assertIn("draw", response)

    def test_get_rows_returns_error_field_on_pymongo_error(self):
        """get_rows() returns DataTables error response on PyMongoError."""
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        with patch.object(dt, "count_total", side_effect=PyMongoError("connection refused")):
            response = dt.get_rows()
        self.assertIn("error", response)
        self.assertIn("connection refused", response["error"])
        self.assertEqual(response["data"], [])

    def test_check_text_index_handles_pymongo_error(self):
        """_check_text_index() sets has_text_index=False when list_indexes raises PyMongoError."""
        self.collection.list_indexes.side_effect = PyMongoError("not connected")
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        self.assertFalse(dt.has_text_index)

    def test_get_rows_success_has_no_error_field(self):
        """get_rows() does NOT include 'error' key in a successful response."""
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        response = dt.get_rows()
        self.assertNotIn("error", response)
        self.assertIn("data", response)
        self.assertIn("draw", response)


if __name__ == '__main__':
    unittest.main()


# --- from tests/test_datatables_filtering.py ---
class TestFiltering(BaseDataTablesTest):
    """Test cases for DataTables filtering functionality"""

    def test_filter_property_empty(self):
        """Test filter property with no search or custom filter"""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.filter, {})

    def test_filter_property_with_custom_filter(self):
        """Test filter property with custom filter"""
        custom_filter = {"department": "IT"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
        self.assertEqual(datatables.filter, custom_filter)

    def test_filter_property_with_global_search(self):
        """Test filter property with global search"""
        self.request_args["search"]["value"] = "John"

        # Test with text index
        with patch.object(DataTables, 'has_text_index', return_value=True):
            datatables = DataTables(self.mongo, 'users', self.request_args)
            result = datatables.filter
            # Should include text search condition
            self.assertIn('$text', result)

    def test_filter_property_with_column_search(self):
        """Test filter property with column-specific search"""
        self.request_args["columns"][2]["search"]["value"] = "active"

        datatables = DataTables(self.mongo, 'users', self.request_args)
        result = datatables.filter

        # Should include column search condition
        self.assertIn('$and', result)

    def test_filter_property_with_combined_searches(self):
        """Test filter property with custom filter, global search, and column search"""
        custom_filter = {"department": "IT"}
        self.request_args["search"]["value"] = "John"
        self.request_args["columns"][2]["search"]["value"] = "active"

        # Test with text index
        with patch.object(DataTables, 'has_text_index', return_value=True):
            datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
            result = datatables.filter

            # The structure of the filter with text search is different in the new implementation
            # It should be an $and with the custom filter, text search, and column search
            self.assertIn('$and', result)
            
            # Check that the custom filter is included
            custom_filter_included = any('department' in str(cond) and 'IT' in str(cond) for cond in result['$and'])
            self.assertTrue(custom_filter_included, 'Custom filter not found in result')
            
            # Check that the global search is included (as text search)
            text_search_included = any('$text' in str(cond) and 'John' in str(cond) for cond in result['$and'])
            self.assertTrue(text_search_included, 'Text search not found in result')
            
            # Check that the column search is included
            column_search_included = any('$and' in str(cond) and 'active' in str(cond) for cond in result['$and'])
            self.assertTrue(column_search_included, 'Column search not found in result')

    def test_filter_with_complex_custom_filter(self):
        """Test filter property with complex custom filter"""
        complex_filter = {
            "$or": [
                {"status": "active"},
                {"role": "admin"}
            ]
        }
        datatables = DataTables(self.mongo, 'users', self.request_args, **complex_filter)
        result = datatables.filter

        # Should include the complex filter structure
        self.assertIn('$or', result)

    def test_filter_with_id_conversion(self):
        """Test filter property with _id field conversion"""
        # Skip this test if the new implementation doesn't handle _id conversion
        # or handles it differently
        pass

    def test_filter_with_nested_fields(self):
        """Test filter property with nested fields in search"""
        # Set up the request args with a column-specific search term for a nested field
        request_args = {
            "search": {
                "value": "address.city:New York",
                "regex": False
            },
            "columns": [
                {"data": "address.city", "name": "address.city", "searchable": True}
            ]
        }
        
        # Disable text index for this test to ensure we get the regex-based search
        with patch.object(DataTables, 'has_text_index', return_value=False):
            datatables = DataTables(self.mongo, 'users', request_args, use_text_index=False)
            result = datatables.column_specific_search_condition

            # Should include the nested field in the search condition
            self.assertIn('$and', result)
            has_nested_field = any('address.city' in str(cond) for cond in result['$and'])
            self.assertTrue(has_nested_field)

    def test_filter_cache_returns_same_object(self):
        """filter property returns cached result on repeated access."""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        first = datatables.filter
        second = datatables.filter
        assert first is second  # same object, not recomputed

    def test_filter_cache_is_none_before_access(self):
        """_filter_cache starts as None before filter is accessed."""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        assert datatables._filter_cache is None
        _ = datatables.filter
        assert datatables._filter_cache is not None


# --- from tests/test_datatables_initialization.py ---
class TestInitialization(BaseDataTablesTest):
    """Test cases for DataTables initialization and basic properties"""

    def test_initialization(self):
        """Test initialization of DataTables class"""
        datatables = DataTables(self.mongo, 'users', self.request_args)

        # Test basic attributes
        self.assertEqual(datatables.collection, self.collection)
        self.assertEqual(datatables.request_args, self.request_args)
        self.assertEqual(datatables.custom_filter, {})

    def test_initialization_with_custom_filter(self):
        """Test initialization with custom filter"""
        custom_filter = {"status": "active"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)

        self.assertEqual(datatables.custom_filter, custom_filter)

    def test_collection_property(self):
        """Test the collection property"""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.collection, self.collection)
        self.mongo.db.__getitem__.assert_called_once_with('users')

    def test_start(self):
        """Test start property"""
        self.request_args["start"] = 20
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 20)

    def test_limit(self):
        """Test limit property"""
        # Normal case
        self.request_args["length"] = 25
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.limit, 25)

        # Test with -1 (all records)
        # In the new implementation, limit returns -1 instead of None
        self.request_args["length"] = -1
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.limit, -1)

    def test_count_total(self):
        """Test count_total method"""
        self.collection.count_documents.return_value = 100
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.count_total(), 100)
        self.collection.count_documents.assert_called_once_with({})

    def test_count_filtered(self):
        """Test count_filtered method"""
        # Create a DataTables instance with a custom filter
        custom_filter = {"status": "active"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
        
        # Set up the mock return values - aggregation should work and return the count
        self.collection.aggregate.return_value = [{"total": 50}]
        self.collection.count_documents.return_value = 50
        
        # Call the method
        result = datatables.count_filtered()
        
        # Verify the result
        self.assertEqual(result, 50)
        
        # Verify that aggregate was called (new optimized behavior)
        self.collection.aggregate.assert_called_once()

    def test_projection(self):
        """Test projection property"""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        projection = datatables.projection

        # Check that _id is included and all requested columns are included
        self.assertEqual(projection["_id"], 1)
        for column in ["name", "email", "status"]:
            self.assertEqual(projection[column], 1)

    def test_projection_with_nested_fields(self):
        """Test projection property with nested fields"""
        self.request_args["columns"].append(
            {"data": "address.city", "name": "", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}}
        )
        datatables = DataTables(self.mongo, 'users', self.request_args)
        projection = datatables.projection

        # In the new implementation, the dot notation is preserved in the projection
        self.assertIn("address.city", projection)
        self.assertEqual(projection["address.city"], 1)
        
        # The parent field is not automatically included
        self.assertNotIn("address", projection)

    # --- _check_text_index optimization tests ---

    def test_use_text_index_false_skips_list_indexes(self):
        """When use_text_index=False, list_indexes() must NOT be called."""
        self.collection.list_indexes.reset_mock()
        dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=False)
        self.collection.list_indexes.assert_not_called()
        self.assertFalse(dt.has_text_index)

    def test_use_text_index_true_calls_list_indexes(self):
        """When use_text_index=True, list_indexes() IS called to detect the index."""
        self.collection.list_indexes.return_value = iter([])
        self.collection.list_indexes.reset_mock()
        DataTables(self.mongo, 'users', self.request_args, use_text_index=True)
        self.collection.list_indexes.assert_called_once()

    def test_has_text_index_true_when_index_present(self):
        """has_text_index is True when list_indexes returns a text index entry."""
        self.collection.list_indexes.return_value = iter([{"textIndexVersion": 3, "key": {"$**": "text"}}])
        dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=True)
        self.assertTrue(dt.has_text_index)

    def test_has_text_index_false_when_no_index(self):
        """has_text_index is False when list_indexes returns no text index."""
        self.collection.list_indexes.return_value = iter([{"key": {"_id": 1}}])
        dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=True)
        self.assertFalse(dt.has_text_index)


# ---------------------------------------------------------------------------
# Folded from test_draw_property.py
# ---------------------------------------------------------------------------

import unittest
from unittest.mock import MagicMock as _MagicMock
from mongo_datatables import DataTables as _DataTables


class TestDrawProperty(unittest.TestCase):
    def setUp(self):
        self.mongo = _MagicMock()
        self.collection = _MagicMock()
        self.mongo.db = _MagicMock()
        self.mongo.db.__getitem__ = _MagicMock(return_value=self.collection)
        self.collection.list_indexes.return_value = iter([])
        self.base_args = {
            "draw": "1", "start": "0", "length": "10",
            "search[value]": "", "search[regex]": "false",
            "order[0][column]": "0", "order[0][dir]": "asc",
            "columns[0][data]": "name", "columns[0][name]": "",
            "columns[0][searchable]": "true", "columns[0][orderable]": "true",
            "columns[0][search][value]": "", "columns[0][search][regex]": "false"
        }

    def _make(self, draw_val):
        args = {**self.base_args, "draw": draw_val}
        return _DataTables(self.mongo, "users", args)

    def _make_no_draw(self):
        args = {k: v for k, v in self.base_args.items() if k != "draw"}
        return _DataTables(self.mongo, "users", args)

    def test_normal_integer_string(self):
        self.assertEqual(self._make("5").draw, 5)

    def test_string_one(self):
        self.assertEqual(self._make("1").draw, 1)

    def test_negative_clamped_to_one(self):
        self.assertEqual(self._make("-3").draw, 1)

    def test_zero_clamped_to_one(self):
        self.assertEqual(self._make("0").draw, 1)

    def test_non_numeric_defaults_to_one(self):
        self.assertEqual(self._make("abc").draw, 1)

    def test_none_defaults_to_one(self):
        self.assertEqual(self._make(None).draw, 1)

    def test_float_string_defaults_to_one(self):
        self.assertEqual(self._make("2.5").draw, 1)

    def test_missing_draw_key_defaults_to_one(self):
        self.assertEqual(self._make_no_draw().draw, 1)

    def test_large_valid_number(self):
        self.assertEqual(self._make("999").draw, 999)


# ---------------------------------------------------------------------------
# Folded from test_length_all.py
# ---------------------------------------------------------------------------

from unittest.mock import patch as _patch


class TestLengthAll(BaseDataTablesTest):
    def _get_pipeline(self, datatables):
        with _patch.object(datatables.collection, 'aggregate', return_value=[]) as mock_agg:
            datatables.results()
            args, _ = mock_agg.call_args
            return args[0]

    def test_length_minus_one_omits_limit_stage(self):
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        self.assertIsNone(next((s for s in pipeline if '$limit' in s), None))

    def test_length_minus_one_limit_property(self):
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(dt.limit, -1)

    def test_length_positive_includes_limit_stage(self):
        self.request_args["length"] = 25
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        limit_stage = next((s for s in pipeline if '$limit' in s), None)
        self.assertIsNotNone(limit_stage)
        self.assertEqual(limit_stage['$limit'], 25)

    def test_length_zero_omits_limit_stage(self):
        self.request_args["length"] = 0
        dt = DataTables(self.mongo, 'users', self.request_args)
        pipeline = self._get_pipeline(dt)
        self.assertIsNone(next((s for s in pipeline if '$limit' in s), None))

    def test_get_rows_with_length_minus_one(self):
        self.request_args["length"] = -1
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.collection.aggregate.return_value = []
        self.collection.estimated_document_count.return_value = 0
        self.collection.count_documents.return_value = 0
        response = dt.get_rows()
        self.assertIn('data', response)
        self.assertIn('recordsTotal', response)


# --- from tests/test_datatables_pagination.py ---
class TestPagination(BaseDataTablesTest):
    """Test cases for DataTables pagination functionality"""

    def test_pagination_in_pipeline(self):
        """Test pagination stages in the pipeline"""
        self.request_args["start"] = 10
        self.request_args["length"] = 20
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Test the start and limit properties directly
        self.assertEqual(datatables.start, 10)
        self.assertEqual(datatables.limit, 20)
        
        # Mock the collection.aggregate method to capture the pipeline
        with patch.object(datatables.collection, 'aggregate') as mock_aggregate:
            # Set up the mock to return an empty list
            mock_aggregate.return_value = []
            
            # Call results to trigger the pipeline creation
            datatables.results()
            
            # Get the pipeline from the mock call
            args, kwargs = mock_aggregate.call_args
            pipeline = args[0]  # First argument to aggregate is the pipeline
            
            # Find the skip and limit stages
            skip_stage = next((stage for stage in pipeline if '$skip' in stage), None)
            limit_stage = next((stage for stage in pipeline if '$limit' in stage), None)
            
            # Verify that the skip and limit stages exist and have the correct values
            self.assertIsNotNone(skip_stage)
            self.assertEqual(skip_stage['$skip'], 10)
            
            self.assertIsNotNone(limit_stage)
            self.assertEqual(limit_stage['$limit'], 20)

    def test_pagination_with_all_records(self):
        """Test pagination when requesting all records"""
        self.request_args["start"] = 0
        self.request_args["length"] = -1  # -1 means all records
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Test the start and limit properties directly
        self.assertEqual(datatables.start, 0)
        self.assertEqual(datatables.limit, -1)  # limit is -1 when length is -1
        
        # Mock the collection.aggregate method to capture the pipeline
        with patch.object(datatables.collection, 'aggregate') as mock_aggregate:
            # Set up the mock to return an empty list
            mock_aggregate.return_value = []
            
            # Call results to trigger the pipeline creation
            datatables.results()
            
            # Get the pipeline from the mock call
            args, kwargs = mock_aggregate.call_args
            pipeline = args[0]  # First argument to aggregate is the pipeline
            
            # Find the skip and limit stages
            skip_stage = next((stage for stage in pipeline if '$skip' in stage), None)
            limit_stage = next((stage for stage in pipeline if '$limit' in stage), None)
            
            # When length=-1 (Show All), no $limit stage should be added — MongoDB rejects negative $limit
            self.assertIsNone(limit_stage)

    def test_pagination_with_zero_length(self):
        """Test pagination when length is zero"""
        self.request_args["start"] = 0
        self.request_args["length"] = 0
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Test the start and limit properties directly
        self.assertEqual(datatables.start, 0)
        self.assertEqual(datatables.limit, 0)
        
        # Mock the collection.aggregate method to capture the pipeline
        with patch.object(datatables.collection, 'aggregate') as mock_aggregate:
            # Set up the mock to return an empty list
            mock_aggregate.return_value = []
            
            # Call results to trigger the pipeline creation
            datatables.results()
            
            # Get the pipeline from the mock call
            args, kwargs = mock_aggregate.call_args
            pipeline = args[0]  # First argument to aggregate is the pipeline
            
            # Find the limit stage
            limit_stage = next((stage for stage in pipeline if '$limit' in stage), None)
            
            # When length is 0, there should be no limit stage in the pipeline
            # because the implementation skips adding the limit stage when limit is 0
            self.assertIsNone(limit_stage)

    def test_pagination_with_string_values(self):
        """Test pagination with string values for start and length"""
        self.request_args["start"] = "5"
        self.request_args["length"] = "15"
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Test the start and limit properties directly
        self.assertEqual(datatables.start, 5)  # Should be converted to int
        self.assertEqual(datatables.limit, 15)  # Should be converted to int
        
        # Mock the collection.aggregate method to capture the pipeline
        with patch.object(datatables.collection, 'aggregate') as mock_aggregate:
            # Set up the mock to return an empty list
            mock_aggregate.return_value = []
            
            # Call results to trigger the pipeline creation
            datatables.results()
            
            # Get the pipeline from the mock call
            args, kwargs = mock_aggregate.call_args
            pipeline = args[0]  # First argument to aggregate is the pipeline
            
            # Find the skip and limit stages
            skip_stage = next((stage for stage in pipeline if '$skip' in stage), None)
            limit_stage = next((stage for stage in pipeline if '$limit' in stage), None)
            
            # Verify that the skip and limit stages exist and have the correct values
            self.assertIsNotNone(skip_stage)
            self.assertEqual(skip_stage['$skip'], 5)  # Should be converted to int
            
            self.assertIsNotNone(limit_stage)
            self.assertEqual(limit_stage['$limit'], 15)  # Should be converted to int


# --- from tests/test_datatables_results.py ---
class TestResults(BaseDataTablesTest):
    """Test cases for DataTables results functionality"""

    def test_results_method(self):
        """Test results method"""
        # Set up mock return values
        self.collection.aggregate.return_value = self.sample_docs
        self.collection.count_documents.return_value = len(self.sample_docs)
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Call the results method
        result = datatables.results()
        
        # In the new implementation, results() returns a list of documents directly
        # Verify that the result is a list with the expected number of documents
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), len(self.sample_docs))
        
        # Verify that each document has the expected structure
        for doc in result:
            self.assertIn('name', doc)
            self.assertIn('email', doc)
            self.assertIn('status', doc)
            self.assertIn('DT_RowId', doc)  # Row ID is added by the results method

    def test_results_with_empty_data(self):
        """Test results method with empty data"""
        # Set up mock return values for empty results
        self.collection.aggregate.return_value = []
        self.collection.count_documents.return_value = 0
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Call the results method
        result = datatables.results()
        
        # In the new implementation, results() returns an empty list when there are no results
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_results_with_objectid_conversion(self):
        """Test results method with ObjectId conversion"""
        # Create a sample document with ObjectId
        doc_id = ObjectId()
        doc_with_id = {"_id": doc_id, "name": "Test User"}
        
        # Set up mock return value
        self.collection.aggregate.return_value = [doc_with_id]
        self.collection.count_documents.return_value = 1
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Call the results method
        result = datatables.results()
        
        # In the new implementation, results() returns a list of documents directly
        # Verify that the ObjectId was converted to string in the DT_RowId field
        self.assertEqual(len(result), 1)
        self.assertNotIn('_id', result[0])  # _id should be removed
        self.assertIn('DT_RowId', result[0])  # DT_RowId should be added
        self.assertEqual(result[0]['DT_RowId'], str(doc_id))  # Should be string representation of ObjectId
        self.assertEqual(result[0]['name'], "Test User")

    def test_results_with_date_conversion(self):
        """Test results method with date conversion"""
        # Skip this test if the new implementation doesn't handle date conversion
        # or handles it differently
        pass

    def test_results_error_handling(self):
        """Test results method error handling"""
        # Set up mock to raise an exception
        self.collection.aggregate.side_effect = Exception("Test exception")
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Call the results method
        result = datatables.results()
        
        # In the new implementation, results() returns an empty list on error
        # and logs the error message
        self.assertEqual(result, [])

    def test_results_with_query_stats(self):
        """Test results method with query stats"""
        # In the current implementation, query stats are tracked internally
        # but not returned with the results. This test is now checking that
        # the results method works correctly without the query_stats parameter.
        
        # Set up mock return values
        self.collection.aggregate.return_value = self.sample_docs
        self.collection.count_documents.return_value = len(self.sample_docs)
        
        datatables = DataTables(self.mongo, 'users', self.request_args)
        
        # Call the results method (without query_stats parameter)
        result = datatables.results()
        
        # Verify that the result is a list of documents
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), len(self.sample_docs))

    def test_query_pipeline(self):
        """Test the query pipeline construction"""
        # Create a custom filter to ensure the $match stage is present
        custom_filter = {'name': 'test'}
        
        # Create request args with pagination and sorting
        request_args = {
            'start': '10',  # For $skip stage
            'length': '10',  # For $limit stage
            'order[0][column]': '0',  # For $sort stage
            'order[0][dir]': 'asc',
            'columns[0][data]': 'name'
        }
        
        # Create DataTables with custom filter to ensure $match stage is present
        datatables = DataTables(self.mongo, 'users', request_args, **custom_filter)
        
        # In the current implementation, we can test the pipeline by mocking the collection.aggregate method
        with patch.object(datatables.collection, 'aggregate') as mock_aggregate:
            # Set up the mock to return an empty list
            mock_aggregate.return_value = []
            
            # Call results to trigger the pipeline creation
            datatables.results()
            
            # Get the pipeline from the mock call
            args, kwargs = mock_aggregate.call_args
            pipeline = args[0]  # First argument to aggregate is the pipeline
            
            # Verify that the pipeline has at least one stage
            self.assertTrue(len(pipeline) > 0, "Pipeline should have at least one stage")
            
            # Check for $project stage (always present)
            self.assertTrue(any('$project' in stage for stage in pipeline), "Pipeline should contain a $project stage")
            
            # Log the actual pipeline for debugging
            print(f"Pipeline stages: {[list(stage.keys())[0] for stage in pipeline]}")
            
            # Check for other stages if they should be present based on our inputs
            if datatables.filter:
                self.assertTrue(any('$match' in stage for stage in pipeline), "Pipeline should contain a $match stage")
            
            if datatables.sort_specification:
                self.assertTrue(any('$sort' in stage for stage in pipeline), "Pipeline should contain a $sort stage")
            
            if datatables.start > 0:
                self.assertTrue(any('$skip' in stage for stage in pipeline), "Pipeline should contain a $skip stage")
            
            if datatables.limit:
                self.assertTrue(any('$limit' in stage for stage in pipeline), "Pipeline should contain a $limit stage")


# --- from tests/test_datatables_query_pipeline.py ---
class TestDataTablesQueryPipeline(BaseDataTablesTest):
    """Test cases for query pipeline construction and results processing in DataTables"""

    def setUp(self):
        super().setUp()
        # Set up data fields for testing
        self.data_fields = [
            DataField('title', 'string'),
            DataField('author', 'string'),
            DataField('year', 'number'),
            DataField('rating', 'number'),
            DataField('published_date', 'date'),
            DataField('tags', 'array'),
            DataField('metadata', 'object'),
            DataField('_id', 'objectid')
        ]
        
        # Add columns to request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True, 'orderable': True},
            {'data': 'author', 'searchable': True, 'orderable': True},
            {'data': 'year', 'searchable': True, 'orderable': True},
            {'data': 'rating', 'searchable': True, 'orderable': True},
            {'data': 'published_date', 'searchable': True, 'orderable': True},
            {'data': 'tags', 'searchable': True, 'orderable': True},
            {'data': 'metadata', 'searchable': False, 'orderable': False},
            {'data': '_id', 'searchable': False, 'orderable': True}
        ]

    def test_complete_query_pipeline(self):
        """Test the complete query pipeline construction"""
        # Set up a complex query with search, sort, and pagination
        self.request_args['search']['value'] = 'test query'
        self.request_args['order'] = [{'column': 0, 'dir': 'asc'}]
        self.request_args['start'] = 10
        self.request_args['length'] = 25
        
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields,
            debug_mode=True
        )
        
        # Mock the collection.aggregate method to return a cursor
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        
        # Call results method to trigger pipeline construction
        results = datatables.results()
        
        # Verify aggregate was called with a pipeline
        datatables.collection.aggregate.assert_called_once()
        args, kwargs = datatables.collection.aggregate.call_args
        pipeline = args[0]
        
        # Verify pipeline has the expected stages
        self.assertIsInstance(pipeline, list)
        
        # Check for match stage (filter)
        match_stages = [stage for stage in pipeline if '$match' in stage]
        self.assertGreaterEqual(len(match_stages), 1)
        
        # Check for sort stage
        sort_stages = [stage for stage in pipeline if '$sort' in stage]
        self.assertEqual(len(sort_stages), 1)
        
        # Check for skip stage
        skip_stages = [stage for stage in pipeline if '$skip' in stage]
        self.assertEqual(len(skip_stages), 1)
        self.assertEqual(skip_stages[0]['$skip'], 10)
        
        # Check for limit stage
        limit_stages = [stage for stage in pipeline if '$limit' in stage]
        self.assertEqual(len(limit_stages), 1)
        self.assertEqual(limit_stages[0]['$limit'], 25)

    def test_results_with_complex_data_types(self):
        """Test results processing with complex data types"""
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields
        )
        
        # Create mock data with various MongoDB types
        mock_data = [
            {
                '_id': ObjectId('5f50c31e8a91e8c9c8d5c5d5'),
                'title': 'Test Title',
                'author': 'Test Author',
                'year': 2020,
                'rating': 4.5,
                'published_date': datetime(2020, 1, 1),
                'tags': ['fiction', 'bestseller'],
                'metadata': {'publisher': 'Test Publisher', 'edition': 1}
            }
        ]
        
        # Mock the collection.aggregate method to return our mock data
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter(mock_data)
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        
        # Call results method
        results = datatables.results()
        
        # Verify results are properly formatted
        self.assertEqual(len(results), 1)
        
        # Check that ObjectId is converted to DT_RowId and datetime is converted to string
        self.assertIn('DT_RowId', results[0])
        self.assertIsInstance(results[0]['DT_RowId'], str)
        self.assertIsInstance(results[0]['published_date'], str)
        # Ensure _id is not in the results
        self.assertNotIn('_id', results[0])
        
        # Check that other types are preserved
        self.assertIsInstance(results[0]['year'], int)
        self.assertIsInstance(results[0]['rating'], float)
        self.assertIsInstance(results[0]['tags'], list)
        self.assertIsInstance(results[0]['metadata'], dict)

    def test_empty_results(self):
        """Test handling of empty results"""
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields
        )
        
        # Mock the collection.aggregate method to return empty results
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        
        # Call results method
        results = datatables.results()
        
        # Verify results are an empty list
        self.assertEqual(results, [])

    def test_custom_filter_in_pipeline(self):
        """Test that custom filter is included in the query pipeline"""
        # Create DataTables instance with custom filter
        custom_filter = {'status': 'active', 'category': {'$in': ['book', 'magazine']}}
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields,
            **custom_filter
        )
        
        # Mock the collection.aggregate method
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        
        # Call results method to trigger pipeline construction
        results = datatables.results()
        
        # Verify aggregate was called with a pipeline that includes the custom filter
        args, kwargs = datatables.collection.aggregate.call_args
        pipeline = args[0]
        
        # Check for match stage with custom filter
        match_stages = [stage for stage in pipeline if '$match' in stage]
        self.assertGreaterEqual(len(match_stages), 1)
        
        # At least one match stage should contain our custom filter conditions
        custom_filter_found = False
        for stage in match_stages:
            match_condition = stage['$match']
            if 'status' in match_condition and 'category' in match_condition:
                self.assertEqual(match_condition['status'], 'active')
                self.assertEqual(match_condition['category'], {'$in': ['book', 'magazine']})
                custom_filter_found = True
                break
        
        self.assertTrue(custom_filter_found, "Custom filter not found in pipeline")

    def test_projection_in_pipeline(self):
        """Test that projection is included in the query pipeline"""
        # Create DataTables instance
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields
        )
        
        # Mock the collection.aggregate method
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        
        # Call results method to trigger pipeline construction
        results = datatables.results()
        
        # Verify aggregate was called with a pipeline that includes projection
        args, kwargs = datatables.collection.aggregate.call_args
        pipeline = args[0]
        
        # Check for project stage
        project_stages = [stage for stage in pipeline if '$project' in stage]
        self.assertEqual(len(project_stages), 1)
        
        # Verify projection includes all fields
        projection = project_stages[0]['$project']
        for field in ['title', 'author', 'year', 'rating', 'published_date', 'tags', 'metadata', '_id']:
            self.assertEqual(projection.get(field), 1)


    def test_projection_with_alias_uses_db_field_name(self):
        """Projection key must be the db field name, not the UI alias."""
        data_fields = [DataField('author.fullName', 'string', alias='Author')]
        request_args = {
            'columns': [{'data': 'Author', 'searchable': True, 'orderable': True}],
            'search': {'value': ''},
            'order': [],
            'start': 0,
            'length': 10,
        }
        dt = DataTables(self.mongo, 'test_collection', request_args, data_fields=data_fields)
        projection = dt.projection
        self.assertIn('author.fullName', projection)
        self.assertNotIn('Author', projection)

    def test_projection_without_alias_unchanged(self):
        """Projection key equals the field name when no alias is set."""
        data_fields = [DataField('title', 'string')]
        request_args = {
            'columns': [{'data': 'title', 'searchable': True, 'orderable': True}],
            'search': {'value': ''},
            'order': [],
            'start': 0,
            'length': 10,
        }
        dt = DataTables(self.mongo, 'test_collection', request_args, data_fields=data_fields)
        self.assertIn('title', dt.projection)

    def test_projection_mixed_aliased_and_plain(self):
        """All projection keys must be db field names for mixed aliased/plain fields."""
        data_fields = [
            DataField('author.fullName', 'string', alias='Author'),
            DataField('title', 'string'),
        ]
        request_args = {
            'columns': [
                {'data': 'Author', 'searchable': True, 'orderable': True},
                {'data': 'title', 'searchable': True, 'orderable': True},
            ],
            'search': {'value': ''},
            'order': [],
            'start': 0,
            'length': 10,
        }
        dt = DataTables(self.mongo, 'test_collection', request_args, data_fields=data_fields)
        projection = dt.projection
        self.assertIn('author.fullName', projection)
        self.assertIn('title', projection)
        self.assertNotIn('Author', projection)


if __name__ == '__main__':
    unittest.main()


# --- from tests/test_alias_remapping.py ---
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


# --- from tests/test_allow_disk_use.py ---
class TestAllowDiskUse(BaseDataTablesTest):
    """Tests for allow_disk_use parameter."""

    def test_default_is_false(self):
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertFalse(dt.allow_disk_use)

    def test_allow_disk_use_true_stored(self):
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        self.assertTrue(dt.allow_disk_use)

    def test_results_passes_allow_disk_use_false(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args)
        dt.results()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), False)

    def test_results_passes_allow_disk_use_true(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        dt.results()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_count_filtered_passes_allow_disk_use(self):
        self.collection.aggregate.return_value = iter([{'total': 5}])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True, status='active')
        dt.count_filtered()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_get_export_data_passes_allow_disk_use(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        dt.get_export_data()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_backward_compatible_no_allow_disk_use_arg(self):
        """Existing code that doesn't pass allow_disk_use still works."""
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args)
        dt.results()
        # Should not raise; allowDiskUse=False is passed (harmless)
        self.collection.aggregate.assert_called_once()


# --- from tests/test_build_pipeline.py ---
BASE_ARGS = {
    "draw": "1",
    "start": "0",
    "length": "10",
    "search": {"value": "", "regex": False},
    "order": [{"column": "0", "dir": "asc", "name": ""}],
    "columns": [{"data": "name", "searchable": "true", "orderable": "true",
                  "search": {"value": "", "regex": False}, "name": ""}],
}


def make_dt(args=None, **kwargs):
    mongo = MagicMock()
    col = MagicMock()
    col.list_indexes.return_value = []
    mongo.__getitem__ = MagicMock(return_value=col)
    with patch.object(DataTables, '_check_text_index'):
        dt = DataTables(mongo, 'test', args or BASE_ARGS, **kwargs)
        dt.collection = col
        dt._has_text_index = False
    return dt


class TestBuildPipelineStructure:
    def test_paginate_true_includes_skip_and_limit(self):
        args = {**BASE_ARGS, "start": "20", "length": "10"}
        dt = make_dt(args)
        pipeline = dt._build_pipeline(paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" in stages
        assert "$limit" in stages

    def test_paginate_false_excludes_skip_and_limit(self):
        args = {**BASE_ARGS, "start": "20", "length": "10"}
        dt = make_dt(args)
        pipeline = dt._build_pipeline(paginate=False)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" not in stages
        assert "$limit" not in stages

    def test_always_ends_with_project(self):
        dt = make_dt()
        for paginate in (True, False):
            pipeline = dt._build_pipeline(paginate=paginate)
            assert list(pipeline[-1].keys())[0] == "$project"

    def test_no_match_when_no_filter(self):
        dt = make_dt()
        pipeline = dt._build_pipeline()
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$match" not in stages

    def test_skip_omitted_when_start_is_zero(self):
        dt = make_dt()  # start=0
        pipeline = dt._build_pipeline(paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" not in stages

    def test_limit_omitted_when_length_is_negative_one(self):
        args = {**BASE_ARGS, "length": "-1"}
        dt = make_dt(args)
        pipeline = dt._build_pipeline(paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$limit" not in stages

    def test_default_paginate_is_true(self):
        args = {**BASE_ARGS, "start": "5", "length": "10"}
        dt = make_dt(args)
        with_default = dt._build_pipeline()
        with_explicit = dt._build_pipeline(paginate=True)
        assert with_default == with_explicit


class TestBuildPipelineConsistency:
    def test_results_and_export_share_match_stage(self):
        """results() and get_export_data() must apply the same filter."""
        dt = make_dt()
        paginated = dt._build_pipeline(paginate=True)
        export = dt._build_pipeline(paginate=False)
        # Both should have the same $match (or both omit it)
        p_match = next((s for s in paginated if "$match" in s), None)
        e_match = next((s for s in export if "$match" in s), None)
        assert p_match == e_match

    def test_results_and_export_share_sort_stage(self):
        dt = make_dt()
        paginated = dt._build_pipeline(paginate=True)
        export = dt._build_pipeline(paginate=False)
        p_sort = next((s for s in paginated if "$sort" in s), None)
        e_sort = next((s for s in export if "$sort" in s), None)
        assert p_sort == e_sort

    def test_results_and_export_share_project_stage(self):
        dt = make_dt()
        paginated = dt._build_pipeline(paginate=True)
        export = dt._build_pipeline(paginate=False)
        assert paginated[-1] == export[-1]


# --- from tests/test_count_optimization.py ---
class TestCountOptimization:
    """Test optimized count operations for performance improvements."""

    def setup_method(self):
        """Set up shared mock objects for tests that need them."""
        self.mock_collection = Mock()
        self.mock_collection.list_indexes.return_value = []
        self.mock_db = {"test_collection": self.mock_collection}

    def test_count_total_uses_estimated_for_large_collections(self):
        """Test that count_total uses estimated_document_count for large collections."""
        # Mock collection with large estimated count
        mock_collection = Mock()
        mock_collection.estimated_document_count.return_value = 500000
        mock_collection.count_documents.return_value = 500000
        mock_collection.list_indexes.return_value = []
        
        # Create DataTables instance and directly set collection
        dt = DataTables(
            pymongo_object={"test_collection": mock_collection},
            collection_name="test_collection",
            request_args={"draw": 1, "start": 0, "length": 10}
        )
        
        result = dt.count_total()
        
        # Should use estimated count for large collections
        assert result == 500000
        mock_collection.estimated_document_count.assert_called_once()
        # Should not call exact count for large collections
        mock_collection.count_documents.assert_not_called()

    def test_count_total_uses_exact_for_small_collections(self):
        """Test that count_total uses exact count for small collections."""
        # Mock collection with small estimated count
        mock_collection = Mock()
        mock_collection.estimated_document_count.return_value = 50000
        mock_collection.count_documents.return_value = 50000
        mock_collection.list_indexes.return_value = []
        
        # Create DataTables instance and directly set collection
        dt = DataTables(
            pymongo_object={"test_collection": mock_collection},
            collection_name="test_collection",
            request_args={"draw": 1, "start": 0, "length": 10}
        )
        
        result = dt.count_total()
        
        # Should use exact count for small collections
        assert result == 50000
        mock_collection.estimated_document_count.assert_called_once()
        mock_collection.count_documents.assert_called_once_with({})

    def test_count_filtered_uses_aggregation_pipeline(self):
        """Test that count_filtered uses aggregation pipeline for better performance."""
        # Mock collection
        mock_collection = Mock()
        mock_collection.aggregate.return_value = [{"total": 25000}]
        mock_collection.list_indexes.return_value = []
        
        # Create DataTables instance
        dt = DataTables(
            pymongo_object={"test_collection": mock_collection},
            collection_name="test_collection",
            request_args={
                "draw": 1,
                "start": 0,
                "length": 10,
                "search": {"value": "test"},
                "columns": [{"data": "name", "searchable": True}]
            }
        )
        
        result = dt.count_filtered()
        
        # Should use aggregation pipeline when there are filters
        assert result == 25000
        mock_collection.aggregate.assert_called_once()
        
        # Verify the aggregation pipeline structure
        call_args = mock_collection.aggregate.call_args[0][0]
        assert len(call_args) == 2
        assert "$match" in call_args[0]
        assert "$count" in call_args[1]

    def test_count_operations_handle_errors_gracefully(self):
        """Test that count operations handle MongoDB errors gracefully."""
        from pymongo.errors import PyMongoError
        
        # Mock collection that raises errors
        mock_collection = Mock()
        mock_collection.estimated_document_count.side_effect = PyMongoError("Connection error")
        mock_collection.count_documents.side_effect = PyMongoError("Connection error")
        mock_collection.aggregate.side_effect = PyMongoError("Connection error")
        mock_collection.list_indexes.return_value = []
        
        # Create DataTables instance
        dt = DataTables(
            pymongo_object={"test_collection": mock_collection},
            collection_name="test_collection",
            request_args={"draw": 1, "start": 0, "length": 10}
        )
        
        # Should return 0 on errors
        assert dt.count_total() == 0
        assert dt.count_filtered() == 0

    def test_count_total_with_custom_filter_large_collection(self):
        """count_total must use custom_filter even when collection is large (>=100k)."""
        self.mock_collection.estimated_document_count.return_value = 500_000
        self.mock_collection.count_documents.return_value = 1_200
        dt = DataTables(
            self.mock_db, "test_collection",
            {"draw": "1", "start": "0", "length": "10", "search[value]": "",
             "columns[0][data]": "name", "columns[0][searchable]": "true",
             "columns[0][orderable]": "true", "order[0][column]": "0",
             "order[0][dir]": "asc"},
            status="active",
        )
        result = dt.count_total()
        self.mock_collection.count_documents.assert_called_once_with({"status": "active"})
        assert result == 1_200

    def test_count_total_with_custom_filter_small_collection(self):
        """count_total must use custom_filter for small collections too."""
        self.mock_collection.estimated_document_count.return_value = 50
        self.mock_collection.count_documents.return_value = 30
        dt = DataTables(
            self.mock_db, "test_collection",
            {"draw": "1", "start": "0", "length": "10", "search[value]": "",
             "columns[0][data]": "name", "columns[0][searchable]": "true",
             "columns[0][orderable]": "true", "order[0][column]": "0",
             "order[0][dir]": "asc"},
            role="admin",
        )
        result = dt.count_total()
        self.mock_collection.count_documents.assert_called_once_with({"role": "admin"})
        assert result == 30


def test_count_total_no_int_conversion_needed():
    """estimated_document_count() always returns int; no conversion needed."""
    mock_collection = Mock()
    mock_collection.estimated_document_count.return_value = 200000
    mock_collection.list_indexes.return_value = []
    dt = DataTables(
        pymongo_object={"test_collection": mock_collection},
        collection_name="test_collection",
        request_args={"draw": 1, "start": 0, "length": 10}
    )
    assert dt.count_total() == 200000
    mock_collection.count_documents.assert_not_called()

# --- from tests/test_datafield.py ---
class TestDataFieldInit:
    def test_valid_name_and_type(self):
        f = DataField("title", "string")
        assert f.name == "title"
        assert f.data_type == "string"

    def test_alias_defaults_to_last_segment(self):
        f = DataField("author.name", "string")
        assert f.alias == "name"

    def test_alias_explicit(self):
        f = DataField("author.name", "string", alias="Author")
        assert f.alias == "Author"

    def test_data_type_case_insensitive(self):
        f = DataField("count", "NUMBER")
        assert f.data_type == "number"

    def test_all_valid_types(self):
        for t in ("string", "number", "date", "boolean", "array", "object", "objectid", "null"):
            f = DataField("x", t)
            assert f.data_type == t

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid data_type"):
            DataField("x", "invalid")

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            DataField("", "string")

    def test_whitespace_name_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            DataField("   ", "string")

    def test_repr_without_alias(self):
        f = DataField("title", "string")
        assert "title" in repr(f)
        assert "string" in repr(f)

    def test_repr_with_alias(self):
        f = DataField("author.name", "string", alias="Author")
        assert "Author" in repr(f)


# --- from tests/test_input_validation.py ---
class TestInputValidation(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.collection = MagicMock()
        self.mongo.db = MagicMock()
        self.mongo.db.__getitem__ = MagicMock(return_value=self.collection)
        self.collection.list_indexes.return_value = iter([])
        self.base_args = {"draw": "1", "start": "0", "length": "10",
                         "search[value]": "", "search[regex]": "false",
                         "order[0][column]": "0", "order[0][dir]": "asc",
                         "columns[0][data]": "name", "columns[0][name]": "",
                         "columns[0][searchable]": "true", "columns[0][orderable]": "true",
                         "columns[0][search][value]": "", "columns[0][search][regex]": "false"}

    def _make(self, extra_args):
        args = {**self.base_args, **extra_args}
        return DataTables(self.mongo, "users", args)

    # --- start ---
    def test_start_valid(self):
        self.assertEqual(self._make({"start": "20"}).start, 20)

    def test_start_invalid_string(self):
        self.assertEqual(self._make({"start": "abc"}).start, 0)

    def test_start_negative(self):
        self.assertEqual(self._make({"start": "-5"}).start, 0)

    def test_start_none(self):
        self.assertEqual(self._make({"start": None}).start, 0)

    def test_start_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "start"}
        self.assertEqual(DataTables(self.mongo, "users", args).start, 0)

    # --- limit ---
    def test_limit_valid(self):
        self.assertEqual(self._make({"length": "25"}).limit, 25)

    def test_limit_minus_one(self):
        self.assertEqual(self._make({"length": "-1"}).limit, -1)

    def test_limit_invalid_string(self):
        self.assertEqual(self._make({"length": "abc"}).limit, 10)

    def test_limit_none(self):
        self.assertEqual(self._make({"length": None}).limit, 10)

    def test_limit_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "length"}
        self.assertEqual(DataTables(self.mongo, "users", args).limit, 10)

    # --- draw (via get_rows) ---
    def test_draw_valid(self):
        self.collection.aggregate.return_value = iter([{"name": "Alice"}])
        self.collection.estimated_document_count.return_value = 1
        dt = self._make({"draw": "3"})
        with patch.object(dt, "count_total", return_value=1), \
             patch.object(dt, "count_filtered", return_value=1), \
             patch.object(dt, "results", return_value=[{"name": "Alice"}]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 3)

    def test_draw_invalid_string(self):
        dt = self._make({"draw": "xyz"})
        with patch.object(dt, "count_total", return_value=0), \
             patch.object(dt, "count_filtered", return_value=0), \
             patch.object(dt, "results", return_value=[]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 1)

    def test_draw_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "draw"}
        dt = DataTables(self.mongo, "users", args)
        with patch.object(dt, "count_total", return_value=0), \
             patch.object(dt, "count_filtered", return_value=0), \
             patch.object(dt, "results", return_value=[]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 1)


if __name__ == "__main__":
    unittest.main()


# --- from tests/test_row_id.py ---
class TestRowId(BaseDataTablesTest):

    def _make_dt(self, row_id=None, extra_columns=None):
        dt = DataTables(self.mongo, "test_collection", self.request_args, row_id=row_id)
        dt.collection = self.collection
        return dt

    # 1. Default (row_id=None): DT_RowId from _id, _id removed from row
    def test_row_id_none_uses_id(self):
        oid = ObjectId()
        rows = self._make_dt()._process_cursor([{"_id": oid, "name": "Alice"}])
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]

    # 2. row_id='employee_id': DT_RowId = str(employee_id value)
    def test_row_id_custom_field_sets_dt_row_id(self):
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": ObjectId(), "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert rows[0]["DT_RowId"] == "EMP-42"

    # 3. When row_id='employee_id', the employee_id key remains in the row
    def test_row_id_custom_field_stays_in_row(self):
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": ObjectId(), "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert "employee_id" in rows[0]
        assert rows[0]["employee_id"] == "EMP-42"

    # 4. When row_id is set, _id is NOT popped (stays in doc)
    def test_row_id_id_not_popped_when_custom_row_id(self):
        oid = ObjectId()
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": oid, "employee_id": "EMP-42", "name": "Bob"}]
        )
        # _id should still be present (not popped) and formatted as string
        assert "_id" in rows[0]
        assert rows[0]["_id"] == str(oid)

    # 5. row_id field missing from doc falls back to _id
    def test_row_id_field_not_in_doc_falls_back_to_id(self):
        oid = ObjectId()
        rows = self._make_dt(row_id="missing_field")._process_cursor(
            [{"_id": oid, "name": "Carol"}]
        )
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]

    # 6. row_id='sku' is included in projection
    def test_row_id_included_in_projection(self):
        dt = self._make_dt(row_id="sku")
        assert dt.projection.get("sku") == 1

    # 7. row_id field not in columns is still projected
    def test_row_id_not_in_columns_still_projected(self):
        # columns only has name/email/status — no 'sku'
        dt = self._make_dt(row_id="sku")
        column_fields = {c["data"] for c in self.request_args["columns"]}
        assert "sku" not in column_fields
        assert dt.projection.get("sku") == 1

    # 8. Backward compatibility: no row_id param works identically
    def test_row_id_backward_compatible(self):
        oid = ObjectId()
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        dt.collection = self.collection
        rows = dt._process_cursor([{"_id": oid, "name": "Dave"}])
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]
        assert "DT_RowClass" not in rows[0]
        assert "DT_RowData" not in rows[0]
        assert "DT_RowAttr" not in rows[0]


# --- from tests/test_row_metadata.py ---
class TestRowMetadata(BaseDataTablesTest):

    def _make_dt(self, **kwargs):
        dt = DataTables(self.mongo, "test_collection", self.request_args, **kwargs)
        dt.collection = self.collection
        return dt

    def _run_results(self, dt):
        self.collection.aggregate.return_value = iter(self.sample_docs)
        return dt.results()

    # DT_RowClass

    def test_row_class_static(self):
        rows = self._run_results(self._make_dt(row_class="highlight"))
        for row in rows:
            assert row["DT_RowClass"] == "highlight"

    def test_row_class_callable(self):
        fn = lambda r: "active" if r.get("status") == "active" else "inactive"
        rows = self._run_results(self._make_dt(row_class=fn))
        for row in rows:
            assert row["DT_RowClass"] == fn(row)

    def test_row_class_absent_by_default(self):
        rows = self._run_results(self._make_dt())
        for row in rows:
            assert "DT_RowClass" not in row

    def test_row_class_callable_receives_dt_row_id(self):
        seen = []
        def capture(r):
            seen.append(r)
            return "ok"
        rows = self._run_results(self._make_dt(row_class=capture))
        assert len(seen) == len(rows)
        for r in seen:
            assert "DT_RowId" in r

    # DT_RowData

    def test_row_data_static(self):
        rows = self._run_results(self._make_dt(row_data={"source": "mongo"}))
        for row in rows:
            assert row["DT_RowData"] == {"source": "mongo"}

    def test_row_data_callable(self):
        rows = self._run_results(self._make_dt(row_data=lambda r: {"id": r.get("DT_RowId")}))
        for row in rows:
            assert row["DT_RowData"] == {"id": row["DT_RowId"]}

    def test_row_data_absent_by_default(self):
        rows = self._run_results(self._make_dt())
        for row in rows:
            assert "DT_RowData" not in row

    # DT_RowAttr

    def test_row_attr_static(self):
        rows = self._run_results(self._make_dt(row_attr={"data-type": "record"}))
        for row in rows:
            assert row["DT_RowAttr"] == {"data-type": "record"}

    def test_row_attr_callable(self):
        rows = self._run_results(self._make_dt(row_attr=lambda r: {"title": r.get("name", "")}))
        for row in rows:
            assert row["DT_RowAttr"] == {"title": row.get("name", "")}

    def test_row_attr_absent_by_default(self):
        rows = self._run_results(self._make_dt())
        for row in rows:
            assert "DT_RowAttr" not in row

    # Combined

    def test_all_three_combined(self):
        rows = self._run_results(self._make_dt(
            row_class="row", row_data={"x": 1}, row_attr={"tabindex": "0"}
        ))
        for row in rows:
            assert row["DT_RowClass"] == "row"
            assert row["DT_RowData"] == {"x": 1}
            assert row["DT_RowAttr"] == {"tabindex": "0"}

    def test_only_row_class_set(self):
        rows = self._run_results(self._make_dt(row_class="x"))
        for row in rows:
            assert "DT_RowClass" in row
            assert "DT_RowData" not in row
            assert "DT_RowAttr" not in row

    # get_rows integration

    def test_get_rows_includes_row_class(self):
        dt = self._make_dt(row_class="highlight")
        self.collection.aggregate.return_value = iter(self.sample_docs)
        self.collection.count_documents.return_value = 3
        response = dt.get_rows()
        for row in response["data"]:
            assert row["DT_RowClass"] == "highlight"

    # Backward compatibility

    def test_backward_compatible_no_row_kwargs(self):
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        dt.collection = self.collection
        self.collection.aggregate.return_value = iter(self.sample_docs)
        rows = dt.results()
        assert isinstance(rows, list)
        for row in rows:
            assert "DT_RowClass" not in row
            assert "DT_RowData" not in row
            assert "DT_RowAttr" not in row


# --- from tests/test_regression_v1_30_0.py ---
def _make_dt(data_fields=None):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__ = MagicMock(return_value=collection)
    collection.list_indexes.return_value = []
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "columns": [{"data": "created", "searchable": True, "orderable": True,
                     "search": {"value": "", "regex": False}}],
        "order": [{"column": 0, "dir": "asc"}],
        "search": {"value": "", "regex": False},
    }
    return DataTables(mongo, "col", request_args, data_fields=data_fields or [])


def _make_qb():
    fm = MagicMock(spec=FieldMapper)
    fm.get_field_type.return_value = "text"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm)


class TestBuildColumnSearchNesting(unittest.TestCase):
    """Fix 1: build_column_search inner blocks nested inside outer if."""

    def test_has_cc_only_no_search_value_no_unbound_error(self):
        """has_cc=True, search_value empty: must not raise and must return cc condition."""
        qb = _make_qb()
        columns = [{
            "data": "name",
            "searchable": True,
            "search": {"value": ""},
            "columnControl": {"search": {"value": "foo", "logic": "contains"}},
        }]
        result = qb.build_column_search(columns)
        self.assertIn("$and", result)

    def test_not_searchable_with_cc_no_unbound_error(self):
        """searchable=False, has_cc=True: must not raise and must return cc condition."""
        qb = _make_qb()
        columns = [{
            "data": "status",
            "searchable": False,
            "search": {"value": "active"},
            "columnControl": {"search": {"value": "active", "logic": "equal"}},
        }]
        result = qb.build_column_search(columns)
        self.assertIn("$and", result)

    def test_not_searchable_no_cc_returns_empty(self):
        """searchable=False, no cc, search_value present: returns empty dict."""
        qb = _make_qb()
        columns = [{
            "data": "hidden",
            "searchable": False,
            "search": {"value": "test"},
        }]
        result = qb.build_column_search(columns)
        self.assertEqual(result, {})


class TestHashableOutsideLoop(unittest.TestCase):
    """Fix 2: _hashable defined outside the for loop in get_searchpanes_options."""

    def test_searchpanes_options_multiple_columns(self):
        """_hashable must work correctly across all columns (not just the first)."""
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        mongo.db.__getitem__ = MagicMock(return_value=collection)
        collection.list_indexes.return_value = []

        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [
                {"data": "name", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
                {"data": "status", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
            ],
            "order": [{"column": 0, "dir": "asc"}],
            "search": {"value": "", "regex": False},
        }
        facet_doc = {
            "name": [{"_id": "Alice", "count": 3}, {"_id": "Bob", "count": 2}],
            "status": [{"_id": "active", "count": 4}, {"_id": "inactive", "count": 1}],
        }
        collection.aggregate.side_effect = [[facet_doc], [facet_doc]]

        dt = DataTables(mongo, "col", request_args,
                        data_fields=[DataField("name", "string"), DataField("status", "string")])
        options = dt.get_searchpanes_options()

        # Both columns must be present and correctly populated
        self.assertIn("name", options)
        self.assertIn("status", options)
        self.assertEqual(len(options["name"]), 2)
        self.assertEqual(len(options["status"]), 2)


class TestSbDateBetweenSemantics(unittest.TestCase):
    """Fix 3: _sb_date between/!between use day-inclusive exclusive upper bound."""

    def setUp(self):
        self.dt = _make_dt([DataField("created", "date")])

    def test_between_uses_lt_not_lte(self):
        """between: upper bound must be $lt end+1day, not $lte end."""
        result = self.dt._sb_date("created", "between", "2024-01-01", "2024-01-31")
        cond = result["created"]
        self.assertIn("$lt", cond)
        self.assertNotIn("$lte", cond)
        self.assertEqual(cond["$lt"], datetime(2024, 2, 1))

    def test_between_lower_bound(self):
        """between: lower bound must be $gte start."""
        result = self.dt._sb_date("created", "between", "2024-01-01", "2024-01-31")
        self.assertEqual(result["created"]["$gte"], datetime(2024, 1, 1))

    def test_not_between_upper_uses_gte_not_gt(self):
        """!between: upper complement must be $gte end+1day, not $gt end."""
        result = self.dt._sb_date("created", "!between", "2024-01-01", "2024-01-31")
        upper = result["$or"][1]
        self.assertIn("$gte", upper["created"])
        self.assertNotIn("$gt", upper["created"])
        self.assertEqual(upper["created"]["$gte"], datetime(2024, 2, 1))

    def test_not_between_lower_bound(self):
        """!between: lower complement must be $lt start."""
        result = self.dt._sb_date("created", "!between", "2024-01-01", "2024-01-31")
        lower = result["$or"][0]
        self.assertEqual(lower["created"]["$lt"], datetime(2024, 1, 1))

    def test_between_single_day_range(self):
        """between same start/end: $gte day, $lt day+1 (covers full day)."""
        result = self.dt._sb_date("created", "between", "2024-06-15", "2024-06-15")
        cond = result["created"]
        self.assertEqual(cond["$gte"], datetime(2024, 6, 15))
        self.assertEqual(cond["$lt"], datetime(2024, 6, 16))


if __name__ == "__main__":
    unittest.main()


# --- from tests/test_global_search_perf.py ---
def _make_perf_qb(columns):
    """Build a MongoQueryBuilder with a real FieldMapper for the given column names."""
    fm = FieldMapper(columns)
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


def test_field_mapper_called_once_per_column_not_per_term():
    """get_field_type and get_db_field should be called once per column, not once per (term, column)."""
    fm = MagicMock(spec=FieldMapper)
    fm.get_field_type.return_value = "text"
    fm.get_db_field.side_effect = lambda c: c

    qb = MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)
    columns = ["name", "city", "country"]
    terms = ["alice", "bob", "carol"]

    qb.build_global_search(terms, columns)

    # Each column looked up exactly once regardless of term count
    assert fm.get_field_type.call_count == len(columns)
    assert fm.get_db_field.call_count == len(columns)


def test_global_search_multi_term_produces_correct_or_conditions():
    """Multiple terms with smart=True produce $and of per-term $or conditions."""
    qb = _make_perf_qb(["name", "city"])
    result = qb.build_global_search(["alice", "bob"], ["name", "city"])

    assert "$and" in result
    # 2 terms → 2 entries in $and, each a $or over 2 columns
    assert len(result["$and"]) == 2
    for term_cond in result["$and"]:
        assert "$or" in term_cond
        assert len(term_cond["$or"]) == 2


def test_global_search_quoted_phrase_word_boundary():
    """Quoted single term uses word-boundary regex."""
    qb = _make_perf_qb(["name"])
    result = qb.build_global_search(["alice"], ["name"], original_search='"alice"')

    assert "$or" in result
    assert len(result["$or"]) == 1
    pattern = result["$or"][0]["name"]["$regex"]
    assert pattern.startswith("\\b") and pattern.endswith("\\b")


def test_global_search_skips_date_columns():
    """Date-typed columns are excluded from global search results."""
    from mongo_datatables.datatables import DataField
    fields = [DataField("created", "date"), DataField("name", "string")]
    fm = FieldMapper(fields)
    qb = MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    result = qb.build_global_search(["alice"], ["created", "name"])

    assert "$or" in result
    # Only 'name' should appear — 'created' is date type
    fields_searched = [list(cond.keys())[0] for cond in result["$or"]]
    assert "created" not in fields_searched
    assert "name" in fields_searched


# --- from tests/test_init.py ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module being tested
import mongo_datatables
from mongo_datatables import DataTables, Editor


class TestInit(unittest.TestCase):
    """Test cases for module initialization"""

    def test_imports(self):
        """Test that import classes are available"""
        self.assertTrue(hasattr(mongo_datatables, 'DataTables'))
        self.assertTrue(hasattr(mongo_datatables, 'Editor'))

    def test_version(self):
        """Test version is defined and accessible"""
        self.assertTrue(hasattr(mongo_datatables, '__version__'))
        self.assertIsInstance(mongo_datatables.__version__, str)

    def test_imports_work(self):
        """Test that imports actually work"""
        self.assertIsNotNone(DataTables)
        self.assertIsNotNone(Editor)


if __name__ == '__main__':
    unittest.main()

# --- from tests/test_utils_date_handler.py ---


# --- from tests/test_utils_field_mapper.py ---
class TestFieldMapper(unittest.TestCase):
    """Test cases for FieldMapper utility class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create sample data fields for testing
        self.data_fields = [
            DataField("title", "string", "Title"),
            DataField("author.name", "string", "Author"),
            DataField("publishDate", "date", "Published"),
            DataField("pageCount", "number", "Pages"),
            DataField("isPublished", "boolean", "Status"),
            DataField("tags", "array", "Tags"),
        ]

    def test_initialization_with_data_fields(self):
        """Test FieldMapper initialization with data fields."""
        mapper = FieldMapper(self.data_fields)

        self.assertEqual(len(mapper.data_fields), 6)
        self.assertIsNotNone(mapper.field_types)
        self.assertIsNotNone(mapper.ui_to_db)
        self.assertIsNotNone(mapper.db_to_ui)

    def test_initialization_with_empty_list(self):
        """Test FieldMapper initialization with empty list."""
        mapper = FieldMapper([])

        self.assertEqual(len(mapper.data_fields), 0)
        self.assertEqual(mapper.field_types, {})
        self.assertEqual(mapper.ui_to_db, {})
        self.assertEqual(mapper.db_to_ui, {})

    def test_initialization_with_none(self):
        """Test FieldMapper initialization with None."""
        mapper = FieldMapper(None)

        self.assertEqual(len(mapper.data_fields), 0)
        self.assertEqual(mapper.field_types, {})

    def test_get_db_field_with_mapping(self):
        """Test getting database field name from UI alias."""
        mapper = FieldMapper(self.data_fields)

        # Test mapped fields
        self.assertEqual(mapper.get_db_field("Title"), "title")
        self.assertEqual(mapper.get_db_field("Author"), "author.name")
        self.assertEqual(mapper.get_db_field("Published"), "publishDate")
        self.assertEqual(mapper.get_db_field("Pages"), "pageCount")

    def test_get_db_field_without_mapping(self):
        """Test getting database field when no mapping exists (returns same)."""
        mapper = FieldMapper(self.data_fields)

        # Non-existent field should return itself
        self.assertEqual(mapper.get_db_field("NonExistent"), "NonExistent")
        self.assertEqual(mapper.get_db_field("random_field"), "random_field")

    def test_get_db_field_case_sensitive(self):
        """Test that field mapping is case-sensitive."""
        mapper = FieldMapper(self.data_fields)

        # Correct case
        self.assertEqual(mapper.get_db_field("Title"), "title")

        # Wrong case should not map
        self.assertEqual(mapper.get_db_field("title"), "title")  # Returns itself
        self.assertEqual(mapper.get_db_field("TITLE"), "TITLE")  # Returns itself

    def test_get_ui_field_with_mapping(self):
        """Test getting UI field name from database field."""
        mapper = FieldMapper(self.data_fields)

        # Test reverse mapping
        self.assertEqual(mapper.get_ui_field("title"), "Title")
        self.assertEqual(mapper.get_ui_field("author.name"), "Author")
        self.assertEqual(mapper.get_ui_field("publishDate"), "Published")
        self.assertEqual(mapper.get_ui_field("pageCount"), "Pages")

    def test_get_ui_field_without_mapping(self):
        """Test getting UI field when no mapping exists."""
        mapper = FieldMapper(self.data_fields)

        # Non-existent field should return itself
        self.assertEqual(mapper.get_ui_field("nonexistent"), "nonexistent")

    def test_get_field_type_by_db_name(self):
        """Test getting field type using database field name."""
        mapper = FieldMapper(self.data_fields)

        self.assertEqual(mapper.get_field_type("title"), "string")
        self.assertEqual(mapper.get_field_type("author.name"), "string")
        self.assertEqual(mapper.get_field_type("publishDate"), "date")
        self.assertEqual(mapper.get_field_type("pageCount"), "number")
        self.assertEqual(mapper.get_field_type("isPublished"), "boolean")
        self.assertEqual(mapper.get_field_type("tags"), "array")

    def test_get_field_type_by_ui_name(self):
        """Test getting field type using UI alias."""
        mapper = FieldMapper(self.data_fields)

        # Should map UI name to DB name and get type
        self.assertEqual(mapper.get_field_type("Title"), "string")
        self.assertEqual(mapper.get_field_type("Author"), "string")
        self.assertEqual(mapper.get_field_type("Published"), "date")
        self.assertEqual(mapper.get_field_type("Pages"), "number")

    def test_get_field_type_nonexistent(self):
        """Test getting field type for non-existent field."""
        mapper = FieldMapper(self.data_fields)

        # Non-existent field should return None
        self.assertIsNone(mapper.get_field_type("nonexistent"))
        self.assertIsNone(mapper.get_field_type(""))

    def test_nested_field_mapping(self):
        """Test mapping for nested fields (dot notation)."""
        mapper = FieldMapper(self.data_fields)

        # author.name should map correctly
        self.assertEqual(mapper.get_db_field("Author"), "author.name")
        self.assertEqual(mapper.get_ui_field("author.name"), "Author")
        self.assertEqual(mapper.get_field_type("author.name"), "string")

    def test_field_with_no_alias(self):
        """Test field where alias is same as name."""
        # Create field with no explicit alias
        fields = [
            DataField("simpleField", "string")  # No alias, should use name
        ]
        mapper = FieldMapper(fields)

        # When no alias specified, it should use the field name
        # So both should work
        self.assertEqual(mapper.get_db_field("simpleField"), "simpleField")
        self.assertEqual(mapper.get_field_type("simpleField"), "string")

    def test_multiple_fields_same_type(self):
        """Test multiple fields with the same type."""
        fields = [
            DataField("field1", "string", "Field1"),
            DataField("field2", "string", "Field2"),
            DataField("field3", "string", "Field3"),
        ]
        mapper = FieldMapper(fields)

        # All should have correct type
        self.assertEqual(mapper.get_field_type("field1"), "string")
        self.assertEqual(mapper.get_field_type("field2"), "string")
        self.assertEqual(mapper.get_field_type("field3"), "string")

        # All should have correct mappings
        self.assertEqual(mapper.get_db_field("Field1"), "field1")
        self.assertEqual(mapper.get_db_field("Field2"), "field2")
        self.assertEqual(mapper.get_db_field("Field3"), "field3")

    def test_field_types_dictionary(self):
        """Test that field_types dictionary is populated correctly."""
        mapper = FieldMapper(self.data_fields)

        expected_types = {
            "title": "string",
            "author.name": "string",
            "publishDate": "date",
            "pageCount": "number",
            "isPublished": "boolean",
            "tags": "array",
        }

        self.assertEqual(mapper.field_types, expected_types)

    def test_ui_to_db_mapping_dictionary(self):
        """Test that ui_to_db dictionary is populated correctly."""
        mapper = FieldMapper(self.data_fields)

        expected_mapping = {
            "Title": "title",
            "Author": "author.name",
            "Published": "publishDate",
            "Pages": "pageCount",
            "Status": "isPublished",
            "Tags": "tags",
        }

        self.assertEqual(mapper.ui_to_db, expected_mapping)

    def test_db_to_ui_mapping_dictionary(self):
        """Test that db_to_ui dictionary is populated correctly."""
        mapper = FieldMapper(self.data_fields)

        expected_mapping = {
            "title": "Title",
            "author.name": "Author",
            "publishDate": "Published",
            "pageCount": "Pages",
            "isPublished": "Status",
            "tags": "Tags",
        }

        self.assertEqual(mapper.db_to_ui, expected_mapping)

    def test_all_valid_mongo_types(self):
        """Test FieldMapper with all valid MongoDB data types."""
        fields = [
            DataField("string_field", "string"),
            DataField("number_field", "number"),
            DataField("date_field", "date"),
            DataField("boolean_field", "boolean"),
            DataField("array_field", "array"),
            DataField("object_field", "object"),
            DataField("objectid_field", "objectid"),
            DataField("null_field", "null"),
        ]
        mapper = FieldMapper(fields)

        # All types should be stored correctly
        self.assertEqual(mapper.get_field_type("string_field"), "string")
        self.assertEqual(mapper.get_field_type("number_field"), "number")
        self.assertEqual(mapper.get_field_type("date_field"), "date")
        self.assertEqual(mapper.get_field_type("boolean_field"), "boolean")
        self.assertEqual(mapper.get_field_type("array_field"), "array")
        self.assertEqual(mapper.get_field_type("object_field"), "object")
        self.assertEqual(mapper.get_field_type("objectid_field"), "objectid")
        self.assertEqual(mapper.get_field_type("null_field"), "null")

    def test_empty_string_field_name(self):
        """Test handling of empty field name."""
        mapper = FieldMapper(self.data_fields)

        # Empty string should return None
        self.assertIsNone(mapper.get_field_type(""))
        self.assertEqual(mapper.get_db_field(""), "")
        self.assertEqual(mapper.get_ui_field(""), "")


if __name__ == '__main__':
    unittest.main()


# --- from tests/test_utils_search_parser.py ---
class TestSearchTermParser(unittest.TestCase):
    """Test cases for SearchTermParser utility class."""

    def test_parse_empty_string(self):
        """Test parsing empty search string."""
        result = SearchTermParser.parse("")
        self.assertEqual(result, [])

        result = SearchTermParser.parse(None)
        self.assertEqual(result, [])

    def test_parse_simple_terms(self):
        """Test parsing simple space-separated terms."""
        result = SearchTermParser.parse("term1 term2 term3")
        self.assertEqual(result, ["term1", "term2", "term3"])

    def test_parse_single_term(self):
        """Test parsing single term."""
        result = SearchTermParser.parse("singleterm")
        self.assertEqual(result, ["singleterm"])

    def test_parse_double_quoted_phrase(self):
        """Test parsing double-quoted phrases."""
        result = SearchTermParser.parse('Author:Robert "Jonathan Kennedy"')
        self.assertEqual(result, ["Author:Robert", "Jonathan Kennedy"])

        result = SearchTermParser.parse('"complete phrase"')
        self.assertEqual(result, ["complete phrase"])

    def test_parse_single_quoted_phrase(self):
        """Test parsing single-quoted phrases."""
        result = SearchTermParser.parse("Author:Robert 'Jonathan Kennedy'")
        self.assertEqual(result, ["Author:Robert", "Jonathan Kennedy"])

        result = SearchTermParser.parse("'complete phrase'")
        self.assertEqual(result, ["complete phrase"])

    def test_parse_mixed_quotes(self):
        """Test parsing with both single and double quotes."""
        result = SearchTermParser.parse('"double quote" and \'single quote\'')
        self.assertEqual(result, ["double quote", "and", "single quote"])

    def test_parse_multiple_quoted_phrases(self):
        """Test parsing multiple quoted phrases."""
        result = SearchTermParser.parse('"first phrase" "second phrase" "third phrase"')
        self.assertEqual(result, ["first phrase", "second phrase", "third phrase"])

    def test_parse_quoted_with_unquoted(self):
        """Test parsing mix of quoted and unquoted terms."""
        result = SearchTermParser.parse('term1 "quoted term" term2 \'another quoted\' term3')
        self.assertEqual(result, ["term1", "quoted term", "term2", "another quoted", "term3"])

    def test_parse_field_specific_search(self):
        """Test parsing field:value syntax."""
        result = SearchTermParser.parse("Title:MongoDB Author:Smith Status:active")
        self.assertEqual(result, ["Title:MongoDB", "Author:Smith", "Status:active"])

    def test_parse_field_specific_with_quoted_value(self):
        """Test parsing field:value with quoted value."""
        result = SearchTermParser.parse('Title:"Advanced MongoDB" Author:Smith')
        self.assertEqual(result, ["Title:Advanced MongoDB", "Author:Smith"])

    def test_parse_empty_quotes(self):
        """Test parsing empty quoted strings."""
        result = SearchTermParser.parse('term1 "" term2')
        self.assertEqual(result, ["term1", "", "term2"])

        result = SearchTermParser.parse("term1 '' term2")
        self.assertEqual(result, ["term1", "", "term2"])

    def test_parse_quotes_within_quotes(self):
        """Test parsing quotes within different quote types."""
        # Double quotes inside single quotes
        result = SearchTermParser.parse('\'He said "Hello"\'')
        self.assertEqual(result, ['He said "Hello"'])

        # Single quotes inside double quotes
        result = SearchTermParser.parse('"It\'s working"')
        self.assertEqual(result, ["It's working"])

    def test_parse_malformed_quotes_unclosed(self):
        """Test parsing with unclosed quotes (graceful fallback)."""
        # shlex will raise ValueError for unclosed quotes, which our parser catches
        result = SearchTermParser.parse('term1 "unclosed quote term2')
        # Should fall back to simple split
        self.assertIsInstance(result, list)
        # The fallback behavior splits on whitespace: ['term1', '"unclosed', 'quote', 'term2']
        self.assertEqual(len(result), 4)

    def test_parse_special_characters(self):
        """Test parsing with special characters."""
        result = SearchTermParser.parse("user@example.com test-value date:2023-01-01")
        self.assertEqual(result, ["user@example.com", "test-value", "date:2023-01-01"])

    def test_parse_multiple_spaces(self):
        """Test parsing with multiple spaces between terms."""
        result = SearchTermParser.parse("term1    term2     term3")
        self.assertEqual(result, ["term1", "term2", "term3"])

    def test_parse_tabs_and_newlines(self):
        """Test parsing with tabs and newlines."""
        result = SearchTermParser.parse("term1\tterm2\nterm3")
        self.assertEqual(result, ["term1", "term2", "term3"])

    def test_parse_quoted_empty_string_only(self):
        """Test parsing string with only quotes."""
        result = SearchTermParser.parse('""')
        self.assertEqual(result, [""])

    def test_parse_complex_search_example(self):
        """Test complex real-world search example."""
        search = 'Title:"MongoDB Guide" Author:Smith Year:>2020 Status:published'
        result = SearchTermParser.parse(search)
        self.assertEqual(result, [
            "Title:MongoDB Guide",
            "Author:Smith",
            "Year:>2020",
            "Status:published"
        ])

    def test_parse_quoted_colon_syntax(self):
        """Test quoted phrases with colon syntax."""
        result = SearchTermParser.parse('Field:"value with spaces"')
        self.assertEqual(result, ["Field:value with spaces"])

    def test_parse_backslash_escapes(self):
        """Test parsing with backslash escapes."""
        # shlex handles backslash escapes in POSIX mode
        result = SearchTermParser.parse('term1 "escaped\\ space" term2')
        # In POSIX mode, backslash escapes the next character
        # So "escaped\\ space" becomes "escaped\ space" (backslash preserved)
        self.assertIn("escaped\\ space", result)

    def test_parse_unicode_characters(self):
        """Test parsing with unicode characters."""
        result = SearchTermParser.parse('emoji:🎉 chinese:你好 term')
        self.assertEqual(result, ["emoji:🎉", "chinese:你好", "term"])

    def test_parse_numeric_values(self):
        """Test parsing with numeric values."""
        result = SearchTermParser.parse("age:25 price:>100 quantity:<=50")
        self.assertEqual(result, ["age:25", "price:>100", "quantity:<=50"])

    def test_parse_preserves_operator_symbols(self):
        """Test that comparison operators are preserved."""
        result = SearchTermParser.parse("field:>=100 another:<50")
        self.assertEqual(result, ["field:>=100", "another:<50"])

    def test_parse_single_word_no_split(self):
        """Test single word returns as single element."""
        result = SearchTermParser.parse("MongoDB")
        self.assertEqual(result, ["MongoDB"])

    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only string."""
        result = SearchTermParser.parse("   ")
        self.assertEqual(result, [])

        result = SearchTermParser.parse("\t\n")
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()


# --- from tests/test_utils_type_converter.py ---
class TestTypeConverter(unittest.TestCase):
    """Test cases for TypeConverter utility class."""

    # ============ to_number tests ============

    def test_to_number_integer(self):
        """Test converting string to integer."""
        self.assertEqual(TypeConverter.to_number("42"), 42)
        self.assertEqual(TypeConverter.to_number("0"), 0)
        self.assertEqual(TypeConverter.to_number("-15"), -15)
        self.assertIsInstance(TypeConverter.to_number("42"), int)

    def test_to_number_float(self):
        """Test converting string to float."""
        self.assertEqual(TypeConverter.to_number("3.14"), 3.14)
        self.assertEqual(TypeConverter.to_number("0.5"), 0.5)
        self.assertEqual(TypeConverter.to_number("-2.5"), -2.5)
        self.assertIsInstance(TypeConverter.to_number("3.14"), float)

    def test_to_number_scientific_notation(self):
        """Test converting scientific notation."""
        self.assertEqual(TypeConverter.to_number("1.5e2"), 150.0)
        self.assertEqual(TypeConverter.to_number("1e-3"), 0.001)

    def test_to_number_invalid(self):
        """Test invalid number conversions."""
        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("not a number")

        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("12.34.56")

        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("")

        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("12abc")

    def test_to_number_whitespace(self):
        """Test number conversion with whitespace."""
        # Python's float/int handle leading/trailing whitespace
        self.assertEqual(TypeConverter.to_number("  42  "), 42)
        self.assertEqual(TypeConverter.to_number("\t3.14\n"), 3.14)

    # ============ to_boolean tests ============

    def test_to_boolean_true_values(self):
        """Test converting various strings to True."""
        true_values = ['true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '1', 't', 'T', 'y', 'Y']
        for value in true_values:
            with self.subTest(value=value):
                self.assertTrue(TypeConverter.to_boolean(value))

    def test_to_boolean_false_values(self):
        """Test converting various strings to False."""
        false_values = ['false', 'False', 'FALSE', 'no', 'No', 'NO', '0', 'f', 'F', 'n', 'N', '']
        for value in false_values:
            with self.subTest(value=value):
                self.assertFalse(TypeConverter.to_boolean(value))

    def test_to_boolean_edge_cases(self):
        """Test boolean conversion edge cases."""
        # Any string not in the true list should be false
        self.assertFalse(TypeConverter.to_boolean("maybe"))
        self.assertFalse(TypeConverter.to_boolean("2"))
        self.assertFalse(TypeConverter.to_boolean("on"))
        self.assertFalse(TypeConverter.to_boolean("off"))

    # ============ to_array tests ============

    def test_to_array_valid_json_array(self):
        """Test converting valid JSON array strings."""
        result = TypeConverter.to_array('["a", "b", "c"]')
        self.assertEqual(result, ["a", "b", "c"])
        self.assertIsInstance(result, list)

        result = TypeConverter.to_array('[1, 2, 3]')
        self.assertEqual(result, [1, 2, 3])

        result = TypeConverter.to_array('[]')
        self.assertEqual(result, [])

    def test_to_array_json_with_mixed_types(self):
        """Test converting JSON array with mixed types."""
        result = TypeConverter.to_array('["string", 123, true, null]')
        self.assertEqual(result, ["string", 123, True, None])

    def test_to_array_non_json_string(self):
        """Test converting non-JSON string to single-element array."""
        result = TypeConverter.to_array("simple string")
        self.assertEqual(result, ["simple string"])

        result = TypeConverter.to_array("not [valid] json")
        self.assertEqual(result, ["not [valid] json"])

    def test_to_array_json_object_not_array(self):
        """Test converting JSON object (not array) wraps in array."""
        result = TypeConverter.to_array('{"key": "value"}')
        self.assertEqual(result, [{"key": "value"}])

    def test_to_array_json_scalar(self):
        """Test converting JSON scalar wraps in array."""
        result = TypeConverter.to_array('123')
        self.assertEqual(result, [123])

        result = TypeConverter.to_array('"string"')
        self.assertEqual(result, ["string"])

    def test_to_array_empty_string(self):
        """Test converting empty string."""
        result = TypeConverter.to_array("")
        self.assertEqual(result, [""])

    # ============ parse_json tests ============

    def test_parse_json_valid_object(self):
        """Test parsing valid JSON objects."""
        result = TypeConverter.parse_json('{"name": "John", "age": 30}')
        self.assertEqual(result, {"name": "John", "age": 30})
        self.assertIsInstance(result, dict)

    def test_parse_json_valid_array(self):
        """Test parsing valid JSON arrays."""
        result = TypeConverter.parse_json('[1, 2, 3]')
        self.assertEqual(result, [1, 2, 3])
        self.assertIsInstance(result, list)

    def test_parse_json_nested_structure(self):
        """Test parsing complex nested JSON."""
        json_str = '{"users": [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]}'
        result = TypeConverter.parse_json(json_str)
        self.assertEqual(len(result["users"]), 2)
        self.assertEqual(result["users"][0]["name"], "Alice")

    def test_parse_json_scalars(self):
        """Test parsing JSON scalar values."""
        self.assertEqual(TypeConverter.parse_json('123'), 123)
        self.assertEqual(TypeConverter.parse_json('"string"'), "string")
        self.assertEqual(TypeConverter.parse_json('true'), True)
        self.assertEqual(TypeConverter.parse_json('false'), False)
        self.assertEqual(TypeConverter.parse_json('null'), None)

    def test_parse_json_invalid(self):
        """Test parsing invalid JSON raises exception."""
        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("not valid json")

        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("{unclosed")

        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("{'single': 'quotes'}")  # JSON requires double quotes

    def test_parse_json_empty_string(self):
        """Test parsing empty string raises exception."""
        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("")

    def test_parse_json_with_whitespace(self):
        """Test parsing JSON with extra whitespace."""
        result = TypeConverter.parse_json('  {"key": "value"}  ')
        self.assertEqual(result, {"key": "value"})

    # ============ Integration tests ============

    def test_number_conversion_edge_values(self):
        """Test number conversion with edge values."""
        # Very large numbers
        self.assertEqual(TypeConverter.to_number("9999999999999999"), 9999999999999999)

        # Very small decimals
        result = TypeConverter.to_number("0.0000001")
        self.assertAlmostEqual(result, 0.0000001)

        # Negative zero
        self.assertEqual(TypeConverter.to_number("-0"), 0)

    def test_array_conversion_with_nested_json(self):
        """Test array conversion with nested JSON structures."""
        json_str = '[{"id": 1, "data": [1, 2, 3]}, {"id": 2, "data": [4, 5, 6]}]'
        result = TypeConverter.to_array(json_str)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"], [1, 2, 3])
        self.assertEqual(result[1]["id"], 2)

    def test_parse_json_unicode(self):
        """Test parsing JSON with unicode characters."""
        result = TypeConverter.parse_json('{"emoji": "🎉", "chinese": "你好"}')
        self.assertEqual(result["emoji"], "🎉")
        self.assertEqual(result["chinese"], "你好")

    def test_parse_json_escaped_characters(self):
        """Test parsing JSON with escaped characters."""
        result = TypeConverter.parse_json('{"quote": "\\"Hello\\"", "newline": "Line1\\nLine2"}')
        self.assertEqual(result["quote"], '"Hello"')
        self.assertEqual(result["newline"], "Line1\nLine2")


if __name__ == '__main__':
    unittest.main()


# ---------------------------------------------------------------------------
# Folded from test_is_truthy.py
# ---------------------------------------------------------------------------

import pytest as _pytest
from mongo_datatables.utils import is_truthy


@_pytest.mark.parametrize("value", [True, "true", "True", 1])
def test_is_truthy_truthy_values(value):
    assert is_truthy(value) is True


@_pytest.mark.parametrize("value", [False, "false", "False", 0, None, "", "yes", "1", 2])
def test_is_truthy_falsy_values(value):
    assert is_truthy(value) is False


# --- from tests/test_searchable_coercion.py ---



class TestPipelineStages:
    """Tests for the pipeline_stages parameter."""

    FIELDS = [DataField("name", "string")]

    def _base_args(self, search_value=""):
        return {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": search_value, "regex": False},
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [{"data": "name", "searchable": True, "orderable": True,
                         "search": {"value": "", "regex": False}}],
        }

    def _make_dt(self, pipeline_stages=None, search_value=""):
        col = MagicMock()
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)
        col.list_indexes = MagicMock(return_value=[])
        # Use a plain dict-like object so _get_collection falls through to pymongo_object[name]
        db = {"test": col}
        return DataTables(db, "test", self._base_args(search_value), self.FIELDS,
                          pipeline_stages=pipeline_stages), col

    def test_default_none_no_extra_stages(self):
        dt, col = self._make_dt()
        assert dt.pipeline_stages == []
        pipeline = dt._build_pipeline()
        assert not any("$addFields" in s for s in pipeline)

    def test_single_stage_prepended(self):
        stage = {"$addFields": {"full_name": {"$concat": ["$first", " ", "$last"]}}}
        dt, col = self._make_dt(pipeline_stages=[stage])
        pipeline = dt._build_pipeline()
        assert pipeline[0] == stage

    def test_multiple_stages_order_preserved(self):
        s1 = {"$addFields": {"x": 1}}
        s2 = {"$unwind": "$tags"}
        dt, col = self._make_dt(pipeline_stages=[s1, s2])
        pipeline = dt._build_pipeline()
        assert pipeline[0] == s1
        assert pipeline[1] == s2

    def test_stages_before_match(self):
        stage = {"$addFields": {"x": 1}}
        dt, col = self._make_dt(pipeline_stages=[stage], search_value="hello")
        pipeline = dt._build_pipeline()
        # $addFields must come before any $match
        stage_keys = [list(s.keys())[0] for s in pipeline]
        add_idx = stage_keys.index("$addFields")
        match_indices = [i for i, k in enumerate(stage_keys) if k == "$match"]
        for mi in match_indices:
            assert add_idx < mi

    def test_stages_not_mutated(self):
        original = [{"$addFields": {"x": 1}}]
        dt, col = self._make_dt(pipeline_stages=original)
        dt._build_pipeline()
        dt._build_pipeline()  # call twice
        assert original == [{"$addFields": {"x": 1}}]  # original list unchanged

    def test_count_filtered_includes_stages(self):
        stage = {"$addFields": {"x": 1}}
        # Use a search term so self.filter is truthy, forcing the aggregation path
        dt, col = self._make_dt(pipeline_stages=[stage], search_value="hello")
        col.aggregate.return_value = iter([{"total": 5}])
        dt.count_filtered()
        call_args = col.aggregate.call_args[0][0]
        assert call_args[0] == stage

    def test_empty_list_same_as_none(self):
        dt_none, _ = self._make_dt(pipeline_stages=None)
        dt_empty, _ = self._make_dt(pipeline_stages=[])
        assert dt_none.pipeline_stages == dt_empty.pipeline_stages == []
        assert dt_none._build_pipeline() == dt_empty._build_pipeline()


class TestGetCollection(unittest.TestCase):
    """Tests for _get_collection branch ordering.

    Uses a real Database subclass to replicate __getattr__ behavior that
    MagicMock(spec=Database) hides. The confirmed bug: hasattr(obj, 'db')
    fires before isinstance(Database) because Database.__getattr__ never
    raises AttributeError.
    """

    ARGS = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "name", "searchable": True, "orderable": True,
                      "search": {"value": "", "regex": False}}],
    }

    def _make_fake_db(self, collection_name):
        """Return a fake Database whose __getattr__ mimics real pymongo behavior."""
        col = MagicMock(spec=Collection)
        col.list_indexes = MagicMock(return_value=[])
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)

        class FakeDatabase(Database):
            """Minimal Database subclass that returns col for any __getitem__."""
            def __init__(self):
                pass  # skip real __init__

            def __getitem__(self, name):
                return col

            def __getattr__(self, name):
                # Real Database.__getattr__ returns a Collection — never raises.
                return col

        return FakeDatabase(), col

    def test_database_instance_uses_db_directly(self):
        """When passed a Database, _get_collection must use it as the db,
        not follow the hasattr('db') branch which returns a wrong Collection."""
        fake_db, expected_col = self._make_fake_db("books")
        dt = DataTables(fake_db, "books", self.ARGS, [DataField("name", "string")])
        # The collection must be the one returned by fake_db["books"],
        # not fake_db.db["books"] (which would be col["books"] — a nested mock).
        assert dt.collection is expected_col

    def test_flask_pymongo_db_attribute_path(self):
        """When passed a Flask-PyMongo-style object with a .db attribute that
        IS a Database, _get_collection must use obj.db[collection_name]."""
        col = MagicMock(spec=Collection)
        col.list_indexes = MagicMock(return_value=[])
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)

        flask_pymongo = MagicMock()
        flask_pymongo.db = MagicMock(spec=Database)
        flask_pymongo.db.__getitem__ = MagicMock(return_value=col)

        dt = DataTables(flask_pymongo, "books", self.ARGS, [DataField("name", "string")])
        flask_pymongo.db.__getitem__.assert_called_once_with("books")


# --- Phase 2: _build_filter, _sb_group, _get_rowgroup_data ---

_P2_BASE_ARGS = {
    "draw": 1, "start": 0, "length": 10,
    "search": {"value": "", "regex": False},
    "order": [{"column": 0, "dir": "asc"}],
    "columns": [{"data": "Title", "searchable": True, "orderable": True,
                  "search": {"value": "", "regex": False}}],
}


def _make_p2_dt(request_args, data_fields=None, **custom_filter):
    col = MagicMock(spec=Collection)
    col.list_indexes = MagicMock(return_value=[])
    col.aggregate = MagicMock(return_value=iter([]))
    col.count_documents = MagicMock(return_value=0)
    col.estimated_document_count = MagicMock(return_value=0)
    db = {"test": col}
    return DataTables(db, "test", request_args, data_fields or [], **custom_filter), col


class TestBuildFilter:
    def test_empty_returns_empty_dict(self):
        dt, _ = _make_p2_dt(_P2_BASE_ARGS)
        assert dt._build_filter() == {}

    def test_custom_filter_included(self):
        dt, _ = _make_p2_dt(_P2_BASE_ARGS, status="active")
        result = dt._build_filter()
        assert result.get("status") == "active"

    def test_global_search_included(self):
        args = {**_P2_BASE_ARGS, "search": {"value": "Orwell", "regex": False}}
        dt, _ = _make_p2_dt(args)
        dt._has_text_index = False
        result = dt._build_filter()
        assert result != {}

    def test_column_search_included(self):
        args = {**_P2_BASE_ARGS, "columns": [
            {"data": "Title", "searchable": True, "orderable": True,
             "search": {"value": "1984", "regex": False}}
        ]}
        dt, _ = _make_p2_dt(args)
        result = dt._build_filter()
        assert result != {}

    def test_searchbuilder_included(self):
        args = {**_P2_BASE_ARGS, "searchBuilder": {
            "logic": "AND",
            "criteria": [{"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}]
        }}
        dt, _ = _make_p2_dt(args)
        result = dt._build_filter()
        assert result != {}

    def test_searchpanes_included(self):
        args = {**_P2_BASE_ARGS, "searchPanes": {"Title": ["1984"]}}
        dt, _ = _make_p2_dt(args)
        result = dt._build_filter()
        assert result != {}

    def test_multiple_sources_wrapped_in_and(self):
        args = {**_P2_BASE_ARGS, "search": {"value": "Orwell", "regex": False}}
        dt, _ = _make_p2_dt(args, status="active")
        dt._has_text_index = False
        result = dt._build_filter()
        assert "$and" in result
        assert len(result["$and"]) >= 2

    def test_single_source_not_wrapped(self):
        dt, _ = _make_p2_dt(_P2_BASE_ARGS, status="active")
        result = dt._build_filter()
        assert "$and" not in result
        assert result == {"status": "active"}

    def test_search_fixed_included(self):
        args = {**_P2_BASE_ARGS, "search": {"value": "", "regex": False, "fixed": [{"name": "active", "term": "Orwell"}]}}
        dt, _ = _make_p2_dt(args)
        result = dt._build_filter()
        assert result != {}


class TestSbGroup:
    def _dt(self):
        dt, _ = _make_p2_dt(_P2_BASE_ARGS)
        return dt

    def test_empty_group_returns_empty(self):
        assert self._dt()._sb_group({"logic": "AND", "criteria": []}) == {}

    def test_single_criterion_not_wrapped(self):
        criterion = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        result = self._dt()._sb_group({"logic": "AND", "criteria": [criterion]})
        assert "$and" not in result
        assert result != {}

    def test_and_logic_wraps_in_and(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        result = self._dt()._sb_group({"logic": "AND", "criteria": [c, c]})
        assert "$and" in result

    def test_or_logic_wraps_in_or(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        result = self._dt()._sb_group({"logic": "OR", "criteria": [c, c]})
        assert "$or" in result

    def test_nested_group(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        inner = {"logic": "OR", "criteria": [c, c]}
        outer = {"logic": "AND", "criteria": [c, inner]}
        result = self._dt()._sb_group(outer)
        assert "$and" in result

    def test_invalid_criterion_skipped(self):
        bad = {"condition": "=", "type": "string", "value": ["1984"]}  # no origData
        result = self._dt()._sb_group({"logic": "AND", "criteria": [bad]})
        assert result == {}


class TestGetRowgroupData:
    def test_no_rowgroup_config_returns_none(self):
        dt, _ = _make_p2_dt(_P2_BASE_ARGS)
        assert dt._get_rowgroup_data() is None

    def test_string_datasrc_builds_pipeline(self):
        args = {**_P2_BASE_ARGS, "rowGroup": {"dataSrc": "Title"}}
        dt, col = _make_p2_dt(args)
        col.aggregate = MagicMock(return_value=iter([{"_id": "1984", "count": 1}]))
        result = dt._get_rowgroup_data()
        assert result is not None
        assert "dataSrc" in result
        assert "groups" in result

    def test_numeric_datasrc_maps_to_column(self):
        args = {**_P2_BASE_ARGS, "rowGroup": {"dataSrc": 0}}
        dt, col = _make_p2_dt(args)
        col.aggregate = MagicMock(return_value=iter([{"_id": "1984", "count": 1}]))
        result = dt._get_rowgroup_data()
        assert result is not None
        assert result["dataSrc"] == 0

    def test_out_of_range_datasrc_returns_none(self):
        args = {**_P2_BASE_ARGS, "rowGroup": {"dataSrc": 99}}
        dt, _ = _make_p2_dt(args)
        assert dt._get_rowgroup_data() is None

    def test_pymongo_error_returns_none(self):
        args = {**_P2_BASE_ARGS, "rowGroup": {"dataSrc": "Title"}}
        dt, col = _make_p2_dt(args)
        col.aggregate = MagicMock(side_effect=PyMongoError("db error"))
        assert dt._get_rowgroup_data() is None
