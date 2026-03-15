import unittest
from unittest.mock import patch, MagicMock
from tests.base_test import BaseDataTablesTest
from mongo_datatables.datatables import DataTables, DataField
import pymongo
from pymongo.errors import PyMongoError


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
