import unittest
from unittest.mock import patch, MagicMock
from tests.base_test import BaseDataTablesTest
from mongo_datatables.datatables import DataTables, DataField
from bson.objectid import ObjectId
from datetime import datetime


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


if __name__ == '__main__':
    unittest.main()
