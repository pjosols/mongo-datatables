import unittest
from unittest.mock import patch, MagicMock
from tests.base_test import BaseDataTablesTest
from mongo_datatables.datatables import DataTables, DataField


class TestDataTablesDebugMode(BaseDataTablesTest):
    """Test cases for debug mode and query statistics in DataTables"""

    def setUp(self):
        super().setUp()
        # Set up data fields for testing
        self.data_fields = [
            DataField('title', 'string'),
            DataField('author', 'string'),
            DataField('year', 'number'),
            DataField('rating', 'number')
        ]
        
        # Add columns to request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True, 'orderable': True},
            {'data': 'author', 'searchable': True, 'orderable': True},
            {'data': 'year', 'searchable': True, 'orderable': True},
            {'data': 'rating', 'searchable': True, 'orderable': True}
        ]

    def test_debug_mode_in_get_rows(self):
        """Test debug mode in get_rows method"""
        # Create DataTables instance with debug mode enabled
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields,
            debug_mode=True
        )
        
        # Mock the necessary methods to avoid actual database calls
        datatables._results = [{'title': 'Test'}]
        datatables._recordsTotal = 10
        datatables._recordsFiltered = 5
        
        # Get rows with debug mode
        rows = datatables.get_rows()
        
        # Verify query stats are included in the response
        self.assertIn('query_stats', rows)
        self.assertIsInstance(rows['query_stats'], dict)

    def test_debug_mode_with_text_index(self):
        """Test debug mode with text index"""
        # Set up a search
        self.request_args['search']['value'] = 'test search'
        
        # Create DataTables instance with debug mode enabled
        with patch.object(DataTables, 'has_text_index', return_value=True):
            datatables = DataTables(
                self.mongo, 
                'test_collection', 
                self.request_args, 
                data_fields=self.data_fields,
                use_text_index=True,
                debug_mode=True
            )
            
            # Get the global search condition to trigger query stats collection
            condition = datatables.global_search_condition
            
            # Get rows with debug mode
            rows = datatables.get_rows()
            
            # Verify query stats contain text index information
            self.assertIn('query_stats', rows)
            self.assertTrue(rows['query_stats']['used_text_index'])

    def test_debug_mode_with_complex_query(self):
        """Test debug mode with a complex query including search, sort, and pagination"""
        # Set up a complex query
        self.request_args['search']['value'] = 'complex query'
        self.request_args['order'] = [{'column': 0, 'dir': 'asc'}]
        self.request_args['start'] = 10
        self.request_args['length'] = 25
        
        # Add column-specific search
        self.request_args['columns'][0]['search'] = {'value': 'title search', 'regex': False}
        
        # Create DataTables instance with debug mode enabled
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields,
            debug_mode=True
        )
        
        # Mock the necessary methods to avoid actual database calls
        datatables._results = [{'title': 'Test'}]
        datatables._recordsTotal = 100
        datatables._recordsFiltered = 50
        
        # Get rows with debug mode
        rows = datatables.get_rows()
        
        # Verify response contains the expected data
        self.assertEqual(str(rows['draw']), str(self.request_args['draw']))
        self.assertEqual(rows['recordsTotal'], 100)
        self.assertEqual(rows['recordsFiltered'], 50)
        self.assertEqual(rows['data'], [{'title': 'Test'}])
        
        # Verify query stats are included and contain expected information
        self.assertIn('query_stats', rows)
        self.assertIn('search_type', rows['query_stats'])

    def test_debug_mode_disabled(self):
        """Test that query stats are not included when debug mode is disabled"""
        # Create DataTables instance with debug mode disabled
        datatables = DataTables(
            self.mongo, 
            'test_collection', 
            self.request_args, 
            data_fields=self.data_fields,
            debug_mode=False  # Explicitly disable debug mode
        )
        
        # Mock the necessary methods to avoid actual database calls
        datatables._results = [{'title': 'Test'}]
        datatables._recordsTotal = 10
        datatables._recordsFiltered = 5
        
        # Get rows without debug mode
        rows = datatables.get_rows()
        
        # Verify query stats are not included in the response
        self.assertNotIn('query_stats', rows)


if __name__ == '__main__':
    unittest.main()
