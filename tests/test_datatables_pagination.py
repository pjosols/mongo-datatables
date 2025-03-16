from unittest.mock import patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


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
            
            # In the implementation, when length is -1 (all records), it still adds a $limit: -1 stage
            # This is likely to be handled by MongoDB as 'no limit'
            self.assertIsNotNone(limit_stage)
            self.assertEqual(limit_stage['$limit'], -1)

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
