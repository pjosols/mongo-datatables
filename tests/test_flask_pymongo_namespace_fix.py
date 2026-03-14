"""Test for Flask-PyMongo namespace issue fix."""

import pytest
from unittest.mock import Mock
from mongo_datatables import DataTables


class TestFlaskPyMongoNamespaceFix:
    """Test the fix for Flask-PyMongo namespace issue introduced in commit 58f163a."""

    def test_nested_db_attribute_handling(self):
        """Test that nested db attributes are handled correctly to avoid namespace issues.
        
        This test addresses the bug where MongoDB debug logs showed 'database.db.collection'
        instead of 'database.collection' when using certain Flask-PyMongo configurations.
        """
        # Create a mock Flask-PyMongo object with nested db structure
        mock_flask_pymongo = Mock()
        
        # Create the problematic nested structure: pymongo_object.db.db
        mock_outer_db = Mock()
        mock_inner_db = Mock()
        mock_outer_db.db = mock_inner_db
        mock_flask_pymongo.db = mock_outer_db
        
        # Set up the inner database (the actual database)
        mock_inner_db.name = "test_database"
        mock_inner_db.__getitem__ = Mock()
        
        # Mock the collection
        mock_collection = Mock()
        mock_collection.full_name = "test_database.test_collection"
        mock_collection.database = mock_inner_db
        mock_collection.name = "test_collection"
        mock_collection.estimated_document_count.return_value = 100
        mock_collection.count_documents.return_value = 100
        mock_collection.list_indexes.return_value = []
        
        # Set up collection access on the inner db
        mock_inner_db.__getitem__.return_value = mock_collection
        
        # Create DataTables instance
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "search": {"value": ""},
            "columns": [{"data": "name", "searchable": True}]
        }
        
        dt = DataTables(
            pymongo_object=mock_flask_pymongo,
            collection_name="test_collection",
            request_args=request_args
        )
        
        # Verify that the inner db was used (not the outer one)
        mock_inner_db.__getitem__.assert_called_once_with("test_collection")
        
        # Check that we got the correct collection
        assert dt.collection == mock_collection
        
        # Test count operations work correctly
        assert dt.count_total() == 100

    def test_normal_flask_pymongo_still_works(self):
        """Test that normal Flask-PyMongo objects continue to work correctly."""
        # Create a normal Flask-PyMongo object (no nested db)
        mock_flask_pymongo = Mock()
        
        # Mock the database directly
        mock_db = Mock()
        mock_db.name = "test_database"
        mock_flask_pymongo.db = mock_db
        
        # Mock the collection
        mock_collection = Mock()
        mock_collection.full_name = "test_database.test_collection"
        mock_collection.database = mock_db
        mock_collection.name = "test_collection"
        mock_collection.estimated_document_count.return_value = 50
        mock_collection.count_documents.return_value = 50
        mock_collection.list_indexes.return_value = []
        
        # Set up collection access
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "search": {"value": ""},
            "columns": [{"data": "name", "searchable": True}]
        }
        
        dt = DataTables(
            pymongo_object=mock_flask_pymongo,
            collection_name="test_collection",
            request_args=request_args
        )
        
        # Verify normal operation
        mock_db.__getitem__.assert_called_once_with("test_collection")
        assert dt.collection == mock_collection
        assert dt.count_total() == 50