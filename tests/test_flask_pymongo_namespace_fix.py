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
        
        # Create the Flask-PyMongo wrapper structure: pymongo_object.db (Database wrapper)
        mock_db_wrapper = Mock()
        mock_flask_pymongo.db = mock_db_wrapper
        
        # Create the problematic nested structure: db.db (Collection named "db")
        mock_db_collection = Mock()
        mock_db_wrapper.db = mock_db_collection
        
        # Set up the db collection to look like a Collection (has database and name attributes)
        mock_db_collection.database = Mock()
        mock_db_collection.name = "db"
        
        # Mock the actual books collection
        mock_books_collection = Mock()
        mock_books_collection.full_name = "test_database.books"
        mock_books_collection.database = Mock()
        mock_books_collection.name = "books"
        mock_books_collection.estimated_document_count.return_value = 100
        mock_books_collection.count_documents.return_value = 100
        mock_books_collection.list_indexes.return_value = []
        
        # Set up collection access on the db wrapper (not the nested db collection)
        mock_db_wrapper.__getitem__ = Mock(return_value=mock_books_collection)
        
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
            collection_name="books",
            request_args=request_args
        )
        
        # Verify that the db wrapper was used (not the nested db collection)
        mock_db_wrapper.__getitem__.assert_called_once_with("books")
        
        # Check that we got the correct collection
        assert dt.collection == mock_books_collection
        
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
        
        # Set up collection access - make mock_db subscriptable
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        
        # Ensure mock_db doesn't have a 'db' attribute to test normal case
        if hasattr(mock_db, 'db'):
            delattr(mock_db, 'db')
        
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