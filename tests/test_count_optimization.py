"""Tests for optimized count operations."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from mongo_datatables import DataTables


class TestCountOptimization:
    """Test optimized count operations for performance improvements."""

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