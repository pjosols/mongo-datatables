"""Tests for Responsive extension support."""

import pytest
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestResponsive(BaseDataTablesTest):
    """Test Responsive extension functionality."""

    def test_responsive_config_parsing(self):
        """Test parsing of responsive configuration parameters."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "responsive": {
                "breakpoints": {
                    "xs": 0,
                    "sm": 576,
                    "md": 768,
                    "lg": 992,
                    "xl": 1200
                },
                "display": {
                    "childRow": True,
                    "childRowImmediate": False
                },
                "priorities": {
                    "0": 1,
                    "1": 2,
                    "2": 3
                }
            }
        }
        
        dt = DataTables(self.mongo, 'users', request_args)
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "responsive" in response
        assert "breakpoints" in response["responsive"]
        assert "display" in response["responsive"]
        assert "priorities" in response["responsive"]
        assert response["responsive"]["breakpoints"]["sm"] == 576
        assert response["responsive"]["display"]["childRow"] is True
        assert response["responsive"]["priorities"]["0"] == 1

    def test_responsive_config_partial(self):
        """Test parsing with partial responsive configuration."""
        request_args = {
            "draw": "1",
            "start": "0", 
            "length": "10",
            "responsive": {
                "breakpoints": {
                    "sm": 576,
                    "lg": 992
                }
            }
        }
        
        dt = DataTables(self.mongo, 'users', request_args)
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "responsive" in response
        assert "breakpoints" in response["responsive"]
        assert "display" not in response["responsive"]
        assert "priorities" not in response["responsive"]
        assert response["responsive"]["breakpoints"]["sm"] == 576

    def test_no_responsive_config(self):
        """Test that responsive config is not included when not requested."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10"
        }
        
        dt = DataTables(self.mongo, 'users', request_args)
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "responsive" not in response

    def test_empty_responsive_config(self):
        """Test handling of empty responsive configuration."""
        request_args = {
            "draw": "1", 
            "start": "0",
            "length": "10",
            "responsive": {}
        }
        
        dt = DataTables(self.mongo, 'users', request_args)
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "responsive" not in response