"""Tests for SearchPanes extension support."""

import pytest
from unittest.mock import Mock, MagicMock
from pymongo.collection import Collection
from pymongo.database import Database
from mongo_datatables import DataTables, DataField


class TestSearchPanes:
    """Test SearchPanes functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create a mock PyMongo object
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        
        # Mock indexes
        self.collection.list_indexes.return_value = []
        
        self.data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("status", "string"),
        ]

    def test_searchpanes_options_generation(self):
        """Test generation of SearchPanes options."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True},
                {"data": "status", "searchable": True},
            ],
            "searchPanes": True
        }
        
        # Mock aggregation results
        self.collection.aggregate.return_value = [
            {"_id": "Active", "count": 5},
            {"_id": "Inactive", "count": 3},
        ]
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=self.data_fields
        )
        
        options = dt.get_searchpanes_options()
        
        assert "status" in options
        assert len(options["status"]) == 2
        assert options["status"][0]["label"] == "Active"
        assert options["status"][0]["count"] == 5

    def test_searchpanes_filtering(self):
        """Test SearchPanes filtering functionality."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "status", "searchable": True},
            ],
            "searchPanes": {
                "status": ["Active", "Pending"]
            }
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=self.data_fields
        )
        
        filter_condition = dt._parse_searchpanes_filters()
        
        expected = {"$and": [{"status": {"$in": ["Active", "Pending"]}}]}
        assert filter_condition == expected

    def test_searchpanes_in_response(self):
        """Test SearchPanes options included in response."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "status", "searchable": True},
            ],
            "searchPanes": True
        }
        
        # Mock all required methods
        self.collection.count_documents.return_value = 10
        self.collection.aggregate.return_value = [
            {"_id": "Active", "count": 5}
        ]
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=self.data_fields
        )
        
        response = dt.get_rows()
        
        assert "searchPanes" in response
        assert "options" in response["searchPanes"]

    def test_searchpanes_number_conversion(self):
        """Test number conversion in SearchPanes filters."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "age", "searchable": True},
            ],
            "searchPanes": {
                "age": ["25", "30"]
            }
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=self.data_fields
        )
        
        filter_condition = dt._parse_searchpanes_filters()
        
        expected = {"$and": [{"age": {"$in": [25, 30]}}]}
        assert filter_condition == expected