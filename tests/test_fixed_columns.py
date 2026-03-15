"""Tests for FixedColumns extension support."""

import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestFixedColumns(BaseDataTablesTest):
    """Test FixedColumns extension functionality."""

    def test_fixed_columns_left_only(self):
        """Test FixedColumns with left columns only."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True},
                {"data": "city", "searchable": True}
            ],
            "fixedColumns": {"left": 2}
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("city", "string")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "fixedColumns" in response
        assert response["fixedColumns"]["left"] == 2
        assert "right" not in response["fixedColumns"]

    def test_fixed_columns_right_only(self):
        """Test FixedColumns with right columns only."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True},
                {"data": "city", "searchable": True}
            ],
            "fixedColumns": {"right": 1}
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("city", "string")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "fixedColumns" in response
        assert response["fixedColumns"]["right"] == 1
        assert "left" not in response["fixedColumns"]

    def test_fixed_columns_both_sides(self):
        """Test FixedColumns with both left and right columns."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True},
                {"data": "city", "searchable": True},
                {"data": "country", "searchable": True}
            ],
            "fixedColumns": {"left": 1, "right": 1}
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("city", "string"),
            DataField("country", "string")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "fixedColumns" in response
        assert response["fixedColumns"]["left"] == 1
        assert response["fixedColumns"]["right"] == 1

    def test_no_fixed_columns(self):
        """Test response without FixedColumns configuration."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True}
            ]
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "fixedColumns" not in response

    def test_invalid_fixed_columns_values(self):
        """Test handling of invalid FixedColumns values."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True}
            ],
            "fixedColumns": {"left": "invalid", "right": None}
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "fixedColumns" in response
        assert response["fixedColumns"]["left"] == "invalid"  # passed through as-is
        assert response["fixedColumns"]["right"] is None      # passed through as-is