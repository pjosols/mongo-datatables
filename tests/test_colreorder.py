"""Tests for ColReorder extension support."""

import pytest
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables, DataField


class TestColReorder(BaseDataTablesTest):
    """Test ColReorder extension functionality."""

    def test_colreorder_boolean_config(self):
        """Test ColReorder with boolean configuration."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True, "title": "Name"},
                {"data": "age", "searchable": True, "title": "Age"},
                {"data": "city", "searchable": True, "title": "City"}
            ],
            "colReorder": True
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
        
        assert "colReorder" in response
        assert response["colReorder"]["enabled"] is True
        assert "columns" in response["colReorder"]
        assert len(response["colReorder"]["columns"]) == 3

    def test_colreorder_object_config(self):
        """Test ColReorder with object configuration."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True, "title": "Name"},
                {"data": "age", "searchable": True, "title": "Age"},
                {"data": "city", "searchable": True, "title": "City"}
            ],
            "colReorder": {
                "order": [2, 0, 1],
                "realtime": True
            }
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
        
        assert "colReorder" in response
        assert response["colReorder"]["order"] == [2, 0, 1]
        assert response["colReorder"]["realtime"] is True
        assert "columns" in response["colReorder"]

    def test_colreorder_false_config(self):
        """Test ColReorder with false configuration."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True}
            ],
            "colReorder": False
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
        
        assert "colReorder" not in response

    def test_no_colreorder_config(self):
        """Test response without ColReorder configuration."""
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
        
        assert "colReorder" not in response

    def test_colreorder_column_data(self):
        """Test ColReorder column data structure."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "name": "name_col", "title": "Full Name"},
                {"data": "age", "title": "Age"},
                {"data": "city"}
            ],
            "colReorder": True
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
        
        assert "colReorder" in response
        columns = response["colReorder"]["columns"]
        
        # Check first column
        assert columns[0]["index"] == 0
        assert columns[0]["data"] == "name"
        assert columns[0]["name"] == "name_col"
        assert columns[0]["title"] == "Full Name"
        
        # Check second column
        assert columns[1]["index"] == 1
        assert columns[1]["data"] == "age"
        assert columns[1]["title"] == "Age"
        
        # Check third column (no title, should use data)
        assert columns[2]["index"] == 2
        assert columns[2]["data"] == "city"
        assert columns[2]["title"] == "city"