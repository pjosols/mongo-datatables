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

    def test_colreorder_with_empty_columns(self):
        """Test ColReorder with empty columns list."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [],
            "colReorder": True
        }
        
        data_fields = []
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "colReorder" in response
        assert response["colReorder"]["enabled"] is True
        assert "columns" in response["colReorder"]
        assert len(response["colReorder"]["columns"]) == 0

    def test_colreorder_with_nested_field_names(self):
        """Test ColReorder with nested field names."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "user.profile.name", "title": "Name"},
                {"data": "user.profile.age", "title": "Age"},
                {"data": "user.settings.theme", "title": "Theme"}
            ],
            "colReorder": True
        }
        
        data_fields = [
            DataField("user.profile.name", "string"),
            DataField("user.profile.age", "number"),
            DataField("user.settings.theme", "string")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "colReorder" in response
        columns = response["colReorder"]["columns"]
        
        assert columns[0]["data"] == "user.profile.name"
        assert columns[1]["data"] == "user.profile.age"
        assert columns[2]["data"] == "user.settings.theme"

    def test_colreorder_with_invalid_order_array(self):
        """Test ColReorder with invalid order array (non-list)."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "title": "Name"},
                {"data": "age", "title": "Age"}
            ],
            "colReorder": {
                "order": "invalid",  # Should be a list
                "realtime": False
            }
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
        
        assert "colReorder" in response
        # Should not include invalid order
        assert "order" not in response["colReorder"]
        assert response["colReorder"]["realtime"] is False

    def test_colreorder_realtime_false(self):
        """Test ColReorder with realtime set to false."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "title": "Name"},
                {"data": "age", "title": "Age"}
            ],
            "colReorder": {
                "realtime": False
            }
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
        
        assert "colReorder" in response
        assert response["colReorder"]["realtime"] is False

    def test_colreorder_with_other_extensions(self):
        """Test ColReorder working alongside other extensions."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True, "title": "Name"},
                {"data": "age", "searchable": True, "title": "Age"},
                {"data": "status", "searchable": True, "title": "Status"}
            ],
            "colReorder": True,
            "fixedHeader": True,
            "responsive": True,
            "searchBuilder": True
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("status", "string")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        # All extensions should be present
        assert "colReorder" in response
        assert "fixedHeader" in response
        assert "responsive" in response
        assert "searchBuilder" in response
        
        # ColReorder should have proper structure
        assert response["colReorder"]["enabled"] is True
        assert len(response["colReorder"]["columns"]) == 3

    def test_colreorder_column_missing_data_field(self):
        """Test ColReorder with columns missing data field."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "title": "Name"},
                {"title": "No Data Field"},  # Missing data field
                {"data": "age", "title": "Age"}
            ],
            "colReorder": True
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
        
        assert "colReorder" in response
        columns = response["colReorder"]["columns"]
        
        # Should handle missing data field gracefully
        assert columns[0]["data"] == "name"
        assert columns[1]["data"] == ""  # Empty string for missing data
        assert columns[2]["data"] == "age"

    def test_colreorder_empty_object_config(self):
        """Test ColReorder with empty object configuration."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "title": "Name"},
                {"data": "age", "title": "Age"}
            ],
            "colReorder": {}  # Empty object
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
        
        # Empty config should not include colReorder in response
        assert "colReorder" not in response

    def test_colreorder_backward_compatibility(self):
        """Test that ColReorder doesn't break existing functionality."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "search": {"value": "test", "regex": False},
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [
                {"data": "name", "searchable": True, "orderable": True},
                {"data": "age", "searchable": True, "orderable": True}
            ],
            "colReorder": True
        }
        
        data_fields = [
            DataField("name", "string"),
            DataField("age", "number")
        ]
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 5
        self.collection.aggregate.return_value = self.sample_docs[:2]
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        # Standard DataTables response should be intact
        assert "draw" in response
        assert "recordsTotal" in response
        assert "recordsFiltered" in response
        assert "data" in response
        
        # ColReorder should be added without affecting other functionality
        assert "colReorder" in response
        assert response["colReorder"]["enabled"] is True

    def test_colreorder_performance_with_large_column_count(self):
        """Test ColReorder performance with many columns."""
        # Create 50 columns
        columns = []
        data_fields = []
        for i in range(50):
            columns.append({
                "data": f"field_{i}",
                "title": f"Field {i}",
                "searchable": True
            })
            data_fields.append(DataField(f"field_{i}", "string"))
        
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": columns,
            "colReorder": True
        }
        
        # Mock the collection methods
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields)
        response = dt.get_rows()
        
        assert "colReorder" in response
        assert len(response["colReorder"]["columns"]) == 50
        
        # Verify all columns are properly indexed
        for i, column in enumerate(response["colReorder"]["columns"]):
            assert column["index"] == i
            assert column["data"] == f"field_{i}"
            assert column["title"] == f"Field {i}"