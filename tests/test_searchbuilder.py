"""Tests for SearchBuilder extension support."""

import pytest
from datetime import datetime
from bson.objectid import ObjectId
from unittest.mock import MagicMock

from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestSearchBuilder(BaseDataTablesTest):
    """Test SearchBuilder extension functionality."""

    def setUp(self):
        """Set up test data."""
        super().setUp()
        
        # Mock the collection methods for our test data
        test_data = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "name": "John Doe",
                "age": 30,
                "salary": 50000,
                "department": "Engineering",
                "hire_date": datetime(2020, 1, 15),
                "active": True
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "name": "Jane Smith",
                "age": 25,
                "salary": 45000,
                "department": "Marketing",
                "hire_date": datetime(2021, 3, 10),
                "active": True
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439013"),
                "name": "Bob Johnson",
                "age": 35,
                "salary": 60000,
                "department": "Engineering",
                "hire_date": datetime(2019, 6, 20),
                "active": False
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439014"),
                "name": "Alice Brown",
                "age": 28,
                "salary": 52000,
                "department": "Sales",
                "hire_date": datetime(2020, 11, 5),
                "active": True
            }
        ]
        
        # Mock aggregation results for different queries
        self.collection.aggregate.return_value = iter(test_data)
        self.collection.count_documents.return_value = len(test_data)
        self.collection.estimated_document_count.return_value = len(test_data)
        
        # Mock list_indexes for text index check
        self.collection.list_indexes.return_value = []
        
        # Define data fields
        self.data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("salary", "number"),
            DataField("department", "string"),
            DataField("hire_date", "date"),
            DataField("active", "boolean")
        ]

    def test_get_searchbuilder_options(self):
        """Test SearchBuilder options generation."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "searchBuilder": True,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True},
                {"data": "salary", "searchable": True},
                {"data": "department", "searchable": True},
                {"data": "hire_date", "searchable": True},
                {"data": "active", "searchable": True}
            ]
        }
        
        # Mock aggregation results for options
        mock_options = [
            {"_id": "Engineering", "count": 2},
            {"_id": "Marketing", "count": 1},
            {"_id": "Sales", "count": 1}
        ]
        self.collection.aggregate.return_value = iter(mock_options)
        
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        options = dt.get_searchbuilder_options()
        
        # Check that options are generated for all searchable columns
        assert "name" in options
        assert "age" in options
        assert "salary" in options
        assert "department" in options
        assert "hire_date" in options
        assert "active" in options
        
        # Check structure of options
        assert "options" in options["name"]
        assert "operators" in options["name"]
        assert "type" in options["name"]
        
        # Check operators for different field types
        assert "contains" in options["name"]["operators"]  # string field
        assert "=" in options["age"]["operators"]  # number field
        assert ">" in options["age"]["operators"]  # number field
        assert "null" in options["department"]["operators"]  # all fields should have null operators

    def test_searchbuilder_parse_conditions(self):
        """Test SearchBuilder condition parsing."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "searchBuilder": {
                "conditions": [
                    {
                        "data": "department",
                        "condition": "=",
                        "value": "Engineering"
                    }
                ],
                "logic": "AND"
            },
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "department", "searchable": True}
            ]
        }
        
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        searchbuilder_filter = dt._parse_searchbuilder_filters()
        
        # Should create a filter condition
        assert searchbuilder_filter is not None
        assert "department" in str(searchbuilder_filter)

    def test_searchbuilder_build_condition_equals(self):
        """Test SearchBuilder equals condition building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        condition = dt._build_searchbuilder_condition("department", "=", "Engineering", "string")
        expected = {"department": "Engineering"}
        
        assert condition == expected

    def test_searchbuilder_build_condition_not_equals(self):
        """Test SearchBuilder not equals condition building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        condition = dt._build_searchbuilder_condition("department", "!=", "Engineering", "string")
        expected = {"department": {"$ne": "Engineering"}}
        
        assert condition == expected

    def test_searchbuilder_build_condition_greater_than(self):
        """Test SearchBuilder greater than condition building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        condition = dt._build_searchbuilder_condition("age", ">", 30, "number")
        expected = {"age": {"$gt": 30}}
        
        assert condition == expected

    def test_searchbuilder_build_condition_contains(self):
        """Test SearchBuilder contains condition building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        condition = dt._build_searchbuilder_condition("name", "contains", "John", "string")
        
        # Should create a regex condition
        assert "$regex" in condition["name"]
        assert condition["name"]["$options"] == "i"

    def test_searchbuilder_build_condition_null(self):
        """Test SearchBuilder null condition building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        condition = dt._build_searchbuilder_condition("department", "null", None, "string")
        
        # Should create an OR condition for null/missing
        assert "$or" in condition

    def test_searchbuilder_and_logic(self):
        """Test SearchBuilder AND logic building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        conditions = [
            {"data": "department", "condition": "=", "value": "Engineering"},
            {"data": "age", "condition": ">", "value": 30}
        ]
        
        query = dt._build_searchbuilder_query(conditions, "AND")
        
        # Should create an AND condition
        assert "$and" in query

    def test_searchbuilder_or_logic(self):
        """Test SearchBuilder OR logic building."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        conditions = [
            {"data": "department", "condition": "=", "value": "Marketing"},
            {"data": "department", "condition": "=", "value": "Sales"}
        ]
        
        query = dt._build_searchbuilder_query(conditions, "OR")
        
        # Should create an OR condition
        assert "$or" in query

    def test_searchbuilder_nested_conditions(self):
        """Test SearchBuilder with nested condition groups."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        conditions = [
            {
                "conditions": [
                    {"data": "department", "condition": "=", "value": "Engineering"},
                    {"data": "age", "condition": ">", "value": 25}
                ],
                "logic": "AND"
            },
            {"data": "department", "condition": "=", "value": "Sales"}
        ]
        
        query = dt._build_searchbuilder_query(conditions, "OR")
        
        # Should create nested conditions
        assert "$or" in query
        assert len(query["$or"]) == 2

    def test_searchbuilder_response_includes_options(self):
        """Test that SearchBuilder options are included in response when requested."""
        request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "searchBuilder": True,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "department", "searchable": True}
            ]
        }
        
        # Mock aggregation for options
        self.collection.aggregate.return_value = iter([])
        
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        response = dt.get_rows()
        
        assert "searchBuilder" in response
        assert "options" in response["searchBuilder"]

    def test_searchbuilder_empty_conditions(self):
        """Test SearchBuilder with empty conditions."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        query = dt._build_searchbuilder_query([], "AND")
        
        # Should return empty query
        assert query == {}

    def test_searchbuilder_invalid_condition(self):
        """Test SearchBuilder with invalid condition."""
        dt = DataTables(self.mongo, "test_collection", {}, self.data_fields)
        
        # Missing required fields
        condition = dt._build_searchbuilder_condition("", "=", "test", "string")
        
        # Should return None for invalid condition
        assert condition is None