"""Tests for FixedHeader extension support."""

import pytest
from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestFixedHeader(BaseDataTablesTest):
    """Test FixedHeader extension functionality."""

"""Tests for FixedHeader extension support."""

import pytest
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables, DataField


class TestFixedHeader(BaseDataTablesTest):
    """Test FixedHeader extension functionality."""

    def test_fixed_header_boolean_true(self):
        """Test FixedHeader with boolean true configuration."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "fixedHeader": True,
            "columns": [
                {"data": "name", "searchable": True, "orderable": True},
                {"data": "age", "searchable": True, "orderable": True}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            [
                DataField("name", "string"),
                DataField("age", "number")
            ]
        )
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "fixedHeader" in response
        assert response["fixedHeader"]["header"] is True
        assert response["fixedHeader"]["footer"] is False

    def test_fixed_header_boolean_false(self):
        """Test FixedHeader with boolean false configuration."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "fixedHeader": False,
            "columns": [
                {"data": "name", "searchable": True, "orderable": True}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            [DataField("name", "string")]
        )
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "fixedHeader" not in response

    def test_fixed_header_object_config(self):
        """Test FixedHeader with object configuration."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "fixedHeader": {
                "header": True,
                "footer": True
            },
            "columns": [
                {"data": "name", "searchable": True, "orderable": True}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            [DataField("name", "string")]
        )
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "fixedHeader" in response
        assert response["fixedHeader"]["header"] is True
        assert response["fixedHeader"]["footer"] is True

    def test_fixed_header_partial_config(self):
        """Test FixedHeader with partial object configuration."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "fixedHeader": {
                "header": False
            },
            "columns": [
                {"data": "name", "searchable": True, "orderable": True}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            [DataField("name", "string")]
        )
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "fixedHeader" in response
        assert response["fixedHeader"]["header"] is False
        assert "footer" not in response["fixedHeader"]

    def test_no_fixed_header_config(self):
        """Test response when FixedHeader is not configured."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "columns": [
                {"data": "name", "searchable": True, "orderable": True}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            [DataField("name", "string")]
        )
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        assert "fixedHeader" not in response

    def test_fixed_header_with_other_extensions(self):
        """Test FixedHeader works alongside other extensions."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "fixedHeader": True,
            "fixedColumns": {"left": 1},
            "responsive": True,
            "columns": [
                {"data": "name", "searchable": True, "orderable": True},
                {"data": "age", "searchable": True, "orderable": True}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            [
                DataField("name", "string"),
                DataField("age", "number")
            ]
        )
        
        # Mock the collection methods
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        
        response = dt.get_rows()
        
        # Verify FixedHeader is present
        assert "fixedHeader" in response
        assert response["fixedHeader"]["header"] is True
        assert response["fixedHeader"]["footer"] is False
        
        # Verify other extensions are also present
        assert "fixedColumns" in response
        assert "responsive" in response