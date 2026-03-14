"""Tests for Select extension support in mongo-datatables."""

import pytest
from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestSelect(BaseDataTablesTest):
    """Test Select extension functionality."""

    def test_select_not_requested(self):
        """Test that Select configuration is not included when not requested."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "columns": [
                {"data": "name", "searchable": "true"},
                {"data": "position", "searchable": "true"}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "employees",
            request_args,
            data_fields=[
                DataField("name", "string"),
                DataField("position", "string")
            ]
        )
        
        response = dt.get_rows()
        assert "select" not in response

    def test_select_boolean_true(self):
        """Test Select extension with boolean true (default configuration)."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "select": True,
            "columns": [
                {"data": "name", "searchable": "true"},
                {"data": "position", "searchable": "true"}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "employees",
            request_args,
            data_fields=[
                DataField("name", "string"),
                DataField("position", "string")
            ]
        )
        
        response = dt.get_rows()
        assert "select" in response
        assert response["select"]["style"] == "os"

    def test_select_style_configurations(self):
        """Test various Select extension style configurations."""
        styles = ["single", "multi", "multi+shift", "os"]
        
        for style in styles:
            request_args = {
                "draw": "1",
                "start": "0", 
                "length": "10",
                "select": {"style": style},
                "columns": [{"data": "name", "searchable": "true"}]
            }
            
            dt = DataTables(
                self.mongo,
                "employees", 
                request_args,
                data_fields=[DataField("name", "string")]
            )
            
            response = dt.get_rows()
            assert "select" in response
            assert response["select"]["style"] == style