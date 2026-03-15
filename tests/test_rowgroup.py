"""Tests for RowGroup extension support."""

import pytest
from unittest.mock import MagicMock
from bson.objectid import ObjectId
from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestRowGroupExtension(BaseDataTablesTest):
    """Test RowGroup extension functionality."""

    def test_rowgroup_config_parsing(self):
        """Test RowGroup configuration parsing — only dataSrc is extracted."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "rowGroup": {
                "dataSrc": "category",
                "startRender": True,
                "endRender": True
            },
            "columns": [
                {"data": "category", "searchable": "true"},
                {"data": "value", "searchable": "true"}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=[
                DataField("category", "string"),
                DataField("value", "number")
            ]
        )
        
        config = dt._parse_rowgroup_config()
        assert config is not None
        assert config["dataSrc"] == "category"
        # startRender/endRender are client-side concerns; not parsed server-side
        assert "startRender" not in config
        assert "endRender" not in config

    def test_rowgroup_with_numeric_datasrc(self):
        """Test RowGroup with numeric dataSrc (column index)."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "columns": [
                {"data": "name", "searchable": "true"},
                {"data": "category", "searchable": "true"}
            ],
            "rowGroup": {
                "dataSrc": 1  # category column
            }
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=[
                DataField("name", "string"),
                DataField("category", "string")
            ]
        )
        
        config = dt._parse_rowgroup_config()
        assert config is not None
        assert config["dataSrc"] == 1

    def test_rowgroup_data_generation(self):
        """Test RowGroup data generation with aggregation."""
        # Mock aggregation results — only _id and count are used
        mock_groups = [
            {"_id": "A", "count": 2},
            {"_id": "B", "count": 2}
        ]
        self.collection.aggregate.return_value = mock_groups
        
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "rowGroup": {
                "dataSrc": "category"
            },
            "columns": [
                {"data": "category", "searchable": "true"}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=[
                DataField("category", "string"),
                DataField("value", "number"),
                DataField("name", "string")
            ]
        )
        
        rowgroup_data = dt._get_rowgroup_data()
        assert rowgroup_data is not None
        assert rowgroup_data["dataSrc"] == "category"
        assert "groups" in rowgroup_data
        
        groups = rowgroup_data["groups"]
        assert "A" in groups
        assert "B" in groups
        assert groups["A"]["count"] == 2
        assert groups["B"]["count"] == 2

    def test_rowgroup_in_response(self):
        """Test RowGroup data included in response."""
        # Mock the necessary methods
        self.collection.count_documents.return_value = 2
        self.collection.aggregate.side_effect = [
            [{"_id": "X", "count": 1}, {"_id": "Y", "count": 1}],  # For rowgroup data
            []  # For main results
        ]
        
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "rowGroup": {
                "dataSrc": "category"
            },
            "columns": [
                {"data": "category", "searchable": "true"}
            ]
        }
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=[
                DataField("category", "string"),
                DataField("value", "number")
            ]
        )
        
        response = dt.get_rows()
        assert "rowGroup" in response
        assert response["rowGroup"]["dataSrc"] == "category"
        assert "groups" in response["rowGroup"]

    def test_no_rowgroup_config(self):
        """Test behavior when RowGroup is not configured."""
        request_args = {
            "draw": "1",
            "start": "0",
            "length": "10",
            "columns": [
                {"data": "name", "searchable": "true"}
            ]
        }
        
        # Mock the necessary methods
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        
        dt = DataTables(
            self.mongo,
            "test_collection",
            request_args,
            data_fields=[DataField("name", "string")]
        )
        
        config = dt._parse_rowgroup_config()
        assert config is None
        
        rowgroup_data = dt._get_rowgroup_data()
        assert rowgroup_data is None
        
        response = dt.get_rows()
        assert "rowGroup" not in response

    def test_rowgroup_no_numeric_summaries(self):
        """Verify group_data contains no _sum or _avg keys (dead code removed)."""
        self.collection.aggregate.return_value = [
            {"_id": "A", "count": 3},
            {"_id": "B", "count": 1}
        ]
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "rowGroup": {"dataSrc": "category"},
            "columns": [{"data": "category", "searchable": "true"}]
        }
        dt = DataTables(
            self.mongo, "test_collection", request_args,
            data_fields=[DataField("category", "string"), DataField("value", "number")]
        )
        result = dt._get_rowgroup_data()
        for group_values in result["groups"].values():
            assert not any(k.endswith("_sum") or k.endswith("_avg") for k in group_values)

    def test_rowgroup_config_no_datasrc_returns_none(self):
        """Test that rowGroup config without dataSrc returns None."""
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "rowGroup": {"startRender": True},
            "columns": [{"data": "name", "searchable": "true"}]
        }
        dt = DataTables(self.mongo, "test_collection", request_args)
        assert dt._parse_rowgroup_config() is None