"""Test configuration validation functionality."""

import pytest
from unittest.mock import Mock, MagicMock
from mongo_datatables.config_validator import ConfigValidator, ValidationResult
from mongo_datatables.datatables import DataField


class TestConfigValidator:
    """Test configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("created_at", "date")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_validate_colreorder_config_valid(self):
        """Test valid ColReorder configuration."""
        config = {"enabled": True, "realtime": True}
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid
        assert len(result.errors) == 0
    
    def test_validate_colreorder_config_invalid_order(self):
        """Test invalid ColReorder order configuration."""
        config = {"order": [0, 1]}  # Too few columns
        result = self.validator.validate_colreorder_config(config)
        assert not result.is_valid
        assert len(result.errors) == 1
        assert "order length" in result.errors[0]
    
    def test_validate_fixedcolumns_config_valid(self):
        """Test valid FixedColumns configuration."""
        config = {"left": 1, "right": 1}
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid
        assert len(result.errors) == 0
    
    def test_validate_fixedcolumns_config_too_many(self):
        """Test FixedColumns with too many fixed columns."""
        config = {"left": 2, "right": 2}  # 4 total, but only 3 columns
        result = self.validator.validate_fixedcolumns_config(config)
        assert not result.is_valid
        assert len(result.errors) == 1
        assert "exceed total columns" in result.errors[0]
    
    def test_validate_searchbuilder_config_performance_warning(self):
        """Test SearchBuilder performance warning for unindexed fields."""
        # Mock no indexes
        self.mock_collection.list_indexes.return_value = [{"key": {"_id": 1}}]
        
        config = {"enabled": True}
        result = self.validator.validate_searchbuilder_config(config)
        assert result.is_valid
        assert len(result.warnings) == 1
        assert "unindexed fields" in result.warnings[0]
    
    def test_validate_performance_large_dataset(self):
        """Test performance validation with large dataset."""
        self.mock_collection.estimated_document_count.return_value = 200000
        
        result = self.validator.validate_performance({})
        assert result.is_valid
        assert len(result.warnings) == 1
        assert "Large dataset" in result.warnings[0]
    
    def test_validation_result_structure(self):
        """Test ValidationResult structure."""
        result = ValidationResult()
        assert result.is_valid
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
        
        result.add_error("Test error", "Technical detail")
        assert not result.is_valid
        assert len(result.errors) == 1
        assert len(result.technical_details) == 1
        
        result.add_warning("Test warning")
        assert len(result.warnings) == 1