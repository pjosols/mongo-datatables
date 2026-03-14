"""Tests for additional extension validation methods."""

import pytest
from unittest.mock import Mock
from mongo_datatables.config_validator import ConfigValidator, ValidationResult
from mongo_datatables.datatables import DataField


class TestButtonsValidationMethods:
    """Test Buttons extension validation methods."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_buttons_empty_config(self):
        """Test Buttons with empty configuration."""
        result = self.validator.validate_buttons_config({})
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_buttons_none_config(self):
        """Test Buttons with None configuration."""
        result = self.validator.validate_buttons_config(None)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_buttons_boolean_config(self):
        """Test Buttons with boolean configuration."""
        result = self.validator.validate_buttons_config(True)
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        result = self.validator.validate_buttons_config(False)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_buttons_valid_array_config(self):
        """Test Buttons with valid array configuration."""
        config = {
            "buttons": [
                {"extend": "copy"},
                {"extend": "csv"},
                {"extend": "excel"},
                {"extend": "pdf"},
                {"extend": "print"}
            ]
        }
        result = self.validator.validate_buttons_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
    
    def test_buttons_invalid_array_type(self):
        """Test Buttons with invalid array type."""
        config = {"buttons": "invalid"}
        result = self.validator.validate_buttons_config(config)
        assert result.is_valid is False
        assert "Buttons configuration must be a list" in result.errors[0]
        assert "Got <class 'str'>" in result.technical_details[0]
    
    def test_buttons_unknown_extend_type(self):
        """Test Buttons with unknown extend type."""
        config = {
            "buttons": [
                {"extend": "copy"},
                {"extend": "unknown_type"},
                {"extend": "csv"}
            ]
        }
        result = self.validator.validate_buttons_config(config)
        assert result.is_valid is True  # Warnings don't invalidate
        assert len(result.warnings) == 1
        assert "Unknown button type 'unknown_type' at index 1" in result.warnings[0]
        assert "Valid types:" in result.technical_details[0]
    
    def test_buttons_mixed_valid_invalid_types(self):
        """Test Buttons with mix of valid and invalid types."""
        config = {
            "buttons": [
                {"extend": "copy"},
                {"extend": "invalid1"},
                {"extend": "csv"},
                {"extend": "invalid2"}
            ]
        }
        result = self.validator.validate_buttons_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 2
        assert "invalid1" in result.warnings[0]
        assert "invalid2" in result.warnings[1]
    
    def test_buttons_without_extend_property(self):
        """Test Buttons with buttons that don't have extend property."""
        config = {
            "buttons": [
                {"extend": "copy"},
                {"text": "Custom Button", "action": "function() {}"},
                {"extend": "csv"}
            ]
        }
        result = self.validator.validate_buttons_config(config)
        assert result.is_valid is True
        # Should not warn about buttons without extend property
        assert len(result.warnings) == 0


class TestSelectValidationMethods:
    """Test Select extension validation methods."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_select_empty_config(self):
        """Test Select with empty configuration."""
        result = self.validator.validate_select_config({})
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_select_none_config(self):
        """Test Select with None configuration."""
        result = self.validator.validate_select_config(None)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_select_boolean_config(self):
        """Test Select with boolean configuration."""
        result = self.validator.validate_select_config(True)
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        result = self.validator.validate_select_config(False)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_select_valid_styles(self):
        """Test Select with valid style configurations."""
        valid_styles = ["single", "multi", "os", "api"]
        
        for style in valid_styles:
            config = {"style": style}
            result = self.validator.validate_select_config(config)
            assert result.is_valid is True, f"Style '{style}' should be valid"
            assert len(result.errors) == 0
    
    def test_select_invalid_style(self):
        """Test Select with invalid style."""
        config = {"style": "invalid_style"}
        result = self.validator.validate_select_config(config)
        assert result.is_valid is False
        assert "Invalid select style 'invalid_style'" in result.errors[0]
        assert "Valid styles:" in result.technical_details[0]
        assert "single" in result.technical_details[0]
        assert "multi" in result.technical_details[0]
    
    def test_select_with_other_options(self):
        """Test Select with other valid options."""
        config = {
            "style": "multi",
            "selector": "td:first-child",
            "items": "row",
            "toggleable": True
        }
        result = self.validator.validate_select_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0


class TestRowGroupValidationMethods:
    """Test RowGroup extension validation methods."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("category", "string"),
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_rowgroup_empty_config(self):
        """Test RowGroup with empty configuration."""
        result = self.validator.validate_rowgroup_config({})
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_rowgroup_none_config(self):
        """Test RowGroup with None configuration."""
        result = self.validator.validate_rowgroup_config(None)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_rowgroup_boolean_config(self):
        """Test RowGroup with boolean configuration."""
        result = self.validator.validate_rowgroup_config(True)
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        result = self.validator.validate_rowgroup_config(False)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_rowgroup_valid_string_datasrc(self):
        """Test RowGroup with valid string dataSrc."""
        config = {"dataSrc": "category"}
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
    
    def test_rowgroup_invalid_string_datasrc(self):
        """Test RowGroup with invalid string dataSrc."""
        config = {"dataSrc": "nonexistent_field"}
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is True  # Warnings don't invalidate
        assert len(result.warnings) == 1
        assert "RowGroup dataSrc field 'nonexistent_field' not found" in result.warnings[0]
        assert "Ensure the field name matches your DataField definitions" in result.technical_details[0]
    
    def test_rowgroup_valid_numeric_datasrc(self):
        """Test RowGroup with valid numeric dataSrc."""
        config = {"dataSrc": 0}  # First column
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
        
        config = {"dataSrc": 3}  # Last column
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_rowgroup_invalid_numeric_datasrc_negative(self):
        """Test RowGroup with invalid negative numeric dataSrc."""
        config = {"dataSrc": -1}
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is False
        assert "RowGroup dataSrc column index -1 is out of range" in result.errors[0]
        assert "Valid range: 0 to 3" in result.technical_details[0]
    
    def test_rowgroup_invalid_numeric_datasrc_too_high(self):
        """Test RowGroup with invalid too-high numeric dataSrc."""
        config = {"dataSrc": 4}  # We only have 4 columns (0-3)
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is False
        assert "RowGroup dataSrc column index 4 is out of range" in result.errors[0]
        assert "Valid range: 0 to 3" in result.technical_details[0]
    
    def test_rowgroup_with_other_options(self):
        """Test RowGroup with other valid options."""
        config = {
            "dataSrc": "category",
            "startRender": None,
            "endRender": None,
            "className": "group-row"
        }
        result = self.validator.validate_rowgroup_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0


class TestSearchPanesValidationMethods:
    """Test SearchPanes extension validation methods."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("category", "string"),
            DataField("status", "string"),
            DataField("priority", "number"),
            DataField("title", "string"),
            DataField("author", "string")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_searchpanes_empty_config(self):
        """Test SearchPanes with empty configuration."""
        result = self.validator.validate_searchpanes_config({})
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_searchpanes_none_config(self):
        """Test SearchPanes with None configuration."""
        result = self.validator.validate_searchpanes_config(None)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_searchpanes_boolean_config(self):
        """Test SearchPanes with boolean configuration."""
        result = self.validator.validate_searchpanes_config(True)
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        result = self.validator.validate_searchpanes_config(False)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_searchpanes_few_columns_no_warning(self):
        """Test SearchPanes with few columns - no performance warning."""
        config = {"columns": [0, 1, 2]}  # 3 columns
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 0
    
    def test_searchpanes_exactly_five_columns_no_warning(self):
        """Test SearchPanes with exactly 5 columns - no warning."""
        config = {"columns": [0, 1, 2, 3, 4]}  # 5 columns
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 0
    
    def test_searchpanes_many_columns_warning(self):
        """Test SearchPanes with many columns - should warn."""
        config = {"columns": [0, 1, 2, 3, 4, 5]}  # 6 columns
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 1
        assert "SearchPanes with 6 columns may impact performance" in result.warnings[0]
        assert "Consider limiting to 5 or fewer columns" in result.technical_details[0]
    
    def test_searchpanes_valid_threshold_values(self):
        """Test SearchPanes with valid threshold values."""
        valid_thresholds = [0, 0.1, 0.5, 0.9, 1.0]
        
        for threshold in valid_thresholds:
            config = {"threshold": threshold}
            result = self.validator.validate_searchpanes_config(config)
            assert result.is_valid is True, f"Threshold {threshold} should be valid"
            assert len(result.errors) == 0
    
    def test_searchpanes_invalid_threshold_negative(self):
        """Test SearchPanes with invalid negative threshold."""
        config = {"threshold": -0.1}
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is False
        assert "SearchPanes threshold must be a number between 0 and 1" in result.errors[0]
        assert "got -0.1" in result.errors[0]
        assert "Threshold determines when to show/hide panes" in result.technical_details[0]
    
    def test_searchpanes_invalid_threshold_too_high(self):
        """Test SearchPanes with invalid too-high threshold."""
        config = {"threshold": 1.5}
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is False
        assert "SearchPanes threshold must be a number between 0 and 1" in result.errors[0]
        assert "got 1.5" in result.errors[0]
    
    def test_searchpanes_invalid_threshold_type(self):
        """Test SearchPanes with invalid threshold type."""
        config = {"threshold": "invalid"}
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is False
        assert "SearchPanes threshold must be a number between 0 and 1" in result.errors[0]
        assert "got invalid" in result.errors[0]
    
    def test_searchpanes_with_other_options(self):
        """Test SearchPanes with other valid options."""
        config = {
            "columns": [0, 1, 2],
            "threshold": 0.6,
            "layout": "columns-3",
            "cascadePanes": True,
            "viewTotal": True
        }
        result = self.validator.validate_searchpanes_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0


class TestAllExtensionsIntegration:
    """Test validation of all extensions together."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("id", "objectid"),
            DataField("category", "string"),
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number"),
            DataField("status", "string")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_all_extensions_valid_configs(self):
        """Test that all extensions can be validated with valid configs."""
        # Test all extension validation methods exist and work
        validation_methods = [
            ("validate_colreorder_config", {"order": [0, 1, 2, 3, 4, 5]}),
            ("validate_searchbuilder_config", {"columns": True}),
            ("validate_fixedcolumns_config", {"left": 1, "right": 1}),
            ("validate_responsive_config", {"breakpoints": []}),
            ("validate_buttons_config", {"buttons": [{"extend": "copy"}]}),
            ("validate_select_config", {"style": "multi"}),
            ("validate_rowgroup_config", {"dataSrc": "category"}),
            ("validate_searchpanes_config", {"columns": [0, 1], "threshold": 0.5}),
            ("validate_performance", {})
        ]
        
        for method_name, config in validation_methods:
            method = getattr(self.validator, method_name)
            result = method(config)
            assert result.is_valid is True, f"Method {method_name} should validate successfully"
    
    def test_all_extensions_boolean_configs(self):
        """Test that all extensions handle boolean configs properly."""
        boolean_validation_methods = [
            "validate_colreorder_config",
            "validate_fixedcolumns_config", 
            "validate_responsive_config",
            "validate_buttons_config",
            "validate_select_config",
            "validate_rowgroup_config",
            "validate_searchpanes_config"
        ]
        
        for method_name in boolean_validation_methods:
            method = getattr(self.validator, method_name)
            
            # Test True
            result = method(True)
            assert result.is_valid is True, f"Method {method_name} should handle True"
            
            # Test False  
            result = method(False)
            assert result.is_valid is True, f"Method {method_name} should handle False"
    
    def test_all_extensions_empty_configs(self):
        """Test that all extensions handle empty configs properly."""
        validation_methods = [
            "validate_colreorder_config",
            "validate_searchbuilder_config",
            "validate_fixedcolumns_config",
            "validate_responsive_config",
            "validate_buttons_config",
            "validate_select_config",
            "validate_rowgroup_config",
            "validate_searchpanes_config"
        ]
        
        for method_name in validation_methods:
            method = getattr(self.validator, method_name)
            
            # Test empty dict
            result = method({})
            assert result.is_valid is True, f"Method {method_name} should handle empty dict"
            
            # Test None
            result = method(None)
            assert result.is_valid is True, f"Method {method_name} should handle None"