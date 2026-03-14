"""Comprehensive tests for the configuration validation system."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from mongo_datatables.config_validator import ConfigValidator, ValidationResult
from mongo_datatables.datatables import DataField


class TestValidationResult:
    """Test ValidationResult class functionality."""
    
    def test_validation_result_initialization(self):
        """Test ValidationResult initialization."""
        result = ValidationResult()
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []
        assert result.technical_details == []
        
        result_invalid = ValidationResult(is_valid=False)
        assert result_invalid.is_valid is False
    
    def test_add_error(self):
        """Test adding validation errors."""
        result = ValidationResult()
        result.add_error("Test error", "Technical detail")
        
        assert result.is_valid is False
        assert "Test error" in result.errors
        assert "Technical detail" in result.technical_details
    
    def test_add_warning(self):
        """Test adding validation warnings."""
        result = ValidationResult()
        result.add_warning("Test warning", "Technical detail")
        
        assert result.is_valid is True  # Warnings don't invalidate
        assert "Test warning" in result.warnings
        assert "Technical detail" in result.technical_details
    
    def test_multiple_errors_and_warnings(self):
        """Test multiple errors and warnings."""
        result = ValidationResult()
        result.add_error("Error 1")
        result.add_error("Error 2", "Tech detail 2")
        result.add_warning("Warning 1")
        result.add_warning("Warning 2", "Tech detail 4")
        
        assert result.is_valid is False
        assert len(result.errors) == 2
        assert len(result.warnings) == 2
        assert len(result.technical_details) == 2


class TestConfigValidator:
    """Test ConfigValidator class functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number"),
            DataField("published_date", "date"),
            DataField("tags", "array"),
            DataField("metadata", "object"),
            DataField("isbn", "string")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_validator_initialization(self):
        """Test ConfigValidator initialization."""
        assert self.validator.collection == self.mock_collection
        assert self.validator.data_fields == self.data_fields
        assert len(self.validator._field_names) == 7
        assert "title" in self.validator._field_names
        assert "year" in self.validator._field_names


class TestColReorderValidation:
    """Test ColReorder extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_colreorder_empty_config(self):
        """Test ColReorder with empty configuration."""
        result = self.validator.validate_colreorder_config({})
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_colreorder_none_config(self):
        """Test ColReorder with None configuration."""
        result = self.validator.validate_colreorder_config(None)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_colreorder_valid_order(self):
        """Test ColReorder with valid order configuration."""
        config = {"order": [2, 0, 1]}
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_colreorder_invalid_order_type(self):
        """Test ColReorder with invalid order type."""
        config = {"order": "invalid"}
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid is False
        assert "ColReorder order must be a list" in result.errors[0]
        assert "Got <class 'str'>" in result.technical_details[0]
    
    def test_colreorder_wrong_order_length(self):
        """Test ColReorder with wrong order array length."""
        config = {"order": [0, 1]}  # Missing one column
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid is False
        assert "order length (2) doesn't match columns (3)" in result.errors[0]
        assert "Order array must contain all column indices" in result.technical_details[0]
    
    def test_colreorder_too_many_columns(self):
        """Test ColReorder with too many columns in order."""
        config = {"order": [0, 1, 2, 3]}  # One extra column
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid is False
        assert "order length (4) doesn't match columns (3)" in result.errors[0]
    
    def test_colreorder_with_other_options(self):
        """Test ColReorder with other valid options."""
        config = {
            "order": [2, 0, 1],
            "realtime": False,
            "fixedColumnsLeft": 1
        }
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid is True


class TestSearchBuilderValidation:
    """Test SearchBuilder extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number"),
            DataField("published_date", "date"),
            DataField("tags", "array"),
            DataField("metadata", "object")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_searchbuilder_empty_config(self):
        """Test SearchBuilder with empty configuration."""
        result = self.validator.validate_searchbuilder_config({})
        assert result.is_valid is True
    
    def test_searchbuilder_none_config(self):
        """Test SearchBuilder with None configuration."""
        result = self.validator.validate_searchbuilder_config(None)
        assert result.is_valid is True
    
    @patch.object(ConfigValidator, '_get_indexed_fields')
    def test_searchbuilder_performance_warning_unindexed_fields(self, mock_get_indexed):
        """Test SearchBuilder performance warning for unindexed fields."""
        mock_get_indexed.return_value = {"author"}  # Only author is indexed
        
        result = self.validator.validate_searchbuilder_config({"columns": True})
        assert result.is_valid is True
        assert len(result.warnings) == 1
        assert "SearchBuilder may be slow on unindexed fields" in result.warnings[0]
        assert "title" in result.warnings[0]
        assert "year" in result.warnings[0]
        assert "published_date" in result.warnings[0]
        assert "author" not in result.warnings[0]  # Should not warn about indexed field
    
    @patch.object(ConfigValidator, '_get_indexed_fields')
    def test_searchbuilder_no_warning_all_indexed(self, mock_get_indexed):
        """Test SearchBuilder with all searchable fields indexed."""
        mock_get_indexed.return_value = {"title", "author", "year", "published_date"}
        
        result = self.validator.validate_searchbuilder_config({"columns": True})
        assert result.is_valid is True
        assert len(result.warnings) == 0
    
    @patch.object(ConfigValidator, '_get_indexed_fields')
    def test_searchbuilder_complex_types_ignored(self, mock_get_indexed):
        """Test that complex types (array, object) are ignored in warnings."""
        mock_get_indexed.return_value = set()  # No indexes
        
        result = self.validator.validate_searchbuilder_config({"columns": True})
        assert result.is_valid is True
        # Should only warn about string, number, date fields, not array/object
        warning_text = result.warnings[0] if result.warnings else ""
        assert "tags" not in warning_text  # array type
        assert "metadata" not in warning_text  # object type


class TestFixedColumnsValidation:
    """Test FixedColumns extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("col1", "string"),
            DataField("col2", "string"),
            DataField("col3", "string"),
            DataField("col4", "string"),
            DataField("col5", "string")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_fixedcolumns_empty_config(self):
        """Test FixedColumns with empty configuration."""
        result = self.validator.validate_fixedcolumns_config({})
        assert result.is_valid is True
    
    def test_fixedcolumns_none_config(self):
        """Test FixedColumns with None configuration."""
        result = self.validator.validate_fixedcolumns_config(None)
        assert result.is_valid is True
    
    def test_fixedcolumns_valid_left_only(self):
        """Test FixedColumns with valid left columns only."""
        config = {"left": 2}
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_fixedcolumns_valid_right_only(self):
        """Test FixedColumns with valid right columns only."""
        config = {"right": 2}
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_fixedcolumns_valid_both_sides(self):
        """Test FixedColumns with valid left and right columns."""
        config = {"left": 1, "right": 1}
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_fixedcolumns_too_many_left(self):
        """Test FixedColumns with too many left columns."""
        config = {"left": 5}  # All columns fixed
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is False
        assert "Fixed columns (5 left + 0 right) exceed total columns (5)" in result.errors[0]
        assert "Leave at least one column unfixed" in result.technical_details[0]
    
    def test_fixedcolumns_too_many_right(self):
        """Test FixedColumns with too many right columns."""
        config = {"right": 5}  # All columns fixed
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is False
        assert "Fixed columns (0 left + 5 right) exceed total columns (5)" in result.errors[0]
    
    def test_fixedcolumns_too_many_combined(self):
        """Test FixedColumns with too many combined columns."""
        config = {"left": 3, "right": 2}  # 5 total = all columns
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is False
        assert "Fixed columns (3 left + 2 right) exceed total columns (5)" in result.errors[0]
    
    def test_fixedcolumns_edge_case_valid(self):
        """Test FixedColumns edge case with maximum valid configuration."""
        config = {"left": 2, "right": 2}  # 4 out of 5 columns fixed
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is True
    
    def test_fixedcolumns_zero_values(self):
        """Test FixedColumns with zero values."""
        config = {"left": 0, "right": 0}
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is True


class TestResponsiveValidation:
    """Test Responsive extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
    
    def test_responsive_empty_config(self):
        """Test Responsive with empty configuration."""
        data_fields = [DataField(f"col{i}", "string") for i in range(5)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        result = validator.validate_responsive_config({})
        assert result.is_valid is True
    
    def test_responsive_none_config(self):
        """Test Responsive with None configuration."""
        data_fields = [DataField(f"col{i}", "string") for i in range(5)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        result = validator.validate_responsive_config(None)
        assert result.is_valid is True
    
    def test_responsive_few_columns_no_warning(self):
        """Test Responsive with few columns - no warning."""
        data_fields = [DataField(f"col{i}", "string") for i in range(5)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        config = {"breakpoints": [{"name": "desktop", "width": 1024}]}
        result = validator.validate_responsive_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 0
    
    def test_responsive_many_columns_warning(self):
        """Test Responsive with many columns - should warn."""
        data_fields = [DataField(f"col{i}", "string") for i in range(15)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        config = {"breakpoints": [{"name": "desktop", "width": 1024}]}
        result = validator.validate_responsive_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 1
        assert "Responsive with 15 columns may cause layout issues" in result.warnings[0]
        assert "Consider reducing columns or using column priorities" in result.technical_details[0]
    
    def test_responsive_exactly_ten_columns_no_warning(self):
        """Test Responsive with exactly 10 columns - no warning."""
        data_fields = [DataField(f"col{i}", "string") for i in range(10)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        config = {"breakpoints": [{"name": "desktop", "width": 1024}]}
        result = validator.validate_responsive_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 0
    
    def test_responsive_eleven_columns_warning(self):
        """Test Responsive with 11 columns - should warn."""
        data_fields = [DataField(f"col{i}", "string") for i in range(11)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        config = {"breakpoints": [{"name": "desktop", "width": 1024}]}
        result = validator.validate_responsive_config(config)
        assert result.is_valid is True
        assert len(result.warnings) == 1


class TestPerformanceValidation:
    """Test performance validation functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_performance_small_dataset_no_warning(self):
        """Test performance validation with small dataset."""
        self.mock_collection.estimated_document_count.return_value = 1000
        
        result = self.validator.validate_performance({})
        assert result.is_valid is True
        assert len(result.warnings) == 0
    
    def test_performance_large_dataset_warning(self):
        """Test performance validation with large dataset."""
        self.mock_collection.estimated_document_count.return_value = 500000
        
        result = self.validator.validate_performance({})
        assert result.is_valid is True
        assert len(result.warnings) == 1
        assert "Large dataset (500,000 documents) detected" in result.warnings[0]
        assert "Consider using indexes and limiting result sets" in result.technical_details[0]
    
    def test_performance_count_error_handled(self):
        """Test performance validation handles count errors gracefully."""
        self.mock_collection.estimated_document_count.side_effect = Exception("Connection error")
        
        result = self.validator.validate_performance({})
        assert result.is_valid is True
        # Should not crash, may or may not have warnings depending on other factors
    
    @patch.object(ConfigValidator, '_has_text_index')
    def test_performance_search_without_text_index_warning(self, mock_has_text_index):
        """Test performance warning for search without text index."""
        mock_has_text_index.return_value = False
        self.mock_collection.estimated_document_count.return_value = 1000
        
        request_args = {"search": {"value": "test search"}}
        result = self.validator.validate_performance(request_args)
        assert result.is_valid is True
        assert len(result.warnings) == 1
        assert "Global search without text index may be slow" in result.warnings[0]
        assert "Consider creating a text index for better search performance" in result.technical_details[0]
    
    @patch.object(ConfigValidator, '_has_text_index')
    def test_performance_search_with_text_index_no_warning(self, mock_has_text_index):
        """Test no performance warning for search with text index."""
        mock_has_text_index.return_value = True
        self.mock_collection.estimated_document_count.return_value = 1000
        
        request_args = {"search": {"value": "test search"}}
        result = self.validator.validate_performance(request_args)
        assert result.is_valid is True
        # Should not warn about text index since it exists
        text_index_warnings = [w for w in result.warnings if "text index" in w]
        assert len(text_index_warnings) == 0
    
    def test_performance_no_search_no_text_index_warning(self):
        """Test no text index warning when no search is performed."""
        self.mock_collection.estimated_document_count.return_value = 1000
        
        request_args = {"search": {"value": ""}}
        result = self.validator.validate_performance(request_args)
        assert result.is_valid is True
        # Should not warn about text index when no search is performed
        text_index_warnings = [w for w in result.warnings if "text index" in w]
        assert len(text_index_warnings) == 0


class TestIndexUtilityMethods:
    """Test utility methods for index checking."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [DataField("title", "string")]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_get_indexed_fields_success(self):
        """Test successful retrieval of indexed fields."""
        mock_indexes = [
            {"key": {"title": 1}},
            {"key": {"author": 1, "year": -1}},
            {"key": {"_id": 1}}
        ]
        self.mock_collection.list_indexes.return_value = mock_indexes
        
        indexed_fields = self.validator._get_indexed_fields()
        assert "title" in indexed_fields
        assert "author" in indexed_fields
        assert "year" in indexed_fields
        assert "_id" in indexed_fields
        assert len(indexed_fields) == 4
    
    def test_get_indexed_fields_error_handled(self):
        """Test error handling in get_indexed_fields."""
        self.mock_collection.list_indexes.side_effect = Exception("Connection error")
        
        indexed_fields = self.validator._get_indexed_fields()
        assert indexed_fields == set()
    
    def test_has_text_index_true(self):
        """Test text index detection when text index exists."""
        mock_indexes = [
            {"key": {"title": 1}},
            {"key": {"content": "text", "title": "text"}},
            {"key": {"_id": 1}}
        ]
        self.mock_collection.list_indexes.return_value = mock_indexes
        
        has_text_index = self.validator._has_text_index()
        assert has_text_index is True
    
    def test_has_text_index_false(self):
        """Test text index detection when no text index exists."""
        mock_indexes = [
            {"key": {"title": 1}},
            {"key": {"author": 1}},
            {"key": {"_id": 1}}
        ]
        self.mock_collection.list_indexes.return_value = mock_indexes
        
        has_text_index = self.validator._has_text_index()
        assert has_text_index is False
    
    def test_has_text_index_error_handled(self):
        """Test error handling in has_text_index."""
        self.mock_collection.list_indexes.side_effect = Exception("Connection error")
        
        has_text_index = self.validator._has_text_index()
        assert has_text_index is False


class TestExtensionCombinations:
    """Test validation of extension combinations and conflicts."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [DataField(f"col{i}", "string") for i in range(10)]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_multiple_extensions_no_conflicts(self):
        """Test multiple extensions without conflicts."""
        # Test individual validations that should all pass
        colreorder_result = self.validator.validate_colreorder_config({"order": list(range(10))})
        fixedcolumns_result = self.validator.validate_fixedcolumns_config({"left": 2, "right": 1})
        responsive_result = self.validator.validate_responsive_config({"breakpoints": []})
        
        assert colreorder_result.is_valid is True
        assert fixedcolumns_result.is_valid is True
        assert responsive_result.is_valid is True
    
    def test_fixedcolumns_with_responsive_warning(self):
        """Test potential conflict between FixedColumns and Responsive."""
        # Create a scenario with many columns that might cause responsive issues
        data_fields = [DataField(f"col{i}", "string") for i in range(15)]
        validator = ConfigValidator(self.mock_collection, data_fields)
        
        responsive_result = validator.validate_responsive_config({"breakpoints": [{"name": "mobile"}]})
        assert responsive_result.is_valid is True
        assert len(responsive_result.warnings) == 1
        assert "layout issues" in responsive_result.warnings[0]


class TestErrorMessageQuality:
    """Test the quality and helpfulness of error messages."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [DataField(f"col{i}", "string") for i in range(3)]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_error_messages_are_user_friendly(self):
        """Test that error messages are user-friendly and actionable."""
        # Test ColReorder error
        result = self.validator.validate_colreorder_config({"order": [0, 1]})
        assert result.is_valid is False
        error_msg = result.errors[0]
        # Should mention the specific numbers and what's expected
        assert "2" in error_msg  # actual length
        assert "3" in error_msg  # expected length
        assert "order" in error_msg.lower()
        assert "column" in error_msg.lower()
    
    def test_technical_details_provide_context(self):
        """Test that technical details provide useful context."""
        # Test type error
        result = self.validator.validate_colreorder_config({"order": "invalid"})
        assert result.is_valid is False
        technical_detail = result.technical_details[0]
        assert "str" in technical_detail  # Should show the actual type received
    
    def test_warning_messages_are_constructive(self):
        """Test that warning messages provide constructive guidance."""
        self.mock_collection.estimated_document_count.return_value = 200000
        
        result = self.validator.validate_performance({})
        if result.warnings:
            warning_msg = result.warnings[0]
            # Should provide specific numbers and actionable advice
            assert "200,000" in warning_msg
            assert "documents" in warning_msg
            
            technical_detail = result.technical_details[0]
            assert "index" in technical_detail.lower() or "limit" in technical_detail.lower()


class TestBackwardCompatibility:
    """Test that validation doesn't break existing valid configurations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number"),
            DataField("published", "date")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_legacy_colreorder_boolean_true(self):
        """Test legacy ColReorder boolean true configuration."""
        # Boolean true should be valid (enables default behavior)
        result = self.validator.validate_colreorder_config(True)
        assert result.is_valid is True
    
    def test_legacy_colreorder_boolean_false(self):
        """Test legacy ColReorder boolean false configuration."""
        # Boolean false should be valid (disables extension)
        result = self.validator.validate_colreorder_config(False)
        assert result.is_valid is True
    
    def test_legacy_fixedcolumns_boolean_true(self):
        """Test legacy FixedColumns boolean configuration."""
        result = self.validator.validate_fixedcolumns_config(True)
        assert result.is_valid is True
    
    def test_legacy_responsive_boolean_true(self):
        """Test legacy Responsive boolean configuration."""
        result = self.validator.validate_responsive_config(True)
        assert result.is_valid is True
    
    def test_minimal_valid_configurations(self):
        """Test minimal valid configurations for all extensions."""
        configs = [
            ({}, "colreorder"),
            ({}, "searchbuilder"),
            ({}, "fixedcolumns"),
            ({}, "responsive")
        ]
        
        for config, extension in configs:
            method_name = f"validate_{extension}_config"
            method = getattr(self.validator, method_name)
            result = method(config)
            assert result.is_valid is True, f"Empty config should be valid for {extension}"
    
    def test_common_valid_configurations(self):
        """Test common valid configurations that users might have."""
        # Common ColReorder config
        colreorder_result = self.validator.validate_colreorder_config({
            "order": [3, 0, 1, 2],
            "realtime": False
        })
        assert colreorder_result.is_valid is True
        
        # Common FixedColumns config
        fixedcolumns_result = self.validator.validate_fixedcolumns_config({
            "left": 1,
            "right": 0
        })
        assert fixedcolumns_result.is_valid is True
        
        # Common SearchBuilder config
        searchbuilder_result = self.validator.validate_searchbuilder_config({
            "columns": [0, 1, 2],
            "conditions": ["=", "!=", "contains"]
        })
        assert searchbuilder_result.is_valid is True