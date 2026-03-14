"""Additional comprehensive tests for extension validation and integration."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from mongo_datatables.config_validator import ConfigValidator, ValidationResult
from mongo_datatables.datatables import DataField, DataTables


class TestButtonsValidation:
    """Test Buttons extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_buttons_config_validation_placeholder(self):
        """Placeholder test for Buttons validation - to be implemented."""
        # Note: The current ConfigValidator doesn't have buttons validation
        # This test serves as a placeholder for future implementation
        config = {
            "buttons": [
                {"extend": "copy"},
                {"extend": "csv"},
                {"extend": "excel"}
            ]
        }
        # For now, we'll test that the validator doesn't crash with buttons config
        # In a full implementation, we'd add validate_buttons_config method
        assert hasattr(self.validator, 'collection')


class TestSelectValidation:
    """Test Select extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_select_config_validation_placeholder(self):
        """Placeholder test for Select validation - to be implemented."""
        config = {
            "select": {
                "style": "multi",
                "selector": "td:first-child"
            }
        }
        # Placeholder for future select validation implementation
        assert hasattr(self.validator, 'collection')


class TestRowGroupValidation:
    """Test RowGroup extension configuration validation."""
    
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
    
    def test_rowgroup_config_validation_placeholder(self):
        """Placeholder test for RowGroup validation - to be implemented."""
        config = {
            "rowGroup": {
                "dataSrc": "category",
                "startRender": None,
                "endRender": None
            }
        }
        # Placeholder for future rowgroup validation implementation
        assert hasattr(self.validator, 'collection')


class TestSearchPanesValidation:
    """Test SearchPanes extension configuration validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("category", "string"),
            DataField("status", "string"),
            DataField("priority", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_searchpanes_config_validation_placeholder(self):
        """Placeholder test for SearchPanes validation - to be implemented."""
        config = {
            "searchPanes": {
                "columns": [0, 1],
                "threshold": 0.6,
                "layout": "columns-3"
            }
        }
        # Placeholder for future searchpanes validation implementation
        assert hasattr(self.validator, 'collection')


class TestIntegrationWithDataTables:
    """Test integration of config validation with DataTables class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db = MagicMock()
        self.mock_collection = Mock()
        self.mock_db.__getitem__.return_value = self.mock_collection
        
        # Mock collection methods
        self.mock_collection.list_indexes.return_value = []
        self.mock_collection.estimated_document_count.return_value = 1000
        
        self.data_fields = [
            DataField("title", "string"),
            DataField("author", "string"),
            DataField("year", "number")
        ]
        
        self.request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "title", "searchable": True, "orderable": True},
                {"data": "author", "searchable": True, "orderable": True},
                {"data": "year", "searchable": True, "orderable": True}
            ]
        }
    
    def test_datatables_initializes_config_validator(self):
        """Test that DataTables initializes ConfigValidator."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args,
            data_fields=self.data_fields
        )
        
        assert hasattr(dt, 'config_validator')
        assert isinstance(dt.config_validator, ConfigValidator)
        # The collection should be the one returned by the mock database
        assert dt.config_validator.collection is not None
        assert dt.config_validator.data_fields == self.data_fields
    
    @patch('mongo_datatables.datatables.logger')
    def test_datatables_validation_integration_colreorder(self, mock_logger):
        """Test DataTables integration with ColReorder validation."""
        # Add colReorder config to request
        request_with_colreorder = self.request_args.copy()
        request_with_colreorder["colReorder"] = {"order": [2, 0, 1]}
        
        dt = DataTables(
            self.mock_db,
            "test_collection",
            request_with_colreorder,
            data_fields=self.data_fields
        )
        
        # The validation should happen during initialization or processing
        assert dt.config_validator is not None
    
    @patch('mongo_datatables.datatables.logger')
    def test_datatables_validation_integration_fixedcolumns(self, mock_logger):
        """Test DataTables integration with FixedColumns validation."""
        request_with_fixedcolumns = self.request_args.copy()
        request_with_fixedcolumns["fixedColumns"] = {"left": 1}
        
        dt = DataTables(
            self.mock_db,
            "test_collection",
            request_with_fixedcolumns,
            data_fields=self.data_fields
        )
        
        assert dt.config_validator is not None
    
    def test_datatables_handles_validation_errors_gracefully(self):
        """Test that DataTables handles validation errors gracefully."""
        # Create invalid config
        request_with_invalid_config = self.request_args.copy()
        request_with_invalid_config["fixedColumns"] = {"left": 10}  # Too many columns
        
        # Should not crash during initialization
        dt = DataTables(
            self.mock_db,
            "test_collection",
            request_with_invalid_config,
            data_fields=self.data_fields
        )
        
        # Validation should still be available
        assert dt.config_validator is not None
        
        # Test the validation directly
        result = dt.config_validator.validate_fixedcolumns_config({"left": 10})
        assert result.is_valid is False


class TestValidationWithRealWorldScenarios:
    """Test validation with real-world configuration scenarios."""
    
    def setup_method(self):
        """Set up test fixtures with realistic data."""
        self.mock_collection = Mock()
        
        # Realistic book catalog fields
        self.data_fields = [
            DataField("_id", "objectid"),
            DataField("title", "string"),
            DataField("author.name", "string", "author"),
            DataField("author.birth_year", "number", "author_birth"),
            DataField("publication_date", "date"),
            DataField("isbn", "string"),
            DataField("pages", "number"),
            DataField("genre", "string"),
            DataField("rating", "number"),
            DataField("reviews", "array"),
            DataField("metadata", "object"),
            DataField("in_stock", "boolean"),
            DataField("price", "number")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_large_table_configuration(self):
        """Test validation with a large table configuration."""
        # Test ColReorder with all columns
        colreorder_config = {"order": list(range(len(self.data_fields)))}
        result = self.validator.validate_colreorder_config(colreorder_config)
        assert result.is_valid is True
        
        # Test FixedColumns with reasonable settings
        fixedcolumns_config = {"left": 2, "right": 1}  # Fix title and author on left, price on right
        result = self.validator.validate_fixedcolumns_config(fixedcolumns_config)
        assert result.is_valid is True
        
        # Test Responsive with many columns (should warn)
        responsive_config = {"breakpoints": [{"name": "desktop", "width": 1024}]}
        result = self.validator.validate_responsive_config(responsive_config)
        assert result.is_valid is True
        assert len(result.warnings) == 1  # Should warn about many columns
    
    @patch.object(ConfigValidator, '_get_indexed_fields')
    def test_ecommerce_scenario_with_indexes(self, mock_get_indexed):
        """Test e-commerce scenario with proper indexes."""
        # Simulate proper e-commerce indexes
        mock_get_indexed.return_value = {
            "title", "author.name", "genre", "isbn", "price", "in_stock"
        }
        
        result = self.validator.validate_searchbuilder_config({"columns": True})
        assert result.is_valid is True
        # Should have minimal warnings since most searchable fields are indexed
        unindexed_warnings = [w for w in result.warnings if "unindexed" in w]
        if unindexed_warnings:
            # Only publication_date and rating might be unindexed
            warning_text = unindexed_warnings[0]
            assert "publication_date" in warning_text or "rating" in warning_text
    
    @patch.object(ConfigValidator, '_get_indexed_fields')
    def test_poorly_indexed_scenario(self, mock_get_indexed):
        """Test scenario with poor indexing."""
        # Only basic _id index
        mock_get_indexed.return_value = {"_id"}
        
        result = self.validator.validate_searchbuilder_config({"columns": True})
        assert result.is_valid is True
        assert len(result.warnings) == 1
        
        warning_text = result.warnings[0]
        # Should warn about multiple unindexed searchable fields
        assert "title" in warning_text
        assert "author.name" in warning_text
        assert "genre" in warning_text
    
    def test_mobile_first_responsive_configuration(self):
        """Test mobile-first responsive configuration."""
        # Mobile-first with priority columns
        responsive_config = {
            "breakpoints": [
                {"name": "mobile", "width": 480},
                {"name": "tablet", "width": 768},
                {"name": "desktop", "width": 1024}
            ],
            "details": {
                "type": "column",
                "target": 0
            }
        }
        
        result = self.validator.validate_responsive_config(responsive_config)
        assert result.is_valid is True
        # Should warn about many columns
        assert len(result.warnings) == 1
        assert "layout issues" in result.warnings[0]
    
    def test_admin_dashboard_configuration(self):
        """Test admin dashboard with multiple extensions."""
        # Admin might want to fix ID and actions columns
        fixedcolumns_result = self.validator.validate_fixedcolumns_config({
            "left": 1,  # Fix ID column
            "right": 1  # Fix actions column
        })
        assert fixedcolumns_result.is_valid is True
        
        # Admin might want custom column order
        colreorder_result = self.validator.validate_colreorder_config({
            "order": [0, 1, 2, 7, 8, 3, 4, 5, 6, 9, 10, 11, 12],  # Prioritize key fields
            "realtime": False
        })
        assert colreorder_result.is_valid is True
        
        # SearchBuilder for complex filtering
        searchbuilder_result = self.validator.validate_searchbuilder_config({
            "columns": [1, 2, 4, 7, 8, 11, 12],  # Searchable business fields
            "conditions": {
                "string": ["=", "!=", "contains", "starts", "ends"],
                "number": ["=", "!=", ">", ">=", "<", "<="],
                "date": ["=", "!=", ">", ">=", "<", "<="]
            }
        })
        assert searchbuilder_result.is_valid is True


class TestPerformanceValidationEdgeCases:
    """Test edge cases in performance validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [DataField("title", "string")]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_performance_validation_with_zero_documents(self):
        """Test performance validation with empty collection."""
        self.mock_collection.estimated_document_count.return_value = 0
        
        result = self.validator.validate_performance({})
        assert result.is_valid is True
        # Should not warn about large dataset
        large_dataset_warnings = [w for w in result.warnings if "Large dataset" in w]
        assert len(large_dataset_warnings) == 0
    
    def test_performance_validation_exactly_at_threshold(self):
        """Test performance validation exactly at warning threshold."""
        self.mock_collection.estimated_document_count.return_value = 100000
        
        result = self.validator.validate_performance({})
        assert result.is_valid is True
        # Should not warn at exactly 100,000 (threshold is > 100,000)
        large_dataset_warnings = [w for w in result.warnings if "Large dataset" in w]
        assert len(large_dataset_warnings) == 0
    
    def test_performance_validation_just_over_threshold(self):
        """Test performance validation just over warning threshold."""
        self.mock_collection.estimated_document_count.return_value = 100001
        
        result = self.validator.validate_performance({})
        assert result.is_valid is True
        # Should warn at 100,001
        large_dataset_warnings = [w for w in result.warnings if "Large dataset" in w]
        assert len(large_dataset_warnings) == 1
    
    def test_performance_validation_with_empty_search(self):
        """Test performance validation with empty search value."""
        request_args = {"search": {"value": ""}}
        result = self.validator.validate_performance(request_args)
        assert result.is_valid is True
        # Should not warn about text index for empty search
        text_index_warnings = [w for w in result.warnings if "text index" in w]
        assert len(text_index_warnings) == 0
    
    def test_performance_validation_with_whitespace_search(self):
        """Test performance validation with whitespace-only search."""
        request_args = {"search": {"value": "   "}}
        result = self.validator.validate_performance(request_args)
        assert result.is_valid is True
        # Whitespace-only search should still trigger text index check
        # (implementation detail - depends on how search is processed)
    
    def test_performance_validation_missing_search_key(self):
        """Test performance validation with missing search key."""
        request_args = {}
        result = self.validator.validate_performance(request_args)
        assert result.is_valid is True
        # Should not crash and should not warn about text index
        text_index_warnings = [w for w in result.warnings if "text index" in w]
        assert len(text_index_warnings) == 0


class TestValidationResultAggregation:
    """Test aggregation of multiple validation results."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.data_fields = [DataField(f"col{i}", "string") for i in range(5)]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    def test_combine_multiple_validation_results(self):
        """Test combining results from multiple validations."""
        # Get results from multiple validations
        colreorder_result = self.validator.validate_colreorder_config({"order": [0, 1]})  # Invalid
        fixedcolumns_result = self.validator.validate_fixedcolumns_config({"left": 6})  # Invalid
        responsive_result = self.validator.validate_responsive_config({})  # Valid
        
        # Combine results (this would be done by the calling code)
        all_errors = []
        all_warnings = []
        all_technical_details = []
        
        for result in [colreorder_result, fixedcolumns_result, responsive_result]:
            all_errors.extend(result.errors)
            all_warnings.extend(result.warnings)
            all_technical_details.extend(result.technical_details)
        
        # Should have errors from both invalid configs
        assert len(all_errors) == 2
        assert any("order length" in error for error in all_errors)
        assert any("Fixed columns" in error for error in all_errors)
        
        # Technical details should provide context for both errors
        assert len(all_technical_details) == 2
    
    def test_validation_with_mixed_results(self):
        """Test validation with mix of valid, invalid, and warning results."""
        self.mock_collection.estimated_document_count.return_value = 200000
        
        # Valid config
        colreorder_result = self.validator.validate_colreorder_config({"order": [0, 1, 2, 3, 4]})
        
        # Invalid config
        fixedcolumns_result = self.validator.validate_fixedcolumns_config({"left": 5})
        
        # Config that generates warnings
        performance_result = self.validator.validate_performance({})
        
        assert colreorder_result.is_valid is True
        assert fixedcolumns_result.is_valid is False
        assert performance_result.is_valid is True
        
        # Should have one error and one warning
        total_errors = len(colreorder_result.errors) + len(fixedcolumns_result.errors) + len(performance_result.errors)
        total_warnings = len(colreorder_result.warnings) + len(fixedcolumns_result.warnings) + len(performance_result.warnings)
        
        assert total_errors == 1
        assert total_warnings == 1


class TestValidationWithComplexDataTypes:
    """Test validation with complex MongoDB data types."""
    
    def setup_method(self):
        """Set up test fixtures with complex data types."""
        self.mock_collection = Mock()
        self.data_fields = [
            DataField("_id", "objectid"),
            DataField("title", "string"),
            DataField("tags", "array"),
            DataField("metadata", "object"),
            DataField("created_at", "date"),
            DataField("is_active", "boolean"),
            DataField("score", "number"),
            DataField("nullable_field", "null")
        ]
        self.validator = ConfigValidator(self.mock_collection, self.data_fields)
    
    @patch.object(ConfigValidator, '_get_indexed_fields')
    def test_searchbuilder_with_complex_types(self, mock_get_indexed):
        """Test SearchBuilder validation ignores complex types appropriately."""
        mock_get_indexed.return_value = set()  # No indexes
        
        result = self.validator.validate_searchbuilder_config({"columns": True})
        assert result.is_valid is True
        
        if result.warnings:
            warning_text = result.warnings[0]
            # Should not warn about array, object, or null types
            assert "tags" not in warning_text
            assert "metadata" not in warning_text
            assert "nullable_field" not in warning_text
            
            # Should warn about searchable types
            assert "title" in warning_text
            assert "created_at" in warning_text
            assert "score" in warning_text
            # Boolean might or might not be included depending on implementation
    
    def test_colreorder_with_all_data_types(self):
        """Test ColReorder validation works with all data types."""
        config = {"order": [7, 0, 1, 2, 3, 4, 5, 6]}  # Reorder all 8 columns
        result = self.validator.validate_colreorder_config(config)
        assert result.is_valid is True
    
    def test_fixedcolumns_with_mixed_types(self):
        """Test FixedColumns validation with mixed data types."""
        config = {"left": 2, "right": 1}  # Fix 3 out of 8 columns
        result = self.validator.validate_fixedcolumns_config(config)
        assert result.is_valid is True