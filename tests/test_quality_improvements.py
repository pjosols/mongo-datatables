"""Tests for ConfigParser and streaming functionality."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from mongo_datatables import DataTables, DataField, ConfigParser
from mongo_datatables.config_validator import ConfigValidator


class TestConfigParser:
    """Test the ConfigParser class functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.request_args = {}
        self.config_validator = Mock()
        self.config_parser = ConfigParser(self.request_args, self.config_validator)
    
    def test_parse_boolean_or_object_config_none(self):
        """Test parsing when parameter is not present."""
        result = self.config_parser.parse_boolean_or_object_config("nonexistent")
        assert result is None
    
    def test_parse_boolean_or_object_config_false(self):
        """Test parsing when parameter is False."""
        self.request_args["test"] = False
        result = self.config_parser.parse_boolean_or_object_config("test")
        assert result is None
    
    def test_parse_boolean_or_object_config_true(self):
        """Test parsing when parameter is True."""
        self.request_args["test"] = True
        result = self.config_parser.parse_boolean_or_object_config("test", {"default": True})
        assert result == {"default": True}
    
    def test_parse_boolean_or_object_config_object(self):
        """Test parsing when parameter is an object."""
        self.request_args["test"] = {"key": "value"}
        result = self.config_parser.parse_boolean_or_object_config("test")
        assert result == {"key": "value"}
    
    def test_parse_fixed_columns_config(self):
        """Test parsing FixedColumns configuration."""
        self.request_args["fixedColumns"] = {"left": "2", "right": "1"}
        
        # Mock validation
        validation_result = Mock()
        validation_result.is_valid = True
        validation_result.warnings = []
        self.config_validator.validate_fixedcolumns_config.return_value = validation_result
        
        result = self.config_parser.parse_fixed_columns_config()
        assert result == {"left": 2, "right": 1}
        self.config_validator.validate_fixedcolumns_config.assert_called_once()
    
    def test_parse_fixed_header_config_boolean_true(self):
        """Test parsing FixedHeader with boolean true."""
        self.request_args["fixedHeader"] = True
        result = self.config_parser.parse_fixed_header_config()
        assert result == {"header": True, "footer": False}
    
    def test_parse_fixed_header_config_object(self):
        """Test parsing FixedHeader with object configuration."""
        self.request_args["fixedHeader"] = {"header": True, "footer": True}
        result = self.config_parser.parse_fixed_header_config()
        assert result == {"header": True, "footer": True}
    
    def test_parse_responsive_config_with_validation(self):
        """Test parsing Responsive configuration with validation."""
        self.request_args["responsive"] = {"breakpoints": {"tablet": 768}}
        
        # Mock validation
        validation_result = Mock()
        validation_result.warnings = ["Some warning"]
        self.config_validator.validate_responsive_config.return_value = validation_result
        
        with patch('mongo_datatables.config_parser.logger') as mock_logger:
            result = self.config_parser.parse_responsive_config()
            assert result == {"breakpoints": {"tablet": 768}}
            mock_logger.info.assert_called_once()
    
    def test_parse_searchpanes_config_with_validation_errors(self):
        """Test parsing SearchPanes configuration with validation errors."""
        self.request_args["searchPanes"] = {"columns": [0, 1, 2]}
        
        # Mock validation
        validation_result = Mock()
        validation_result.is_valid = False
        validation_result.errors = ["Too many columns"]
        validation_result.warnings = ["Performance warning"]
        self.config_validator.validate_searchpanes_config.return_value = validation_result
        
        with patch('mongo_datatables.config_parser.logger') as mock_logger:
            result = self.config_parser.parse_searchpanes_config()
            assert result == {"columns": [0, 1, 2]}
            mock_logger.warning.assert_called_once()
            mock_logger.info.assert_called_once()


class TestDataTablesStreaming:
    """Test the streaming functionality in DataTables."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.mock_collection.list_indexes.return_value = []
        self.mock_collection.estimated_document_count.return_value = 1000
        self.mock_collection.count_documents.return_value = 1000
        
        # Create a proper mock database that supports collection access
        self.mock_db = MagicMock()
        self.mock_db.__getitem__.return_value = self.mock_collection
        
        self.request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [
                {"data": "name", "searchable": True, "orderable": True},
                {"data": "age", "searchable": True, "orderable": True}
            ],
            "order": [{"column": 0, "dir": "asc"}],
            "search": {"value": ""}
        }
        
        self.data_fields = [
            DataField("name", "string"),
            DataField("age", "number")
        ]
    
    def test_datatables_initialization_with_streaming(self):
        """Test DataTables initialization with streaming enabled."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args,
            self.data_fields,
            stream_results=True
        )
        
        assert dt.stream_results is True
        assert hasattr(dt, 'config_parser')
        assert hasattr(dt, '_query_stats')
    
    def test_stream_results_method(self):
        """Test the _stream_results method."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args,
            self.data_fields,
            stream_results=True
        )
        
        # Mock cursor data
        mock_cursor = [
            {"_id": "507f1f77bcf86cd799439011", "name": "John", "age": 30},
            {"_id": "507f1f77bcf86cd799439012", "name": "Jane", "age": 25}
        ]
        
        results = list(dt._stream_results(mock_cursor))
        
        assert len(results) == 2
        assert results[0]["DT_RowId"] == "507f1f77bcf86cd799439011"
        assert results[0]["name"] == "John"
        assert "_id" not in results[0]
    
    @patch('mongo_datatables.datatables.time.time')
    def test_results_with_performance_logging(self, mock_time):
        """Test results method with performance logging."""
        # Mock time progression
        mock_time.side_effect = [0.0, 0.1, 0.6, 0.7]  # start, query_start, query_end, total_end
        
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args,
            self.data_fields
        )
        
        # Mock aggregation pipeline
        mock_cursor = [
            {"_id": "507f1f77bcf86cd799439011", "name": "John", "age": 30}
        ]
        self.mock_collection.aggregate.return_value = mock_cursor
        
        with patch('mongo_datatables.datatables.logger') as mock_logger:
            results = dt.results()
            
            # Check that performance was logged (should be debug level for fast queries)
            mock_logger.debug.assert_called()
            
            # Check query stats
            stats = dt.query_stats
            assert "query_time" in stats
            assert "total_time" in stats
            assert "result_count" in stats
            assert stats["streaming_enabled"] is False
    
    @patch('mongo_datatables.datatables.time.time')
    def test_slow_query_warning(self, mock_time):
        """Test that slow queries generate warnings."""
        # Mock slow query (>1 second)
        mock_time.side_effect = [0.0, 0.1, 1.5, 1.6]  # 1.4 second query time
        
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args,
            self.data_fields
        )
        
        mock_cursor = [{"_id": "507f1f77bcf86cd799439011", "name": "John"}]
        self.mock_collection.aggregate.return_value = mock_cursor
        
        with patch('mongo_datatables.datatables.logger') as mock_logger:
            dt.results()
            
            # Should log a warning for slow query
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args[0][0]
            assert "Slow query detected" in call_args
    
    def test_config_parser_integration(self):
        """Test that ConfigParser is properly integrated."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args,
            self.data_fields
        )
        
        # Test that config parsing methods delegate to ConfigParser
        assert hasattr(dt, 'config_parser')
        assert isinstance(dt.config_parser, ConfigParser)
        
        # Test a config parsing method
        with patch.object(dt.config_parser, 'parse_fixed_columns_config') as mock_parse:
            mock_parse.return_value = {"left": 1}
            result = dt._parse_fixed_columns_config()
            assert result == {"left": 1}
            mock_parse.assert_called_once()


class TestBackwardCompatibility:
    """Test that all existing APIs still work unchanged."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_collection = Mock()
        self.mock_collection.list_indexes.return_value = []
        self.mock_collection.estimated_document_count.return_value = 1000
        self.mock_collection.count_documents.return_value = 1000
        
        # Create a proper mock database that supports collection access
        self.mock_db = MagicMock()
        self.mock_db.__getitem__.return_value = self.mock_collection
        
        self.request_args = {
            "draw": 1,
            "start": 0,
            "length": 10,
            "columns": [{"data": "name", "searchable": True, "orderable": True}],
            "order": [{"column": 0, "dir": "asc"}],
            "search": {"value": ""}
        }
    
    def test_existing_initialization_still_works(self):
        """Test that existing initialization patterns still work."""
        # Without stream_results parameter (should default to False)
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args
        )
        
        assert dt.stream_results is False
        assert hasattr(dt, 'config_parser')
    
    def test_existing_config_methods_still_work(self):
        """Test that existing config parsing methods still work."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args
        )
        
        # These methods should still exist and work
        assert callable(dt._parse_fixed_columns_config)
        assert callable(dt._parse_fixed_header_config)
        assert callable(dt._parse_responsive_config)
        assert callable(dt._parse_buttons_config)
        assert callable(dt._parse_select_config)
        assert callable(dt._parse_searchpanes_config)
        assert callable(dt._parse_rowgroup_config)
        assert callable(dt._parse_colreorder_config)
    
    def test_results_method_signature_unchanged(self):
        """Test that results method signature is unchanged."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args
        )
        
        # Mock the aggregation
        self.mock_collection.aggregate.return_value = []
        
        # Should return a list as before
        results = dt.results()
        assert isinstance(results, list)
    
    def test_query_stats_is_new_feature(self):
        """Test that query_stats is a new feature that doesn't break existing code."""
        dt = DataTables(
            self.mock_db,
            "test_collection",
            self.request_args
        )
        
        # Should have query_stats property
        assert hasattr(dt, 'query_stats')
        stats = dt.query_stats
        assert isinstance(stats, dict)