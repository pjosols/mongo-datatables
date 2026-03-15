"""Tests for query_builder.py coverage gaps."""

import pytest
from unittest.mock import Mock, MagicMock
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


class TestQueryBuilderCoverage:
    """Test coverage gaps in QueryBuilder."""

    def setup_method(self):
        """Set up test fixtures."""
        self.field_mapper = FieldMapper({})
        self.query_builder = MongoQueryBuilder(
            field_mapper=self.field_mapper,
            use_text_index=False,
            has_text_index=False
        )

    def test_build_column_search_exception_handling(self):
        """Test exception handling in build_column_search (line 71)."""
        columns = [
            {
                "data": "test_field",
                "searchable": True,
                "search": {"value": "invalid_number"}
            }
        ]
        
        # Mock field_mapper to return "number" type
        self.field_mapper.get_field_type = Mock(return_value="number")
        
        # Mock TypeConverter.to_number to raise exception
        with pytest.MonkeyPatch().context() as m:
            m.setattr('mongo_datatables.query_builder.TypeConverter.to_number', 
                     Mock(side_effect=ValueError("Invalid number")))
            result = self.query_builder.build_column_search(columns)
            
        # Should return empty dict when exception occurs
        assert result == {}

    def test_build_global_search_date_field_skip(self):
        """Test skipping date fields in global search (line 110)."""
        search_terms = ["test"]
        searchable_columns = ["date_field", "text_field"]
        
        # Mock field_mapper to return different types
        def mock_get_field_type(column):
            if column == "date_field":
                return "date"
            return "text"
        
        self.field_mapper.get_field_type = Mock(side_effect=mock_get_field_type)
        
        result = self.query_builder.build_global_search(
            search_terms, searchable_columns, '"test"'
        )
        
        # Should only include text_field, not date_field
        expected = {"$or": [{"text_field": {"$regex": "\\btest\\b", "$options": "i"}}]}
        assert result == expected

    def test_build_global_search_number_field_skip(self):
        """Test skipping number fields in global search (line 110)."""
        search_terms = ["test"]
        searchable_columns = ["number_field", "text_field"]
        
        # Mock field_mapper to return different types
        def mock_get_field_type(column):
            if column == "number_field":
                return "number"
            return "text"
        
        self.field_mapper.get_field_type = Mock(side_effect=mock_get_field_type)
        
        result = self.query_builder.build_global_search(
            search_terms, searchable_columns, '"test"'
        )
        
        # Should only include text_field, not number_field
        expected = {"$or": [{"text_field": {"$regex": "\\btest\\b", "$options": "i"}}]}
        assert result == expected

    def test_build_global_search_quoted_no_conditions(self):
        """Test quoted search with no valid conditions (line 126)."""
        search_terms = ["test"]
        searchable_columns = ["date_field", "number_field"]
        
        # Mock field_mapper to return only date/number types
        def mock_get_field_type(column):
            if column == "date_field":
                return "date"
            return "number"
        
        self.field_mapper.get_field_type = Mock(side_effect=mock_get_field_type)
        
        result = self.query_builder.build_global_search(
            search_terms, searchable_columns, '"test"'
        )
        
        # Should return empty dict when no valid conditions
        assert result == {}

    def test_build_global_search_text_index_enabled(self):
        """Test text index usage when enabled (line 134)."""
        # Create query builder with text index enabled
        query_builder = MongoQueryBuilder(
            field_mapper=self.field_mapper,
            use_text_index=True,
            has_text_index=True
        )
        
        search_terms = ["test", "search"]
        searchable_columns = ["text_field"]
        
        result = query_builder.build_global_search(
            search_terms, searchable_columns
        )
        
        # Should use $text search when text index is available
        expected = {"$text": {"$search": "test search"}}
        assert result == expected

    def test_build_column_search_date_field_regex(self):
        """Test date field uses regex search (line 74)."""
        columns = [
            {
                "data": "date_field",
                "searchable": True,
                "search": {"value": "2023"}
            }
        ]
        
        # Mock field_mapper to return "date" type
        self.field_mapper.get_field_type = Mock(return_value="date")
        
        result = self.query_builder.build_column_search(columns)
        
        expected = {"$and": [{"date_field": {"$regex": "2023", "$options": "i"}}]}
        assert result == expected

    def test_build_column_search_empty_conditions(self):
        """Test empty conditions return (line 68)."""
        columns = [
            {
                "data": "test_field",
                "searchable": False,  # Not searchable
                "search": {"value": "test"}
            }
        ]
        
        result = self.query_builder.build_column_search(columns)
        
        # Should return empty dict when no searchable columns
        assert result == {}