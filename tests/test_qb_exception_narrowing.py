"""Tests that query_builder.py uses narrow exception types (not bare except Exception)."""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.exceptions import FieldMappingError


def make_qb():
    fm = MagicMock()
    fm.get_field_type.return_value = "string"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm)


class TestBuildColumnSearchExceptions:
    def test_invalid_number_returns_empty(self):
        """Invalid value for number column produces no condition (not a crash)."""
        qb = make_qb()
        qb.field_mapper.get_field_type.return_value = "number"
        col = {"data": "price", "search": {"value": "notanumber", "regex": False}, "searchable": True}
        result = qb.build_column_search([col])
        assert result == {}

    def test_invalid_date_range_returns_empty(self):
        """Invalid date range for date column produces no condition."""
        qb = make_qb()
        qb.field_mapper.get_field_type.return_value = "date"
        col = {"data": "created", "search": {"value": "notadate|alsonotadate", "regex": False}, "searchable": True}
        result = qb.build_column_search([col])
        assert result == {}

    def test_unexpected_exception_propagates_in_number_column(self):
        """Non-ValueError/TypeError/FieldMappingError propagates (not silently swallowed)."""
        qb = make_qb()
        qb.field_mapper.get_field_type.return_value = "number"
        col = {"data": "price", "search": {"value": "5", "regex": False}, "searchable": True}
        with patch("mongo_datatables.query_builder.TypeConverter.to_number", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb.build_column_search([col])


class TestBuildNumberConditionExceptions:
    def test_invalid_value_returns_none(self):
        """_build_number_condition returns None for invalid number string."""
        qb = make_qb()
        result = qb._build_number_condition("price", "notanumber", None)
        assert result is None

    def test_unexpected_exception_propagates(self):
        """Non-ValueError/TypeError/FieldMappingError propagates from _build_number_condition."""
        qb = make_qb()
        with patch("mongo_datatables.query_builder.TypeConverter.to_number", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb._build_number_condition("price", "5", None)


class TestBuildDateConditionExceptions:
    def test_invalid_date_returns_regex_fallback(self):
        """_build_date_condition returns regex fallback for non-date string."""
        qb = make_qb()
        result = qb._build_date_condition("created", "notadate", None)
        # Non-date strings fall back to regex, not None
        assert result is not None
        assert "created" in result

    def test_unexpected_exception_propagates(self):
        """Non-ValueError/TypeError/FieldMappingError propagates from _build_date_condition."""
        qb = make_qb()
        with patch("mongo_datatables.query_builder.DateHandler.get_date_range_for_comparison", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb._build_date_condition("created", "2024-01-15", None)


class TestColumnControlExceptions:
    def test_invalid_num_stype_returns_empty(self):
        """Invalid number value in ColumnControl num stype produces no condition."""
        qb = make_qb()
        cc = {"search": {"value": "notanumber", "logic": "equal", "type": "num"}}
        result = qb._build_column_control_condition("price", "number", cc)
        assert result == []

    def test_invalid_date_stype_returns_empty(self):
        """Invalid date value in ColumnControl date stype produces no condition."""
        qb = make_qb()
        cc = {"search": {"value": "notadate", "logic": "equal", "type": "date"}}
        result = qb._build_column_control_condition("created", "date", cc)
        assert result == []
