"""MongoQueryBuilder exception narrowing tests."""
import pytest
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables, DataField
from mongo_datatables.query_builder import MongoQueryBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_qb():
    fm = MagicMock()
    fm.get_field_type.return_value = "string"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm)


# ---------------------------------------------------------------------------
# build_column_search exceptions
# ---------------------------------------------------------------------------

class TestBuildColumnSearchExceptions:
    def test_invalid_number_returns_empty(self):
        qb = _make_qb()
        qb.field_mapper.get_field_type.return_value = "number"
        col = {"data": "price", "search": {"value": "notanumber", "regex": False}, "searchable": True}
        assert qb.build_column_search([col]) == {}

    def test_invalid_date_range_returns_empty(self):
        qb = _make_qb()
        qb.field_mapper.get_field_type.return_value = "date"
        col = {"data": "created", "search": {"value": "notadate|alsonotadate", "regex": False}, "searchable": True}
        assert qb.build_column_search([col]) == {}

    def test_unexpected_exception_propagates_in_number_column(self):
        qb = _make_qb()
        qb.field_mapper.get_field_type.return_value = "number"
        col = {"data": "price", "search": {"value": "5", "regex": False}, "searchable": True}
        with patch("mongo_datatables.query_builder.TypeConverter.to_number", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb.build_column_search([col])


# ---------------------------------------------------------------------------
# _build_number_condition exceptions
# ---------------------------------------------------------------------------

class TestBuildNumberConditionExceptions:
    def test_invalid_value_returns_none(self):
        assert _make_qb()._build_number_condition("price", "notanumber", None) is None

    def test_unexpected_exception_propagates(self):
        qb = _make_qb()
        with patch("mongo_datatables.query_builder.TypeConverter.to_number", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb._build_number_condition("price", "5", None)


# ---------------------------------------------------------------------------
# _build_date_condition exceptions
# ---------------------------------------------------------------------------

class TestBuildDateConditionExceptions:
    def test_invalid_date_returns_regex_fallback(self):
        result = _make_qb()._build_date_condition("created", "notadate", None)
        assert result is not None
        assert "created" in result

    def test_unexpected_exception_propagates(self):
        qb = _make_qb()
        with patch("mongo_datatables.query_builder.DateHandler.get_date_range_for_comparison", side_effect=RuntimeError("unexpected")):
            with pytest.raises(RuntimeError):
                qb._build_date_condition("created", "2024-01-15", None)


# ---------------------------------------------------------------------------
# _build_column_control_condition exceptions
# ---------------------------------------------------------------------------

class TestColumnControlExceptions:
    def test_invalid_num_stype_returns_empty(self):
        qb = _make_qb()
        cc = {"search": {"value": "notanumber", "logic": "equal", "type": "num"}}
        assert qb._build_column_control_condition("price", "number", cc) == []

    def test_invalid_date_stype_returns_empty(self):
        qb = _make_qb()
        cc = {"search": {"value": "notadate", "logic": "equal", "type": "date"}}
        assert qb._build_column_control_condition("created", "date", cc) == []
