"""SearchBuilder date-specific tests: _sb_date operators, ISO datetime, exception narrowing, DateHandler."""
import pytest
import unittest
from datetime import datetime
from unittest.mock import patch

from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.datatables.search.builder import _sb_date, _sb_number
from mongo_datatables.utils import DateHandler


# ---------------------------------------------------------------------------
# _sb_date: ISO datetime string input
# ---------------------------------------------------------------------------

class TestSbDateIsoDatetime(unittest.TestCase):
    def setUp(self):
        self.field = "created_at"

    def test_equal_iso_datetime_string(self):
        result = _sb_date(self.field, "=", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 1, 15), "$lt": datetime(2024, 1, 16)}})

    def test_greater_iso_datetime_string(self):
        result = _sb_date(self.field, ">", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$gt": datetime(2024, 1, 15)}})

    def test_less_iso_datetime_string(self):
        result = _sb_date(self.field, "<", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$lt": datetime(2024, 1, 15)}})

    def test_plain_date_string_unchanged(self):
        result = _sb_date(self.field, "=", "2024-01-15", None)
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 1, 15), "$lt": datetime(2024, 1, 16)}})


# ---------------------------------------------------------------------------
# _sb_date: operator coverage
# ---------------------------------------------------------------------------

class TestSbDateOperators(unittest.TestCase):
    def setUp(self):
        self.field = "created_at"
        self.date_str = "2024-03-15"
        self.day_start = datetime(2024, 3, 15)
        self.next_day = datetime(2024, 3, 16)

    def test_lte_returns_lt_next_day(self):
        result = _sb_date(self.field, "<=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$lt": self.next_day}})

    def test_gte_returns_gte_day_start(self):
        result = _sb_date(self.field, ">=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gte": self.day_start}})

    def test_lt_still_works(self):
        result = _sb_date(self.field, "<", self.date_str, None)
        self.assertEqual(result, {self.field: {"$lt": self.day_start}})

    def test_gt_still_works(self):
        result = _sb_date(self.field, ">", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gt": self.day_start}})

    def test_eq_still_works(self):
        result = _sb_date(self.field, "=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gte": self.day_start, "$lt": self.next_day}})

    def test_between_still_works(self):
        result = _sb_date(self.field, "between", "2024-03-01", "2024-03-31")
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 3, 1), "$lt": datetime(2024, 4, 1)}})

    def test_invalid_date_returns_empty(self):
        result = _sb_date(self.field, "<=", "not-a-date", None)
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# Exception narrowing: _sb_number and _sb_date
# ---------------------------------------------------------------------------

class TestSbNumberExceptionNarrowing:
    def test_invalid_number_returns_empty(self):
        assert _sb_number('price', '=', 'not-a-number', None) == {}

    def test_invalid_number_between_returns_empty(self):
        assert _sb_number('price', 'between', 'abc', 'xyz') == {}

    def test_valid_number_works(self):
        assert _sb_number('price', '=', '42', None) == {'price': 42}

    def test_valid_number_gt_works(self):
        assert _sb_number('price', '>', '10', None) == {'price': {'$gt': 10}}


class TestSbDateExceptionNarrowing:
    def test_invalid_date_returns_empty(self):
        assert _sb_date('created', '=', 'not-a-date', None) == {}

    def test_invalid_date_between_returns_empty(self):
        assert _sb_date('created', 'between', 'bad', 'also-bad') == {}

    def test_valid_date_works(self):
        result = _sb_date('created', '=', '2024-01-15', None)
        assert '$gte' in result['created']
        assert '$lt' in result['created']

    def test_valid_date_gt_works(self):
        result = _sb_date('created', '>', '2024-01-15', None)
        assert '$gt' in result['created']


# ---------------------------------------------------------------------------
# DateHandler.get_date_range_for_comparison
# ---------------------------------------------------------------------------

class TestDateHandlerGetDateRange:
    """Direct tests for DateHandler.get_date_range_for_comparison."""

    def setup_method(self):
        self.dh = DateHandler

    def test_gt_operator_uses_next_day_gte(self):
        result = self.dh.get_date_range_for_comparison("2024-06-01", ">")
        assert "$gte" in result
        assert result["$gte"] == datetime(2024, 6, 2)

    def test_lt_operator_uses_start_date(self):
        result = self.dh.get_date_range_for_comparison("2024-06-01", "<")
        assert result == {"$lt": datetime(2024, 6, 1)}

    def test_invalid_operator_raises_field_mapping_error(self):
        with pytest.raises(FieldMappingError, match="Invalid date comparison operator"):
            self.dh.get_date_range_for_comparison("2024-06-01", "!!")
