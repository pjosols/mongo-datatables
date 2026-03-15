"""Tests for narrowed exception handling in _sb_number and _sb_date."""
import pytest
from unittest.mock import patch, MagicMock
from mongo_datatables import DataTables, DataField


def make_dt():
    col = MagicMock()
    col.database = MagicMock()
    return DataTables(col, 'test', {}, [DataField('price', 'number'), DataField('created', 'date')])


class TestSbNumberExceptionNarrowing:
    def test_invalid_number_returns_empty(self):
        dt = make_dt()
        result = dt._sb_number('price', '=', 'not-a-number', None)
        assert result == {}

    def test_invalid_number_between_returns_empty(self):
        dt = make_dt()
        result = dt._sb_number('price', 'between', 'abc', 'xyz')
        assert result == {}

    def test_valid_number_works(self):
        dt = make_dt()
        result = dt._sb_number('price', '=', '42', None)
        assert result == {'price': 42}

    def test_valid_number_gt_works(self):
        dt = make_dt()
        result = dt._sb_number('price', '>', '10', None)
        assert result == {'price': {'$gt': 10}}


class TestSbDateExceptionNarrowing:
    def test_invalid_date_returns_empty(self):
        dt = make_dt()
        result = dt._sb_date('created', '=', 'not-a-date', None)
        assert result == {}

    def test_invalid_date_between_returns_empty(self):
        dt = make_dt()
        result = dt._sb_date('created', 'between', 'bad', 'also-bad')
        assert result == {}

    def test_valid_date_works(self):
        from datetime import datetime, timedelta
        dt = make_dt()
        result = dt._sb_date('created', '=', '2024-01-15', None)
        assert '$gte' in result['created']
        assert '$lt' in result['created']

    def test_valid_date_gt_works(self):
        from datetime import datetime
        dt = make_dt()
        result = dt._sb_date('created', '>', '2024-01-15', None)
        assert '$gt' in result['created']
