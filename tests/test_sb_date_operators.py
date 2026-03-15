"""Tests for _sb_date <= and >= operators (and regression tests for existing operators)."""
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from mongo_datatables import DataTables


def _make_dt():
    collection = MagicMock()
    collection.list_indexes.return_value = []
    mongo = MagicMock()
    mongo.__getitem__ = MagicMock(return_value=collection)
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "date_field", "searchable": True, "orderable": True,
                     "search": {"value": "", "regex": False}}],
    }
    return DataTables(mongo, "col", request_args)


class TestSbDateOperators(unittest.TestCase):

    def setUp(self):
        self.dt = _make_dt()
        self.field = "created_at"
        self.date_str = "2024-03-15"
        self.day_start = datetime(2024, 3, 15)
        self.next_day = datetime(2024, 3, 16)

    def test_sb_date_lte_returns_lt_next_day(self):
        result = self.dt._sb_date(self.field, "<=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$lt": self.next_day}})

    def test_sb_date_gte_returns_gte_day_start(self):
        result = self.dt._sb_date(self.field, ">=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gte": self.day_start}})

    def test_sb_date_lt_still_works(self):
        result = self.dt._sb_date(self.field, "<", self.date_str, None)
        self.assertEqual(result, {self.field: {"$lt": self.day_start}})

    def test_sb_date_gt_still_works(self):
        result = self.dt._sb_date(self.field, ">", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gt": self.day_start}})

    def test_sb_date_eq_still_works(self):
        result = self.dt._sb_date(self.field, "=", self.date_str, None)
        self.assertEqual(result, {self.field: {"$gte": self.day_start, "$lt": self.next_day}})

    def test_sb_date_between_still_works(self):
        result = self.dt._sb_date(self.field, "between", "2024-03-01", "2024-03-31")
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 3, 1), "$lte": datetime(2024, 3, 31)}})

    def test_sb_date_invalid_date_returns_empty(self):
        result = self.dt._sb_date(self.field, "<=", "not-a-date", None)
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
