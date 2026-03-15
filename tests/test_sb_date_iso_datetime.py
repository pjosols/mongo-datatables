"""Tests for _sb_date ISO datetime string handling (v1.29.4 fix)."""
import unittest
from datetime import datetime
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
        "columns": [{"data": "created_at", "searchable": True, "orderable": True,
                     "search": {"value": "", "regex": False}}],
    }
    return DataTables(mongo, "col", request_args)


class TestSbDateIsoDatetime(unittest.TestCase):

    def setUp(self):
        self.dt = _make_dt()
        self.field = "created_at"

    def test_equal_iso_datetime_string(self):
        """ISO datetime string with '=' produces day-range condition."""
        result = self.dt._sb_date(self.field, "=", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 1, 15), "$lt": datetime(2024, 1, 16)}})

    def test_greater_iso_datetime_string(self):
        """ISO datetime string with '>' produces $gt condition."""
        result = self.dt._sb_date(self.field, ">", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$gt": datetime(2024, 1, 15)}})

    def test_less_iso_datetime_string(self):
        """ISO datetime string with '<' produces $lt condition."""
        result = self.dt._sb_date(self.field, "<", "2024-01-15T00:00:00.000Z", None)
        self.assertEqual(result, {self.field: {"$lt": datetime(2024, 1, 15)}})

    def test_plain_date_string_unchanged(self):
        """Plain YYYY-MM-DD string still works correctly."""
        result = self.dt._sb_date(self.field, "=", "2024-01-15", None)
        self.assertEqual(result, {self.field: {"$gte": datetime(2024, 1, 15), "$lt": datetime(2024, 1, 16)}})


if __name__ == "__main__":
    unittest.main()
