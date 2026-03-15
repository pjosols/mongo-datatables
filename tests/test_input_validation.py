"""Tests for input validation on request_args parameters (start, limit, draw)."""
import unittest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables


class TestInputValidation(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.collection = MagicMock()
        self.mongo.db = MagicMock()
        self.mongo.db.__getitem__ = MagicMock(return_value=self.collection)
        self.collection.list_indexes.return_value = iter([])
        self.base_args = {"draw": "1", "start": "0", "length": "10",
                         "search[value]": "", "search[regex]": "false",
                         "order[0][column]": "0", "order[0][dir]": "asc",
                         "columns[0][data]": "name", "columns[0][name]": "",
                         "columns[0][searchable]": "true", "columns[0][orderable]": "true",
                         "columns[0][search][value]": "", "columns[0][search][regex]": "false"}

    def _make(self, extra_args):
        args = {**self.base_args, **extra_args}
        return DataTables(self.mongo, "users", args)

    # --- start ---
    def test_start_valid(self):
        self.assertEqual(self._make({"start": "20"}).start, 20)

    def test_start_invalid_string(self):
        self.assertEqual(self._make({"start": "abc"}).start, 0)

    def test_start_negative(self):
        self.assertEqual(self._make({"start": "-5"}).start, 0)

    def test_start_none(self):
        self.assertEqual(self._make({"start": None}).start, 0)

    def test_start_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "start"}
        self.assertEqual(DataTables(self.mongo, "users", args).start, 0)

    # --- limit ---
    def test_limit_valid(self):
        self.assertEqual(self._make({"length": "25"}).limit, 25)

    def test_limit_minus_one(self):
        self.assertEqual(self._make({"length": "-1"}).limit, -1)

    def test_limit_invalid_string(self):
        self.assertEqual(self._make({"length": "abc"}).limit, 10)

    def test_limit_none(self):
        self.assertEqual(self._make({"length": None}).limit, 10)

    def test_limit_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "length"}
        self.assertEqual(DataTables(self.mongo, "users", args).limit, 10)

    # --- draw (via get_rows) ---
    def test_draw_valid(self):
        self.collection.aggregate.return_value = iter([{"name": "Alice"}])
        self.collection.estimated_document_count.return_value = 1
        dt = self._make({"draw": "3"})
        with patch.object(dt, "count_total", return_value=1), \
             patch.object(dt, "count_filtered", return_value=1), \
             patch.object(dt, "results", return_value=[{"name": "Alice"}]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 3)

    def test_draw_invalid_string(self):
        dt = self._make({"draw": "xyz"})
        with patch.object(dt, "count_total", return_value=0), \
             patch.object(dt, "count_filtered", return_value=0), \
             patch.object(dt, "results", return_value=[]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 1)

    def test_draw_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "draw"}
        dt = DataTables(self.mongo, "users", args)
        with patch.object(dt, "count_total", return_value=0), \
             patch.object(dt, "count_filtered", return_value=0), \
             patch.object(dt, "results", return_value=[]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 1)


if __name__ == "__main__":
    unittest.main()
