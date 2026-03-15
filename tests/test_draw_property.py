"""Tests for the draw property on DataTables."""
import unittest
from unittest.mock import MagicMock
from mongo_datatables import DataTables


class TestDrawProperty(unittest.TestCase):
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

    def _make(self, draw_val):
        args = {**self.base_args, "draw": draw_val}
        return DataTables(self.mongo, "users", args)

    def _make_no_draw(self):
        args = {k: v for k, v in self.base_args.items() if k != "draw"}
        return DataTables(self.mongo, "users", args)

    def test_normal_integer_string(self):
        self.assertEqual(self._make("5").draw, 5)

    def test_string_one(self):
        self.assertEqual(self._make("1").draw, 1)

    def test_negative_clamped_to_one(self):
        self.assertEqual(self._make("-3").draw, 1)

    def test_zero_clamped_to_one(self):
        self.assertEqual(self._make("0").draw, 1)

    def test_non_numeric_defaults_to_one(self):
        self.assertEqual(self._make("abc").draw, 1)

    def test_none_defaults_to_one(self):
        self.assertEqual(self._make(None).draw, 1)

    def test_float_string_defaults_to_one(self):
        self.assertEqual(self._make("2.5").draw, 1)

    def test_missing_draw_key_defaults_to_one(self):
        self.assertEqual(self._make_no_draw().draw, 1)

    def test_large_valid_number(self):
        self.assertEqual(self._make("999").draw, 999)


if __name__ == "__main__":
    unittest.main()
