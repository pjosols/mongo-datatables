"""Tests for columns[i][orderData] support."""
import pytest
from unittest.mock import MagicMock, patch
from tests.base_test import BaseDataTablesTest


class TestOrderData(BaseDataTablesTest):
    """columns[i][orderData] redirects sort to other column(s)."""

    def _make_columns(self, order_data_map=None):
        """Build 4-column list; order_data_map = {col_idx: orderData value}."""
        cols = [
            {"data": "name", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "last_name", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "display_name", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "score", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
        if order_data_map:
            for idx, val in order_data_map.items():
                cols[idx]["orderData"] = val
        return cols

    def _dt(self, columns, order):
        from mongo_datatables import DataTables
        args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "", "regex": False},
            "columns": columns,
            "order": order,
        }
        return DataTables(self.mongo.db, "test", args, [])

    def test_single_orderdata_int(self):
        """orderData=1 on col 2 → sort by col 1's field."""
        cols = self._make_columns({2: 1})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.get_sort_specification()
        assert "last_name" in spec
        assert "display_name" not in spec
        assert spec["last_name"] == 1

    def test_single_orderdata_list(self):
        """orderData=[0, 1] on col 2 → sort by col 0 then col 1."""
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "desc", "name": ""}])
        spec = dt.get_sort_specification()
        assert "name" in spec
        assert "last_name" in spec
        assert "display_name" not in spec
        assert spec["name"] == -1
        assert spec["last_name"] == -1

    def test_no_orderdata_unchanged(self):
        """Column without orderData sorts by its own field."""
        cols = self._make_columns()
        dt = self._dt(cols, [{"column": 3, "dir": "asc", "name": ""}])
        spec = dt.get_sort_specification()
        assert "score" in spec

    def test_orderdata_out_of_range_skipped(self):
        """orderData index out of range is silently skipped."""
        cols = self._make_columns({0: 99})
        dt = self._dt(cols, [{"column": 0, "dir": "asc", "name": ""}])
        spec = dt.get_sort_specification()
        # Only _id tiebreaker
        assert spec == {"_id": 1}

    def test_orderdata_non_orderable_target_skipped(self):
        """orderData pointing to a non-orderable column is skipped."""
        cols = self._make_columns({2: 1})
        cols[1]["orderable"] = False
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.get_sort_specification()
        assert "last_name" not in spec
        assert spec == {"_id": 1}

    def test_orderdata_mixed_valid_invalid(self):
        """orderData list with one valid and one out-of-range index."""
        cols = self._make_columns({2: [0, 99]})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.get_sort_specification()
        assert "name" in spec
        assert spec["name"] == 1

    def test_orderdata_dedup_across_order_entries(self):
        """First occurrence wins when same field appears via orderData and direct sort."""
        cols = self._make_columns({2: [0]})
        # col 2 → orderData → col 0 (name); col 0 also sorted directly
        dt = self._dt(cols, [
            {"column": 2, "dir": "asc", "name": ""},
            {"column": 0, "dir": "desc", "name": ""},
        ])
        spec = dt.get_sort_specification()
        assert spec["name"] == 1  # first occurrence (asc) wins

    def test_orderdata_id_tiebreaker_always_present(self):
        """_id tiebreaker is always appended."""
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.get_sort_specification()
        assert "_id" in spec
