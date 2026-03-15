"""Tests for DataTables 2.x search.return optimization.

When search[return]=false, get_rows() returns recordsFiltered=-1
to skip the expensive count_filtered() aggregation.
"""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables

BASE_ARGS = {
    "draw": "1",
    "start": "0",
    "length": "10",
    "search": {"value": "", "regex": False},
    "columns": [],
    "order": [],
}


def make_dt(args=None, **kwargs):
    mongo = MagicMock()
    col = MagicMock()
    col.list_indexes.return_value = []
    col.estimated_document_count.return_value = 100
    col.count_documents.return_value = 100
    col.aggregate.return_value = iter([{"total": 100}])
    mongo.__getitem__ = MagicMock(return_value=col)
    with patch.object(DataTables, "_check_text_index"):
        dt = DataTables(mongo, "test", args or BASE_ARGS, **kwargs)
        dt._has_text_index = False
    return dt


class TestSearchReturn:
    def test_default_returns_filtered_count(self):
        """Without search.return, recordsFiltered is computed normally."""
        dt = make_dt()
        dt._results = []
        dt._recordsTotal = 100
        dt._recordsFiltered = 42
        rows = dt.get_rows()
        assert rows["recordsFiltered"] == 42

    def test_search_return_true_computes_count(self):
        """search.return=True (explicit) still computes recordsFiltered."""
        args = {**BASE_ARGS, "search": {"value": "", "regex": False, "return": True}}
        dt = make_dt(args)
        dt._results = []
        dt._recordsTotal = 100
        dt._recordsFiltered = 55
        rows = dt.get_rows()
        assert rows["recordsFiltered"] == 55

    def test_search_return_false_bool_skips_count(self):
        """search.return=False (bool) returns -1 for recordsFiltered."""
        args = {**BASE_ARGS, "search": {"value": "", "regex": False, "return": False}}
        dt = make_dt(args)
        dt._results = []
        dt._recordsTotal = 100
        rows = dt.get_rows()
        assert rows["recordsFiltered"] == -1

    def test_search_return_false_string_skips_count(self):
        """search.return='false' (string from HTTP form) returns -1."""
        args = {**BASE_ARGS, "search": {"value": "", "regex": False, "return": "false"}}
        dt = make_dt(args)
        dt._results = []
        dt._recordsTotal = 100
        rows = dt.get_rows()
        assert rows["recordsFiltered"] == -1

    def test_search_return_false_does_not_call_count_filtered(self):
        """When search.return=False, count_filtered() is never called."""
        args = {**BASE_ARGS, "search": {"value": "", "regex": False, "return": False}}
        dt = make_dt(args)
        dt._results = []
        dt._recordsTotal = 100
        with patch.object(dt, "count_filtered") as mock_cf:
            dt.get_rows()
            mock_cf.assert_not_called()

    def test_search_return_true_calls_count_filtered(self):
        """When search.return=True, count_filtered() IS called."""
        args = {**BASE_ARGS, "search": {"value": "", "regex": False, "return": True}}
        dt = make_dt(args)
        dt._results = []
        dt._recordsTotal = 100
        with patch.object(dt, "count_filtered", return_value=77) as mock_cf:
            rows = dt.get_rows()
            mock_cf.assert_called_once()
            assert rows["recordsFiltered"] == 77

    def test_records_total_still_computed_when_return_false(self):
        """recordsTotal is always returned even when search.return=False."""
        args = {**BASE_ARGS, "search": {"value": "", "regex": False, "return": False}}
        dt = make_dt(args)
        dt._results = []
        dt._recordsTotal = 999
        rows = dt.get_rows()
        assert rows["recordsTotal"] == 999
        assert rows["recordsFiltered"] == -1
