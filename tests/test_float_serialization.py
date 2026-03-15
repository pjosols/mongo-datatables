"""Tests for float NaN/Inf handling in _format_result_values."""
import math
from unittest.mock import MagicMock
from mongo_datatables import DataTables


def _dt():
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": [],
    }
    return DataTables(mock_db, "test", args, data_fields=[])


class TestFloatSerialization:
    def test_nan_converted_to_none(self):
        dt = _dt()
        d = {"score": float("nan")}
        dt._format_result_values(d)
        assert d["score"] is None

    def test_inf_converted_to_none(self):
        dt = _dt()
        d = {"score": float("inf")}
        dt._format_result_values(d)
        assert d["score"] is None

    def test_neg_inf_converted_to_none(self):
        dt = _dt()
        d = {"score": float("-inf")}
        dt._format_result_values(d)
        assert d["score"] is None

    def test_finite_float_unchanged(self):
        dt = _dt()
        d = {"score": 3.14}
        dt._format_result_values(d)
        assert d["score"] == 3.14

    def test_nan_in_nested_dict(self):
        dt = _dt()
        d = {"stats": {"avg": float("nan"), "count": 5}}
        dt._format_result_values(d)
        assert d["stats"]["avg"] is None
        assert d["stats"]["count"] == 5

    def test_nan_in_list_converted_to_none(self):
        dt = _dt()
        d = {"values": [float("nan"), float("inf"), 1.5]}
        dt._format_result_values(d)
        assert d["values"][0] is None
        assert d["values"][1] is None
        assert d["values"][2] == 1.5
