"""Tests for search[smart] AND semantics in global search."""
import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables


def make_dt(search_value, smart=True, columns=None):
    collection = MagicMock()
    collection.find.return_value = MagicMock(skip=MagicMock(return_value=MagicMock(
        limit=MagicMock(return_value=[]),
        __iter__=MagicMock(return_value=iter([]))
    )))
    collection.aggregate.return_value = iter([])
    if columns is None:
        columns = [
            {"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "city", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False, "smart": smart},
        "order": [],
        "columns": columns,
    }
    return DataTables(MagicMock(), collection, request_args, ["name", "city"])


class TestSmartSearchAndSemantics:
    def test_single_term_smart_uses_or(self):
        """Single term: $or across columns regardless of smart flag."""
        dt = make_dt("john", smart=True)
        cond = dt.global_search_condition
        assert "$or" in cond
        assert "$and" not in cond

    def test_multi_term_smart_true_uses_and(self):
        """Multi-word with smart=true: $and of per-term $or."""
        dt = make_dt("john smith", smart=True)
        cond = dt.global_search_condition
        assert "$and" in cond
        assert len(cond["$and"]) == 2  # one entry per term
        for term_cond in cond["$and"]:
            assert "$or" in term_cond

    def test_multi_term_smart_false_uses_or(self):
        """Multi-word with smart=false: flat $or (legacy behavior)."""
        dt = make_dt("john smith", smart=False)
        cond = dt.global_search_condition
        assert "$or" in cond
        assert "$and" not in cond

    def test_smart_true_string_coercion(self):
        """smart='true' string is treated as True."""
        collection = MagicMock()
        columns = [
            {"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "foo bar", "regex": False, "smart": "true"},
            "order": [], "columns": columns,
        }
        dt = DataTables(MagicMock(), collection, request_args, ["name"])
        cond = dt.global_search_condition
        assert "$and" in cond

    def test_smart_false_string_coercion(self):
        """smart='false' string is treated as False."""
        collection = MagicMock()
        columns = [
            {"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "foo bar", "regex": False, "smart": "false"},
            "order": [], "columns": columns,
        }
        dt = DataTables(MagicMock(), collection, request_args, ["name"])
        cond = dt.global_search_condition
        assert "$or" in cond
        assert "$and" not in cond

    def test_smart_default_is_true(self):
        """When smart key is absent, defaults to True (AND semantics)."""
        collection = MagicMock()
        columns = [
            {"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "city", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "foo bar", "regex": False},  # no 'smart' key
            "order": [], "columns": columns,
        }
        dt = DataTables(MagicMock(), collection, request_args, ["name", "city"])
        cond = dt.global_search_condition
        assert "$and" in cond

    def test_three_terms_smart_true(self):
        """Three terms with smart=true: $and with 3 entries."""
        dt = make_dt("john smith london", smart=True)
        cond = dt.global_search_condition
        assert "$and" in cond
        assert len(cond["$and"]) == 3

    def test_empty_search_returns_empty(self):
        """Empty search value returns empty dict."""
        dt = make_dt("", smart=True)
        cond = dt.global_search_condition
        assert cond == {}
