"""Tests for per-column smart (AND) search semantics."""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables


def make_dt(columns, search_value="", search_smart=True):
    """Helper: build a DataTables instance with given column config."""
    db = MagicMock()
    db.validate_collection = MagicMock(side_effect=Exception("no text index"))
    collection = MagicMock()
    collection.aggregate.return_value = iter([])
    collection.count_documents.return_value = 0
    collection.list_indexes.return_value = []
    db.__getitem__ = MagicMock(return_value=collection)

    request_args = {
        "draw": "1",
        "start": "0",
        "length": "10",
        "search": {"value": search_value, "regex": "false", "smart": str(search_smart).lower()},
        "columns": columns,
        "order": [{"column": "0", "dir": "asc"}],
    }
    data_fields = [col.get("data") or col.get("name") for col in columns]
    with patch.object(DataTables, "_check_text_index", return_value=False):
        dt = DataTables(db, "test", request_args, data_fields)
    return dt


def col(data, search_value="", smart=None, regex=False, case_insensitive=None):
    """Build a column dict."""
    s = {"value": search_value, "regex": str(regex).lower()}
    if smart is not None:
        s["smart"] = str(smart).lower()
    if case_insensitive is not None:
        s["caseInsensitive"] = str(case_insensitive).lower()
    return {"data": data, "name": data, "searchable": "true", "orderable": "true", "search": s}


class TestColumnSmartSearch:
    def test_single_word_smart_unchanged(self):
        """Single word with smart=true: no AND splitting needed."""
        dt = make_dt([col("name", search_value="alice", smart=True)])
        cond = dt.column_search_conditions
        assert cond
        inner = cond["$and"][0]
        assert "name" in inner
        assert "$regex" in inner["name"]
        assert inner["name"]["$regex"] == "alice"

    def test_multi_word_smart_true_produces_and(self):
        """Multi-word with smart=true: each word must match (AND)."""
        dt = make_dt([col("name", search_value="foo bar", smart=True)])
        cond = dt.column_search_conditions
        assert "$and" in cond
        inner = cond["$and"][0]
        assert "$and" in inner
        terms = inner["$and"]
        assert len(terms) == 2
        assert terms[0] == {"name": {"$regex": "foo", "$options": "i"}}
        assert terms[1] == {"name": {"$regex": "bar", "$options": "i"}}

    def test_multi_word_smart_false_single_phrase(self):
        """Multi-word with smart=false: treated as single escaped phrase."""
        import re
        dt = make_dt([col("name", search_value="foo bar", smart=False)])
        cond = dt.column_search_conditions
        assert "$and" in cond
        inner = cond["$and"][0]
        assert "name" in inner
        assert inner["name"]["$regex"] == re.escape("foo bar")

    def test_multi_word_smart_default_is_true(self):
        """smart not specified: defaults to true (AND semantics)."""
        dt = make_dt([col("name", search_value="foo bar")])
        cond = dt.column_search_conditions
        inner = cond["$and"][0]
        assert "$and" in inner
        assert len(inner["$and"]) == 2

    def test_multi_word_regex_true_no_splitting(self):
        """regex=true: value used as-is even with smart=true (regex takes precedence)."""
        dt = make_dt([col("name", search_value="foo bar", smart=True, regex=True)])
        cond = dt.column_search_conditions
        inner = cond["$and"][0]
        assert "name" in inner
        assert inner["name"]["$regex"] == "foo bar"

    def test_three_words_smart_true(self):
        """Three words with smart=true: three AND conditions."""
        dt = make_dt([col("name", search_value="foo bar baz", smart=True)])
        cond = dt.column_search_conditions
        inner = cond["$and"][0]
        assert "$and" in inner
        assert len(inner["$and"]) == 3

    def test_smart_respects_case_insensitive_false(self):
        """smart=true with caseInsensitive=false: options should be empty string."""
        dt = make_dt([col("name", search_value="foo bar", smart=True, case_insensitive=False)])
        cond = dt.column_search_conditions
        inner = cond["$and"][0]
        for term_cond in inner["$and"]:
            assert term_cond["name"]["$options"] == ""

    def test_smart_respects_case_insensitive_true(self):
        """smart=true with caseInsensitive=true: options should be 'i'."""
        dt = make_dt([col("name", search_value="foo bar", smart=True, case_insensitive=True)])
        cond = dt.column_search_conditions
        inner = cond["$and"][0]
        for term_cond in inner["$and"]:
            assert term_cond["name"]["$options"] == "i"

    def test_number_field_smart_ignored(self):
        """Number fields: smart has no effect (no regex splitting)."""
        from mongo_datatables.datatables import DataField
        db = MagicMock()
        collection = MagicMock()
        collection.list_indexes.return_value = []
        db.__getitem__ = MagicMock(return_value=collection)
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": "false"},
            "columns": [col("age", search_value="25", smart=True)],
            "order": [{"column": "0", "dir": "asc"}],
        }
        with patch.object(DataTables, "_check_text_index", return_value=False):
            dt = DataTables(db, "test", request_args, [DataField("age", "number")])
        cond = dt.column_search_conditions
        assert "$and" in cond
        inner = cond["$and"][0]
        assert inner == {"age": 25.0} or inner == {"age": 25}

    def test_empty_search_no_condition(self):
        """Empty search value: no condition generated."""
        dt = make_dt([col("name", search_value="", smart=True)])
        cond = dt.column_search_conditions
        assert cond == {}
