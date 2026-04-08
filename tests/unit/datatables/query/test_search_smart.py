"""Smart search tests: AND semantics, column smart search, per-column searchFixed."""
import unittest
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables, DataField


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_smart_dt(search_value, smart=True, columns=None):
    if columns is None:
        columns = [
            {"data": "name", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
            {"data": "city", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ]
    args = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False, "smart": smart},
        "order": [], "columns": columns,
    }
    return DataTables(MagicMock(), MagicMock(), args, ["name", "city"])


def _col(data, search_value="", smart=None, regex=False, case_insensitive=None):
    s = {"value": search_value, "regex": str(regex).lower()}
    if smart is not None:
        s["smart"] = str(smart).lower()
    if case_insensitive is not None:
        s["caseInsensitive"] = str(case_insensitive).lower()
    return {"data": data, "name": data, "searchable": "true", "orderable": "true", "search": s}


def _make_col_smart_dt(columns, search_value="", search_smart=True):
    db = MagicMock()
    db.validate_collection = MagicMock(side_effect=Exception("no text index"))
    collection = MagicMock()
    collection.aggregate.return_value = iter([])
    collection.count_documents.return_value = 0
    collection.list_indexes.return_value = []
    db.__getitem__ = MagicMock(return_value=collection)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": "false",
                   "smart": str(search_smart).lower()},
        "columns": columns,
        "order": [{"column": "0", "dir": "asc"}],
    }
    data_fields = [c.get("data") or c.get("name") for c in columns]
    with patch.object(DataTables, "_check_text_index", return_value=False):
        return DataTables(db, "test", args, data_fields)


# ---------------------------------------------------------------------------
# Smart search AND semantics
# ---------------------------------------------------------------------------

class TestSmartSearchAndSemantics:
    def test_single_term_smart_uses_or(self):
        cond = _make_smart_dt("john", smart=True).global_search_condition
        assert "$or" in cond
        assert "$and" not in cond

    def test_multi_term_smart_true_uses_and(self):
        cond = _make_smart_dt("john smith", smart=True).global_search_condition
        assert "$and" in cond
        assert len(cond["$and"]) == 2
        for term_cond in cond["$and"]:
            assert "$or" in term_cond

    def test_multi_term_smart_false_uses_or(self):
        cond = _make_smart_dt("john smith", smart=False).global_search_condition
        assert "$or" in cond
        assert "$and" not in cond

    def test_smart_true_string_coercion(self):
        cols = [{"data": "name", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}}]
        args = {"draw": 1, "start": 0, "length": 10,
                "search": {"value": "foo bar", "regex": False, "smart": "true"},
                "order": [], "columns": cols}
        cond = DataTables(MagicMock(), MagicMock(), args, ["name"]).global_search_condition
        assert "$and" in cond

    def test_smart_false_string_coercion(self):
        cols = [{"data": "name", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}}]
        args = {"draw": 1, "start": 0, "length": 10,
                "search": {"value": "foo bar", "regex": False, "smart": "false"},
                "order": [], "columns": cols}
        cond = DataTables(MagicMock(), MagicMock(), args, ["name"]).global_search_condition
        assert "$or" in cond
        assert "$and" not in cond

    def test_smart_default_is_true(self):
        cols = [
            {"data": "name", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
            {"data": "city", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ]
        args = {"draw": 1, "start": 0, "length": 10,
                "search": {"value": "foo bar", "regex": False},
                "order": [], "columns": cols}
        cond = DataTables(MagicMock(), MagicMock(), args, ["name", "city"]).global_search_condition
        assert "$and" in cond

    def test_three_terms_smart_true(self):
        cond = _make_smart_dt("john smith london", smart=True).global_search_condition
        assert "$and" in cond
        assert len(cond["$and"]) == 3

    def test_empty_search_returns_empty(self):
        assert _make_smart_dt("", smart=True).global_search_condition == {}


# ---------------------------------------------------------------------------
# Column smart search
# ---------------------------------------------------------------------------

class TestColumnSmartSearch:
    def test_single_word_smart_unchanged(self):
        dt = _make_col_smart_dt([_col("name", search_value="alice", smart=True)])
        cond = dt.column_search_conditions
        assert cond
        inner = cond["$and"][0]
        assert "name" in inner
        assert "$regex" in inner["name"]
        assert inner["name"]["$regex"] == "alice"

    def test_multi_word_smart_true_produces_and(self):
        dt = _make_col_smart_dt([_col("name", search_value="foo bar", smart=True)])
        cond = dt.column_search_conditions
        assert "$and" in cond
        inner = cond["$and"][0]
        assert "$and" in inner
        terms = inner["$and"]
        assert len(terms) == 2
        assert terms[0] == {"name": {"$regex": "foo", "$options": "i"}}
        assert terms[1] == {"name": {"$regex": "bar", "$options": "i"}}

    def test_multi_word_smart_false_single_phrase(self):
        import re as _re
        dt = _make_col_smart_dt([_col("name", search_value="foo bar", smart=False)])
        inner = dt.column_search_conditions["$and"][0]
        assert "name" in inner
        assert inner["name"]["$regex"] == _re.escape("foo bar")

    def test_multi_word_smart_default_is_true(self):
        dt = _make_col_smart_dt([_col("name", search_value="foo bar")])
        inner = dt.column_search_conditions["$and"][0]
        assert "$and" in inner
        assert len(inner["$and"]) == 2

    def test_multi_word_regex_true_no_splitting(self):
        dt = _make_col_smart_dt([_col("name", search_value="foo bar", smart=True, regex=True)])
        inner = dt.column_search_conditions["$and"][0]
        assert "name" in inner
        assert inner["name"]["$regex"] == "foo bar"

    def test_three_words_smart_true(self):
        dt = _make_col_smart_dt([_col("name", search_value="foo bar baz", smart=True)])
        inner = dt.column_search_conditions["$and"][0]
        assert "$and" in inner
        assert len(inner["$and"]) == 3

    def test_smart_case_insensitive_false(self):
        dt = _make_col_smart_dt([_col("name", search_value="foo bar", smart=True,
                                      case_insensitive=False)])
        inner = dt.column_search_conditions["$and"][0]
        for term_cond in inner["$and"]:
            assert term_cond["name"]["$options"] == ""

    def test_smart_case_insensitive_true(self):
        dt = _make_col_smart_dt([_col("name", search_value="foo bar", smart=True,
                                      case_insensitive=True)])
        inner = dt.column_search_conditions["$and"][0]
        for term_cond in inner["$and"]:
            assert term_cond["name"]["$options"] == "i"

    def test_number_field_smart_ignored(self):
        db = MagicMock()
        collection = MagicMock()
        collection.list_indexes.return_value = []
        db.__getitem__ = MagicMock(return_value=collection)
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": "false"},
            "columns": [_col("age", search_value="25", smart=True)],
            "order": [{"column": "0", "dir": "asc"}],
        }
        with patch.object(DataTables, "_check_text_index", return_value=False):
            dt = DataTables(db, "test", args, [DataField("age", "number")])
        cond = dt.column_search_conditions
        assert "$and" in cond
        inner = cond["$and"][0]
        assert inner == {"age": 25.0} or inner == {"age": 25}

    def test_empty_search_no_condition(self):
        dt = _make_col_smart_dt([_col("name", search_value="", smart=True)])
        assert dt.column_search_conditions == {}


# ---------------------------------------------------------------------------
# Per-column searchFixed
# ---------------------------------------------------------------------------

def _make_dt_col(columns_extra=None, search_fixed_global=None):
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    columns = [
        {"data": "name", "search": {"value": "", "regex": False},
         "searchable": True, "orderable": True},
        {"data": "status", "search": {"value": "", "regex": False},
         "searchable": True, "orderable": True},
        {"data": "age", "search": {"value": "", "regex": False},
         "searchable": True, "orderable": True},
    ]
    if columns_extra:
        for i, extra in enumerate(columns_extra):
            if extra and i < len(columns):
                columns[i].update(extra)

    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "columns": columns,
        "order": [{"column": 0, "dir": "asc"}],
    }
    if search_fixed_global:
        request_args["searchFixed"] = search_fixed_global

    data_fields = [
        DataField("name", data_type="string"),
        DataField("status", data_type="string"),
        DataField("age", data_type="number"),
    ]
    dt = DataTables(db, "users", request_args, data_fields=data_fields)
    with patch.object(type(dt), "has_text_index",
                      new_callable=lambda: property(lambda self: False)):
        return dt


class TestColumnSearchFixed:
    def test_single_column_fixed_search(self):
        dt = _make_dt_col(columns_extra=[{"searchFixed": {"lock": "Alice"}}])
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "name" in str(f)

    def test_column_fixed_search_scoped_to_column(self):
        dt = _make_dt_col(columns_extra=[None, {"searchFixed": {"statusLock": "active"}}])
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "status" in str(f)

    def test_multiple_column_fixed_searches_anded(self):
        dt = _make_dt_col(columns_extra=[
            {"searchFixed": {"nameLock": "Alice"}},
            {"searchFixed": {"statusLock": "active"}},
        ])
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "$and" in str(f)
        assert "name" in str(f)
        assert "status" in str(f)

    def test_empty_column_fixed_search_ignored(self):
        dt = _make_dt_col(columns_extra=[{"searchFixed": {"lock": ""}}])
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_non_dict_column_fixed_search_ignored(self):
        dt = _make_dt_col(columns_extra=[{"searchFixed": "not-a-dict"}])
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_column_fixed_and_global_fixed_combined(self):
        dt = _make_dt_col(
            columns_extra=[{"searchFixed": {"nameLock": "Alice"}}],
            search_fixed_global={"tenantFilter": "acme"},
        )
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "$and" in str(f)

    def test_no_column_fixed_search_no_effect(self):
        dt = _make_dt_col()
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_multiple_named_searches_in_one_column(self):
        dt = _make_dt_col(columns_extra=[{"searchFixed": {"lock1": "Alice", "lock2": "Bob"}}])
        with patch.object(type(dt), "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "name" in str(f)
