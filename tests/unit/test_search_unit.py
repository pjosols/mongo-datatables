"""Consolidated search tests.

Merged from:
- test_datatables_search.py
- test_datatables_text_search.py
- test_datatables_text_search_advanced.py
- test_smart_search.py
- test_column_smart_search.py
- test_datatables_regex_search.py
- test_multi_colon_search.py
- test_regex_escape_colon_search.py
- test_regex_quoted_phrase.py
- test_invalid_number_search.py
"""
import re
import unittest
from unittest.mock import MagicMock, patch

import pytest
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables import DataField as DFAlias
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper
from tests.base_test import BaseDataTablesTest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _base_request(search_value="", columns=None, extra=None):
    cols = columns or [
        {"data": "name", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "email", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "status", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ]
    args = {
        "draw": "1", "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": cols,
    }
    if extra:
        args.update(extra)
    return args


def _mock_mongo():
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    col = MagicMock(spec=Collection)
    col.estimated_document_count.return_value = 0
    mongo.db.__getitem__.return_value = col
    return mongo, col


def _make_dt_simple(search_value="", columns=None, extra=None, data_fields=None,
                    use_text_index=None):
    mongo, _ = _mock_mongo()
    args = _base_request(search_value, columns, extra)
    kwargs = {}
    if data_fields is not None:
        kwargs["data_fields"] = data_fields
    if use_text_index is not None:
        kwargs["use_text_index"] = use_text_index
    return DataTables(mongo, "users", args, **kwargs)


def _qb(field_types=None):
    data_fields = [DataField(n, t) for n, t in (field_types or {}).items()]
    fm = FieldMapper(data_fields)
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)



# ---------------------------------------------------------------------------
# Search terms and searchable columns (from test_datatables_search.py)
# ---------------------------------------------------------------------------

class TestSearchTerms(unittest.TestCase):
    def setUp(self):
        self.mongo, self.col = _mock_mongo()

    def _dt(self, search_value="", extra_cols=None):
        args = _base_request(search_value)
        return DataTables(self.mongo, "users", args)

    def test_search_terms_empty(self):
        dt = self._dt("")
        self.assertEqual(dt.search_terms, [])

    def test_search_terms_split(self):
        dt = self._dt("John active")
        self.assertEqual(dt.search_terms, ["John", "active"])

    def test_search_terms_without_colon(self):
        dt = self._dt("John status:active email:example.com")
        self.assertEqual(dt.search_terms_without_a_colon, ["John"])

    def test_search_terms_with_colon(self):
        dt = self._dt("John status:active email:example.com")
        self.assertEqual(set(dt.search_terms_with_a_colon),
                         {"status:active", "email:example.com"})

    def test_searchable_columns(self):
        dt = self._dt()
        self.assertEqual(dt.searchable_columns, ["name", "email", "status"])

    def test_column_search_conditions(self):
        args = _base_request()
        args["columns"][0]["search"]["value"] = "John"
        args["columns"][0]["search"]["regex"] = True
        dt = DataTables(self.mongo, "users", args)
        result = dt.column_search_conditions
        self.assertIn("$and", result)
        self.assertTrue(any("name" in cond for cond in result["$and"]))

    def test_column_specific_search_condition(self):
        dt = self._dt("status:active")
        result = dt.column_specific_search_condition
        self.assertIn("$and", result)
        self.assertTrue(any("status" in cond for cond in result["$and"]))

    def test_global_search_condition_empty(self):
        dt = self._dt("")
        self.assertEqual(dt.global_search_condition, {})

    def test_global_search_with_text_index(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("John"), use_text_index=True)
            result = dt.global_search_condition
        self.assertIn("$text", result)
        self.assertEqual(result["$text"]["$search"], "John")

    def test_global_search_without_text_index(self):
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", _base_request("John"), use_text_index=False)
            result = dt.global_search_condition
        self.assertIn("$or", result)

    def test_number_field_gt_operator(self):
        args = _base_request("number_field:>10", columns=[
            {"data": "number_field", "name": "number_field", "searchable": True,
             "search": {"value": "", "regex": False}},
        ])
        dt = DataTables(self.mongo, "test", args,
                        data_fields=[DataField("number_field", "number")],
                        use_text_index=False)
        result = dt.column_specific_search_condition
        self.assertIn("$and", result)
        cond = next((c.get("number_field") for c in result["$and"] if "number_field" in c), None)
        self.assertIsNotNone(cond)
        self.assertIn("$gt", cond)

    def test_number_field_gte_operator(self):
        args = _base_request("number_field:>=10", columns=[
            {"data": "number_field", "name": "number_field", "searchable": True,
             "search": {"value": "", "regex": False}},
        ])
        dt = DataTables(self.mongo, "test", args,
                        data_fields=[DataField("number_field", "number")],
                        use_text_index=False)
        result = dt.column_specific_search_condition
        cond = next((c.get("number_field") for c in result["$and"] if "number_field" in c), None)
        self.assertIn("$gte", cond)


# ---------------------------------------------------------------------------
# Text search (from test_datatables_text_search.py + test_datatables_text_search_advanced.py)
# ---------------------------------------------------------------------------

class TestTextSearch(unittest.TestCase):
    def setUp(self):
        self.mongo, _ = _mock_mongo()

    def test_text_index_search(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("John"), use_text_index=True)
            cond = dt.global_search_condition
        self.assertIn("$text", cond)
        self.assertEqual(cond["$text"]["$search"], "John")

    def test_text_index_quoted_phrase(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request('"John Doe"'), use_text_index=True)
            cond = dt.global_search_condition
        self.assertIn("$text", cond)
        self.assertEqual(cond["$text"]["$search"], '"John Doe"')

    def test_no_text_index_uses_regex(self):
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", _base_request("John"), use_text_index=False)
            cond = dt.global_search_condition
        self.assertIn("$or", cond)

    def test_multiple_terms_nonempty(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("John active"))
            cond = dt.global_search_condition
        self.assertTrue(cond)

    def test_empty_search_empty_condition(self):
        dt = DataTables(self.mongo, "users", _base_request(""))
        self.assertEqual(dt.global_search_condition, {})

    def test_field_specific_search_nonempty_filter(self):
        dt = DataTables(self.mongo, "users", _base_request("name:John"))
        self.assertTrue(dt.filter)

    def test_quoted_phrase_search_terms_nonempty(self):
        dt = DataTables(self.mongo, "users", _base_request('"John Doe" active "example.com"'))
        self.assertTrue(dt.search_terms)

    # Advanced text search tests
    def test_advanced_text_index_search(self):
        data_fields = [DataField("title", "string"), DataField("author", "string"),
                       DataField("year", "number")]
        args = _base_request("test search", columns=[
            {"data": "title", "searchable": True, "search": {"value": "", "regex": False}},
            {"data": "author", "searchable": True, "search": {"value": "", "regex": False}},
            {"data": "year", "searchable": True, "search": {"value": "", "regex": False}},
        ])
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "test", args, data_fields=data_fields, use_text_index=True)
            cond = dt.global_search_condition
        self.assertIn("$text", cond)

    def test_advanced_exact_phrase_text_index(self):
        args = _base_request('"exact phrase"', columns=[
            {"data": "title", "searchable": True, "search": {"value": "", "regex": False}},
            {"data": "author", "searchable": True, "search": {"value": "", "regex": False}},
        ])
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "test", args, use_text_index=True)
            cond = dt.global_search_condition
        self.assertIn("$text", cond)
        self.assertEqual(cond["$text"]["$search"], '"exact phrase"')

    def test_advanced_no_text_index_uses_or(self):
        args = _base_request('"no text index"', columns=[
            {"data": "title", "searchable": True, "search": {"value": "", "regex": False}},
            {"data": "author", "searchable": True, "search": {"value": "", "regex": False}},
        ])
        dt = DataTables(self.mongo, "test", args, use_text_index=True)
        dt._has_text_index = False
        cond = dt.global_search_condition
        self.assertIn("$or", cond)
        self.assertNotIn("$text", cond)

    def test_advanced_text_search_disabled(self):
        args = _base_request("disabled text search", columns=[
            {"data": "title", "searchable": True, "search": {"value": "", "regex": False}},
            {"data": "author", "searchable": True, "search": {"value": "", "regex": False}},
        ])
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "test", args, use_text_index=False)
            cond = dt.global_search_condition
        self.assertIn("$and", cond)
        self.assertNotIn("$text", cond)


# ---------------------------------------------------------------------------
# Smart search AND semantics (from test_smart_search.py)
# ---------------------------------------------------------------------------

def _make_smart_dt(search_value, smart=True, columns=None):
    collection = MagicMock()
    collection.aggregate.return_value = iter([])
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
    return DataTables(MagicMock(), collection, args, ["name", "city"])


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
        cond = _make_smart_dt("", smart=True).global_search_condition
        assert cond == {}


# ---------------------------------------------------------------------------
# Column smart search (from test_column_smart_search.py)
# ---------------------------------------------------------------------------

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


def _col(data, search_value="", smart=None, regex=False, case_insensitive=None):
    s = {"value": search_value, "regex": str(regex).lower()}
    if smart is not None:
        s["smart"] = str(smart).lower()
    if case_insensitive is not None:
        s["caseInsensitive"] = str(case_insensitive).lower()
    return {"data": data, "name": data, "searchable": "true", "orderable": "true", "search": s}


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
        dt = _make_col_smart_dt([_col("name", search_value="foo bar", smart=False)])
        cond = dt.column_search_conditions
        inner = cond["$and"][0]
        assert "name" in inner
        assert inner["name"]["$regex"] == re.escape("foo bar")

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
# Regex search flag (from test_datatables_regex_search.py)
# ---------------------------------------------------------------------------

class TestRegexSearchFlag(unittest.TestCase):
    def setUp(self):
        self.mongo, _ = _mock_mongo()

    def test_column_regex_false_escapes_special_chars(self):
        result = _qb().build_column_search([{
            "data": "name", "searchable": True,
            "search": {"value": "john.doe", "regex": False}
        }])
        self.assertEqual(result["$and"][0]["name"]["$regex"], "john\\.doe")

    def test_column_regex_true_uses_raw_pattern(self):
        result = _qb().build_column_search([{
            "data": "name", "searchable": True,
            "search": {"value": "^john.*doe$", "regex": True}
        }])
        self.assertEqual(result["$and"][0]["name"]["$regex"], "^john.*doe$")

    def test_column_regex_default_is_false(self):
        result = _qb().build_column_search([{
            "data": "name", "searchable": True,
            "search": {"value": "a+b"}
        }])
        self.assertEqual(result["$and"][0]["name"]["$regex"], "a\\+b")

    def test_column_number_field_unaffected_by_regex(self):
        result = _qb({"salary": "number"}).build_column_search([{
            "data": "salary", "searchable": True,
            "search": {"value": "50000", "regex": True}
        }])
        self.assertEqual(result["$and"][0]["salary"], 50000)

    def test_global_regex_false_escapes(self):
        result = _qb().build_global_search(
            ["john.doe"], ["name", "email"],
            original_search="john.doe", search_regex=False
        )
        patterns = [cond[c]["$regex"] for cond in result["$or"] for c in cond]
        self.assertTrue(all(p == "john\\.doe" for p in patterns))

    def test_global_regex_true_raw_pattern(self):
        result = _qb().build_global_search(
            ["^john"], ["name", "email"],
            original_search="^john", search_regex=True
        )
        patterns = [cond[c]["$regex"] for cond in result["$or"] for c in cond]
        self.assertTrue(all(p == "^john" for p in patterns))

    def test_global_regex_default_is_false(self):
        result = _qb().build_global_search(["a+b"], ["name"])
        self.assertEqual(result["$or"][0]["name"]["$regex"], "a\\+b")

    def test_datatables_passes_search_regex_true(self):
        args = _base_request("^john")
        args["search"]["regex"] = True
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", args, use_text_index=False)
            result = dt.global_search_condition
        patterns = [list(c.values())[0].get("$regex") for c in result.get("$or", [])
                    if isinstance(list(c.values())[0], dict)]
        self.assertTrue(any(p == "^john" for p in patterns if p))

    def test_datatables_search_regex_false_escapes(self):
        args = _base_request("john.doe")
        args["search"]["regex"] = False
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", args, use_text_index=False)
            result = dt.global_search_condition
        patterns = [list(c.values())[0].get("$regex") for c in result.get("$or", [])
                    if isinstance(list(c.values())[0], dict)]
        self.assertTrue(any(p == "john\\.doe" for p in patterns if p))

    def test_datatables_column_regex_integration(self):
        args = _base_request()
        args["columns"][0]["search"]["value"] = "^J.*n$"
        args["columns"][0]["search"]["regex"] = True
        dt = DataTables(self.mongo, "users", args)
        result = dt.column_search_conditions
        self.assertIn("$and", result)
        name_cond = next((c["name"] for c in result["$and"] if "name" in c), None)
        self.assertIsNotNone(name_cond)
        self.assertEqual(name_cond["$regex"], "^J.*n$")

    def test_global_regex_string_false_escapes(self):
        args = _base_request("john.doe")
        args["search"]["regex"] = "false"
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", args, use_text_index=False)
            result = dt.global_search_condition
        patterns = [list(c.values())[0].get("$regex") for c in result.get("$or", [])
                    if isinstance(list(c.values())[0], dict)]
        self.assertTrue(any(p == "john\\.doe" for p in patterns if p))

    def test_global_regex_string_true_raw(self):
        args = _base_request("^john")
        args["search"]["regex"] = "true"
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", args, use_text_index=False)
            result = dt.global_search_condition
        patterns = [list(c.values())[0].get("$regex") for c in result.get("$or", [])
                    if isinstance(list(c.values())[0], dict)]
        self.assertTrue(any(p == "^john" for p in patterns if p))

    def test_global_regex_absent_escapes(self):
        args = _base_request("a+b")
        args["search"].pop("regex", None)
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", args, use_text_index=False)
            result = dt.global_search_condition
        patterns = [list(c.values())[0].get("$regex") for c in result.get("$or", [])
                    if isinstance(list(c.values())[0], dict)]
        self.assertTrue(any(p == "a\\+b" for p in patterns if p))


# ---------------------------------------------------------------------------
# Multi-colon search terms (from test_multi_colon_search.py)
# ---------------------------------------------------------------------------

def _make_multi_colon_dt(search_value, data_fields=None):
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_col.aggregate.return_value = iter([])
    mock_col.estimated_document_count.return_value = 0
    mock_col.count_documents.return_value = 0
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": False},
        "columns": [
            {"data": "url", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
            {"data": "title", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ],
        "order": [{"column": 0, "dir": "asc"}],
    }
    return DataTables(mock_db, "test", args, data_fields or [])


class TestMultiColonSearchTerms:
    def test_single_colon_term_included(self):
        dt = _make_multi_colon_dt("title:python")
        assert "title:python" in dt.search_terms_with_a_colon

    def test_multi_colon_term_included(self):
        dt = _make_multi_colon_dt("url:https://example.com")
        assert "url:https://example.com" in dt.search_terms_with_a_colon

    def test_multi_colon_term_not_in_global_search(self):
        dt = _make_multi_colon_dt("url:https://example.com")
        assert "url:https://example.com" not in dt.search_terms_without_a_colon

    def test_multi_colon_term_not_silently_dropped(self):
        dt = _make_multi_colon_dt("url:https://example.com")
        assert "url:https://example.com" in dt.search_terms_with_a_colon
        assert "url:https://example.com" not in dt.search_terms_without_a_colon

    def test_multi_colon_split_uses_first_colon(self):
        dt = _make_multi_colon_dt("url:https://example.com")
        terms = dt.search_terms_with_a_colon
        assert len(terms) == 1
        field, value = terms[0].split(":", 1)
        assert field == "url"
        assert value == "https://example.com"

    def test_no_colon_term_excluded(self):
        dt = _make_multi_colon_dt("python")
        assert dt.search_terms_with_a_colon == []

    def test_mixed_terms(self):
        dt = _make_multi_colon_dt("python title:flask url:https://x.com")
        assert "python" in dt.search_terms_without_a_colon
        assert "title:flask" in dt.search_terms_with_a_colon
        assert "url:https://x.com" in dt.search_terms_with_a_colon


class TestHtmlNumSearchBuilderTypes:
    def _make_sb_dt(self, sb_type, condition, value):
        mock_col = MagicMock()
        mock_col.list_indexes.return_value = []
        mock_col.aggregate.return_value = iter([])
        mock_col.estimated_document_count.return_value = 0
        mock_col.count_documents.return_value = 0
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "columns": [{"data": "price", "searchable": True, "orderable": True,
                         "search": {"value": "", "regex": False}}],
            "order": [{"column": 0, "dir": "asc"}],
            "searchBuilder": {
                "logic": "AND",
                "criteria": [{"origData": "price", "condition": condition,
                               "type": sb_type, "value": [value]}]
            }
        }
        return DataTables(mock_db, "test", args, [DataField("price", "number")])

    def test_html_num_equals_numeric(self):
        f = self._make_sb_dt("html-num", "=", "42")._parse_search_builder()
        assert f == {"price": 42} or f == {"price": 42.0}

    def test_html_num_fmt_gt_numeric(self):
        f = self._make_sb_dt("html-num-fmt", ">", "100")._parse_search_builder()
        assert f == {"price": {"$gt": 100}} or f == {"price": {"$gt": 100.0}}

    def test_html_num_not_regex(self):
        f = self._make_sb_dt("html-num", "=", "42")._parse_search_builder()
        assert "$regex" not in str(f)

    def test_html_num_fmt_not_regex(self):
        f = self._make_sb_dt("html-num-fmt", "=", "42")._parse_search_builder()
        assert "$regex" not in str(f)


# ---------------------------------------------------------------------------
# Regex escape in colon search (from test_regex_escape_colon_search.py)
# ---------------------------------------------------------------------------

def _make_colon_escape_dt(search_value, fields=None):
    if fields is None:
        fields = [DataField("title", "string"), DataField("email", "string"),
                  DataField("price", "number"), DataField("created", "date")]
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_collection)
    def _col(name):
        return {"data": name, "searchable": True, "orderable": True,
                "search": {"value": "", "regex": False}}
    args = {
        "draw": "1", "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [_col("title"), _col("email"), _col("price"), _col("created")],
    }
    return DataTables(mock_client, "test_col", args, fields)


def _get_regex_from_filter(f, field):
    and_clause = f.get("$and", [])
    conds = [c for c in and_clause if field in c]
    assert conds, f"Expected '{field}' in $and, got: {f!r}"
    return conds[0][field]["$regex"]


class TestColonSearchRegexEscape:
    def test_dot_in_email_escaped(self):
        assert r"\." in _get_regex_from_filter(
            _make_colon_escape_dt("email:user@domain.com").filter, "email")

    def test_plus_escaped(self):
        assert r"\+" in _get_regex_from_filter(
            _make_colon_escape_dt("title:c++").filter, "title")

    def test_brackets_escaped(self):
        assert r"\[" in _get_regex_from_filter(
            _make_colon_escape_dt("title:foo[bar]").filter, "title")

    def test_plain_value_unchanged(self):
        assert _get_regex_from_filter(
            _make_colon_escape_dt("title:hello").filter, "title") == "hello"

    def test_caret_escaped(self):
        assert r"\^" in _get_regex_from_filter(
            _make_colon_escape_dt("title:^start").filter, "title")

    def test_dollar_escaped(self):
        assert r"\$" in _get_regex_from_filter(
            _make_colon_escape_dt("title:end$").filter, "title")

    def test_parentheses_escaped(self):
        assert r"\(" in _get_regex_from_filter(
            _make_colon_escape_dt("title:foo(bar)").filter, "title")

    def test_star_escaped(self):
        assert r"\*" in _get_regex_from_filter(
            _make_colon_escape_dt("title:foo*bar").filter, "title")

    def test_question_mark_escaped(self):
        assert r"\?" in _get_regex_from_filter(
            _make_colon_escape_dt("title:foo?bar").filter, "title")

    def test_pipe_escaped(self):
        assert r"\|" in _get_regex_from_filter(
            _make_colon_escape_dt("title:a|b").filter, "title")


# ---------------------------------------------------------------------------
# Quoted phrase regex flag (from test_regex_quoted_phrase.py)
# ---------------------------------------------------------------------------

def _qb_no_index():
    return MongoQueryBuilder(FieldMapper([]), use_text_index=False, has_text_index=False)


def _phrase_search(term, search_regex=False):
    return _qb_no_index().build_global_search(
        [term], ["name"], original_search=f'"{term}"', search_regex=search_regex
    )


def _phrase_pattern(result, column="name"):
    return result["$or"][0][column]["$regex"]


class TestQuotedPhraseRegexFalse:
    def test_plain_word_gets_word_boundaries(self):
        p = _phrase_pattern(_phrase_search("hello", search_regex=False))
        assert p.startswith("\\b") and p.endswith("\\b")

    def test_special_chars_escaped(self):
        p = _phrase_pattern(_phrase_search("john.doe", search_regex=False))
        assert "john\\.doe" in p

    def test_dot_not_wildcard(self):
        p = _phrase_pattern(_phrase_search("a.b", search_regex=False))
        assert "\\." in p


class TestQuotedPhraseRegexTrue:
    def test_anchor_pattern_not_wrapped(self):
        assert _phrase_pattern(_phrase_search("^foo", search_regex=True)) == "^foo"

    def test_end_anchor_preserved(self):
        assert _phrase_pattern(_phrase_search("bar$", search_regex=True)) == "bar$"

    def test_complex_regex_not_corrupted(self):
        assert _phrase_pattern(_phrase_search("(foo|bar)", search_regex=True)) == "(foo|bar)"

    def test_special_chars_not_escaped_in_regex_mode(self):
        assert _phrase_pattern(_phrase_search("a.b", search_regex=True)) == "a.b"

    def test_no_word_boundary_prefix(self):
        assert not _phrase_pattern(_phrase_search("test", search_regex=True)).startswith("\\b")

    def test_no_word_boundary_suffix(self):
        assert not _phrase_pattern(_phrase_search("test", search_regex=True)).endswith("\\b")


# ---------------------------------------------------------------------------
# Invalid number search (from test_invalid_number_search.py)
# ---------------------------------------------------------------------------

def _make_invalid_num_dt(search_value, column_search_value=""):
    mongo = MagicMock()
    args = {
        "draw": "1", "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "price", "name": "price", "searchable": True,
                     "search": {"value": column_search_value, "regex": False}}],
    }
    return DataTables(mongo, "products", args,
                      data_fields=[DataField("price", "number")],
                      use_text_index=False)


class TestInvalidNumberSearch(unittest.TestCase):
    def test_invalid_colon_search_empty(self):
        self.assertEqual(_make_invalid_num_dt("price:abc").column_specific_search_condition, {})

    def test_invalid_colon_no_regex_on_number(self):
        result = _make_invalid_num_dt("price:notanumber").column_specific_search_condition
        if "$and" in result:
            for cond in result["$and"]:
                self.assertNotIn("$regex", cond.get("price", {}))

    def test_invalid_operator_colon_empty(self):
        self.assertEqual(_make_invalid_num_dt("price:>abc").column_specific_search_condition, {})

    def test_valid_number_colon_works(self):
        result = _make_invalid_num_dt("price:42").column_specific_search_condition
        self.assertIn("$and", result)
        self.assertEqual(result["$and"][0]["price"], 42)

    def test_valid_operator_colon_works(self):
        result = _make_invalid_num_dt("price:>10").column_specific_search_condition
        self.assertIn("$and", result)
        self.assertIn("$gt", result["$and"][0]["price"])
        self.assertEqual(result["$and"][0]["price"]["$gt"], 10)

    def test_invalid_column_search_empty(self):
        self.assertEqual(_make_invalid_num_dt("", "notanumber").column_search_conditions, {})

    def test_invalid_column_search_no_regex(self):
        result = _make_invalid_num_dt("", "xyz").column_search_conditions
        if "$and" in result:
            for cond in result["$and"]:
                self.assertNotIn("$regex", cond.get("price", {}))





# --- from tests/test_search_fixed.py ---
class TestSearchFixed(BaseDataTablesTest):

    def _make_dt(self, extra_args):
        args = dict(self.request_args)
        args.update(extra_args)
        with patch.object(DataTables, 'has_text_index', new_callable=lambda: property(lambda self: False)):
            return DataTables(self.mongo, 'test_collection', args,
                              [DataField('name', 'string'),
                               DataField('email', 'string'),
                               DataField('status', 'string')])

    def test_no_search_fixed_returns_empty(self):
        dt = self._make_dt({})
        self.assertEqual(dt._parse_search_fixed(), {})

    def test_empty_dict_returns_empty(self):
        dt = self._make_dt({'searchFixed': {}})
        self.assertEqual(dt._parse_search_fixed(), {})

    def test_single_fixed_search_produces_or_across_columns(self):
        dt = self._make_dt({'searchFixed': {'role': 'admin'}})
        result = dt._parse_search_fixed()
        self.assertIn('$or', result)
        # Should search across all searchable columns
        fields = [list(cond.keys())[0] for cond in result['$or']]
        self.assertIn('name', fields)
        self.assertIn('email', fields)
        self.assertIn('status', fields)

    def test_multiple_fixed_searches_are_anded(self):
        dt = self._make_dt({'searchFixed': {'role': 'admin', 'dept': 'eng'}})
        result = dt._parse_search_fixed()
        self.assertIn('$and', result)
        self.assertEqual(len(result['$and']), 2)

    def test_empty_value_is_skipped(self):
        dt = self._make_dt({'searchFixed': {'role': '', 'dept': 'eng'}})
        result = dt._parse_search_fixed()
        # Only 'eng' produces a condition — no $and wrapper needed
        self.assertNotIn('$and', result)
        self.assertIn('$or', result)

    def test_search_fixed_included_in_filter(self):
        dt = self._make_dt({'searchFixed': {'role': 'admin'}})
        f = dt.filter
        # filter should contain the searchFixed condition
        self.assertNotEqual(f, {})

    def test_search_fixed_combined_with_global_search(self):
        args = {'searchFixed': {'role': 'admin'}}
        args['search'] = {'value': 'john', 'regex': 'false'}
        dt = self._make_dt(args)
        f = dt.filter
        # Both conditions present — must be $and at top level
        self.assertIn('$and', f)

    def test_non_dict_search_fixed_ignored(self):
        dt = self._make_dt({'searchFixed': 'invalid'})
        self.assertEqual(dt._parse_search_fixed(), {})


if __name__ == '__main__':
    unittest.main()



# --- from tests/test_search_fixed_flags.py ---
def _make_dt_flags(search_extra=None, col0_search_extra=None):
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    search = {"value": "", "regex": False}
    if search_extra:
        search.update(search_extra)

    col_search = {"value": "", "regex": False}
    if col0_search_extra:
        col_search.update(col0_search_extra)

    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": search,
        "columns": [
            {"data": "name", "searchable": True, "orderable": True, "search": col_search},
        ],
        "order": [{"column": 0, "dir": "asc"}],
    }
    data_fields = [DataField("name", "string")]
    dt = DataTables(db, "users", request_args, data_fields=data_fields)
    with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
        return dt


# ---------------------------------------------------------------------------
# _parse_search_fixed — search.regex flag
# ---------------------------------------------------------------------------

def test_global_fixed_regex_false_escapes_term():
    """search.regex=False (default): special chars are regex-escaped."""
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "a.b"}], "regex": False})
    result = dt._parse_search_fixed()
    # In the dict the $regex value is 'a\\.b'; str() repr doubles the backslash
    import re as _re
    regex_vals = _re.findall(r"\$regex['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any("\\." in v for v in regex_vals)


def test_global_fixed_regex_true_uses_raw_pattern():
    """search.regex=True: term is used as raw regex pattern (not escaped)."""
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "a.b"}], "regex": True})
    result = dt._parse_search_fixed()
    # raw pattern — dot should NOT be escaped
    assert "a\\.b" not in str(result)
    assert "a.b" in str(result)


def test_global_fixed_regex_string_true_treated_as_truthy():
    """search.regex='true' (string): treated as truthy, raw pattern used."""
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "a.b"}], "regex": "true"})
    result = dt._parse_search_fixed()
    assert "a\\.b" not in str(result)


# ---------------------------------------------------------------------------
# _parse_search_fixed — search.caseInsensitive flag
# ---------------------------------------------------------------------------

def test_global_fixed_case_insensitive_true_adds_i_option():
    """search.caseInsensitive=True (default): regex uses 'i' option."""
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "alice"}], "caseInsensitive": True})
    result = dt._parse_search_fixed()
    assert "'i'" in str(result) or '"i"' in str(result) or "options': 'i'" in str(result) or "options\": \"i\"" in str(result) or "i" in str(result)
    # More precise: check the options value in the regex dict
    result_str = str(result)
    assert "$options" in result_str
    # Find options value — should be 'i'
    import re
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", result_str)
    assert any(o == "i" for o in opts)


def test_global_fixed_case_insensitive_false_no_i_option():
    """search.caseInsensitive=False: regex uses no 'i' option."""
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "alice"}], "caseInsensitive": False})
    result = dt._parse_search_fixed()
    import re
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert all(o == "" for o in opts)


def test_global_fixed_case_insensitive_string_false_treated_as_falsy():
    """search.caseInsensitive='false' (string): treated as falsy, no 'i' option."""
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "alice"}], "caseInsensitive": "false"})
    result = dt._parse_search_fixed()
    import re
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert all(o == "" for o in opts)


# ---------------------------------------------------------------------------
# _parse_column_search_fixed — smart flag
# ---------------------------------------------------------------------------

def test_column_fixed_smart_true_splits_multiword():
    """Column search.smart=True (default): multi-word term splits into AND terms."""
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "hello world"}], "smart": True})
    result = dt._parse_column_search_fixed()
    # smart split produces nested $and with individual word patterns
    assert "$and" in str(result)
    assert "hello" in str(result)
    assert "world" in str(result)


def test_column_fixed_smart_false_single_phrase():
    """Column search.smart=False: multi-word term is a single regex (not AND-split)."""
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "hello world"}], "smart": False})
    result = dt._parse_column_search_fixed()
    # Inspect the actual regex value in the dict — should contain the escaped space
    import re as _re
    regex_vals = _re.findall(r"\$regex['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any("hello" in v and "world" in v for v in regex_vals)
    # Should NOT have a nested $and splitting the words
    and_count = str(result).count("'$and'") + str(result).count('"$and"')
    assert and_count <= 1


# ---------------------------------------------------------------------------
# _parse_column_search_fixed — caseInsensitive flag
# ---------------------------------------------------------------------------

def test_column_fixed_case_insensitive_true_adds_i_option():
    """Column search.caseInsensitive=True (default): regex uses 'i' option."""
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "alice"}], "caseInsensitive": True})
    result = dt._parse_column_search_fixed()
    import re
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any(o == "i" for o in opts)


def test_column_fixed_case_insensitive_false_no_i_option():
    """Column search.caseInsensitive=False: regex uses no 'i' option."""
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "alice"}], "caseInsensitive": False})
    result = dt._parse_column_search_fixed()
    import re
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert all(o == "" for o in opts)



# --- from tests/test_search_fixed_wire_format.py ---
def _make_dt_wire(search_extra=None, columns_search_extra=None, search_fixed_legacy=None):
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    search = {"value": "", "regex": False}
    if search_extra:
        search.update(search_extra)

    columns = [
        {"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        {"data": "status", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
    ]
    if columns_search_extra:
        for i, extra in enumerate(columns_search_extra):
            if extra and i < len(columns):
                columns[i]["search"].update(extra)

    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": search,
        "columns": columns,
        "order": [{"column": 0, "dir": "asc"}],
    }
    if search_fixed_legacy is not None:
        request_args["searchFixed"] = search_fixed_legacy

    data_fields = [DataField("name", "string"), DataField("status", "string")]
    dt = DataTables(db, "users", request_args, data_fields=data_fields)
    with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
        return dt


# --- Global search.fixed (new wire format) ---

def test_global_fixed_array_single_entry():
    """Single {name, term} in search.fixed applies as global search across columns."""
    dt = _make_dt_wire(search_extra={"fixed": [{"name": "lock", "term": "Alice"}]})
    result = dt._parse_search_fixed()
    assert "$or" in result
    fields = [list(c.keys())[0] for c in result["$or"]]
    assert "name" in fields
    assert "status" in fields


def test_global_fixed_array_multiple_entries_anded():
    """Multiple entries in search.fixed are ANDed."""
    dt = _make_dt_wire(search_extra={"fixed": [
        {"name": "a", "term": "Alice"},
        {"name": "b", "term": "active"},
    ]})
    result = dt._parse_search_fixed()
    assert "$and" in result
    assert len(result["$and"]) == 2


def test_global_fixed_array_function_term_skipped():
    """Entries with term == 'function' are skipped."""
    dt = _make_dt_wire(search_extra={"fixed": [{"name": "fn", "term": "function"}]})
    result = dt._parse_search_fixed()
    assert result == {}


def test_global_fixed_array_empty_produces_no_filter():
    """Empty search.fixed array produces no filter."""
    dt = _make_dt_wire(search_extra={"fixed": []})
    result = dt._parse_search_fixed()
    assert result == {}


# --- Per-column columns[i].search.fixed (new wire format) ---

def test_column_fixed_array_single_entry():
    """Single {name, term} in column search.fixed applies to that column only."""
    dt = _make_dt_wire(columns_search_extra=[{"fixed": [{"name": "lock", "term": "Alice"}]}])
    result = dt._parse_column_search_fixed()
    assert "name" in str(result)
    assert "Alice" in str(result)


def test_column_fixed_array_function_term_skipped():
    """Column search.fixed with term == 'function' is skipped."""
    dt = _make_dt_wire(columns_search_extra=[{"fixed": [{"name": "fn", "term": "function"}]}])
    result = dt._parse_column_search_fixed()
    assert result == {}


def test_global_and_column_fixed_combined():
    """Global search.fixed and per-column search.fixed both apply."""
    dt = _make_dt_wire(
        search_extra={"fixed": [{"name": "g", "term": "Alice"}]},
        columns_search_extra=[None, {"fixed": [{"name": "s", "term": "active"}]}],
    )
    g = dt._parse_search_fixed()
    c = dt._parse_column_search_fixed()
    assert g != {}
    assert c != {}
    assert "status" in str(c)


# --- Legacy dict format (backward compat) ---

def test_legacy_global_searchFixed_dict():
    """Legacy top-level searchFixed dict still works."""
    dt = _make_dt_wire(search_fixed_legacy={"role": "admin"})
    result = dt._parse_search_fixed()
    assert "$or" in result


def test_legacy_column_searchFixed_dict():
    """Legacy per-column searchFixed dict still works."""
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}
    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "columns": [
            {"data": "name", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}, "searchFixed": {"lock": "Alice"}},
            {"data": "status", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ],
        "order": [{"column": 0, "dir": "asc"}],
    }
    dt = DataTables(db, "users", request_args, data_fields=[DataField("name", "string"), DataField("status", "string")])
    result = dt._parse_column_search_fixed()
    assert "name" in str(result)
    assert "Alice" in str(result)


def test_mixed_new_array_and_legacy_dict():
    """New array format and legacy dict format are both applied (ANDed)."""
    dt = _make_dt_wire(
        search_extra={"fixed": [{"name": "new", "term": "Alice"}]},
        search_fixed_legacy={"old": "admin"},
    )
    result = dt._parse_search_fixed()
    assert "$and" in result
    assert len(result["$and"]) == 2



# --- from tests/test_column_search_fixed.py ---
def _make_dt_col(columns_extra=None, search_fixed_global=None):
    """Build a DataTables instance with optional per-column searchFixed."""
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    columns = [
        {"data": "name", "search": {"value": "", "regex": False}, "searchable": True, "orderable": True},
        {"data": "status", "search": {"value": "", "regex": False}, "searchable": True, "orderable": True},
        {"data": "age", "search": {"value": "", "regex": False}, "searchable": True, "orderable": True},
    ]
    if columns_extra:
        for i, extra in enumerate(columns_extra):
            if extra and i < len(columns):
                columns[i].update(extra)

    request_args = {
        "draw": "1",
        "start": "0",
        "length": "10",
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
    with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
        return dt


class TestColumnSearchFixed:

    def test_single_column_fixed_search(self):
        """Per-column searchFixed applies only to that column's field."""
        dt = _make_dt_col(columns_extra=[{"searchFixed": {"lock": "Alice"}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        # Should produce a regex condition on 'name' field only
        assert "name" in str(f)
        assert "status" not in str(f) or "$and" in str(f)

    def test_column_fixed_search_scoped_to_column(self):
        """Per-column searchFixed on status column targets status field."""
        dt = _make_dt_col(columns_extra=[None, {"searchFixed": {"statusLock": "active"}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "status" in str(f)

    def test_multiple_column_fixed_searches_anded(self):
        """Multiple per-column searchFixed conditions are ANDed."""
        dt = _make_dt_col(columns_extra=[
            {"searchFixed": {"nameLock": "Alice"}},
            {"searchFixed": {"statusLock": "active"}},
        ])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "$and" in str(f)
        assert "name" in str(f)
        assert "status" in str(f)

    def test_empty_column_fixed_search_ignored(self):
        """Empty searchFixed values are skipped."""
        dt = _make_dt_col(columns_extra=[{"searchFixed": {"lock": ""}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_non_dict_column_fixed_search_ignored(self):
        """Non-dict searchFixed on a column is ignored."""
        dt = _make_dt_col(columns_extra=[{"searchFixed": "not-a-dict"}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_column_fixed_and_global_fixed_combined(self):
        """Per-column and global searchFixed both apply (ANDed)."""
        dt = _make_dt_col(
            columns_extra=[{"searchFixed": {"nameLock": "Alice"}}],
            search_fixed_global={"tenantFilter": "acme"}
        )
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "$and" in str(f)

    def test_no_column_fixed_search_no_effect(self):
        """Columns without searchFixed produce no extra conditions."""
        dt = _make_dt_col()
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_multiple_named_searches_in_one_column(self):
        """Multiple named searches in one column's searchFixed are all applied."""
        dt = _make_dt_col(columns_extra=[{"searchFixed": {"lock1": "Alice", "lock2": "Bob"}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        # Both values should produce conditions on 'name'
        assert "name" in str(f)



# --- from tests/test_search_terms_cache.py ---
def _make_dt_cache(search_value=""):
    col = MagicMock()
    col.find.return_value = []
    col.count_documents.return_value = 0
    args = {"draw": "1", "start": "0", "length": "10",
            "search": {"value": search_value, "regex": "false"},
            "columns": [], "order": []}
    return DataTables(col, "test", args)


def test_search_terms_cache_initialized_none():
    dt = _make_dt_cache("hello world")
    assert dt._search_terms_cache is None


def test_search_terms_cache_populated_on_first_access():
    dt = _make_dt_cache("hello world")
    _ = dt.search_terms
    assert dt._search_terms_cache is not None


def test_search_terms_parse_called_once():
    dt = _make_dt_cache("foo bar")
    with patch("mongo_datatables.datatables.SearchTermParser.parse",
               wraps=lambda v: v.split()) as mock_parse:
        _ = dt.search_terms
        _ = dt.search_terms
        _ = dt.search_terms
    mock_parse.assert_called_once()


def test_search_terms_cache_returns_same_object():
    dt = _make_dt_cache("alpha beta")
    first = dt.search_terms
    second = dt.search_terms
    assert first is second


def test_search_terms_empty_string_cached():
    dt = _make_dt_cache("")
    result = dt.search_terms
    # Empty string parses to empty list; cache should hold it
    assert result == []
    assert dt._search_terms_cache == []



# --- from tests/test_case_insensitive.py ---
@pytest.fixture
def qb():
    fm = MagicMock()
    fm.get_field_type.return_value = "string"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


# --- build_global_search ---

def test_global_search_default_case_insensitive(qb):
    result = qb.build_global_search(["hello"], ["name"])
    assert result["$or"][0]["name"]["$options"] == "i"


def test_global_search_explicit_case_insensitive_true(qb):
    result = qb.build_global_search(["hello"], ["name"], case_insensitive=True)
    assert result["$or"][0]["name"]["$options"] == "i"


def test_global_search_case_sensitive(qb):
    result = qb.build_global_search(["hello"], ["name"], case_insensitive=False)
    assert result["$or"][0]["name"]["$options"] == ""


def test_global_search_smart_multi_term_case_sensitive(qb):
    result = qb.build_global_search(["foo", "bar"], ["name"], search_smart=True, case_insensitive=False)
    # $and of $or per term
    assert result["$and"][0]["name"]["$options"] == ""
    assert result["$and"][1]["name"]["$options"] == ""


def test_global_search_quoted_phrase_case_sensitive(qb):
    result = qb.build_global_search(["hello world"], ["name"], original_search='"hello world"', case_insensitive=False)
    assert result["$or"][0]["name"]["$options"] == ""


# --- build_column_search ---

def test_column_search_default_case_insensitive(qb):
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False}}]
    result = qb.build_column_search(columns)
    assert result["$and"][0]["name"]["$options"] == "i"


def test_column_search_global_case_sensitive(qb):
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False}}]
    result = qb.build_column_search(columns, case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == ""


def test_column_search_per_column_override_sensitive(qb):
    """Per-column caseInsensitive=False overrides global True."""
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False, "caseInsensitive": False}}]
    result = qb.build_column_search(columns, case_insensitive=True)
    assert result["$and"][0]["name"]["$options"] == ""


def test_column_search_per_column_override_insensitive(qb):
    """Per-column caseInsensitive=True overrides global False."""
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False, "caseInsensitive": True}}]
    result = qb.build_column_search(columns, case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == "i"


def test_column_search_per_column_string_false(qb):
    """String 'false' coerced to False."""
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False, "caseInsensitive": "false"}}]
    result = qb.build_column_search(columns, case_insensitive=True)
    assert result["$and"][0]["name"]["$options"] == ""


# --- build_column_specific_search ---

def test_colon_search_default_case_insensitive(qb):
    result = qb.build_column_specific_search(["name:Alice"], ["name"])
    assert result["$and"][0]["name"]["$options"] == "i"


def test_colon_search_case_sensitive(qb):
    result = qb.build_column_specific_search(["name:Alice"], ["name"], case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == ""


# --- datatables.py integration ---

def _make_dt(search_dict):
    from mongo_datatables import DataTables
    collection = MagicMock()
    collection.aggregate.return_value = iter([])
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "search": search_dict,
        "order": [],
        "columns": [{"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}}],
    }
    return DataTables(MagicMock(), collection, request_args, ["name"])


def test_datatables_global_search_case_insensitive_false():
    """DataTables request with search[caseInsensitive]=false produces case-sensitive query."""
    dt = _make_dt({"value": "Alice", "regex": False, "caseInsensitive": "false"})
    cond = dt.global_search_condition
    assert cond["$or"][0]["name"]["$options"] == ""


def test_datatables_global_search_case_insensitive_default():
    """DataTables request without caseInsensitive defaults to case-insensitive."""
    dt = _make_dt({"value": "Alice", "regex": False})
    cond = dt.global_search_condition
    assert cond["$or"][0]["name"]["$options"] == "i"

