"""Text search tests: text index, stemming, quoted phrases, search terms cache."""
import unittest
from unittest.mock import MagicMock, patch

from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _base_request(search_value="", columns=None):
    cols = columns or [
        {"data": "name", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "email", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "status", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ]
    return {
        "draw": "1", "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": cols,
    }


def _mock_mongo():
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    col = MagicMock(spec=Collection)
    col.estimated_document_count.return_value = 0
    mongo.db.__getitem__.return_value = col
    return mongo, col


# ---------------------------------------------------------------------------
# Text index search
# ---------------------------------------------------------------------------

class TestTextSearch(unittest.TestCase):
    def setUp(self):
        self.mongo, _ = _mock_mongo()

    def test_text_index_search(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("John"))
            cond = dt.global_search_condition
        self.assertIn("$text", cond)
        self.assertEqual(cond["$text"]["$search"], '"John"')

    def test_text_index_stemming_true_uses_plus_prefix(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("City"), stemming=True)
            cond = dt.global_search_condition
        self.assertIn("$text", cond)
        self.assertEqual(cond["$text"]["$search"], "+City")

    def test_text_index_stemming_true_multiple_terms(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("New York"), stemming=True)
            cond = dt.global_search_condition
        self.assertIn("$text", cond)
        self.assertEqual(cond["$text"]["$search"], "+New +York")

    def test_text_index_quoted_phrase(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request('"John Doe"'))
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
        self.assertEqual(
            DataTables(self.mongo, "users", _base_request("")).global_search_condition, {}
        )

    def test_field_specific_search_nonempty_filter(self):
        self.assertTrue(DataTables(self.mongo, "users", _base_request("name:John")).filter)

    def test_quoted_phrase_search_terms_nonempty(self):
        self.assertTrue(
            DataTables(self.mongo, "users",
                       _base_request('"John Doe" active "example.com"')).search_terms
        )

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
# Search terms cache
# ---------------------------------------------------------------------------

def _make_dt_cache(search_value=""):
    col = MagicMock()
    col.find.return_value = []
    col.count_documents.return_value = 0
    args = {"draw": "1", "start": "0", "length": "10",
            "search": {"value": search_value, "regex": "false"},
            "columns": [], "order": []}
    return DataTables(col, "test", args)


def test_search_terms_cache_initialized_none():
    assert _make_dt_cache("hello world")._search_terms_cache is None


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
    assert dt.search_terms is dt.search_terms


def test_search_terms_empty_string_cached():
    dt = _make_dt_cache("")
    result = dt.search_terms
    assert result == []
    assert dt._search_terms_cache == []


# ---------------------------------------------------------------------------
# case_insensitive flag on global / column / colon search
# ---------------------------------------------------------------------------

import pytest


@pytest.fixture
def qb():
    fm = MagicMock()
    fm.get_field_type.return_value = "string"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


def test_global_search_default_case_insensitive(qb):
    assert qb.build_global_search(["hello"], ["name"])["$or"][0]["name"]["$options"] == "i"


def test_global_search_explicit_case_insensitive_true(qb):
    result = qb.build_global_search(["hello"], ["name"], case_insensitive=True)
    assert result["$or"][0]["name"]["$options"] == "i"


def test_global_search_case_sensitive(qb):
    result = qb.build_global_search(["hello"], ["name"], case_insensitive=False)
    assert result["$or"][0]["name"]["$options"] == ""


def test_global_search_smart_multi_term_case_sensitive(qb):
    result = qb.build_global_search(["foo", "bar"], ["name"], search_smart=True,
                                    case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == ""
    assert result["$and"][1]["name"]["$options"] == ""


def test_global_search_quoted_phrase_case_sensitive(qb):
    result = qb.build_global_search(["hello world"], ["name"],
                                    original_search='"hello world"', case_insensitive=False)
    assert result["$or"][0]["name"]["$options"] == ""


def test_column_search_default_case_insensitive(qb):
    columns = [{"data": "name", "searchable": True,
                "search": {"value": "Alice", "regex": False}}]
    assert qb.build_column_search(columns)["$and"][0]["name"]["$options"] == "i"


def test_column_search_global_case_sensitive(qb):
    columns = [{"data": "name", "searchable": True,
                "search": {"value": "Alice", "regex": False}}]
    assert qb.build_column_search(columns, case_insensitive=False)["$and"][0]["name"]["$options"] == ""


def test_column_search_per_column_override_sensitive(qb):
    columns = [{"data": "name", "searchable": True,
                "search": {"value": "Alice", "regex": False, "caseInsensitive": False}}]
    assert qb.build_column_search(columns, case_insensitive=True)["$and"][0]["name"]["$options"] == ""


def test_column_search_per_column_override_insensitive(qb):
    columns = [{"data": "name", "searchable": True,
                "search": {"value": "Alice", "regex": False, "caseInsensitive": True}}]
    assert qb.build_column_search(columns, case_insensitive=False)["$and"][0]["name"]["$options"] == "i"


def test_column_search_per_column_string_false(qb):
    columns = [{"data": "name", "searchable": True,
                "search": {"value": "Alice", "regex": False, "caseInsensitive": "false"}}]
    assert qb.build_column_search(columns, case_insensitive=True)["$and"][0]["name"]["$options"] == ""


def test_colon_search_default_case_insensitive(qb):
    result = qb.build_column_specific_search(["name:Alice"], ["name"])
    assert result["$and"][0]["name"]["$options"] == "i"


def test_colon_search_case_sensitive(qb):
    result = qb.build_column_specific_search(["name:Alice"], ["name"], case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == ""


def _make_dt_ci(search_dict):
    collection = MagicMock()
    collection.aggregate.return_value = iter([])
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "search": search_dict,
        "order": [],
        "columns": [{"data": "name", "searchable": True, "orderable": True,
                     "search": {"value": "", "regex": False}}],
    }
    return DataTables(MagicMock(), collection, request_args, ["name"])


def test_datatables_global_search_case_insensitive_false():
    dt = _make_dt_ci({"value": "Alice", "regex": False, "caseInsensitive": "false"})
    assert dt.global_search_condition["$or"][0]["name"]["$options"] == ""


def test_datatables_global_search_case_insensitive_default():
    dt = _make_dt_ci({"value": "Alice", "regex": False})
    assert dt.global_search_condition["$or"][0]["name"]["$options"] == "i"
