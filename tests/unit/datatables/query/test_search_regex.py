"""Regex flag tests: column regex, global regex, quoted phrase regex handling."""
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


def _qb(field_types=None):
    data_fields = [DataField(n, t) for n, t in (field_types or {}).items()]
    fm = FieldMapper(data_fields)
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


# ---------------------------------------------------------------------------
# Regex search flag
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
# Quoted phrase regex flag
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
