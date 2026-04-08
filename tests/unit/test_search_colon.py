"""Colon search tests: multi-colon terms, regex escaping, invalid numbers, search_fixed flags."""
import re
import unittest
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


# ---------------------------------------------------------------------------
# Shared helpers
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


# ---------------------------------------------------------------------------
# Multi-colon search terms
# ---------------------------------------------------------------------------

class TestMultiColonSearchTerms:
    def test_single_colon_term_included(self):
        assert "title:python" in _make_multi_colon_dt("title:python").search_terms_with_a_colon

    def test_multi_colon_term_included(self):
        assert "url:https://example.com" in \
               _make_multi_colon_dt("url:https://example.com").search_terms_with_a_colon

    def test_multi_colon_term_not_in_global_search(self):
        assert "url:https://example.com" not in \
               _make_multi_colon_dt("url:https://example.com").search_terms_without_a_colon

    def test_multi_colon_term_not_silently_dropped(self):
        dt = _make_multi_colon_dt("url:https://example.com")
        assert "url:https://example.com" in dt.search_terms_with_a_colon
        assert "url:https://example.com" not in dt.search_terms_without_a_colon

    def test_multi_colon_split_uses_first_colon(self):
        terms = _make_multi_colon_dt("url:https://example.com").search_terms_with_a_colon
        assert len(terms) == 1
        field, value = terms[0].split(":", 1)
        assert field == "url"
        assert value == "https://example.com"

    def test_no_colon_term_excluded(self):
        assert _make_multi_colon_dt("python").search_terms_with_a_colon == []

    def test_mixed_terms(self):
        dt = _make_multi_colon_dt("python title:flask url:https://x.com")
        assert "python" in dt.search_terms_without_a_colon
        assert "title:flask" in dt.search_terms_with_a_colon
        assert "url:https://x.com" in dt.search_terms_with_a_colon


# ---------------------------------------------------------------------------
# Regex escaping in colon search values
# ---------------------------------------------------------------------------

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
# html-num / html-num-fmt SearchBuilder types
# ---------------------------------------------------------------------------

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
# Invalid number search
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


# ---------------------------------------------------------------------------
# search_fixed flags: regex and caseInsensitive
# ---------------------------------------------------------------------------

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
    with patch.object(type(dt), "has_text_index",
                      new_callable=lambda: property(lambda self: False)):
        return dt


def test_global_fixed_regex_false_escapes_term():
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "a.b"}], "regex": False})
    result = dt._parse_search_fixed()
    regex_vals = re.findall(r"\$regex['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any("\\." in v for v in regex_vals)


def test_global_fixed_regex_true_uses_raw_pattern():
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "a.b"}], "regex": True})
    result = dt._parse_search_fixed()
    assert "a\\.b" not in str(result)
    assert "a.b" in str(result)


def test_global_fixed_regex_string_true_treated_as_truthy():
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "a.b"}], "regex": "true"})
    result = dt._parse_search_fixed()
    assert "a\\.b" not in str(result)


def test_global_fixed_case_insensitive_true_adds_i_option():
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "alice"}],
                                      "caseInsensitive": True})
    result = dt._parse_search_fixed()
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any(o == "i" for o in opts)


def test_global_fixed_case_insensitive_false_no_i_option():
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "alice"}],
                                      "caseInsensitive": False})
    result = dt._parse_search_fixed()
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert all(o == "" for o in opts)


def test_global_fixed_case_insensitive_string_false_treated_as_falsy():
    dt = _make_dt_flags(search_extra={"fixed": [{"name": "f", "term": "alice"}],
                                      "caseInsensitive": "false"})
    result = dt._parse_search_fixed()
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert all(o == "" for o in opts)


def test_column_fixed_smart_true_splits_multiword():
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "hello world"}],
                                           "smart": True})
    result = dt._parse_column_search_fixed()
    assert "$and" in str(result)
    assert "hello" in str(result)
    assert "world" in str(result)


def test_column_fixed_smart_false_single_phrase():
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "hello world"}],
                                           "smart": False})
    result = dt._parse_column_search_fixed()
    regex_vals = re.findall(r"\$regex['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any("hello" in v and "world" in v for v in regex_vals)
    and_count = str(result).count("'$and'") + str(result).count('"$and"')
    assert and_count <= 1


def test_column_fixed_case_insensitive_true_adds_i_option():
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "alice"}],
                                           "caseInsensitive": True})
    result = dt._parse_column_search_fixed()
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert any(o == "i" for o in opts)


def test_column_fixed_case_insensitive_false_no_i_option():
    dt = _make_dt_flags(col0_search_extra={"fixed": [{"name": "f", "term": "alice"}],
                                           "caseInsensitive": False})
    result = dt._parse_column_search_fixed()
    opts = re.findall(r"\$options['\"]?\s*:\s*['\"]([^'\"]*)['\"]", str(result))
    assert all(o == "" for o in opts)
