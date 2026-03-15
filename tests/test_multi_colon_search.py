"""Tests for multi-colon search term handling and html-num SearchBuilder types."""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables, DataField


def make_dt(search_value, data_fields=None):
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_col.aggregate.return_value = iter([])
    mock_col.estimated_document_count.return_value = 0
    mock_col.count_documents.return_value = 0
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    request_args = {
        "draw": "1",
        "start": "0",
        "length": "10",
        "search": {"value": search_value, "regex": False},
        "columns": [
            {"data": "url", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "title", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ],
        "order": [{"column": 0, "dir": "asc"}],
    }
    return DataTables(mock_db, "test", request_args, data_fields or [])


class TestMultiColonSearchTerms:
    def test_single_colon_term_included(self):
        dt = make_dt("title:python")
        assert "title:python" in dt.search_terms_with_a_colon

    def test_multi_colon_term_included(self):
        """url:https://example.com has 3 colons — must be included in field search."""
        dt = make_dt("url:https://example.com")
        assert "url:https://example.com" in dt.search_terms_with_a_colon

    def test_multi_colon_term_not_in_global_search(self):
        """Multi-colon term should NOT appear in global search (it has a colon)."""
        dt = make_dt("url:https://example.com")
        assert "url:https://example.com" not in dt.search_terms_without_a_colon

    def test_multi_colon_term_not_silently_dropped(self):
        """Multi-colon term must appear in exactly one of the two term lists."""
        dt = make_dt("url:https://example.com")
        with_colon = dt.search_terms_with_a_colon
        without_colon = dt.search_terms_without_a_colon
        assert "url:https://example.com" in with_colon
        assert "url:https://example.com" not in without_colon

    def test_multi_colon_split_uses_first_colon(self):
        """build_column_specific_search splits on first colon, so field=url, value=https://example.com."""
        dt = make_dt("url:https://example.com")
        terms = dt.search_terms_with_a_colon
        assert len(terms) == 1
        field, value = terms[0].split(":", 1)
        assert field == "url"
        assert value == "https://example.com"

    def test_no_colon_term_excluded_from_field_search(self):
        dt = make_dt("python")
        assert dt.search_terms_with_a_colon == []

    def test_mixed_terms(self):
        """Mix of plain, single-colon, and multi-colon terms."""
        dt = make_dt("python title:flask url:https://x.com")
        with_colon = dt.search_terms_with_a_colon
        without_colon = dt.search_terms_without_a_colon
        assert "python" in without_colon
        assert "title:flask" in with_colon
        assert "url:https://x.com" in with_colon


class TestHtmlNumSearchBuilderTypes:
    def _make_dt_with_sb(self, sb_type, condition, value):
        mock_col = MagicMock()
        mock_col.list_indexes.return_value = []
        mock_col.aggregate.return_value = iter([])
        mock_col.estimated_document_count.return_value = 0
        mock_col.count_documents.return_value = 0
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_col)
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "columns": [{"data": "price", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}}],
            "order": [{"column": 0, "dir": "asc"}],
            "searchBuilder": {
                "logic": "AND",
                "criteria": [{"origData": "price", "condition": condition, "type": sb_type, "value": [value]}]
            }
        }
        return DataTables(mock_db, "test", request_args, [DataField("price", "number")])

    def test_html_num_equals_produces_numeric_condition(self):
        dt = self._make_dt_with_sb("html-num", "=", "42")
        f = dt._parse_search_builder()
        assert f == {"price": 42} or f == {"price": 42.0}

    def test_html_num_fmt_greater_than_produces_numeric_condition(self):
        dt = self._make_dt_with_sb("html-num-fmt", ">", "100")
        f = dt._parse_search_builder()
        assert f == {"price": {"$gt": 100}} or f == {"price": {"$gt": 100.0}}

    def test_html_num_not_regex_condition(self):
        """html-num must NOT produce a $regex condition (which would be wrong for numbers)."""
        dt = self._make_dt_with_sb("html-num", "=", "42")
        f = dt._parse_search_builder()
        assert "$regex" not in str(f)

    def test_html_num_fmt_not_regex_condition(self):
        dt = self._make_dt_with_sb("html-num-fmt", "=", "42")
        f = dt._parse_search_builder()
        assert "$regex" not in str(f)
