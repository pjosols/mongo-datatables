"""Tests for regex flag behavior in quoted-phrase global search."""
import pytest
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


def _builder():
    """Return a MongoQueryBuilder with no field types and no text index."""
    return MongoQueryBuilder(FieldMapper([]), use_text_index=False, has_text_index=False)


def _search(term, search_regex=False):
    """Call build_global_search with a quoted phrase and return the result."""
    return _builder().build_global_search(
        [term], ["name"], original_search=f'"{term}"', search_regex=search_regex
    )


def _pattern(result, column="name"):
    return result["$or"][0][column]["$regex"]


class TestQuotedPhraseRegexFalse:
    """When search_regex=False, quoted phrase should use \\b anchors and escape special chars."""

    def test_plain_word_gets_word_boundary_anchors(self):
        pattern = _pattern(_search("hello", search_regex=False))
        assert pattern.startswith("\\b") and pattern.endswith("\\b")

    def test_special_chars_are_escaped(self):
        pattern = _pattern(_search("john.doe", search_regex=False))
        assert "john\\.doe" in pattern

    def test_dot_not_treated_as_wildcard(self):
        pattern = _pattern(_search("a.b", search_regex=False))
        assert "\\." in pattern  # dot is escaped


class TestQuotedPhraseRegexTrue:
    """When search_regex=True, quoted phrase should use raw pattern WITHOUT \\b anchors."""

    def test_anchor_pattern_not_wrapped_in_word_boundaries(self):
        pattern = _pattern(_search("^foo", search_regex=True))
        assert pattern == "^foo"

    def test_end_anchor_pattern_preserved(self):
        pattern = _pattern(_search("bar$", search_regex=True))
        assert pattern == "bar$"

    def test_complex_regex_not_corrupted(self):
        pattern = _pattern(_search("(foo|bar)", search_regex=True))
        assert pattern == "(foo|bar)"

    def test_special_chars_not_escaped_in_regex_mode(self):
        pattern = _pattern(_search("a.b", search_regex=True))
        assert pattern == "a.b"  # dot NOT escaped

    def test_no_word_boundary_prefix(self):
        pattern = _pattern(_search("test", search_regex=True))
        assert not pattern.startswith("\\b")

    def test_no_word_boundary_suffix(self):
        pattern = _pattern(_search("test", search_regex=True))
        assert not pattern.endswith("\\b")
