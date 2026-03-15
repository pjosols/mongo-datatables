"""Tests for re.escape() in colon-syntax (build_column_specific_search) and fallback paths."""
import re
import pytest
from unittest.mock import MagicMock
from mongo_datatables import DataTables, DataField


def make_dt(search_value, fields=None):
    """Helper: build a DataTables instance with a global search containing colon-syntax."""
    if fields is None:
        fields = [DataField("title", "string"), DataField("email", "string"), DataField("price", "number"), DataField("created", "date")]
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_collection)
    col = lambda name: {"data": name, "searchable": True, "orderable": True, "search": {"value": "", "regex": False}}
    request_args = {
        "draw": "1",
        "start": 0,
        "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [col("title"), col("email"), col("price"), col("created")],
    }
    return DataTables(mock_client, "test_col", request_args, fields)


class TestColonSearchRegexEscape:
    """build_column_specific_search must escape regex metacharacters in the value."""

    def _get_regex(self, f: dict, field: str) -> str:
        """Extract $regex value for a field from an $and filter."""
        and_clause = f.get("$and", [])
        assert and_clause, f"Expected $and clause, got: {f!r}"
        conds = [c for c in and_clause if field in c]
        assert conds, f"Expected '{field}' condition in $and clause"
        return conds[0][field]["$regex"]

    def test_dot_in_email_is_escaped(self):
        dt = make_dt("email:user@domain.com")
        assert r"\." in self._get_regex(dt.filter, "email")

    def test_plus_in_value_is_escaped(self):
        dt = make_dt("title:c++")
        assert r"\+" in self._get_regex(dt.filter, "title")

    def test_brackets_in_value_are_escaped(self):
        dt = make_dt("title:foo[bar]")
        assert r"\[" in self._get_regex(dt.filter, "title")

    def test_plain_value_unchanged(self):
        """Values without metacharacters should be unchanged after re.escape."""
        dt = make_dt("title:hello")
        assert self._get_regex(dt.filter, "title") == "hello"

    def test_caret_in_value_is_escaped(self):
        dt = make_dt("title:^start")
        assert r"\^" in self._get_regex(dt.filter, "title")

    def test_dollar_in_value_is_escaped(self):
        dt = make_dt("title:end$")
        assert r"\$" in self._get_regex(dt.filter, "title")

    def test_parentheses_in_value_are_escaped(self):
        dt = make_dt("title:foo(bar)")
        assert r"\(" in self._get_regex(dt.filter, "title")

    def test_star_in_value_is_escaped(self):
        dt = make_dt("title:foo*bar")
        assert r"\*" in self._get_regex(dt.filter, "title")

    def test_question_mark_in_value_is_escaped(self):
        dt = make_dt("title:foo?bar")
        assert r"\?" in self._get_regex(dt.filter, "title")

    def test_pipe_not_treated_as_range_in_colon_search(self):
        """Pipe in colon-syntax value should be escaped (not treated as range)."""
        dt = make_dt("title:a|b")
        assert r"\|" in self._get_regex(dt.filter, "title")
