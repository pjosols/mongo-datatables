"""Tests for DataField class."""
import pytest

from mongo_datatables import DataField


class TestDataFieldInit:
    def test_valid_name_and_type(self):
        f = DataField("title", "string")
        assert f.name == "title"
        assert f.data_type == "string"

    def test_alias_defaults_to_last_segment(self):
        f = DataField("author.name", "string")
        assert f.alias == "name"

    def test_alias_explicit(self):
        f = DataField("author.name", "string", alias="Author")
        assert f.alias == "Author"

    def test_data_type_case_insensitive(self):
        f = DataField("count", "NUMBER")
        assert f.data_type == "number"

    def test_all_valid_types(self):
        for t in ("string", "number", "date", "boolean", "array", "object", "objectid", "null"):
            f = DataField("x", t)
            assert f.data_type == t

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid data_type"):
            DataField("x", "invalid")

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            DataField("", "string")

    def test_whitespace_name_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            DataField("   ", "string")

    def test_repr_without_alias(self):
        f = DataField("title", "string")
        assert "title" in repr(f)
        assert "string" in repr(f)

    def test_repr_with_alias(self):
        f = DataField("author.name", "string", alias="Author")
        assert "Author" in repr(f)
