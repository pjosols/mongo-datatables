"""Text search tests: field mapping for column and global search."""
import unittest
from unittest.mock import MagicMock

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


def _make_qb(*data_fields):
    fm = FieldMapper(list(data_fields))
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


def _make_dt(data_fields, search_value, searchable_columns):
    """Build a DataTables instance with no text index and given data_fields."""
    mock_col = MagicMock()
    mock_col.list_indexes.return_value = []
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_col)
    columns = [
        {"data": col, "name": col, "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}}
        for col in searchable_columns
    ]
    args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": search_value, "regex": False},
        "order": [], "columns": columns,
    }
    return DataTables(mock_db, "test", args, data_fields=data_fields, use_text_index=False)


class TestColumnSearchFieldMapping(unittest.TestCase):
    """build_column_search uses db field name (not UI alias) in query keys."""

    def test_text_column_uses_db_field_name(self):
        """Text regex condition key must be the DB field path, not the UI alias."""
        qb = _make_qb(DataField("author.fullName", "string", "Author"))
        columns = [{"data": "Author", "searchable": True, "search": {"value": "smith", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("author.fullName", cond)
        self.assertNotIn("Author", cond)

    def test_text_column_no_alias_unchanged(self):
        """When no alias is set, column name equals db field — key is unchanged."""
        qb = _make_qb(DataField("title", "string"))
        columns = [{"data": "title", "searchable": True, "search": {"value": "mongo", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("title", cond)

    def test_text_column_regex_true_uses_db_field_name(self):
        """Raw-regex condition key must also be the DB field path."""
        qb = _make_qb(DataField("meta.tags", "string", "Tags"))
        columns = [{"data": "Tags", "searchable": True, "search": {"value": "^py", "regex": True}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("meta.tags", cond)
        self.assertNotIn("Tags", cond)

    def test_number_column_uses_db_field_name(self):
        """Number branch uses db field name."""
        qb = _make_qb(DataField("stats.score", "number", "Score"))
        columns = [{"data": "Score", "searchable": True, "search": {"value": "90", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("stats.score", cond)
        self.assertNotIn("Score", cond)


class TestGlobalSearchFieldMapping(unittest.TestCase):
    """build_global_search() must use DB field names, not UI aliases."""

    def test_unquoted_term_uses_db_field(self):
        data_fields = [DataField("author_name", "string", alias="Author")]
        dt = _make_dt(data_fields, "Smith", ["Author"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "author_name" in keys
        assert "Author" not in keys

    def test_quoted_phrase_uses_db_field(self):
        data_fields = [DataField("author_name", "string", alias="Author")]
        dt = _make_dt(data_fields, '"Jonathan Kennedy"', ["Author"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "author_name" in keys
        assert "Author" not in keys

    def test_no_alias_field_unchanged(self):
        data_fields = [DataField("status", "string")]
        dt = _make_dt(data_fields, "active", ["status"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "status" in keys

    def test_multiple_aliased_columns(self):
        data_fields = [
            DataField("first_name", "string", alias="FirstName"),
            DataField("last_name", "string", alias="LastName"),
        ]
        dt = _make_dt(data_fields, "Alice", ["FirstName", "LastName"])
        result = dt.global_search_condition
        assert "$or" in result
        keys = [list(cond.keys())[0] for cond in result["$or"]]
        assert "first_name" in keys
        assert "last_name" in keys
        assert "FirstName" not in keys
        assert "LastName" not in keys


class TestGlobalSearchBranches(unittest.TestCase):
    """Cover global search branches in query_builder.py."""

    def _qb(self, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    def test_no_searchable_columns_returns_empty(self):
        qb = self._qb()
        result = qb.build_global_search(["hello"], [])
        self.assertEqual(result, {})

    def test_quoted_skips_date_number_cols(self):
        qb = self._qb([DataField("created", "date"), DataField("amount", "number")])
        result = qb.build_global_search(
            ["exact"], ["created", "amount"],
            original_search='"exact"'
        )
        self.assertEqual(result, {})

    def test_keyword_col_skipped(self):
        qb = self._qb([DataField("status", "keyword"), DataField("name", "string")])
        result = qb.build_global_search(["active"], ["status", "name"])
        self.assertIn("$or", result)
        fields = [list(c.keys())[0] for c in result["$or"]]
        self.assertNotIn("status", fields)
        self.assertIn("name", fields)

    def test_quoted_with_string_col(self):
        qb = self._qb([DataField("name", "string")])
        result = qb.build_global_search(
            ["hello"], ["name"],
            original_search='"hello"'
        )
        self.assertIn("$or", result)
        self.assertIn("\\b", result["$or"][0]["name"]["$regex"])

    def test_smart_multi_term_all_number_cols_non_numeric(self):
        qb = self._qb([DataField("price", "number")])
        result = qb.build_global_search(
            ["hello", "world"], ["price"],
            search_smart=True
        )
        self.assertEqual(result, {})

    def test_smart_multi_term_numeric_values(self):
        qb = self._qb([DataField("price", "number")])
        result = qb.build_global_search(
            ["10", "20"], ["price"],
            search_smart=True
        )
        self.assertIn("$and", result)


if __name__ == "__main__":
    unittest.main()
