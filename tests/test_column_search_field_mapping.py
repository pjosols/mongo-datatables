import unittest
from mongo_datatables.datatables import DataField
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


def _make_qb(*data_fields):
    fm = FieldMapper(list(data_fields))
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


class TestColumnSearchFieldMapping(unittest.TestCase):
    """Verify build_column_search uses db field name (not UI alias) in query keys."""

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

    def test_number_column_already_used_db_field(self):
        """Number branch was already correct — verify it still uses db field name."""
        qb = _make_qb(DataField("stats.score", "number", "Score"))
        columns = [{"data": "Score", "searchable": True, "search": {"value": "90", "regex": False}}]
        result = qb.build_column_search(columns)
        cond = result["$and"][0]
        self.assertIn("stats.score", cond)
        self.assertNotIn("Score", cond)


if __name__ == "__main__":
    unittest.main()
