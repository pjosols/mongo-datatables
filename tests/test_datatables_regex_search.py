"""Tests for DataTables regex search flag support.

Verifies that search[regex] and columns[i][search][regex] flags
are correctly honored per the DataTables server-side protocol.
"""
from unittest.mock import patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


class TestRegexSearchFlag(BaseDataTablesTest):
    """Tests for regex flag support in global and column search."""

    def _make_query_builder(self, field_types=None):
        """Build a MongoQueryBuilder with optional field type definitions.

        Args:
            field_types: Dict mapping field name to type string, e.g. {"salary": "number"}
        """
        data_fields = [DataField(name, dtype) for name, dtype in (field_types or {}).items()]
        fm = FieldMapper(data_fields)
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    # --- Column search: regex=False (default) should escape special chars ---

    def test_column_search_regex_false_escapes_special_chars(self):
        """When regex=False, special regex chars in search value are escaped."""
        qb = self._make_query_builder()
        columns = [{
            "data": "name", "searchable": True,
            "search": {"value": "john.doe", "regex": False}
        }]
        result = qb.build_column_search(columns)
        pattern = result["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "john\\.doe")  # dot is escaped

    def test_column_search_regex_true_uses_raw_pattern(self):
        """When regex=True, search value is used as-is as a regex pattern."""
        qb = self._make_query_builder()
        columns = [{
            "data": "name", "searchable": True,
            "search": {"value": "^john.*doe$", "regex": True}
        }]
        result = qb.build_column_search(columns)
        pattern = result["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "^john.*doe$")  # raw pattern preserved

    def test_column_search_regex_default_is_false(self):
        """When regex key is absent, defaults to False (escape)."""
        qb = self._make_query_builder()
        columns = [{
            "data": "name", "searchable": True,
            "search": {"value": "a+b"}  # no regex key
        }]
        result = qb.build_column_search(columns)
        pattern = result["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "a\\+b")  # + is escaped

    def test_column_search_number_field_unaffected_by_regex_flag(self):
        """Number fields use exact match regardless of regex flag."""
        qb = self._make_query_builder({"salary": "number"})
        columns = [{
            "data": "salary", "searchable": True,
            "search": {"value": "50000", "regex": True}
        }]
        result = qb.build_column_search(columns)
        self.assertEqual(result["$and"][0]["salary"], 50000)

    # --- Global search: search_regex=False (default) should escape ---

    def test_global_search_regex_false_escapes_terms(self):
        """When search_regex=False, terms are escaped before regex matching."""
        qb = self._make_query_builder()
        result = qb.build_global_search(
            ["john.doe"], ["name", "email"],
            original_search="john.doe", search_regex=False
        )
        self.assertIn("$or", result)
        patterns = [cond[col]["$regex"] for cond in result["$or"] for col in cond]
        self.assertTrue(all(p == "john\\.doe" for p in patterns))

    def test_global_search_regex_true_uses_raw_pattern(self):
        """When search_regex=True, terms are used as raw regex patterns."""
        qb = self._make_query_builder()
        result = qb.build_global_search(
            ["^john"], ["name", "email"],
            original_search="^john", search_regex=True
        )
        self.assertIn("$or", result)
        patterns = [cond[col]["$regex"] for cond in result["$or"] for col in cond]
        self.assertTrue(all(p == "^john" for p in patterns))

    def test_global_search_regex_default_is_false(self):
        """search_regex defaults to False (backward compatible)."""
        qb = self._make_query_builder()
        result = qb.build_global_search(["a+b"], ["name"])
        pattern = result["$or"][0]["name"]["$regex"]
        self.assertEqual(pattern, "a\\+b")

    # --- Integration: DataTables reads search[regex] from request_args ---

    def test_datatables_passes_search_regex_flag(self):
        """DataTables passes search[regex]=True to build_global_search."""
        self.request_args["search"]["value"] = "^john"
        self.request_args["search"]["regex"] = True
        with patch.object(DataTables, 'has_text_index', return_value=False):
            dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=False)
            result = dt.global_search_condition
        self.assertIn("$or", result)
        patterns = [list(cond.values())[0].get("$regex") for cond in result["$or"] if isinstance(list(cond.values())[0], dict)]
        self.assertTrue(any(p == "^john" for p in patterns if p))

    def test_datatables_search_regex_false_escapes(self):
        """DataTables with search[regex]=False escapes special chars."""
        self.request_args["search"]["value"] = "john.doe"
        self.request_args["search"]["regex"] = False
        with patch.object(DataTables, 'has_text_index', return_value=False):
            dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=False)
            result = dt.global_search_condition
        self.assertIn("$or", result)
        patterns = [list(cond.values())[0].get("$regex") for cond in result["$or"] if isinstance(list(cond.values())[0], dict)]
        self.assertTrue(any(p == "john\\.doe" for p in patterns if p))

    def test_datatables_column_regex_flag_integration(self):
        """DataTables column search with regex=True passes raw pattern to MongoDB."""
        self.request_args["columns"][0]["search"]["value"] = "^J.*n$"
        self.request_args["columns"][0]["search"]["regex"] = True
        dt = DataTables(self.mongo, 'users', self.request_args)
        result = dt.column_search_conditions
        self.assertIn("$and", result)
        name_cond = next((c["name"] for c in result["$and"] if "name" in c), None)
        self.assertIsNotNone(name_cond)
        self.assertEqual(name_cond["$regex"], "^J.*n$")
