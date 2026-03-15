"""Tests for columns[i][name] support in column search (ColReorder compatibility)
and regex flag string coercion in build_column_search."""
import unittest
from unittest.mock import MagicMock, patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestColReorderColumnSearch(BaseDataTablesTest):
    """columns[i][name] resolution in build_column_search."""

    def _make_dt(self, columns):
        args = dict(self.request_args)
        args["columns"] = columns
        args["order"] = [{"column": "0", "dir": "asc"}]
        return DataTables(self.mongo, "test", args)

    def _col(self, data, name="", search_value="", regex="false", searchable="true"):
        return {
            "data": data, "name": name, "searchable": searchable,
            "orderable": "true", "search": {"value": search_value, "regex": regex},
        }

    def test_name_used_for_field_lookup_when_set(self):
        """When columns[i][name] is set, it is used for field/type lookup."""
        columns = [self._col("name_alias", name="name", search_value="Alice")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        # field_mapper resolves "name" → db field "name"; regex-escaped "Alice"
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Alice", "$options": "i"}})

    def test_data_used_as_fallback_when_name_empty(self):
        """When columns[i][name] is empty, falls back to columns[i][data]."""
        columns = [self._col("name", name="", search_value="Bob")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Bob", "$options": "i"}})

    def test_data_used_as_fallback_when_name_absent(self):
        """When columns[i][name] key is absent, falls back to columns[i][data]."""
        col = {
            "data": "name", "searchable": "true", "orderable": "true",
            "search": {"value": "Carol", "regex": "false"},
        }
        dt = self._make_dt([col])
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Carol", "$options": "i"}})

    def test_name_takes_priority_over_data(self):
        """columns[i][name] takes priority over columns[i][data] when both set."""
        # "email" is a known field; "email_display" is the data alias
        columns = [self._col("email_display", name="email", search_value="test@")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        # Should resolve via "email" (name), not "email_display" (data)
        self.assertEqual(cond["$and"][0]["email"]["$regex"], "test@")

    def test_no_search_value_returns_empty(self):
        """Column with empty search value produces no condition."""
        columns = [self._col("name", name="name", search_value="")]
        dt = self._make_dt(columns)
        self.assertEqual(dt.column_search_conditions, {})

    def test_not_searchable_returns_empty(self):
        """Non-searchable column (bool False) is skipped even with name set."""
        columns = [self._col("name", name="name", search_value="Alice", searchable=False)]
        dt = self._make_dt(columns)
        self.assertEqual(dt.column_search_conditions, {})


class TestRegexFlagStringCoercion(BaseDataTablesTest):
    """regex flag sent as string 'false'/'true' by DataTables is coerced correctly."""

    def _make_dt(self, search_value, regex_flag):
        args = dict(self.request_args)
        args["columns"] = [{
            "data": "name", "name": "", "searchable": "true", "orderable": "true",
            "search": {"value": search_value, "regex": regex_flag},
        }]
        args["order"] = [{"column": "0", "dir": "asc"}]
        return DataTables(self.mongo, "test", args)

    def test_string_false_applies_re_escape(self):
        """regex='false' (string) must apply re.escape — special chars are escaped."""
        dt = self._make_dt("test.value", "false")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"test\.value")

    def test_string_true_uses_raw_pattern(self):
        """regex='true' (string) must use raw pattern without escaping."""
        dt = self._make_dt("test.value", "true")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "test.value")

    def test_bool_false_applies_re_escape(self):
        """regex=False (bool) must apply re.escape."""
        dt = self._make_dt("a+b", False)
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"a\+b")

    def test_bool_true_uses_raw_pattern(self):
        """regex=True (bool) must use raw pattern."""
        dt = self._make_dt("a+b", True)
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "a+b")

    def test_string_False_capital_applies_re_escape(self):
        """regex='False' (capital F string) is treated as falsy — re.escape applied."""
        dt = self._make_dt("x.y", "False")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"x\.y")


if __name__ == "__main__":
    unittest.main()
