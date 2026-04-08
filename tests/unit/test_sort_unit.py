"""Test consolidated sort: multi-column sort, ColReorder, orderData, orderable coercion."""
from unittest.mock import MagicMock
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestMultiColumnSort(BaseDataTablesTest):
    def _make_dt(self):
        return DataTables(self.mongo, 'users', self.request_args)

    def test_single_column_sort_asc(self):
        self.request_args["order"] = [{"column": "0", "dir": "asc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["name"], 1)
        self.assertIn("_id", spec)

    def test_single_column_sort_desc(self):
        self.request_args["order"] = [{"column": "1", "dir": "desc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["email"], -1)

    def test_multi_column_sort_two_columns(self):
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["name"], 1)
        self.assertEqual(spec["email"], -1)
        keys = list(spec.keys())
        self.assertLess(keys.index("name"), keys.index("email"))

    def test_multi_column_sort_three_columns(self):
        self.request_args["order"] = [
            {"column": "2", "dir": "asc"},
            {"column": "0", "dir": "desc"},
            {"column": "1", "dir": "asc"},
        ]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["status"], 1)
        self.assertEqual(spec["name"], -1)
        self.assertEqual(spec["email"], 1)

    def test_non_orderable_column_skipped(self):
        self.request_args["columns"][0]["orderable"] = "false"
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertNotIn("name", spec)
        self.assertEqual(spec["email"], -1)

    def test_duplicate_field_first_wins(self):
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "0", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["name"], 1)

    def test_empty_order_falls_back_to_id(self):
        self.request_args["order"] = []
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec, {"_id": 1})

    def test_out_of_range_column_index_skipped(self):
        self.request_args["order"] = [{"column": "99", "dir": "asc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec, {"_id": 1})

    def test_id_not_duplicated_when_sorting_by_id_column(self):
        self.request_args["columns"].append(
            {"data": "_id", "name": "", "searchable": "true", "orderable": "true",
             "search": {"value": "", "regex": "false"}}
        )
        self.request_args["order"] = [{"column": "3", "dir": "desc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["_id"], -1)
        self.assertEqual(list(spec.keys()).count("_id"), 1)

    def test_sort_specification_property_alias(self):
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        self.assertEqual(dt.sort_specification, dt.sort_specification)


class TestSorting(BaseDataTablesTest):
    def test_orderable_columns(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        orderable_columns = [col['data'] for col in datatables.columns if col.get('orderable', True)]
        self.assertEqual(set(orderable_columns), set(["name", "email", "status"]))


class TestColReorderNameBasedSort(BaseDataTablesTest):
    def _make_dt(self, order, columns=None):
        if columns is None:
            columns = [
                {"data": "name", "name": "name", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                {"data": "email", "name": "email", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                {"data": "status", "name": "status", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
            ]
        args = dict(self.request_args)
        args["columns"] = columns
        args["order"] = order
        return DataTables(self.mongo, "test", args)

    def test_name_based_sort_asc(self):
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["email"], 1)

    def test_name_based_sort_desc(self):
        dt = self._make_dt([{"column": "99", "name": "status", "dir": "desc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["status"], -1)

    def test_name_overrides_invalid_index(self):
        dt = self._make_dt([{"column": "50", "name": "name", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertIn("name", spec)
        self.assertEqual(spec["name"], 1)

    def test_name_match_by_data_field(self):
        columns = [
            {"data": "name", "name": "", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
            {"data": "email", "name": "", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
        ]
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "desc"}], columns=columns)
        spec = dt.sort_specification
        self.assertEqual(spec["email"], -1)

    def test_index_fallback_when_no_name(self):
        dt = self._make_dt([{"column": "0", "dir": "desc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["name"], -1)

    def test_index_fallback_when_name_empty(self):
        dt = self._make_dt([{"column": "1", "name": "", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["email"], 1)

    def test_multi_column_name_based(self):
        dt = self._make_dt([
            {"column": "99", "name": "status", "dir": "asc"},
            {"column": "99", "name": "name", "dir": "desc"},
        ])
        spec = dt.sort_specification
        self.assertEqual(spec["status"], 1)
        self.assertEqual(spec["name"], -1)

    def test_name_not_found_falls_back_to_index(self):
        dt = self._make_dt([{"column": "0", "name": "nonexistent", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertIn("name", spec)

    def test_name_non_orderable_skipped(self):
        columns = [
            {"data": "name", "name": "name", "searchable": "true", "orderable": "false", "search": {"value": "", "regex": "false"}},
        ]
        dt = self._make_dt([{"column": "99", "name": "name", "dir": "asc"}], columns=columns)
        spec = dt.sort_specification
        self.assertNotIn("name", spec)
        self.assertEqual(spec, {"_id": 1})

    def test_id_tiebreaker_always_appended(self):
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertIn("_id", spec)


class TestColReorderColumnSearch(BaseDataTablesTest):
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
        columns = [self._col("name_alias", name="name", search_value="Alice")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Alice", "$options": "i"}})

    def test_data_used_as_fallback_when_name_empty(self):
        columns = [self._col("name", name="", search_value="Bob")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Bob", "$options": "i"}})

    def test_data_used_as_fallback_when_name_absent(self):
        col = {"data": "name", "searchable": "true", "orderable": "true", "search": {"value": "Carol", "regex": "false"}}
        dt = self._make_dt([col])
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Carol", "$options": "i"}})

    def test_name_takes_priority_over_data(self):
        columns = [self._col("email_display", name="email", search_value="test@")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0]["email"]["$regex"], "test@")

    def test_no_search_value_returns_empty(self):
        columns = [self._col("name", name="name", search_value="")]
        dt = self._make_dt(columns)
        self.assertEqual(dt.column_search_conditions, {})

    def test_not_searchable_returns_empty(self):
        columns = [self._col("name", name="name", search_value="Alice", searchable=False)]
        dt = self._make_dt(columns)
        self.assertEqual(dt.column_search_conditions, {})


class TestRegexFlagStringCoercion(BaseDataTablesTest):
    def _make_dt(self, search_value, regex_flag):
        args = dict(self.request_args)
        args["columns"] = [{
            "data": "name", "name": "", "searchable": "true", "orderable": "true",
            "search": {"value": search_value, "regex": regex_flag},
        }]
        args["order"] = [{"column": "0", "dir": "asc"}]
        return DataTables(self.mongo, "test", args)

    def test_string_false_applies_re_escape(self):
        dt = self._make_dt("test.value", "false")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"test\.value")

    def test_string_true_uses_raw_pattern(self):
        dt = self._make_dt("test.value", "true")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "test.value")

    def test_bool_false_applies_re_escape(self):
        dt = self._make_dt("a+b", False)
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"a\+b")

    def test_bool_true_uses_raw_pattern(self):
        dt = self._make_dt("a+b", True)
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "a+b")

    def test_string_False_capital_applies_re_escape(self):
        dt = self._make_dt("x.y", "False")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"x\.y")


class TestOrderData(BaseDataTablesTest):
    def _make_columns(self, order_data_map=None):
        cols = [
            {"data": "name", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "last_name", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "display_name", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "score", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
        if order_data_map:
            for idx, val in order_data_map.items():
                cols[idx]["orderData"] = val
        return cols

    def _dt(self, columns, order):
        args = {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": "", "regex": False},
            "columns": columns,
            "order": order,
        }
        return DataTables(self.mongo.db, "test", args, [])

    def test_single_orderdata_int(self):
        cols = self._make_columns({2: 1})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "last_name" in spec
        assert "display_name" not in spec
        assert spec["last_name"] == 1

    def test_single_orderdata_list(self):
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "desc", "name": ""}])
        spec = dt.sort_specification
        assert "name" in spec
        assert "last_name" in spec
        assert "display_name" not in spec
        assert spec["name"] == -1
        assert spec["last_name"] == -1

    def test_no_orderdata_unchanged(self):
        cols = self._make_columns()
        dt = self._dt(cols, [{"column": 3, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "score" in spec

    def test_orderdata_out_of_range_skipped(self):
        cols = self._make_columns({0: 99})
        dt = self._dt(cols, [{"column": 0, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert spec == {"_id": 1}

    def test_orderdata_non_orderable_target_skipped(self):
        cols = self._make_columns({2: 1})
        cols[1]["orderable"] = False
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "last_name" not in spec
        assert spec == {"_id": 1}

    def test_orderdata_mixed_valid_invalid(self):
        cols = self._make_columns({2: [0, 99]})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "name" in spec
        assert spec["name"] == 1

    def test_orderdata_dedup_across_order_entries(self):
        cols = self._make_columns({2: [0]})
        dt = self._dt(cols, [
            {"column": 2, "dir": "asc", "name": ""},
            {"column": 0, "dir": "desc", "name": ""},
        ])
        spec = dt.sort_specification
        assert spec["name"] == 1  # first occurrence (asc) wins

    def test_orderdata_id_tiebreaker_always_present(self):
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "_id" in spec


class TestOrderableCoercion(BaseDataTablesTest):
    def _make_dt(self, orderable_val, include_orderable=True):
        col = MagicMock()
        col.list_indexes.return_value = []
        col.aggregate.return_value = iter([])
        col.count_documents.return_value = 0
        col.estimated_document_count.return_value = 0
        col_def = {"data": "name", "name": "", "searchable": "true",
                   "search": {"value": "", "regex": "false"}}
        if include_orderable:
            col_def["orderable"] = orderable_val
        args = {"draw": "1", "start": "0", "length": "10",
                "search": {"value": "", "regex": "false"},
                "order": [{"column": "0", "dir": "asc"}], "columns": [col_def]}
        return DataTables(col, "test", args)

    def test_orderable_string_false_excluded(self):
        self.assertNotIn("name", self._make_dt("false").sort_specification)

    def test_orderable_bool_false_excluded(self):
        self.assertNotIn("name", self._make_dt(False).sort_specification)

    def test_orderable_string_true_included(self):
        self.assertIn("name", self._make_dt("true").sort_specification)

    def test_orderable_bool_true_included(self):
        self.assertIn("name", self._make_dt(True).sort_specification)

    def test_orderable_absent_defaults_to_sortable(self):
        self.assertIn("name", self._make_dt(None, include_orderable=False).sort_specification)
