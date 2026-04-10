"""Test DataTables sorting: multi-column, ColReorder, orderData, orderable coercion."""
from tests.unit.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestMultiColumnSort(BaseDataTablesTest):
    """Verify multi-column sort specification respects order array and orderable flag."""

    def _make_dt(self):
        """Create a DataTables instance with current request_args."""
        return DataTables(self.mongo, 'users', self.request_args)

    def test_single_column_sort_asc(self):
        """Ascending sort on single column includes _id tiebreaker."""
        self.request_args["order"] = [{"column": "0", "dir": "asc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["name"], 1)
        self.assertIn("_id", spec)

    def test_single_column_sort_desc(self):
        """Descending sort on single column sets direction to -1."""
        self.request_args["order"] = [{"column": "1", "dir": "desc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["email"], -1)

    def _two_column_sort_spec(self):
        """Return sort specification for two-column order (name asc, email desc)."""
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        return self._make_dt().sort_specification

    def test_multi_column_sort_name_direction(self):
        """First sort column (name) has ascending direction."""
        self.assertEqual(self._two_column_sort_spec()["name"], 1)

    def test_multi_column_sort_email_direction(self):
        """Second sort column (email) has descending direction."""
        self.assertEqual(self._two_column_sort_spec()["email"], -1)

    def test_multi_column_sort_key_order(self):
        """Sort keys appear in order array sequence."""
        keys = list(self._two_column_sort_spec().keys())
        self.assertLess(keys.index("name"), keys.index("email"))

    def test_multi_column_sort_three_columns_status(self):
        """Three-column sort: status (first, asc) has direction 1."""
        self.request_args["order"] = [
            {"column": "2", "dir": "asc"},
            {"column": "0", "dir": "desc"},
            {"column": "1", "dir": "asc"},
        ]
        self.assertEqual(self._make_dt().sort_specification["status"], 1)

    def test_multi_column_sort_three_columns_name(self):
        """Three-column sort: name (second, desc) has direction -1."""
        self.request_args["order"] = [
            {"column": "2", "dir": "asc"},
            {"column": "0", "dir": "desc"},
            {"column": "1", "dir": "asc"},
        ]
        self.assertEqual(self._make_dt().sort_specification["name"], -1)

    def test_multi_column_sort_three_columns_email(self):
        """Three-column sort: email (third, asc) has direction 1."""
        self.request_args["order"] = [
            {"column": "2", "dir": "asc"},
            {"column": "0", "dir": "desc"},
            {"column": "1", "dir": "asc"},
        ]
        self.assertEqual(self._make_dt().sort_specification["email"], 1)

    def test_non_orderable_column_excluded_from_sort(self):
        """Non-orderable column is excluded from sort specification."""
        self.request_args["columns"][0]["orderable"] = "false"
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        self.assertNotIn("name", self._make_dt().sort_specification)

    def test_orderable_column_included_when_peer_is_non_orderable(self):
        """Orderable column is included even when another column is non-orderable."""
        self.request_args["columns"][0]["orderable"] = "false"
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        self.assertEqual(self._make_dt().sort_specification["email"], -1)

    def test_duplicate_field_first_wins(self):
        """Duplicate field in order array uses first occurrence direction."""
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "0", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec["name"], 1)

    def test_empty_order_falls_back_to_id(self):
        """Empty order array defaults to _id ascending."""
        self.request_args["order"] = []
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec, {"_id": 1})

    def test_out_of_range_column_index_skipped(self):
        """Out-of-range column index is skipped, falls back to _id."""
        self.request_args["order"] = [{"column": "99", "dir": "asc"}]
        dt = self._make_dt()
        spec = dt.sort_specification
        self.assertEqual(spec, {"_id": 1})

    def test_id_not_duplicated_when_sorting_by_id_column(self):
        """_id field appears once when explicitly sorted, not duplicated."""
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
        """sort_specification property returns consistent results on repeated access."""
        self.request_args["order"] = [
            {"column": "0", "dir": "asc"},
            {"column": "1", "dir": "desc"},
        ]
        dt = self._make_dt()
        spec1 = dt.sort_specification
        spec2 = dt.sort_specification
        self.assertEqual(spec1, spec2)


class TestSorting(BaseDataTablesTest):
    """Verify orderable columns are correctly identified."""

    def test_orderable_columns(self):
        """Orderable columns are extracted from column definitions."""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        orderable_columns = [col['data'] for col in datatables.columns if col.get('orderable', True)]
        self.assertEqual(set(orderable_columns), set(["name", "email", "status"]))


class TestColReorderNameBasedSort(BaseDataTablesTest):
    """Support ColReorder name-based sort via `order[i][name]` field lookup."""

    def _make_dt(self, order, columns=None):
        """Create DataTables instance with custom order and columns.
        
        order: list of order specifications.
        columns: column definitions; defaults to name, email, status.
        """
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
        """Name-based sort with invalid column index uses name field."""
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["email"], 1)

    def test_name_based_sort_desc(self):
        """Name-based sort descending resolves to correct field."""
        dt = self._make_dt([{"column": "99", "name": "status", "dir": "desc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["status"], -1)

    def test_name_overrides_invalid_index(self):
        """Name field overrides out-of-range column index."""
        dt = self._make_dt([{"column": "50", "name": "name", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertIn("name", spec)
        self.assertEqual(spec["name"], 1)

    def test_name_match_by_data_field(self):
        """Name lookup matches against column data field when name is empty."""
        columns = [
            {"data": "name", "name": "", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
            {"data": "email", "name": "", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
        ]
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "desc"}], columns=columns)
        spec = dt.sort_specification
        self.assertEqual(spec["email"], -1)

    def test_index_fallback_when_no_name(self):
        """Column index is used when name field is absent."""
        dt = self._make_dt([{"column": "0", "dir": "desc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["name"], -1)

    def test_index_fallback_when_name_empty(self):
        """Column index is used when name field is empty string."""
        dt = self._make_dt([{"column": "1", "name": "", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertEqual(spec["email"], 1)

    def test_multi_column_name_based_status_direction(self):
        """Multi-column name-based sort: status (first) has correct direction."""
        dt = self._make_dt([
            {"column": "99", "name": "status", "dir": "asc"},
            {"column": "99", "name": "name", "dir": "desc"},
        ])
        self.assertEqual(dt.sort_specification["status"], 1)

    def test_multi_column_name_based_name_direction(self):
        """Multi-column name-based sort: name (second) has correct direction."""
        dt = self._make_dt([
            {"column": "99", "name": "status", "dir": "asc"},
            {"column": "99", "name": "name", "dir": "desc"},
        ])
        self.assertEqual(dt.sort_specification["name"], -1)

    def test_name_not_found_falls_back_to_index(self):
        """Nonexistent name falls back to column index."""
        dt = self._make_dt([{"column": "0", "name": "nonexistent", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertIn("name", spec)

    def test_name_non_orderable_skipped(self):
        """Non-orderable column resolved by name is skipped."""
        columns = [
            {"data": "name", "name": "name", "searchable": "true", "orderable": "false", "search": {"value": "", "regex": "false"}},
        ]
        dt = self._make_dt([{"column": "99", "name": "name", "dir": "asc"}], columns=columns)
        spec = dt.sort_specification
        self.assertNotIn("name", spec)
        self.assertEqual(spec, {"_id": 1})

    def test_id_tiebreaker_always_appended(self):
        """_id tiebreaker is always included in sort specification."""
        dt = self._make_dt([{"column": "99", "name": "email", "dir": "asc"}])
        spec = dt.sort_specification
        self.assertIn("_id", spec)


class TestColReorderColumnSearch(BaseDataTablesTest):
    """Resolve column search fields via `name` (priority) or `data` (fallback)."""

    def _make_dt(self, columns):
        """Create DataTables instance with custom columns."""
        args = dict(self.request_args)
        args["columns"] = columns
        args["order"] = [{"column": "0", "dir": "asc"}]
        return DataTables(self.mongo, "test", args)

    def _col(self, data, name="", search_value="", regex="false", searchable="true"):
        """Build a column definition dict."""
        return {
            "data": data, "name": name, "searchable": searchable,
            "orderable": "true", "search": {"value": search_value, "regex": regex},
        }

    def test_name_used_for_field_lookup_when_set(self):
        """Column name field is used for search when set."""
        columns = [self._col("name_alias", name="name", search_value="Alice")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Alice", "$options": "i"}})

    def test_data_used_as_fallback_when_name_empty(self):
        """Column data field is used when name is empty."""
        columns = [self._col("name", name="", search_value="Bob")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Bob", "$options": "i"}})

    def test_data_used_as_fallback_when_name_absent(self):
        """Column data field is used when name key is absent."""
        col = {"data": "name", "searchable": "true", "orderable": "true", "search": {"value": "Carol", "regex": "false"}}
        dt = self._make_dt([col])
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0], {"name": {"$regex": "Carol", "$options": "i"}})

    def test_name_takes_priority_over_data(self):
        """Column name field takes priority over data field."""
        columns = [self._col("email_display", name="email", search_value="test@")]
        dt = self._make_dt(columns)
        cond = dt.column_search_conditions
        self.assertIn("$and", cond)
        self.assertEqual(cond["$and"][0]["email"]["$regex"], "test@")

    def test_no_search_value_returns_empty(self):
        """Empty search value returns empty conditions."""
        columns = [self._col("name", name="name", search_value="")]
        dt = self._make_dt(columns)
        self.assertEqual(dt.column_search_conditions, {})

    def test_not_searchable_returns_empty(self):
        """Non-searchable column returns empty conditions."""
        columns = [self._col("name", name="name", search_value="Alice", searchable=False)]
        dt = self._make_dt(columns)
        self.assertEqual(dt.column_search_conditions, {})


class TestRegexFlagStringCoercion(BaseDataTablesTest):
    """Coerce string and bool regex flags to apply `re.escape()` or raw pattern."""

    def _make_dt(self, search_value, regex_flag):
        """Create DataTables instance with custom search value and regex flag."""
        args = dict(self.request_args)
        args["columns"] = [{
            "data": "name", "name": "", "searchable": "true", "orderable": "true",
            "search": {"value": search_value, "regex": regex_flag},
        }]
        args["order"] = [{"column": "0", "dir": "asc"}]
        return DataTables(self.mongo, "test", args)

    def test_string_false_applies_re_escape(self):
        """String 'false' regex flag escapes special characters."""
        dt = self._make_dt("test.value", "false")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"test\.value")

    def test_string_true_uses_raw_pattern(self):
        """String 'true' regex flag uses pattern as-is."""
        dt = self._make_dt("test.value", "true")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "test.value")

    def test_bool_false_applies_re_escape(self):
        """Bool False regex flag escapes special characters."""
        dt = self._make_dt("a+b", False)
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"a\+b")

    def test_bool_true_uses_raw_pattern(self):
        """Bool True regex flag uses pattern as-is."""
        dt = self._make_dt("a+b", True)
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, "a+b")

    def test_string_False_capital_applies_re_escape(self):
        """String 'False' (capital) regex flag escapes special characters."""
        dt = self._make_dt("x.y", "False")
        cond = dt.column_search_conditions
        pattern = cond["$and"][0]["name"]["$regex"]
        self.assertEqual(pattern, r"x\.y")


class TestOrderData(BaseDataTablesTest):
    """Support `orderData` column redirect: sorting one column redirects to another."""

    def _make_columns(self, order_data_map=None):
        """Build column definitions with optional orderData redirects.
        
        order_data_map: dict mapping column index to orderData value (int or list).
        """
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
        """Create DataTables instance with custom columns and order."""
        args = dict(self.request_args)
        args["columns"] = columns
        args["order"] = order
        return DataTables(self.mongo, "test", args, [])

    def test_single_orderdata_int(self):
        """Single orderData int redirects sort to target column."""
        cols = self._make_columns({2: 1})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "last_name" in spec
        assert "display_name" not in spec
        assert spec["last_name"] == 1

    def test_single_orderdata_list_includes_redirected_fields(self):
        """OrderData list includes all redirected fields in sort."""
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "desc", "name": ""}])
        spec = dt.sort_specification
        assert "name" in spec
        assert "last_name" in spec

    def test_single_orderdata_list_excludes_source_field(self):
        """OrderData list excludes source column from sort."""
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "desc", "name": ""}])
        spec = dt.sort_specification
        assert "display_name" not in spec

    def test_single_orderdata_list_direction(self):
        """OrderData list applies sort direction to all redirected fields."""
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "desc", "name": ""}])
        spec = dt.sort_specification
        assert spec["name"] == -1
        assert spec["last_name"] == -1

    def test_no_orderdata_unchanged(self):
        """Column without orderData sorts normally."""
        cols = self._make_columns()
        dt = self._dt(cols, [{"column": 3, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "score" in spec

    def test_orderdata_out_of_range_skipped(self):
        """OrderData with out-of-range target is skipped."""
        cols = self._make_columns({0: 99})
        dt = self._dt(cols, [{"column": 0, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert spec == {"_id": 1}

    def test_orderdata_non_orderable_target_skipped(self):
        """OrderData target that is non-orderable is skipped."""
        cols = self._make_columns({2: 1})
        cols[1]["orderable"] = False
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "last_name" not in spec
        assert spec == {"_id": 1}

    def test_orderdata_mixed_valid_invalid(self):
        """OrderData list with mixed valid/invalid indices includes valid ones."""
        cols = self._make_columns({2: [0, 99]})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "name" in spec
        assert spec["name"] == 1

    def test_orderdata_dedup_across_order_entries(self):
        """Duplicate field across order entries uses first occurrence direction."""
        cols = self._make_columns({2: [0]})
        dt = self._dt(cols, [
            {"column": 2, "dir": "asc", "name": ""},
            {"column": 0, "dir": "desc", "name": ""},
        ])
        spec = dt.sort_specification
        assert spec["name"] == 1  # first occurrence (asc) wins

    def test_orderdata_id_tiebreaker_always_present(self):
        """_id tiebreaker is always included with orderData redirects."""
        cols = self._make_columns({2: [0, 1]})
        dt = self._dt(cols, [{"column": 2, "dir": "asc", "name": ""}])
        spec = dt.sort_specification
        assert "_id" in spec


class TestOrderableCoercion(BaseDataTablesTest):
    """Coerce string and bool `orderable` flag to include or exclude columns from sort."""

    def _make_dt(self, orderable_val, include_orderable=True):
        """Create DataTables instance with custom orderable flag.
        
        orderable_val: value to set for orderable flag.
        include_orderable: whether to include orderable key in column definition.
        """
        col_def = {"data": "name", "name": "", "searchable": "true",
                   "search": {"value": "", "regex": "false"}}
        if include_orderable:
            col_def["orderable"] = orderable_val
        args = dict(self.request_args)
        args["order"] = [{"column": "0", "dir": "asc"}]
        args["columns"] = [col_def]
        return DataTables(self.mongo, "test", args)

    def test_orderable_string_false_excluded(self):
        """String 'false' orderable flag excludes column from sort."""
        self.assertNotIn("name", self._make_dt("false").sort_specification)

    def test_orderable_bool_false_excluded(self):
        """Bool False orderable flag excludes column from sort."""
        self.assertNotIn("name", self._make_dt(False).sort_specification)

    def test_orderable_string_true_included(self):
        """String 'true' orderable flag includes column in sort."""
        self.assertIn("name", self._make_dt("true").sort_specification)

    def test_orderable_bool_true_included(self):
        """Bool True orderable flag includes column in sort."""
        self.assertIn("name", self._make_dt(True).sort_specification)

    def test_orderable_absent_defaults_to_sortable(self):
        """Missing orderable flag defaults to sortable."""
        self.assertIn("name", self._make_dt(None, include_orderable=False).sort_specification)
