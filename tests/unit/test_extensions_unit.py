"""Test consolidated extensions: Buttons, FixedColumns, Responsive, RowGroup, Select."""
from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


class TestButtonsExtension(BaseDataTablesTest):
    """Tests for the Buttons extension config parsing and response inclusion."""

    def setUp(self) -> None:
        """Set up mock collection with count and aggregate return values."""
        super().setUp()
        self.collection.count_documents.return_value = 100
        self.collection.aggregate.return_value = [
            {"_id": "1", "name": "Test 1", "value": 10},
            {"_id": "2", "name": "Test 2", "value": 20}
        ]
        self.data_fields = [DataField("name", "string"), DataField("value", "number")]

    def test_buttons_config_parsing(self) -> None:
        """Validates that buttons config with export and colvis keys is parsed correctly."""
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": True}, {"data": "value", "searchable": True}],
            "buttons": {"export": {"csv": True, "excel": True}, "colvis": {"enabled": True}}
        }
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        buttons_config = dt._parse_extension_config("buttons")
        assert buttons_config is not None
        assert "export" in buttons_config
        assert "colvis" in buttons_config

    def test_buttons_config_in_response(self) -> None:
        """Validates that buttons config is included in the get_rows response."""
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": True}, {"data": "value", "searchable": True}],
            "buttons": {"export": {"csv": True}}
        }
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        response = dt.get_rows()
        assert "buttons" in response
        assert response["buttons"]["export"]["csv"] is True

    def test_get_export_data(self) -> None:
        """Validates that get_export_data returns a list."""
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": True}, {"data": "value", "searchable": True}]
        }
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        self.collection.aggregate.reset_mock()
        assert isinstance(dt.get_export_data(), list)

    def test_no_buttons_config(self) -> None:
        """Validates that buttons key is absent from response when not configured."""
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": True}, {"data": "value", "searchable": True}]
        }
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        assert dt._parse_extension_config("buttons") is None
        assert "buttons" not in dt.get_rows()

    def test_export_data_with_filters(self) -> None:
        """Validates that get_export_data returns a list when a search filter is active."""
        request_args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "Test"},
            "columns": [{"data": "name", "searchable": True}, {"data": "value", "searchable": True}]
        }
        dt = DataTables(self.mongo, "test_collection", request_args, self.data_fields)
        self.collection.aggregate.reset_mock()
        assert isinstance(dt.get_export_data(), list)


class TestFixedColumns(BaseDataTablesTest):
    """Tests for the FixedColumns extension config in responses."""

    def _dt(self, request_args, data_fields=None) -> DataTables:
        """Build a DataTables instance with zeroed-out mock collection."""
        self.collection.list_indexes.return_value = []
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        return DataTables(self.mongo, "test_collection", request_args, data_fields or [])

    def test_fixed_columns_left_only(self) -> None:
        """Validates that only left fixedColumns key appears when only left is configured."""
        response = self._dt(
            {"draw": 1, "start": 0, "length": 10,
             "columns": [{"data": "name", "searchable": True}, {"data": "age", "searchable": True}],
             "fixedColumns": {"left": 2}},
            [DataField("name", "string"), DataField("age", "number")]
        ).get_rows()
        assert response["fixedColumns"]["left"] == 2
        assert "right" not in response["fixedColumns"]

    def test_fixed_columns_right_only(self) -> None:
        """Validates that only right fixedColumns key appears when only right is configured."""
        response = self._dt(
            {"draw": 1, "start": 0, "length": 10,
             "columns": [{"data": "name", "searchable": True}, {"data": "age", "searchable": True}],
             "fixedColumns": {"right": 1}},
            [DataField("name", "string"), DataField("age", "number")]
        ).get_rows()
        assert response["fixedColumns"]["right"] == 1
        assert "left" not in response["fixedColumns"]

    def test_fixed_columns_both_sides(self) -> None:
        """Validates that both left and right fixedColumns keys appear when both are configured."""
        response = self._dt(
            {"draw": 1, "start": 0, "length": 10,
             "columns": [{"data": "name", "searchable": True}, {"data": "age", "searchable": True}],
             "fixedColumns": {"left": 1, "right": 1}},
            [DataField("name", "string"), DataField("age", "number")]
        ).get_rows()
        assert response["fixedColumns"]["left"] == 1
        assert response["fixedColumns"]["right"] == 1

    def test_no_fixed_columns(self) -> None:
        """Validates that fixedColumns key is absent from response when not configured."""
        response = self._dt(
            {"draw": 1, "start": 0, "length": 10,
             "columns": [{"data": "name", "searchable": True}]},
            [DataField("name", "string")]
        ).get_rows()
        assert "fixedColumns" not in response

    def test_invalid_fixed_columns_values(self) -> None:
        """Validates that invalid fixedColumns values are passed through as-is."""
        response = self._dt(
            {"draw": 1, "start": 0, "length": 10,
             "columns": [{"data": "name", "searchable": True}],
             "fixedColumns": {"left": "invalid", "right": None}},
            [DataField("name", "string")]
        ).get_rows()
        assert response["fixedColumns"]["left"] == "invalid"
        assert response["fixedColumns"]["right"] is None


class TestResponsive(BaseDataTablesTest):
    """Tests for the Responsive extension config parsing and response inclusion."""

    def test_responsive_config_parsing(self) -> None:
        """Validates that a full responsive config with breakpoints and display is included in response."""
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        request_args = {"draw": "1", "start": "0", "length": "10",
            "responsive": {"breakpoints": {"xs": 0, "sm": 576}, "display": {"childRow": True}, "priorities": {"0": 1}}}
        response = DataTables(self.mongo, 'users', request_args).get_rows()
        assert "responsive" in response
        assert response["responsive"]["breakpoints"]["sm"] == 576
        assert response["responsive"]["display"]["childRow"] is True

    def test_responsive_config_partial(self) -> None:
        """Validates that a partial responsive config with only breakpoints omits display key."""
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        request_args = {"draw": "1", "start": "0", "length": "10",
            "responsive": {"breakpoints": {"sm": 576, "lg": 992}}}
        response = DataTables(self.mongo, 'users', request_args).get_rows()
        assert "responsive" in response
        assert "display" not in response["responsive"]

    def test_no_responsive_config(self) -> None:
        """Validates that responsive key is absent from response when not configured."""
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        response = DataTables(self.mongo, 'users', {"draw": "1", "start": "0", "length": "10"}).get_rows()
        assert "responsive" not in response

    def test_empty_responsive_config(self) -> None:
        """Validates that an empty responsive dict results in no responsive key in response."""
        self.collection.count_documents.return_value = 3
        self.collection.aggregate.return_value = self.sample_docs
        response = DataTables(self.mongo, 'users', {"draw": "1", "start": "0", "length": "10", "responsive": {}}).get_rows()
        assert "responsive" not in response


class TestRowGroupExtension(BaseDataTablesTest):
    """Tests for the RowGroup extension config parsing, data generation, and response inclusion."""

    def test_rowgroup_config_parsing(self) -> None:
        """Validates that rowGroup config returns only dataSrc, stripping render callbacks."""
        request_args = {"draw": "1", "start": "0", "length": "10",
            "rowGroup": {"dataSrc": "category", "startRender": True, "endRender": True},
            "columns": [{"data": "category", "searchable": "true"}, {"data": "value", "searchable": "true"}]}
        dt = DataTables(self.mongo, "test_collection", request_args,
            data_fields=[DataField("category", "string"), DataField("value", "number")])
        config = dt._parse_extension_config("rowGroup")
        assert config["dataSrc"] == "category"
        assert "startRender" not in config
        assert "endRender" not in config

    def test_rowgroup_with_numeric_datasrc(self) -> None:
        """Validates that a numeric dataSrc column index is preserved in parsed config."""
        request_args = {"draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": "true"}, {"data": "category", "searchable": "true"}],
            "rowGroup": {"dataSrc": 1}}
        dt = DataTables(self.mongo, "test_collection", request_args,
            data_fields=[DataField("name", "string"), DataField("category", "string")])
        assert dt._parse_extension_config("rowGroup")["dataSrc"] == 1

    def test_rowgroup_data_generation(self) -> None:
        """Validates that get_rowgroup_data returns correct dataSrc and group counts."""
        from mongo_datatables.datatables.results import get_rowgroup_data
        self.collection.aggregate.return_value = [{"_id": "A", "count": 2}, {"_id": "B", "count": 2}]
        request_args = {"draw": "1", "start": "0", "length": "10",
            "rowGroup": {"dataSrc": "category"},
            "columns": [{"data": "category", "searchable": "true"}]}
        dt = DataTables(self.mongo, "test_collection", request_args,
            data_fields=[DataField("category", "string"), DataField("value", "number")])
        rowgroup_data = get_rowgroup_data(self.collection, dt.columns, dt.field_mapper, dt.filter, dt.request_args, dt.allow_disk_use)
        assert rowgroup_data["dataSrc"] == "category"
        assert rowgroup_data["groups"]["A"]["count"] == 2
        assert rowgroup_data["groups"]["B"]["count"] == 2

    def test_rowgroup_in_response(self) -> None:
        """Validates that rowGroup with dataSrc and groups appears in get_rows response."""
        self.collection.count_documents.return_value = 2
        self.collection.aggregate.side_effect = [
            [{"_id": "X", "count": 1}, {"_id": "Y", "count": 1}], []
        ]
        request_args = {"draw": "1", "start": "0", "length": "10",
            "rowGroup": {"dataSrc": "category"},
            "columns": [{"data": "category", "searchable": "true"}]}
        response = DataTables(self.mongo, "test_collection", request_args,
            data_fields=[DataField("category", "string"), DataField("value", "number")]).get_rows()
        assert response["rowGroup"]["dataSrc"] == "category"
        assert "groups" in response["rowGroup"]

    def test_no_rowgroup_config(self) -> None:
        """Validates that rowGroup is absent from response and helpers return None when not configured."""
        from mongo_datatables.datatables.results import get_rowgroup_data
        self.collection.count_documents.return_value = 0
        self.collection.aggregate.return_value = []
        request_args = {"draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": "true"}]}
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields=[DataField("name", "string")])
        assert dt._parse_extension_config("rowGroup") is None
        assert get_rowgroup_data(self.collection, dt.columns, dt.field_mapper, dt.filter, dt.request_args, dt.allow_disk_use) is None
        assert "rowGroup" not in dt.get_rows()

    def test_rowgroup_no_numeric_summaries(self) -> None:
        """Validates that group data does not include _sum or _avg keys."""
        from mongo_datatables.datatables.results import get_rowgroup_data
        self.collection.aggregate.return_value = [{"_id": "A", "count": 3}, {"_id": "B", "count": 1}]
        request_args = {"draw": "1", "start": "0", "length": "10",
            "rowGroup": {"dataSrc": "category"},
            "columns": [{"data": "category", "searchable": "true"}]}
        dt = DataTables(self.mongo, "test_collection", request_args,
            data_fields=[DataField("category", "string"), DataField("value", "number")])
        result = get_rowgroup_data(self.collection, dt.columns, dt.field_mapper, dt.filter, dt.request_args, dt.allow_disk_use)
        for group_values in result["groups"].values():
            assert not any(k.endswith("_sum") or k.endswith("_avg") for k in group_values)

    def test_rowgroup_config_no_datasrc_returns_none(self) -> None:
        """Validates that rowGroup config without dataSrc returns None from _parse_extension_config."""
        request_args = {"draw": "1", "start": "0", "length": "10",
            "rowGroup": {"startRender": True},
            "columns": [{"data": "name", "searchable": "true"}]}
        assert DataTables(self.mongo, "test_collection", request_args)._parse_extension_config("rowGroup") is None


class TestSelect(BaseDataTablesTest):
    """Tests for the Select extension config in responses."""

    def test_select_not_requested(self) -> None:
        """Validates that select key is absent from response when not configured."""
        request_args = {"draw": "1", "start": "0", "length": "10",
            "columns": [{"data": "name", "searchable": "true"}]}
        dt = DataTables(self.mongo, "employees", request_args, data_fields=[DataField("name", "string")])
        assert "select" not in dt.get_rows()

    def test_select_boolean_true(self) -> None:
        """Validates that select: True in request results in an empty select dict in response."""
        request_args = {"draw": "1", "start": "0", "length": "10",
            "select": True,
            "columns": [{"data": "name", "searchable": "true"}]}
        dt = DataTables(self.mongo, "employees", request_args, data_fields=[DataField("name", "string")])
        response = dt.get_rows()
        assert "select" in response
        assert response["select"] == {}

    def test_select_style_configurations(self) -> None:
        """Validates that each select style value is preserved in the response."""
        for style in ["single", "multi", "multi+shift", "os"]:
            request_args = {"draw": "1", "start": "0", "length": "10",
                "select": {"style": style},
                "columns": [{"data": "name", "searchable": "true"}]}
            dt = DataTables(self.mongo, "employees", request_args, data_fields=[DataField("name", "string")])
            response = dt.get_rows()
            assert response["select"]["style"] == style
