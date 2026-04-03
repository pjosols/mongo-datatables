"""Tests for request_validator and editor_validator input validation."""
import pytest
from unittest.mock import MagicMock
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.request_validator import (
    validate_request_args,
    _coerce_int,
    _validate_columns,
    _validate_order,
    _validate_search_dict,
)
from mongo_datatables.editor.validator import (
    validate_editor_request_args,
    validate_doc_id,
    validate_field_name,
    validate_data_fields_whitelist,
    validate_upload_data,
)
from mongo_datatables import DataTables, DataField
from mongo_datatables.editor import Editor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_valid_request_args(**overrides):
    args = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [
            {"data": "name", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ],
    }
    args.update(overrides)
    return args


def _make_dt(request_args):
    col = MagicMock(spec=Collection)
    col.list_indexes.return_value = []
    db = MagicMock(spec=Database)
    db.__getitem__.return_value = col
    return DataTables(db, "test", request_args)


def _make_editor(request_args, doc_id=""):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    col = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = col
    return Editor(mongo, "test", request_args, doc_id=doc_id)


# ---------------------------------------------------------------------------
# validate_request_args — top-level structure
# ---------------------------------------------------------------------------

class TestValidateRequestArgs:
    def test_non_dict_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            validate_request_args("not a dict")

    def test_missing_required_key_raises(self):
        args = _make_valid_request_args()
        del args["draw"]
        with pytest.raises(InvalidDataError, match="missing required key 'draw'"):
            validate_request_args(args)

    def test_all_required_keys_present_passes(self):
        args = _make_valid_request_args()
        result = validate_request_args(args)
        assert isinstance(result, dict)

    def test_draw_coerced_to_int(self):
        args = _make_valid_request_args(draw="5")
        result = validate_request_args(args)
        assert result["draw"] == 5

    def test_draw_minimum_clamped_to_1(self):
        args = _make_valid_request_args(draw=0)
        result = validate_request_args(args)
        assert result["draw"] == 1

    def test_start_coerced_and_clamped(self):
        args = _make_valid_request_args(start="-3")
        result = validate_request_args(args)
        assert result["start"] == 0

    def test_length_coerced(self):
        args = _make_valid_request_args(length="25")
        result = validate_request_args(args)
        assert result["length"] == 25

    def test_non_integer_draw_raises(self):
        args = _make_valid_request_args(draw="abc")
        with pytest.raises(InvalidDataError, match="'draw' must be an integer"):
            validate_request_args(args)


# ---------------------------------------------------------------------------
# _validate_search_dict
# ---------------------------------------------------------------------------

class TestValidateSearchDict:
    def test_non_dict_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            _validate_search_dict("bad", "search")

    def test_missing_value_key_raises(self):
        with pytest.raises(InvalidDataError, match="missing required key 'value'"):
            _validate_search_dict({"regex": False}, "search")

    def test_missing_regex_key_raises(self):
        with pytest.raises(InvalidDataError, match="missing required key 'regex'"):
            _validate_search_dict({"value": ""}, "search")

    def test_valid_search_dict_passes(self):
        _validate_search_dict({"value": "foo", "regex": False}, "search")  # no exception


# ---------------------------------------------------------------------------
# _validate_columns
# ---------------------------------------------------------------------------

class TestValidateColumns:
    def test_non_list_raises(self):
        with pytest.raises(InvalidDataError, match="'columns' must be a list"):
            _validate_columns("bad")

    def test_non_dict_column_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            _validate_columns(["not_a_dict"])

    def test_missing_column_key_raises(self):
        col = {"data": "x", "searchable": True, "orderable": True}  # missing 'search'
        with pytest.raises(InvalidDataError, match="missing required key 'search'"):
            _validate_columns([col])

    def test_invalid_field_name_in_data_raises(self):
        col = {
            "data": "name; DROP TABLE",
            "searchable": True,
            "orderable": True,
            "search": {"value": "", "regex": False},
        }
        with pytest.raises(InvalidDataError):
            _validate_columns([col])

    def test_valid_column_passes(self):
        col = {
            "data": "field_name",
            "searchable": True,
            "orderable": True,
            "search": {"value": "", "regex": False},
        }
        _validate_columns([col])  # no exception


# ---------------------------------------------------------------------------
# _validate_order
# ---------------------------------------------------------------------------

class TestValidateOrder:
    def test_non_list_raises(self):
        with pytest.raises(InvalidDataError, match="'order' must be a list"):
            _validate_order("bad", 1)

    def test_non_dict_entry_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            _validate_order(["not_a_dict"], 1)

    def test_missing_column_key_raises(self):
        with pytest.raises(InvalidDataError, match="missing required key 'column'"):
            _validate_order([{"dir": "asc"}], 1)

    def test_out_of_range_column_index_raises(self):
        with pytest.raises(InvalidDataError, match="out of range"):
            _validate_order([{"column": 5, "dir": "asc"}], 3)

    def test_invalid_dir_raises(self):
        with pytest.raises(InvalidDataError, match="'asc' or 'desc'"):
            _validate_order([{"column": 0, "dir": "sideways"}], 1)

    def test_valid_order_passes(self):
        _validate_order([{"column": 0, "dir": "desc"}], 1)  # no exception

    def test_non_integer_column_raises(self):
        with pytest.raises(InvalidDataError, match="must be an integer"):
            _validate_order([{"column": "abc", "dir": "asc"}], 1)


# ---------------------------------------------------------------------------
# DataTables.__init__ — validate_request_args integration
# ---------------------------------------------------------------------------

class TestDataTablesValidation:
    def test_missing_draw_raises_on_init(self):
        args = _make_valid_request_args()
        del args["draw"]
        with pytest.raises(InvalidDataError):
            _make_dt(args)

    def test_non_dict_request_args_raises(self):
        with pytest.raises(InvalidDataError):
            _make_dt(None)

    def test_columns_with_injection_attempt_raises(self):
        args = _make_valid_request_args()
        args["columns"][0]["data"] = "$where"
        with pytest.raises(InvalidDataError):
            _make_dt(args)

    def test_valid_args_initializes_successfully(self):
        dt = _make_dt(_make_valid_request_args())
        assert dt.draw == 1


# ---------------------------------------------------------------------------
# validate_field_name
# ---------------------------------------------------------------------------

class TestValidateFieldName:
    def test_valid_names_pass(self):
        for name in ("field", "field_name", "field.nested", "field-name", "Field123"):
            validate_field_name(name)  # no exception

    def test_dollar_sign_raises(self):
        with pytest.raises(InvalidDataError):
            validate_field_name("$where")

    def test_space_raises(self):
        with pytest.raises(InvalidDataError):
            validate_field_name("field name")

    def test_semicolon_raises(self):
        with pytest.raises(InvalidDataError):
            validate_field_name("field;drop")

    def test_empty_string_raises(self):
        with pytest.raises(InvalidDataError):
            validate_field_name("")


# ---------------------------------------------------------------------------
# validate_editor_request_args
# ---------------------------------------------------------------------------

class TestValidateEditorRequestArgs:
    def test_non_dict_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            validate_editor_request_args("bad")

    def test_invalid_action_raises(self):
        with pytest.raises(InvalidDataError, match="Invalid action"):
            validate_editor_request_args({"action": "drop_table"})

    def test_valid_actions_pass(self):
        for action in ("create", "edit", "remove", "upload", "search", "dependent"):
            validate_editor_request_args({"action": action})  # no exception

    def test_no_action_passes(self):
        validate_editor_request_args({})  # no exception


# ---------------------------------------------------------------------------
# validate_doc_id
# ---------------------------------------------------------------------------

class TestValidateDocId:
    def test_empty_string_passes(self):
        validate_doc_id("")  # no exception

    def test_valid_object_id_passes(self):
        validate_doc_id(str(ObjectId()))

    def test_multiple_valid_ids_pass(self):
        ids = ",".join(str(ObjectId()) for _ in range(3))
        validate_doc_id(ids)

    def test_invalid_id_raises(self):
        with pytest.raises(InvalidDataError, match="Invalid document ID"):
            validate_doc_id("not-a-valid-id")

    def test_mixed_valid_invalid_raises(self):
        valid = str(ObjectId())
        with pytest.raises(InvalidDataError):
            validate_doc_id(f"{valid},bad_id")


# ---------------------------------------------------------------------------
# validate_data_fields_whitelist
# ---------------------------------------------------------------------------

class TestValidateDataFieldsWhitelist:
    def test_no_whitelist_allows_anything(self):
        validate_data_fields_whitelist({"any_field": "val"}, {}, [])  # no exception

    def test_allowed_field_passes(self):
        fields = {"name": DataField("name", "string")}
        data_fields = [DataField("name", "string")]
        validate_data_fields_whitelist({"name": "Alice"}, fields, data_fields)

    def test_disallowed_field_raises(self):
        fields = {"name": DataField("name", "string")}
        data_fields = [DataField("name", "string")]
        with pytest.raises(InvalidDataError, match="not in the allowed"):
            validate_data_fields_whitelist({"evil_field": "x"}, fields, data_fields)

    def test_non_dict_data_passes_silently(self):
        fields = {"name": DataField("name", "string")}
        data_fields = [DataField("name", "string")]
        validate_data_fields_whitelist("not_a_dict", fields, data_fields)  # no exception


# ---------------------------------------------------------------------------
# validate_upload_data
# ---------------------------------------------------------------------------

class TestValidateUploadData:
    def test_valid_upload_passes(self):
        validate_upload_data({
            "filename": "file.txt",
            "content_type": "text/plain",
            "data": b"hello",
        })

    def test_non_dict_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            validate_upload_data("bad")

    def test_empty_filename_raises(self):
        with pytest.raises(InvalidDataError, match="filename"):
            validate_upload_data({"filename": "", "content_type": "text/plain", "data": b"x"})

    def test_path_traversal_raises(self):
        with pytest.raises(InvalidDataError, match="invalid path characters"):
            validate_upload_data({"filename": "../etc/passwd", "content_type": "text/plain", "data": b"x"})

    def test_empty_data_raises(self):
        with pytest.raises(InvalidDataError, match="must not be empty"):
            validate_upload_data({"filename": "f.txt", "content_type": "text/plain", "data": b""})

    def test_non_bytes_data_raises(self):
        with pytest.raises(InvalidDataError, match="must be bytes"):
            validate_upload_data({"filename": "f.txt", "content_type": "text/plain", "data": "string"})


# ---------------------------------------------------------------------------
# Editor — whitelist enforcement on create/edit
# ---------------------------------------------------------------------------

class TestEditorWhitelistEnforcement:
    def _editor_with_fields(self, action, data):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        request_args = {"action": action, "data": data}
        return Editor(
            mongo, "test", request_args,
            data_fields=[DataField("name", "string"), DataField("email", "string")],
        )

    def test_create_with_unknown_field_raises(self):
        editor = self._editor_with_fields("create", {"0": {"name": "Alice", "evil": "x"}})
        result = editor.process()
        assert "error" in result

    def test_edit_with_unknown_field_raises(self):
        doc_id = str(ObjectId())
        editor = self._editor_with_fields("edit", {doc_id: {"name": "Alice", "evil": "x"}})
        result = editor.process()
        assert "error" in result

    def test_create_with_allowed_fields_succeeds(self):
        col = MagicMock(spec=Collection)
        col.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        col.find_one.return_value = {"_id": ObjectId(), "name": "Alice", "email": "a@b.com"}
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        mongo.db.__getitem__.return_value = col
        editor = Editor(
            mongo, "test",
            {"action": "create", "data": {"0": {"name": "Alice", "email": "a@b.com"}}},
            data_fields=[DataField("name", "string"), DataField("email", "string")],
        )
        result = editor.process()
        assert "error" not in result
