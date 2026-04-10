"""Tests for mongo_datatables.field_utils — validate_field_name and FieldMapper."""
import pytest

from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.field_utils import validate_field_name, FieldMapper


# ---------------------------------------------------------------------------
# validate_field_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", [
    "field",
    "field_name",
    "field-name",
    "field.name",
    "Field123",
    "a",
    "A1_b-c.d",
])
def test_validate_field_name_accepts_valid(name: str) -> None:
    validate_field_name(name)  # must not raise


@pytest.mark.parametrize("name", [
    "$where",
    "field name",
    "field;drop",
    "field\x00",
    "",
    "field!",
    "field@name",
    "field#",
])
def test_validate_field_name_rejects_invalid(name: str) -> None:
    with pytest.raises(InvalidDataError):
        validate_field_name(name)


def test_validate_field_name_error_message_contains_name() -> None:
    with pytest.raises(InvalidDataError, match="bad field!"):
        validate_field_name("bad field!")


# ---------------------------------------------------------------------------
# Import location — no cross-subpackage dependency
# ---------------------------------------------------------------------------

def test_validate_field_name_importable_from_field_utils() -> None:
    """validate_field_name must live in field_utils, not editor.validators."""
    import mongo_datatables.field_utils as fu
    assert hasattr(fu, "validate_field_name")


def test_validate_field_name_not_defined_in_editor_validators_identity() -> None:
    """identity.py must re-export from field_utils, not define its own copy."""
    import mongo_datatables.editor.validators.identity as identity
    import mongo_datatables.field_utils as fu
    # Both names must resolve to the same function object
    assert identity.validate_field_name is fu.validate_field_name


def test_datatables_request_validator_imports_from_field_utils() -> None:
    """request_validator must import validate_field_name from field_utils."""
    import mongo_datatables.datatables.request_validator as rv
    import mongo_datatables.field_utils as fu
    assert rv.validate_field_name is fu.validate_field_name


def test_search_builder_imports_from_field_utils() -> None:
    """builder.py must import validate_field_name from field_utils."""
    import mongo_datatables.datatables.search.builder as builder
    import mongo_datatables.field_utils as fu
    assert builder.validate_field_name is fu.validate_field_name


def test_search_panes_imports_from_field_utils() -> None:
    """panes.py must import validate_field_name from field_utils."""
    import mongo_datatables.datatables.search.panes as panes
    import mongo_datatables.field_utils as fu
    assert panes.validate_field_name is fu.validate_field_name


# ---------------------------------------------------------------------------
# FieldMapper
# ---------------------------------------------------------------------------

def test_field_mapper_empty() -> None:
    fm = FieldMapper([])
    assert fm.get_db_field("x") == "x"
    assert fm.get_ui_field("x") == "x"
    assert fm.get_field_type("x") is None


def test_field_mapper_with_data_fields() -> None:
    from mongo_datatables.data_field import DataField
    fields = [DataField("db_name", alias="ui_name", data_type="string")]
    fm = FieldMapper(fields)
    assert fm.get_db_field("ui_name") == "db_name"
    assert fm.get_ui_field("db_name") == "ui_name"
    assert fm.get_field_type("db_name") == "string"


def test_field_mapper_get_field_type_via_ui_name() -> None:
    from mongo_datatables.data_field import DataField
    fields = [DataField("salary", alias="pay", data_type="number")]
    fm = FieldMapper(fields)
    assert fm.get_field_type("pay") == "number"


def test_field_mapper_unknown_field_returns_none() -> None:
    from mongo_datatables.data_field import DataField
    fm = FieldMapper([DataField("x", alias="y", data_type="string")])
    assert fm.get_field_type("unknown") is None
