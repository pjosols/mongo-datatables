"""Validation for DataTables request_args parameters."""

from typing import Any, Dict

from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.editor_validator import validate_field_name

# Keys that must be present in a valid DataTables request
_REQUIRED_KEYS = ("draw", "start", "length", "columns", "order", "search")

# Required keys within each column dict
_COLUMN_REQUIRED_KEYS = ("data", "searchable", "orderable", "search")

# Required keys within each order dict
_ORDER_REQUIRED_KEYS = ("column", "dir")

# Required keys within a search dict
_SEARCH_REQUIRED_KEYS = ("value", "regex")


def _coerce_int(value: Any, name: str, default: int, minimum: int | None = None) -> int:
    """Coerce value to int, applying an optional minimum.

    value: raw value from request_args.
    name: parameter name for error messages.
    default: fallback when value is missing or None.
    minimum: if set, clamp result to this floor.
    Returns coerced int.
    Raises InvalidDataError if value cannot be converted.
    """
    if value is None:
        return default
    try:
        result = int(value)
    except (ValueError, TypeError) as exc:
        raise InvalidDataError(f"'{name}' must be an integer, got {value!r}") from exc
    if minimum is not None and result < minimum:
        result = minimum
    return result


def _validate_search_dict(search: Any, context: str) -> None:
    """Validate a DataTables search dict has required keys.

    search: the value to validate.
    context: description for error messages (e.g. 'search', 'columns[0][search]').
    Raises InvalidDataError if invalid.
    """
    if not isinstance(search, dict):
        raise InvalidDataError(f"'{context}' must be a dict, got {type(search).__name__}")
    for key in _SEARCH_REQUIRED_KEYS:
        if key not in search:
            raise InvalidDataError(f"'{context}' is missing required key '{key}'")


def _validate_columns(columns: Any) -> None:
    """Validate the columns list from request_args.

    columns: the value to validate.
    Raises InvalidDataError if invalid.
    """
    if not isinstance(columns, list):
        raise InvalidDataError(f"'columns' must be a list, got {type(columns).__name__}")
    for i, col in enumerate(columns):
        if not isinstance(col, dict):
            raise InvalidDataError(f"'columns[{i}]' must be a dict, got {type(col).__name__}")
        for key in _COLUMN_REQUIRED_KEYS:
            if key not in col:
                raise InvalidDataError(f"'columns[{i}]' is missing required key '{key}'")
        _validate_search_dict(col["search"], f"columns[{i}][search]")
        data_val = col.get("data")
        if data_val and isinstance(data_val, str):
            try:
                validate_field_name(data_val)
            except InvalidDataError as exc:
                raise InvalidDataError(f"'columns[{i}][data]' {exc}") from exc


def _validate_order(order: Any, num_columns: int) -> None:
    """Validate the order list from request_args.

    order: the value to validate.
    num_columns: number of columns for index bounds checking.
    Raises InvalidDataError if invalid.
    """
    if not isinstance(order, list):
        raise InvalidDataError(f"'order' must be a list, got {type(order).__name__}")
    for i, entry in enumerate(order):
        if not isinstance(entry, dict):
            raise InvalidDataError(f"'order[{i}]' must be a dict, got {type(entry).__name__}")
        for key in _ORDER_REQUIRED_KEYS:
            if key not in entry:
                raise InvalidDataError(f"'order[{i}]' is missing required key '{key}'")
        try:
            col_idx = int(entry["column"])
        except (ValueError, TypeError) as exc:
            raise InvalidDataError(f"'order[{i}][column]' must be an integer") from exc
        if num_columns > 0 and not (0 <= col_idx < num_columns):
            raise InvalidDataError(
                f"'order[{i}][column]' index {col_idx} is out of range (0–{num_columns - 1})"
            )
        if entry.get("dir") not in ("asc", "desc"):
            raise InvalidDataError(
                f"'order[{i}][dir]' must be 'asc' or 'desc', got {entry.get('dir')!r}"
            )


def validate_request_args(request_args: Any) -> Dict[str, Any]:
    """Validate and sanitize a DataTables request_args dict.

    Checks for required top-level keys, validates types of columns/order/search
    sub-structures, and sanitizes numeric parameters (draw, start, length).

    request_args: raw request parameters from the DataTables Ajax call.
    Returns the validated dict (same object, numeric fields coerced).
    Raises InvalidDataError if validation fails.
    """
    if not isinstance(request_args, dict):
        raise InvalidDataError(
            f"request_args must be a dict, got {type(request_args).__name__}"
        )

    for key in _REQUIRED_KEYS:
        if key not in request_args:
            raise InvalidDataError(f"request_args is missing required key '{key}'")

    _validate_search_dict(request_args["search"], "search")
    _validate_columns(request_args["columns"])
    _validate_order(request_args["order"], len(request_args["columns"]))

    # Sanitize numeric parameters in-place
    request_args["draw"] = _coerce_int(request_args.get("draw"), "draw", default=1, minimum=1)
    request_args["start"] = _coerce_int(request_args.get("start"), "start", default=0, minimum=0)
    request_args["length"] = _coerce_int(request_args.get("length"), "length", default=10)

    return request_args
