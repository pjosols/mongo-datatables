"""Validation for Editor request_args structure and action."""

from typing import Any

from mongo_datatables.exceptions import InvalidDataError

_VALID_ACTIONS = frozenset({"create", "edit", "remove", "upload", "search", "dependent"})

_ALLOWED_REQUEST_KEYS = frozenset({
    "action", "data", "field", "value", "upload", "uploadField", "id", "ids",
    "search", "values", "rows",
})

_MAX_REQUEST_ARGS_DEPTH = 5
_MAX_REQUEST_ARGS_KEYS = 20


def _check_depth(value: Any, depth: int) -> None:
    """Recursively check nesting depth of a value.

    value: value to inspect.
    depth: current depth counter.
    Raises InvalidDataError if depth exceeds _MAX_REQUEST_ARGS_DEPTH.
    """
    if depth > _MAX_REQUEST_ARGS_DEPTH:
        raise InvalidDataError(
            f"request_args nesting exceeds maximum depth of {_MAX_REQUEST_ARGS_DEPTH}"
        )
    if isinstance(value, dict):
        for v in value.values():
            _check_depth(v, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _check_depth(item, depth + 1)


def _validate_request_args_structure(request_args: Any) -> None:
    """Validate Editor request_args structure without checking action validity.

    Checks dict type, key count, unknown keys, and nesting depth only.
    Used by Editor.__init__ so that invalid actions are handled by process().

    request_args: raw dict from the Editor Ajax call.
    Raises InvalidDataError if structural validation fails.
    """
    if not isinstance(request_args, dict):
        raise InvalidDataError(
            f"request_args must be a dict, got {type(request_args).__name__}"
        )
    if len(request_args) > _MAX_REQUEST_ARGS_KEYS:
        raise InvalidDataError(
            f"request_args has too many keys ({len(request_args)}); "
            f"maximum is {_MAX_REQUEST_ARGS_KEYS}"
        )
    unknown = set(request_args.keys()) - _ALLOWED_REQUEST_KEYS
    if unknown:
        raise InvalidDataError(
            f"request_args contains unexpected keys: {sorted(unknown)}"
        )
    for v in request_args.values():
        _check_depth(v, 0)


def validate_editor_request_args(request_args: Any) -> None:
    """Validate Editor request_args dict for required structure and valid action.

    Checks that request_args is a dict, that it contains only recognised
    top-level keys, that nesting depth is within bounds, and that the
    action (when present) is a recognised Editor action string.

    request_args: raw dict from the Editor Ajax call.
    Raises InvalidDataError if validation fails.
    """
    _validate_request_args_structure(request_args)
    action = request_args.get("action")
    if action is not None and action not in _VALID_ACTIONS:
        raise InvalidDataError(
            f"Invalid action {action!r}. Must be one of: {', '.join(sorted(_VALID_ACTIONS))}"
        )
