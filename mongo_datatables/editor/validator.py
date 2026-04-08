"""Validation for Editor request_args, document IDs, and file uploads."""

import re
from typing import Any

from bson.objectid import ObjectId
from bson.errors import InvalidId as ObjectIdError

from mongo_datatables.exceptions import InvalidDataError

# Valid Editor actions
_VALID_ACTIONS = frozenset({"create", "edit", "remove", "upload", "search", "dependent"})

# Allowed field name pattern: alphanumeric, underscore, hyphen, dot (for nested)
_FIELD_NAME_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")

# Valid MongoDB collection name: no $, no \0, no empty string, max 120 bytes
_COLLECTION_NAME_RE = re.compile(r"^[^\x00$]{1,120}$")

# Max lengths for upload fields
_MAX_FILENAME_LEN = 255
_MAX_CONTENT_TYPE_LEN = 127
_MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

# Document payload limits
_MAX_DOC_KEYS = 200
_MAX_DOC_NESTING = 10
_MAX_STRING_VALUE_LEN = 1_000_000  # 1 MB per string field


# Allowed top-level keys in request_args
_ALLOWED_REQUEST_KEYS = frozenset({
    "action", "data", "field", "value", "upload", "uploadField", "id", "ids",
})

# Maximum nesting depth for request_args values
_MAX_REQUEST_ARGS_DEPTH = 5

# Maximum number of top-level keys in request_args
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


def validate_editor_request_args(request_args: Any) -> None:
    """Validate Editor request_args dict for required structure and valid action.

    Checks that request_args is a dict, that it contains only recognised
    top-level keys, that nesting depth is within bounds, and that the
    action (when present) is a recognised Editor action string.

    request_args: raw dict from the Editor Ajax call.
    Raises InvalidDataError if validation fails.
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
    action = request_args.get("action")
    if action is not None and action not in _VALID_ACTIONS:
        raise InvalidDataError(
            f"Invalid action {action!r}. Must be one of: {', '.join(sorted(_VALID_ACTIONS))}"
        )


def validate_collection_name(name: str) -> None:
    """Validate a MongoDB collection name against MongoDB naming rules.

    Rejects empty strings, names containing null bytes or '$', and names
    exceeding 120 bytes — preventing injection via collection name.

    name: collection name string to validate.
    Raises InvalidDataError if the name is invalid.
    """
    if not isinstance(name, str) or not _COLLECTION_NAME_RE.match(name):
        raise InvalidDataError(
            f"Invalid collection name {name!r}. Must be a non-empty string "
            "without null bytes or '$', and at most 120 characters."
        )


def validate_doc_id(doc_id: str) -> None:
    """Validate that every ID in a comma-separated doc_id string is a valid ObjectId.

    doc_id: comma-separated string of MongoDB ObjectId hex strings.
    Raises InvalidDataError if any ID is malformed.
    """
    if not doc_id:
        return
    for raw in doc_id.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        try:
            ObjectId(candidate)
        except (ObjectIdError, ValueError, TypeError) as exc:
            raise InvalidDataError(
                f"Invalid document ID format: {candidate!r}"
            ) from exc


def validate_upload_data(upload: Any) -> None:
    """Validate the upload dict from an Editor upload request for required fields and bounds.

    Checks that filename, content_type, and data are present and sane, rejecting
    path traversal attempts and oversized payloads.

    upload: dict expected to contain 'filename', 'content_type', and 'data'.
    Raises InvalidDataError if any field is missing or invalid.
    """
    if not isinstance(upload, dict):
        raise InvalidDataError(
            f"upload must be a dict, got {type(upload).__name__}"
        )

    filename = upload.get("filename", "")
    if not isinstance(filename, str) or not filename.strip():
        raise InvalidDataError("upload 'filename' must be a non-empty string")
    if len(filename) > _MAX_FILENAME_LEN:
        raise InvalidDataError(
            f"upload 'filename' exceeds maximum length of {_MAX_FILENAME_LEN}"
        )
    # Reject path traversal attempts
    if ".." in filename or "/" in filename or "\\" in filename:
        raise InvalidDataError("upload 'filename' contains invalid path characters")

    content_type = upload.get("content_type", "")
    if not isinstance(content_type, str) or not content_type.strip():
        raise InvalidDataError("upload 'content_type' must be a non-empty string")
    if len(content_type) > _MAX_CONTENT_TYPE_LEN:
        raise InvalidDataError(
            f"upload 'content_type' exceeds maximum length of {_MAX_CONTENT_TYPE_LEN}"
        )

    data = upload.get("data")
    if not isinstance(data, (bytes, bytearray)):
        raise InvalidDataError("upload 'data' must be bytes")
    if len(data) == 0:
        raise InvalidDataError("upload 'data' must not be empty")
    if len(data) > _MAX_UPLOAD_BYTES:
        raise InvalidDataError(
            f"upload 'data' exceeds maximum size of {_MAX_UPLOAD_BYTES // (1024 * 1024)} MB"
        )


def validate_field_name(name: str) -> None:
    """Validate a single field name against the allowed character whitelist.

    Rejects names containing special characters that could be used for injection.

    name: field name string to validate.
    Raises InvalidDataError if the name contains disallowed characters.
    """
    if not _FIELD_NAME_RE.match(name):
        raise InvalidDataError(
            f"Field name {name!r} contains invalid characters. "
            "Only alphanumeric characters, underscores, hyphens, and dots are allowed."
        )


def validate_document_payload(doc: Any, _depth: int = 0) -> None:
    """Validate a document payload for bounds and structure safety against resource exhaustion.

    Rejects payloads that are too deeply nested, have too many keys, or
    contain excessively large string values — guarding against memory
    exhaustion from malicious or malformed input.

    doc: Document dict to validate.
    Raises InvalidDataError if any limit is exceeded.
    """
    if not isinstance(doc, dict):
        return
    if _depth > _MAX_DOC_NESTING:
        raise InvalidDataError(
            f"Document nesting exceeds maximum depth of {_MAX_DOC_NESTING}"
        )
    if len(doc) > _MAX_DOC_KEYS:
        raise InvalidDataError(
            f"Document has too many keys ({len(doc)}); maximum is {_MAX_DOC_KEYS}"
        )
    for key, value in doc.items():
        if isinstance(value, str) and len(value) > _MAX_STRING_VALUE_LEN:
            raise InvalidDataError(
                f"Value for field {key!r} exceeds maximum string length of {_MAX_STRING_VALUE_LEN}"
            )
        if isinstance(value, dict):
            validate_document_payload(value, _depth + 1)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    validate_document_payload(item, _depth + 1)


def validate_data_fields_whitelist(
    data: Any,
    fields: dict,
    data_fields: list,
) -> None:
    """Validate that all field names in Editor data are in the data_fields whitelist.

    Only enforced when data_fields is non-empty. Silently passes when no
    whitelist is configured (fields and data_fields both empty).

    data: Editor data row dict (field -> value).
    fields: Dict of alias -> DataField.
    data_fields: List of DataField objects.
    Raises InvalidDataError if a field name is not in the whitelist.
    """
    if not fields and not data_fields:
        return
    if not isinstance(data, dict):
        return
    allowed_roots = set(fields.keys()) | {f.name.split(".")[0] for f in data_fields}
    # Also allow any field that is a prefix of a declared nested field
    declared_prefixes = {
        part
        for f in data_fields
        for part in f.name.split(".")
    }
    for key in data:
        if key.startswith("DT_Row"):
            continue  # DT_Row* metadata keys are always allowed
        root = key.split(".")[0]
        if root not in allowed_roots and root not in declared_prefixes:
            raise InvalidDataError(
                f"Field {key!r} is not in the allowed data_fields whitelist."
            )
