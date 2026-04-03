"""Validation helpers for Editor request_args and file uploads."""

import re
from typing import Any

from bson.objectid import ObjectId
from bson.errors import InvalidId as ObjectIdError

from mongo_datatables.exceptions import InvalidDataError

# Valid Editor actions
_VALID_ACTIONS = frozenset({"create", "edit", "remove", "upload", "search", "dependent"})

# Allowed field name pattern: alphanumeric, underscore, hyphen, dot (for nested)
_FIELD_NAME_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")

# Max lengths for upload fields
_MAX_FILENAME_LEN = 255
_MAX_CONTENT_TYPE_LEN = 127
_MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB


def validate_editor_request_args(request_args: Any) -> None:
    """Validate Editor request_args dict for required structure.

    Checks that request_args is a dict and, when an action is present,
    that it is a recognised Editor action string.

    request_args: raw dict from the Editor Ajax call.
    Raises InvalidDataError if validation fails.
    """
    if not isinstance(request_args, dict):
        raise InvalidDataError(
            f"request_args must be a dict, got {type(request_args).__name__}"
        )
    action = request_args.get("action")
    if action is not None and action not in _VALID_ACTIONS:
        raise InvalidDataError(
            f"Invalid action {action!r}. Must be one of: {', '.join(sorted(_VALID_ACTIONS))}"
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
    """Validate the upload dict from an Editor upload request.

    Checks that filename, content_type, and data are present and sane.

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

    name: field name string to validate.
    Raises InvalidDataError if the name contains disallowed characters.
    """
    if not _FIELD_NAME_RE.match(name):
        raise InvalidDataError(
            f"Field name {name!r} contains invalid characters. "
            "Only alphanumeric characters, underscores, hyphens, and dots are allowed."
        )


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
    for key in data:
        root = key.split(".")[0]
        if root not in allowed_roots:
            raise InvalidDataError(
                f"Field {key!r} is not in the allowed data_fields whitelist."
            )
