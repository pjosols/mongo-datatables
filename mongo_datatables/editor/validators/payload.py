"""Validation for upload payloads and document structure."""

from typing import Any

from mongo_datatables.exceptions import InvalidDataError

_MAX_FILENAME_LEN = 255
_MAX_CONTENT_TYPE_LEN = 127
_MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

_MAX_DOC_KEYS = 200
_MAX_DOC_NESTING = 10
_MAX_STRING_VALUE_LEN = 1_000_000  # 1 MB per string field


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
    declared_prefixes = {
        part
        for f in data_fields
        for part in f.name.split(".")
    }
    for key in data:
        if key.startswith("DT_Row"):
            continue
        root = key.split(".")[0]
        if root not in allowed_roots and root not in declared_prefixes:
            raise InvalidDataError(
                f"Field {key!r} is not in the allowed data_fields whitelist."
            )
