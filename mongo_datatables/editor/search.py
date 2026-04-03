"""Search, dependent-field, and upload handlers for Editor."""
import re
import logging
from typing import Any, Dict, List, Optional

from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.editor.validator import validate_upload_data
from mongo_datatables.utils import FieldMapper

logger = logging.getLogger(__name__)


def handle_search(
    request_args: Dict[str, Any],
    collection: Any,
    field_mapper: FieldMapper,
    fields: Dict[str, Any],
) -> Dict[str, Any]:
    """Handle action=search for autocomplete and tags field types.

    Performs a case-insensitive prefix regex match when ``search`` is present,
    or an exact ``$in`` lookup when ``values[]`` is present.

    request_args: Parsed Editor request dict.
    collection: PyMongo collection.
    field_mapper: FieldMapper for alias -> db field resolution.
    fields: Dict of alias -> DataField for type coercion.
    Returns ``{"data": [{"label": str, "value": any}, ...]}``.
    """
    field = request_args.get("field", "")
    search_term = request_args.get("search", None)
    values = request_args.get("values", [])
    db_field = field_mapper.get_db_field(field) or field

    if search_term is not None:
        query = {db_field: {"$regex": re.escape(search_term), "$options": "i"}}
    elif values:
        query = {db_field: {"$in": _coerce_values(field, values, fields)}}
    else:
        return {"data": []}

    docs = collection.find(query, {db_field: 1}).limit(100)
    seen: set = set()
    results: List[Dict[str, Any]] = []
    for doc in docs:
        val = doc.get(db_field)
        if val is None:
            continue
        key = str(val)
        if key not in seen:
            seen.add(key)
            results.append({"label": str(val), "value": val})
    return {"data": results}


def _coerce_values(field: str, values: list, fields: Dict[str, Any]) -> list:
    """Coerce string values from the request to the field's declared type.

    field: Field alias.
    values: List of raw values from the request.
    fields: Dict of alias -> DataField.
    Returns coerced list.
    """
    data_field = fields.get(field)
    if data_field is None:
        return values
    field_type = getattr(data_field, "data_type", None)
    if field_type == "number":
        coerced = []
        for v in values:
            try:
                coerced.append(int(v) if isinstance(v, str) and "." not in v else float(v))
            except (ValueError, TypeError):
                coerced.append(v)
        return coerced
    if field_type == "boolean":
        return [v if isinstance(v, bool) else str(v).lower() in ("true", "1") for v in values]
    return values


def handle_dependent(
    request_args: Dict[str, Any],
    dependent_handlers: Dict[str, Any],
) -> Dict[str, Any]:
    """Handle dependent field Ajax requests.

    Dispatches to a registered handler for the triggering field.

    request_args: Parsed Editor request dict.
    dependent_handlers: Dict of field -> callable(field, values, rows).
    Returns response dict with any of: options, values, messages, errors,
    labels, show, hide, enable, disable.
    Raises InvalidDataError if no handler is registered for the field.
    """
    field = request_args.get("field", "")
    handler = dependent_handlers.get(field)
    if not handler:
        raise InvalidDataError(f"No dependent handler registered for field: {field}")
    values = request_args.get("values", {})
    rows = request_args.get("rows", [])
    return handler(field, values, rows)


def handle_upload(
    request_args: Dict[str, Any],
    storage_adapter: Optional[Any],
) -> Dict[str, Any]:
    """Handle action=upload — store a file via the pluggable storage adapter.

    Expects request_args to contain:
        uploadField: name of the Editor field this file belongs to.
        upload: dict with keys filename, content_type, data (bytes).

    request_args: Parsed Editor request dict.
    storage_adapter: StorageAdapter instance.
    Returns ``{"upload": {"id": "<file_id>"}, "files": {...}}``.
    Raises InvalidDataError if adapter, uploadField, or upload data is missing.
    """
    if not storage_adapter:
        raise InvalidDataError("No storage adapter configured for file uploads")
    field = request_args.get("uploadField", "")
    if not field:
        raise InvalidDataError("uploadField is required for upload action")
    upload = request_args.get("upload")
    if not upload:
        raise InvalidDataError("No file data provided for upload")
    validate_upload_data(upload)
    file_id = storage_adapter.store(
        field,
        upload.get("filename", ""),
        upload.get("content_type", ""),
        upload.get("data", b""),
    )
    files: Dict[str, Any] = {}
    if hasattr(storage_adapter, "files_for_field"):
        files[field] = storage_adapter.files_for_field(field)
    return {"upload": {"id": file_id}, "files": files}
