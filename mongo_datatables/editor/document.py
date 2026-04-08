"""Format, preprocess, and build updates for Editor documents."""
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from bson.objectid import ObjectId
from datetime import datetime

from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.utils import FieldMapper, TypeConverter, DateHandler
from mongo_datatables.editor.validator import validate_field_name, validate_document_payload
from mongo_datatables.data_field import DataField
from mongo_datatables.editor.storage import StorageAdapter

logger = logging.getLogger(__name__)


def format_response_document(
    doc: Dict[str, Any],
    row_class=None,
    row_data=None,
    row_attr=None,
) -> Dict[str, Any]:
    """Format a MongoDB document for the Editor response, converting ObjectId and datetime.

    Converts ObjectId to DT_RowId string, serialises datetime fields to ISO format,
    and attaches optional DT_Row* metadata.

    doc: Document from MongoDB.
    row_class: Optional string or callable(row) -> str for DT_RowClass.
    row_data: Optional dict or callable(row) -> dict for DT_RowData.
    row_attr: Optional dict or callable(row) -> dict for DT_RowAttr.
    Returns formatted document dict.
    """
    response_doc = dict(doc)

    if "_id" in response_doc:
        response_doc["DT_RowId"] = str(response_doc.pop("_id"))

    for key, val in response_doc.items():
        if isinstance(val, ObjectId):
            response_doc[key] = str(val)
        elif isinstance(val, datetime):
            response_doc[key] = val.isoformat()

    if row_class is not None:
        response_doc["DT_RowClass"] = row_class(response_doc) if callable(row_class) else row_class
    if row_data is not None:
        response_doc["DT_RowData"] = row_data(response_doc) if callable(row_data) else row_data
    if row_attr is not None:
        response_doc["DT_RowAttr"] = row_attr(response_doc) if callable(row_attr) else row_attr

    return response_doc


def collect_files(file_fields: List[str], storage_adapter: StorageAdapter) -> Optional[Dict[str, Any]]:
    """Collect file metadata from the storage adapter for all configured upload fields.

    file_fields: List of field names that are upload fields.
    storage_adapter: StorageAdapter instance; must have files_for_field() method.
    Returns dict of {field: {file_id: metadata}}, or None if unavailable.
    """
    if not file_fields or not storage_adapter:
        return None
    if not hasattr(storage_adapter, "files_for_field"):
        return None
    files = {
        field: storage_adapter.files_for_field(field)
        for field in file_fields
        if storage_adapter.files_for_field(field)
    }
    return files if files else None


def preprocess_document(
    doc: Dict[str, Any],
    fields: Dict[str, DataField],
    data_fields: List[DataField],
    field_mapper: FieldMapper,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Process document data before database insert/update, parsing JSON and dates.

    Converts JSON strings to objects, handles date field parsing, and separates
    dot-notation keys for nested updates.

    doc: Raw document data from Editor.
    fields: Dict of alias -> DataField for whitelist checking.
    data_fields: List of DataField objects.
    field_mapper: FieldMapper instance.
    Returns (processed_document, dot_notation_updates).
    """
    validate_document_payload(doc)
    def _allowed(key: str) -> bool:
        if not fields:
            return True
        root = key.split(".")[0]
        return root in fields or root in {f.name.split(".")[0] for f in data_fields}

    processed_doc = {k: v for k, v in doc.items() if v is not None and _allowed(k)}
    dot_notation_updates: Dict[str, Any] = {}

    for key, val in processed_doc.items():
        validate_field_name(key)
        if isinstance(val, str):
            try:
                parsed_val = json.loads(val)
                if "." in key:
                    dot_notation_updates[key] = parsed_val
                else:
                    processed_doc[key] = parsed_val
                continue
            except json.JSONDecodeError:
                pass

        is_date_field = key.lower().endswith(("date", "time", "at")) or key.split(".")[-1].lower().endswith(
            ("date", "time", "at")
        )

        if isinstance(val, str) and is_date_field and val.strip():
            try:
                date_obj = DateHandler.parse_iso_datetime(val)
                if "." in key:
                    dot_notation_updates[key] = date_obj
                else:
                    processed_doc[key] = date_obj
            except FieldMappingError:
                if "." in key:
                    dot_notation_updates[key] = val
        elif "." in key:
            dot_notation_updates[key] = val

    for key in list(processed_doc.keys()):
        if "." in key:
            del processed_doc[key]

    return processed_doc, dot_notation_updates


def build_updates(
    data: Any,
    field_mapper: FieldMapper,
    fields: Dict[str, DataField],
    data_fields: List[DataField],
    updates: Dict[str, Any],
    prefix: str = "",
) -> None:
    """Recursively build a $set updates dict from nested Editor data, applying type conversions.

    Traverses nested dicts, applies field type conversions (date, number, boolean, array),
    and populates the updates dict with dot-notation keys.

    data: Data to process (dict or scalar).
    field_mapper: FieldMapper for type lookups.
    fields: Dict of alias -> DataField for whitelist checking.
    data_fields: List of DataField objects.
    updates: Dict to populate in-place.
    prefix: Dot-notation prefix for the current nesting level.
    """
    if not isinstance(data, dict):
        return

    for key, value in data.items():
        if value is None:
            continue
        full_key = f"{prefix}.{key}" if prefix else key
        validate_field_name(key)

        if isinstance(value, dict):
            build_updates(value, field_mapper, fields, data_fields, updates, full_key)
            continue

        field_type = field_mapper.get_field_type(full_key) or "string"

        if field_type == "date" and isinstance(value, str):
            try:
                date_str = value if "T" in value else f"{value}T00:00:00"
                updates[full_key] = DateHandler.parse_iso_datetime(date_str)
            except FieldMappingError as e:
                logger.warning("Date conversion error for %s: %s", full_key, e)
                updates[full_key] = value
        elif field_type == "number" and isinstance(value, str):
            try:
                updates[full_key] = TypeConverter.to_number(value)
            except FieldMappingError:
                updates[full_key] = value
        elif field_type == "boolean" and isinstance(value, str):
            updates[full_key] = TypeConverter.to_boolean(value)
        elif field_type == "array" and isinstance(value, str):
            updates[full_key] = TypeConverter.to_array(value)
        else:
            updates[full_key] = value
