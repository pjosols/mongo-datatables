"""Result formatting — BSON serialization, alias remapping, and cursor processing."""
import math
import re
import uuid
from typing import Any, Dict, List, Optional

from bson import Binary, Decimal128, ObjectId, Regex

from mongo_datatables.field_utils import FieldMapper

# Bitmask → regex flag letter mapping for BSON Regex serialization
_REGEX_FLAGS = ((re.IGNORECASE, 'i'), (re.MULTILINE, 'm'), (re.DOTALL, 's'), (re.VERBOSE, 'x'))


def _convert_scalar(val: Any) -> Any:
    """Convert a single BSON scalar to a JSON-serializable Python value.

    val: value to convert.
    Returns converted value, or the original if no conversion applies.
    """
    if isinstance(val, ObjectId):
        return str(val)
    if hasattr(val, 'isoformat'):
        return val.isoformat()
    if isinstance(val, float) and not math.isfinite(val):
        return None
    if isinstance(val, Decimal128):
        return float(val.to_decimal())
    if isinstance(val, Binary):
        return str(uuid.UUID(bytes=bytes(val))) if val.subtype in (3, 4) else val.hex()
    if isinstance(val, Regex):
        flags = ''.join(v for k, v in _REGEX_FLAGS if int(val.flags) & int(k))
        return f'/{val.pattern}/{flags}'
    return val


def format_result_values(result_dict: Dict[str, Any], parent_key: str = "") -> None:
    """Recursively format values in result dictionary for JSON serialization.

    result_dict: dictionary to process.
    parent_key: key of parent for nested dictionaries.
    """
    if not result_dict:
        return

    items = list(result_dict.items())
    for key, val in items:
        full_key = f"{parent_key}.{key}" if parent_key else key

        if isinstance(val, dict):
            format_result_values(val, full_key)
        elif isinstance(val, list):
            for i, item in enumerate(val):
                if isinstance(item, dict):
                    format_result_values(item, f"{full_key}[{i}]")
                else:
                    val[i] = _convert_scalar(item)
        else:
            result_dict[key] = _convert_scalar(val)


def _extract_nested(doc: Dict[str, Any], parts: List[str]) -> Any:
    """Walk dot-notation path parts into a nested dict.

    doc: top-level document.
    parts: path segments split on '.'.
    Returns the value at the path, or None if any segment is missing.
    """
    val: Any = doc
    for part in parts:
        if isinstance(val, dict) and part in val:
            val = val[part]
        else:
            return None
    return val


def _should_remove_parent(top: str, db_field: str, field_mapper) -> bool:
    """Return True when no other mapped field shares the same top-level parent.

    top: top-level key (first segment of db_field).
    db_field: the field being remapped.
    field_mapper: FieldMapper whose db_to_ui mapping is checked.
    """
    return not any(f != db_field and f.startswith(top + '.') for f in field_mapper.db_to_ui)


def remap_aliases(doc: Dict[str, Any], field_mapper) -> Dict[str, Any]:
    """Remap DB field names to UI aliases in a result document.

    doc: document returned from MongoDB.
    field_mapper: FieldMapper with db_to_ui alias mapping.
    Returns doc with keys replaced by their UI aliases.
    """
    if not field_mapper.db_to_ui:
        return doc
    for db_field, ui_alias in field_mapper.db_to_ui.items():
        if db_field == ui_alias:
            continue
        if '.' in db_field:
            parts = db_field.split('.')
            val = _extract_nested(doc, parts)
            if val is not None:
                doc[ui_alias] = val
                top = parts[0]
                if _should_remove_parent(top, db_field, field_mapper):
                    del doc[top]
        elif db_field in doc:
            doc[ui_alias] = doc.pop(db_field)
    return doc


def process_cursor(
    cursor: Any,
    row_id: Optional[str],
    field_mapper: FieldMapper,
    row_class: Any = None,
    row_data: Any = None,
    row_attr: Any = None,
) -> List[Dict[str, Any]]:
    """Convert aggregation cursor to DataTables-formatted list.

    cursor: MongoDB aggregation cursor or iterable of documents.
    row_id: optional field name to use as DT_RowId.
    field_mapper: FieldMapper for alias resolution.
    row_class/row_data/row_attr: DT_Row* metadata providers.
    Returns list of formatted document dicts.
    """
    processed = []
    for result in cursor:
        d = dict(result)
        if row_id and row_id in d:
            d["DT_RowId"] = str(d[row_id])
        elif "_id" in d:
            d["DT_RowId"] = str(d.pop("_id"))
        format_result_values(d)
        d = remap_aliases(d, field_mapper)
        if row_class is not None:
            d["DT_RowClass"] = row_class(d) if callable(row_class) else row_class
        if row_data is not None:
            d["DT_RowData"] = row_data(d) if callable(row_data) else row_data
        if row_attr is not None:
            d["DT_RowAttr"] = row_attr(d) if callable(row_attr) else row_attr
        processed.append(d)
    return processed
