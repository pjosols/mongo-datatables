"""Result formatting — BSON serialization, alias remapping, and cursor processing."""
import math
import re
import uuid
from typing import Any, Dict, List, Optional

from bson import Binary, Decimal128, ObjectId, Regex

# Bitmask → regex flag letter mapping for BSON Regex serialization
_REGEX_FLAGS = ((re.IGNORECASE, 'i'), (re.MULTILINE, 'm'), (re.DOTALL, 's'), (re.VERBOSE, 'x'))


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
                elif isinstance(item, ObjectId):
                    val[i] = str(item)
                elif hasattr(item, 'isoformat'):
                    val[i] = item.isoformat()
                elif isinstance(item, Decimal128):
                    val[i] = float(item.to_decimal())
                elif isinstance(item, Binary):
                    val[i] = str(uuid.UUID(bytes=bytes(item))) if item.subtype in (3, 4) else item.hex()
                elif isinstance(item, Regex):
                    flags = ''.join(v for k, v in _REGEX_FLAGS if int(item.flags) & int(k))
                    val[i] = f'/{item.pattern}/{flags}'
                elif isinstance(item, float) and not math.isfinite(item):
                    val[i] = None
        elif isinstance(val, ObjectId):
            result_dict[key] = str(val)
        elif hasattr(val, 'isoformat'):
            result_dict[key] = val.isoformat()
        elif isinstance(val, float) and not math.isfinite(val):
            result_dict[key] = None
        elif isinstance(val, Decimal128):
            result_dict[key] = float(val.to_decimal())
        elif isinstance(val, Binary):
            result_dict[key] = str(uuid.UUID(bytes=bytes(val))) if val.subtype in (3, 4) else val.hex()
        elif isinstance(val, Regex):
            flags = ''.join(v for k, v in _REGEX_FLAGS if int(val.flags) & int(k))
            result_dict[key] = f'/{val.pattern}/{flags}'


def remap_aliases(doc: Dict[str, Any], field_mapper) -> Dict[str, Any]:
    """Remap DB field names to UI aliases in a result document.

    For DataFields with dot-notation names (e.g. 'PublisherInfo.Date'),
    MongoDB returns nested dicts. This method extracts the value and
    stores it under the UI alias key, removing the intermediate nesting
    when no other fields from that parent are needed.
    """
    if not field_mapper.db_to_ui:
        return doc
    for db_field, ui_alias in field_mapper.db_to_ui.items():
        if db_field == ui_alias:
            continue  # no remapping needed
        if '.' in db_field:
            # Extract value from nested structure
            parts = db_field.split('.')
            val = doc
            for part in parts:
                if isinstance(val, dict) and part in val:
                    val = val[part]
                else:
                    val = None
                    break
            if val is not None:
                doc[ui_alias] = val
                # Remove top-level parent key only if it's no longer needed
                top = parts[0]
                other_uses = any(
                    f != db_field and f.startswith(top + '.')
                    for f in field_mapper.db_to_ui
                )
                if not other_uses:
                    del doc[top]
        else:
            # Simple rename: db_field key -> ui_alias key
            if db_field in doc:
                doc[ui_alias] = doc.pop(db_field)
    return doc


def process_cursor(
    cursor,
    row_id: Optional[str],
    field_mapper,
    row_class=None,
    row_data=None,
    row_attr=None,
) -> List[Dict[str, Any]]:
    """Convert aggregation cursor to DataTables-formatted list."""
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
