"""MongoDB condition builders for columnControl DataTables extension."""

import re
from typing import Any, Dict, List

from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.utils import TypeConverter, DateHandler


def build_column_control_conditions(
    db_field: str,
    field_type: str,
    cc: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build MongoDB conditions from a columnControl configuration.

    db_field: The database field name.
    field_type: The field type ('number', 'date', or text).
    cc: The columnControl dict from the DataTables request.
    Returns list of MongoDB condition dicts.
    """
    conditions: List[Dict[str, Any]] = []

    list_data = cc.get("list")
    if list_data and isinstance(list_data, dict):
        values = list(list_data.values())
        if field_type == "number":
            converted = []
            for v in values:
                try:
                    converted.append(TypeConverter.to_number(v))
                except (ValueError, TypeError, FieldMappingError):
                    pass
            if converted:
                conditions.append({db_field: {"$in": converted}})
        else:
            conditions.append({db_field: {"$in": values}})

    search = cc.get("search")
    if search and isinstance(search, dict):
        conditions.extend(_build_cc_search_conditions(db_field, field_type, search))

    return conditions


def _build_cc_search_conditions(
    db_field: str,
    field_type: str,
    search: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build conditions from the search sub-dict of a columnControl config.

    db_field: The database field name.
    field_type: The field type.
    search: The search dict from columnControl.
    Returns list of MongoDB condition dicts.
    """
    value = search.get("value", "")
    logic = search.get("logic", "")
    stype = search.get("type", field_type or "text")

    if logic == "empty":
        return [{db_field: {"$in": [None, ""]}}]
    if logic == "notEmpty":
        return [{db_field: {"$nin": [None, ""]}}]
    if not value:
        return []

    if stype == "num":
        return _build_cc_num_conditions(db_field, value, logic)
    if stype == "date":
        return _build_cc_date_conditions(db_field, value, logic)
    return _build_cc_text_conditions(db_field, value, logic)


def _build_cc_num_conditions(db_field: str, value: str, logic: str) -> List[Dict[str, Any]]:
    """Build numeric columnControl conditions.

    db_field: The database field name.
    value: The search value string.
    logic: The comparison logic string.
    Returns list of MongoDB condition dicts.
    """
    try:
        num = TypeConverter.to_number(value)
        logic_map = {
            "equal": {db_field: num},
            "notEqual": {db_field: {"$ne": num}},
            "greater": {db_field: {"$gt": num}},
            "greaterOrEqual": {db_field: {"$gte": num}},
            "less": {db_field: {"$lt": num}},
            "lessOrEqual": {db_field: {"$lte": num}},
        }
        cond = logic_map.get(logic)
        return [cond] if cond else []
    except (ValueError, TypeError, FieldMappingError):
        return []


def _build_cc_date_conditions(db_field: str, value: str, logic: str) -> List[Dict[str, Any]]:
    """Build date columnControl conditions.

    db_field: The database field name.
    value: The date value string.
    logic: The comparison logic string.
    Returns list of MongoDB condition dicts.
    """
    try:
        parsed = DateHandler.parse_iso_date(value.split("T")[0])
        next_day = DateHandler.get_next_day(parsed)
        logic_map = {
            "equal": {db_field: {"$gte": parsed, "$lt": next_day}},
            "notEqual": {"$or": [{db_field: {"$lt": parsed}}, {db_field: {"$gte": next_day}}]},
            "greater": {db_field: {"$gt": parsed}},
            "less": {db_field: {"$lt": parsed}},
        }
        cond = logic_map.get(logic)
        return [cond] if cond else []
    except (ValueError, TypeError, FieldMappingError):
        return []


def _build_cc_text_conditions(db_field: str, value: str, logic: str) -> List[Dict[str, Any]]:
    """Build text columnControl conditions.

    db_field: The database field name.
    value: The search value string.
    logic: The comparison logic string.
    Returns list of MongoDB condition dicts.
    """
    escaped = re.escape(value)
    logic_map = {
        "contains": {db_field: {"$regex": escaped, "$options": "i"}},
        "notContains": {db_field: {"$not": {"$regex": escaped, "$options": "i"}}},
        "equal": {db_field: {"$regex": f"^{escaped}$", "$options": "i"}},
        "notEqual": {db_field: {"$not": {"$regex": f"^{escaped}$", "$options": "i"}}},
        "starts": {db_field: {"$regex": f"^{escaped}", "$options": "i"}},
        "ends": {db_field: {"$regex": f"{escaped}$", "$options": "i"}},
    }
    cond = logic_map.get(logic)
    return [cond] if cond else []
