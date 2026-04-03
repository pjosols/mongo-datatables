"""MongoDB condition builders for DataTables column search and operators."""

import re
from typing import Any, Dict, List, Optional

from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.utils import TypeConverter, DateHandler, is_truthy
from mongo_datatables.datatables.query.regex_utils import safe_regex


def parse_operator(value: str) -> tuple[Optional[str], str]:
    """Parse a leading comparison operator from a value string.

    value: The raw search value string.
    Returns tuple of (operator, stripped_value) where operator is one of
    '>=', '<=', '>', '<', '=' or None if no operator prefix found.
    """
    if value.startswith(">="):
        return ">=", value[2:].strip()
    if value.startswith("<="):
        return "<=", value[2:].strip()
    if value.startswith(">"):
        return ">", value[1:].strip()
    if value.startswith("<"):
        return "<", value[1:].strip()
    if value.startswith("="):
        return "=", value[1:].strip()
    return None, value


def build_number_condition(
    field: str,
    value: str,
    operator: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Build a MongoDB condition for a number field.

    field: Database field name.
    value: String value to convert to number.
    operator: Comparison operator (>, <, >=, <=, =, or None).
    Returns MongoDB condition dict, or None if conversion fails.
    """
    try:
        numeric_value = TypeConverter.to_number(value)
        op_map = {
            ">": {"$gt": numeric_value},
            "<": {"$lt": numeric_value},
            ">=": {"$gte": numeric_value},
            "<=": {"$lte": numeric_value},
        }
        return {field: op_map.get(operator, numeric_value)}
    except (ValueError, TypeError, FieldMappingError):
        return None


def build_date_condition(
    field: str,
    value: str,
    operator: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Build a MongoDB condition for a date field.

    field: Database field name.
    value: Date string in YYYY-MM-DD format.
    operator: Comparison operator (>, <, >=, <=, =, or None).
    Returns MongoDB condition dict, or None if parsing fails.
    """
    try:
        if "-" in value and len(value.split("-")) == 3:
            return {field: DateHandler.get_date_range_for_comparison(value, operator)}
        return {field: {"$regex": re.escape(value), "$options": "i"}}
    except (ValueError, TypeError, FieldMappingError):
        return {field: {"$regex": re.escape(value), "$options": "i"}}


def build_number_column_conditions(
    db_field: str,
    search_value: str,
) -> List[Dict[str, Any]]:
    """Build number column conditions, supporting range (|) and operator syntax.

    db_field: Database field name.
    search_value: The search value string.
    Returns list of MongoDB condition dicts.
    """
    if "|" in search_value:
        parts = search_value.split("|", 1)
        range_cond: Dict[str, Any] = {}
        try:
            if parts[0].strip():
                range_cond["$gte"] = TypeConverter.to_number(parts[0].strip())
            if parts[1].strip():
                range_cond["$lte"] = TypeConverter.to_number(parts[1].strip())
        except (ValueError, TypeError, FieldMappingError):
            pass
        return [{db_field: range_cond}] if range_cond else []
    op, val = parse_operator(search_value)
    cond = build_number_condition(db_field, val, op)
    return [cond] if cond else []


def build_date_column_conditions(
    db_field: str,
    search_value: str,
) -> List[Dict[str, Any]]:
    """Build date column conditions, supporting range (|) and operator syntax.

    db_field: Database field name.
    search_value: The search value string.
    Returns list of MongoDB condition dicts.
    """
    if "|" in search_value:
        parts = search_value.split("|", 1)
        range_cond: Dict[str, Any] = {}
        try:
            if parts[0].strip():
                dr = DateHandler.get_date_range_for_comparison(parts[0].strip(), ">=")
                range_cond["$gte"] = dr.get("$gte")
            if parts[1].strip():
                dr = DateHandler.get_date_range_for_comparison(parts[1].strip(), "<=")
                range_cond["$lt"] = dr.get("$lt")
        except (ValueError, TypeError, FieldMappingError):
            pass
        return [{db_field: range_cond}] if range_cond else []
    op, val = parse_operator(search_value)
    cond = build_date_condition(db_field, val, op)
    return [cond] if cond else []


def build_text_column_conditions(
    db_field: str,
    search_value: str,
    column_search: Dict[str, Any],
    case_insensitive: bool,
) -> List[Dict[str, Any]]:
    """Build text column conditions with smart/regex support.

    db_field: Database field name.
    search_value: The search value string.
    column_search: The column search config dict.
    case_insensitive: Whether to use case-insensitive matching.
    Returns list of MongoDB condition dicts.
    """
    col_ci_raw = column_search.get("caseInsensitive")
    col_ci = is_truthy(col_ci_raw) if col_ci_raw is not None else case_insensitive
    regex_flag = is_truthy(column_search.get("regex"))
    col_smart = is_truthy(column_search.get("smart", True))
    opts = "i" if col_ci else ""

    if col_smart and not regex_flag:
        words = search_value.split()
        if len(words) > 1:
            return [{"$and": [{db_field: {"$regex": re.escape(w), "$options": opts}} for w in words]}]
        return [{db_field: {"$regex": re.escape(search_value), "$options": opts}}]

    try:
        pattern = safe_regex(search_value, regex_flag)
    except ValueError:
        return []
    return [{db_field: {"$regex": pattern, "$options": opts}}]
