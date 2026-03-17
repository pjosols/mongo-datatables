"""SearchBuilder tree evaluator — converts DataTables SearchBuilder payloads to MongoDB queries."""
import re
from datetime import timedelta
from typing import Any, Dict

from mongo_datatables.utils import TypeConverter, DateHandler
from mongo_datatables.exceptions import FieldMappingError


def parse_search_builder(request_args: Dict[str, Any], field_mapper) -> Dict[str, Any]:
    """Translate a SearchBuilder criteria tree into a MongoDB query.

    The DataTables SearchBuilder extension sends a nested ``searchBuilder``
    parameter when ``serverSide: true`` is enabled.  Each leaf criterion has
    the shape::

        {
            "condition": "=",
            "origData": "salary",
            "type": "num",
            "value": ["50000"]
        }

    Groups are nested via a ``criteria`` list and a ``logic`` key
    (``"AND"`` or ``"OR"``).

    Returns:
        MongoDB query dict, or ``{}`` if no SearchBuilder data is present.
    """
    import json as _json
    sb = request_args.get("searchBuilder")
    if not sb:
        return {}
    # Some DataTables/SearchBuilder versions deliver searchBuilder as a
    # JSON string rather than a decoded object (depends on pipeline and
    # how submitAs:'json' interacts with extension preXhr handlers).
    if isinstance(sb, str):
        try:
            sb = _json.loads(sb)
        except (ValueError, TypeError):
            return {}
    if not isinstance(sb, dict):
        return {}
    return _sb_group(sb, field_mapper)


def _sb_group(group: Dict[str, Any], field_mapper) -> Dict[str, Any]:
    """Recursively convert a SearchBuilder group to a MongoDB condition."""
    logic = group.get("logic", "AND").upper()
    mongo_op = "$and" if logic == "AND" else "$or"
    parts = []
    for criterion in group.get("criteria", []):
        if "criteria" in criterion:
            sub = _sb_group(criterion, field_mapper)
            if sub:
                parts.append(sub)
        else:
            cond = _sb_criterion(criterion, field_mapper)
            if cond:
                parts.append(cond)
    if not parts:
        return {}
    return {mongo_op: parts} if len(parts) > 1 else parts[0]


def _sb_criterion(criterion: Dict[str, Any], field_mapper) -> Dict[str, Any]:
    """Convert a single SearchBuilder leaf criterion to a MongoDB condition."""
    condition = criterion.get("condition", "")
    orig_data = criterion.get("origData") or criterion.get("data", "")
    values = criterion.get("value", [])
    sb_type = criterion.get("type", "string")

    if not orig_data or not condition:
        return {}

    db_field = field_mapper.get_db_field(orig_data)
    v0 = values[0] if values else None
    v1 = values[1] if len(values) > 1 else criterion.get("value2")

    # null / not-null — type-aware: num/date only check None; string/html also check empty string
    if condition == "null":
        if sb_type in ("num", "num-fmt", "html-num", "html-num-fmt", "date", "moment", "luxon"):
            return {db_field: None}
        return {db_field: {"$in": [None, ""]}}
    if condition == "!null":
        if sb_type in ("num", "num-fmt", "html-num", "html-num-fmt", "date", "moment", "luxon"):
            return {db_field: {"$ne": None}}
        return {db_field: {"$nin": [None, ""]}}

    if sb_type in ("num", "num-fmt", "html-num", "html-num-fmt"):
        return _sb_number(db_field, condition, v0, v1)
    if sb_type in ("date", "moment", "luxon"):
        return _sb_date(db_field, condition, v0, v1)
    # string / html / array / default
    return _sb_string(db_field, condition, v0)


def _sb_number(field: str, condition: str, v0, v1) -> Dict[str, Any]:
    """Build a MongoDB condition for a numeric SearchBuilder criterion."""
    def _n(v):
        return TypeConverter.to_number(v)
    try:
        if condition == "=":        return {field: _n(v0)}
        if condition == "!=":       return {field: {"$ne": _n(v0)}}
        if condition == "<":        return {field: {"$lt": _n(v0)}}
        if condition == "<=":       return {field: {"$lte": _n(v0)}}
        if condition == ">":        return {field: {"$gt": _n(v0)}}
        if condition == ">=":       return {field: {"$gte": _n(v0)}}
        if condition == "between":  return {field: {"$gte": _n(v0), "$lte": _n(v1)}}
        if condition == "!between": return {"$or": [{field: {"$lt": _n(v0)}}, {field: {"$gt": _n(v1)}}]}
    except (ValueError, TypeError, FieldMappingError):
        pass
    return {}


def _sb_date(field: str, condition: str, v0, v1) -> Dict[str, Any]:
    """Build a MongoDB condition for a date SearchBuilder criterion."""
    def _d(v):
        return DateHandler.parse_iso_date(v.split('T')[0])
    try:
        if condition == "=":
            d = _d(v0)
            return {field: {"$gte": d, "$lt": d + timedelta(days=1)}}
        if condition == "!=":
            d = _d(v0)
            return {"$or": [{field: {"$lt": d}}, {field: {"$gte": d + timedelta(days=1)}}]}
        if condition == "<":        return {field: {"$lt": _d(v0)}}
        if condition == "<=":       return {field: {"$lt": _d(v0) + timedelta(days=1)}}
        if condition == ">":        return {field: {"$gt": _d(v0)}}
        if condition == ">=":       return {field: {"$gte": _d(v0)}}
        if condition == "between":  return {field: {"$gte": _d(v0), "$lt": _d(v1) + timedelta(days=1)}}
        if condition == "!between": return {"$or": [{field: {"$lt": _d(v0)}}, {field: {"$gte": _d(v1) + timedelta(days=1)}}]}
    except (ValueError, TypeError, FieldMappingError):
        pass
    return {}


def _sb_string(field: str, condition: str, v0) -> Dict[str, Any]:
    """Build a MongoDB condition for a string SearchBuilder criterion."""
    if v0 is None:
        return {}
    s = re.escape(v0)
    if condition == "=":         return {field: {"$regex": f"^{s}$", "$options": "i"}}
    if condition == "!=":        return {field: {"$not": {"$regex": f"^{s}$", "$options": "i"}}}
    if condition == "contains":  return {field: {"$regex": s, "$options": "i"}}
    if condition == "!contains": return {field: {"$not": {"$regex": s, "$options": "i"}}}
    if condition == "starts":    return {field: {"$regex": f"^{s}", "$options": "i"}}
    if condition == "!starts":   return {field: {"$not": {"$regex": f"^{s}", "$options": "i"}}}
    if condition == "ends":      return {field: {"$regex": f"{s}$", "$options": "i"}}
    if condition == "!ends":     return {field: {"$not": {"$regex": f"{s}$", "$options": "i"}}}
    return {}
