"""SearchBuilder tree evaluator — converts DataTables SearchBuilder payloads to MongoDB queries."""
import json as _json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from mongo_datatables.utils import TypeConverter, DateHandler, FieldMapper
from mongo_datatables.exceptions import FieldMappingError, InvalidDataError
from mongo_datatables.editor.validator import validate_field_name

_log = logging.getLogger(__name__)


_MAX_SB_DEPTH = 10
_MAX_SB_CRITERIA = 100
_MAX_VALUE_LEN = 1024

_VALID_CONDITIONS = frozenset({
    "=", "!=", "<", "<=", ">", ">=",
    "between", "!between",
    "contains", "!contains",
    "starts", "!starts",
    "ends", "!ends",
    "null", "!null",
})

_VALID_TYPES = frozenset({
    "string", "html", "num", "num-fmt", "html-num", "html-num-fmt",
    "date", "moment", "luxon", "array",
})


def parse_search_builder(request_args: Dict[str, Any], field_mapper: FieldMapper) -> Dict[str, Any]:
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
    counter: List[int] = [0]
    return _sb_group(sb, field_mapper, depth=0, counter=counter)


def _sb_group(group: Dict[str, Any], field_mapper: FieldMapper, depth: int = 0, counter: Optional[List[int]] = None) -> Dict[str, Any]:
    """Recursively convert a SearchBuilder group to a MongoDB condition.

    depth: current recursion depth; aborts at _MAX_SB_DEPTH to prevent DoS.
    counter: mutable single-element list tracking total criteria processed.
    """
    if counter is None:
        counter = [0]
    if depth >= _MAX_SB_DEPTH:
        return {}
    logic = group.get("logic", "AND").upper()
    mongo_op = "$and" if logic == "AND" else "$or"
    parts = []
    criteria = group.get("criteria", [])
    if not isinstance(criteria, list):
        return {}
    for criterion in criteria:
        if counter[0] >= _MAX_SB_CRITERIA:
            break
        counter[0] += 1
        if not isinstance(criterion, dict):
            continue
        if "criteria" in criterion:
            sub = _sb_group(criterion, field_mapper, depth=depth + 1, counter=counter)
            if sub:
                parts.append(sub)
        else:
            cond = _sb_criterion(criterion, field_mapper)
            if cond:
                parts.append(cond)
    if not parts:
        return {}
    return {mongo_op: parts} if len(parts) > 1 else parts[0]


def _sb_criterion(criterion: Dict[str, Any], field_mapper: FieldMapper) -> Dict[str, Any]:
    """Convert a single SearchBuilder leaf criterion to a MongoDB condition."""
    condition = criterion.get("condition", "")
    orig_data = criterion.get("origData") or criterion.get("data", "")
    values = criterion.get("value", [])
    if not isinstance(values, list):
        values = [values] if values is not None else []
    # Reject non-scalar values to prevent injection via nested objects/lists
    values = [v for v in values if isinstance(v, (str, int, float, bool)) or v is None]
    # Cap string value lengths to prevent regex/resource exhaustion
    values = [v[:_MAX_VALUE_LEN] if isinstance(v, str) else v for v in values]
    sb_type = criterion.get("type", "string")

    if not orig_data or not condition:
        return {}

    if condition not in _VALID_CONDITIONS:
        return {}

    if sb_type not in _VALID_TYPES:
        return {}

    try:
        validate_field_name(orig_data)
    except (ValueError, TypeError, InvalidDataError):
        return {}

    db_field = field_mapper.get_db_field(orig_data)
    v0 = values[0] if values else None
    raw_v1 = values[1] if len(values) > 1 else criterion.get("value2")
    v1 = raw_v1 if isinstance(raw_v1, (str, int, float, bool)) or raw_v1 is None else None

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


def _sb_number(field: str, condition: str, v0: Any, v1: Any) -> Dict[str, Any]:
    """Build a MongoDB condition for a numeric SearchBuilder criterion."""
    def _n(v: Any) -> Union[int, float]:
        return TypeConverter.to_number(v)
    try:
        if condition == "=":
            return {field: _n(v0)}
        if condition == "!=":
            return {field: {"$ne": _n(v0)}}
        if condition == "<":
            return {field: {"$lt": _n(v0)}}
        if condition == "<=":
            return {field: {"$lte": _n(v0)}}
        if condition == ">":
            return {field: {"$gt": _n(v0)}}
        if condition == ">=":
            return {field: {"$gte": _n(v0)}}
        if condition == "between":
            return {field: {"$gte": _n(v0), "$lte": _n(v1)}}
        if condition == "!between":
            return {"$or": [{field: {"$lt": _n(v0)}}, {field: {"$gt": _n(v1)}}]}
    except (ValueError, TypeError, FieldMappingError) as exc:
        _log.debug("Skipping numeric criterion for field %r condition %r: %s", field, condition, exc)
    return {}


def _sb_date(field: str, condition: str, v0: Any, v1: Any) -> Dict[str, Any]:
    """Build a MongoDB condition for a date SearchBuilder criterion."""
    def _d(v: Any) -> datetime:
        return DateHandler.parse_iso_date(v.split('T')[0])
    try:
        if condition == "=":
            d = _d(v0)
            return {field: {"$gte": d, "$lt": d + timedelta(days=1)}}
        if condition == "!=":
            d = _d(v0)
            return {"$or": [{field: {"$lt": d}}, {field: {"$gte": d + timedelta(days=1)}}]}
        if condition == "<":
            return {field: {"$lt": _d(v0)}}
        if condition == "<=":
            return {field: {"$lt": _d(v0) + timedelta(days=1)}}
        if condition == ">":
            return {field: {"$gt": _d(v0)}}
        if condition == ">=":
            return {field: {"$gte": _d(v0)}}
        if condition == "between":
            return {field: {"$gte": _d(v0), "$lt": _d(v1) + timedelta(days=1)}}
        if condition == "!between":
            return {"$or": [{field: {"$lt": _d(v0)}}, {field: {"$gte": _d(v1) + timedelta(days=1)}}]}
    except (ValueError, TypeError, FieldMappingError) as exc:
        _log.debug("Skipping date criterion for field %r condition %r: %s", field, condition, exc)
    return {}


def _sb_string(field: str, condition: str, v0: Any) -> Dict[str, Any]:
    """Build a MongoDB condition for a string SearchBuilder criterion."""
    if v0 is None:
        return {}
    s = re.escape(v0)
    if condition == "=":
        return {field: {"$regex": f"^{s}$", "$options": "i"}}
    if condition == "!=":
        return {field: {"$not": {"$regex": f"^{s}$", "$options": "i"}}}
    if condition == "contains":
        return {field: {"$regex": s, "$options": "i"}}
    if condition == "!contains":
        return {field: {"$not": {"$regex": s, "$options": "i"}}}
    if condition == "starts":
        return {field: {"$regex": f"^{s}", "$options": "i"}}
    if condition == "!starts":
        return {field: {"$not": {"$regex": f"^{s}", "$options": "i"}}}
    if condition == "ends":
        return {field: {"$regex": f"{s}$", "$options": "i"}}
    if condition == "!ends":
        return {field: {"$not": {"$regex": f"{s}$", "$options": "i"}}}
    return {}
