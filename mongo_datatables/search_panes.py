"""SearchPanes support — aggregation pipeline for pane options and filter parsing."""
import logging
from typing import Any, Dict, List

from bson import Decimal128, ObjectId
from bson.errors import InvalidId as ObjectIdError
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables.utils import TypeConverter, DateHandler, FieldMapper, is_truthy
from mongo_datatables.exceptions import FieldMappingError, InvalidDataError
from mongo_datatables.editor.validator import validate_field_name
from mongo_datatables.datatables._limits import MAX_FACET_BRANCHES, MAX_PANE_OPTIONS

logger = logging.getLogger(__name__)


def get_searchpanes_options(
    columns: List[Dict[str, Any]],
    field_mapper: FieldMapper,
    custom_filter: Dict[str, Any],
    current_filter: Dict[str, Any],
    collection: Collection,
    allow_disk_use: bool,
) -> Dict[str, List[Dict[str, Any]]]:
    """Generate SearchPanes options with total and count per value.

    columns: searchable column definitions.
    field_mapper: maps column names to database fields and types.
    custom_filter: base dataset filter (no search/pane filters).
    current_filter: filter with all search and pane selections applied.
    collection: MongoDB collection.
    allow_disk_use: allow aggregation to spill to disk.
    Returns dict mapping column names to option lists with label, value, total, and count.
    """
    eligible = [
        (col.get("data"), field_mapper.get_db_field(col.get("data")), field_mapper.get_field_type(col.get("data")))
        for col in columns
        if is_truthy(col.get("searchable"))
        and col.get("data")
        and field_mapper.get_field_type(col.get("data")) != "object"
    ]
    if not eligible:
        return {}

    if len(eligible) > MAX_FACET_BRANCHES:
        logger.warning(
            "SearchPanes eligible columns truncated from %d to %d",
            len(eligible),
            MAX_FACET_BRANCHES,
        )
        eligible = eligible[:MAX_FACET_BRANCHES]

    # Array fields need $unwind before $group so individual elements become pane options
    facet_branches = {}
    for col_name, db_field, field_type in eligible:
        stages = []
        if field_type == "array":
            stages.append({"$unwind": f"${db_field}"})
        stages.extend([
            {"$group": {"_id": f"${db_field}", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": None}}},
        ])
        facet_branches[col_name] = stages
    total_pipeline = ([{"$match": custom_filter}] if custom_filter else []) + [{"$facet": facet_branches}]
    count_pipeline = ([{"$match": current_filter}] if current_filter else []) + [{"$facet": facet_branches}]

    try:
        total_docs = list(collection.aggregate(total_pipeline, allowDiskUse=allow_disk_use))
        total_result = total_docs[0] if total_docs else {}
        count_docs = list(collection.aggregate(count_pipeline, allowDiskUse=allow_disk_use))
        count_result = count_docs[0] if count_docs else {}
    except PyMongoError as e:
        logger.error("Error generating SearchPanes options: %s", str(e))
        return {col_name: [] for col_name, *_ in eligible}

    def _hashable(v: Any) -> Any:
        return str(v.to_decimal()) if isinstance(v, Decimal128) else v

    options = {}
    for col_name, _, __ in eligible:
        total_map = {_hashable(r["_id"]): r["count"] for r in total_result.get(col_name, [])}
        count_map = {_hashable(r["_id"]): r["count"] for r in count_result.get(col_name, [])}
        column_options = []
        for raw_value, total in sorted(total_map.items(), key=lambda x: -x[1])[:MAX_PANE_OPTIONS]:
            if isinstance(raw_value, ObjectId):
                display_value = str(raw_value)
            elif hasattr(raw_value, 'isoformat'):
                display_value = raw_value.isoformat()
            else:
                display_value = str(raw_value) if raw_value is not None else ""
            column_options.append({
                "label": display_value,
                "value": display_value,
                "total": total,
                "count": count_map.get(_hashable(raw_value), 0),
            })
        options[col_name] = column_options
    return options


def parse_searchpanes_filters(request_args: Dict[str, Any], field_mapper: FieldMapper) -> Dict[str, Any]:
    """Parse SearchPanes filter selections into MongoDB query conditions.

    request_args: DataTables request parameters.
    field_mapper: maps column names to database fields and types.
    Returns MongoDB $and query or empty dict if no filters selected.
    """
    conditions = []

    # Check for searchPanes parameter in request
    searchpanes = request_args.get("searchPanes", {})

    # If searchPanes is just a boolean flag, no filters to apply
    if not isinstance(searchpanes, dict):
        return {}

    for column_name, selected_values in searchpanes.items():
        if not selected_values:
            continue

        try:
            validate_field_name(column_name)
        except InvalidDataError as e:
            logger.debug("SearchPanes field validation failed for %r: %s", column_name, e)
            continue

        # DataTables SearchPanes may send selections as {"0": "val"} instead of ["val"]
        if isinstance(selected_values, dict):
            selected_values = list(selected_values.values())
        elif not isinstance(selected_values, list):
            continue

        # Reject non-scalar values; cap list length
        selected_values = [
            v for v in selected_values if isinstance(v, (str, int, float, bool)) or v is None
        ][:MAX_PANE_OPTIONS]
        if not selected_values:
            continue

        db_field = field_mapper.get_db_field(column_name)
        field_type = field_mapper.get_field_type(column_name)

        # Convert values based on field type
        converted_values = []
        for value in selected_values:
            if field_type == "number":
                try:
                    converted_values.append(TypeConverter.to_number(value))
                except (ValueError, TypeError, FieldMappingError):
                    converted_values.append(value)
            elif field_type == "objectid":
                try:
                    converted_values.append(ObjectId(value))
                except (ObjectIdError, ValueError):
                    converted_values.append(value)
            elif field_type == "date":
                try:
                    converted_values.append(DateHandler.parse_iso_date(value.split('T')[0]))
                except FieldMappingError:
                    converted_values.append(value)
            else:
                converted_values.append(value)

        conditions.append({db_field: {"$in": converted_values}})

    if conditions:
        return {"$and": conditions}
    return {}
