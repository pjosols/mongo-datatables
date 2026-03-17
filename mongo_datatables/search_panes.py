"""SearchPanes support — aggregation pipeline for pane options and filter parsing."""
import logging
from typing import Any, Dict, List

from bson import Decimal128, ObjectId
from bson.errors import InvalidId as ObjectIdError

from mongo_datatables.utils import TypeConverter, DateHandler, is_truthy
from mongo_datatables.exceptions import FieldMappingError

logger = logging.getLogger(__name__)


def get_searchpanes_options(
    columns: List[Dict[str, Any]],
    field_mapper,
    custom_filter: Dict[str, Any],
    current_filter: Dict[str, Any],
    collection,
    allow_disk_use: bool,
) -> Dict[str, List[Dict[str, Any]]]:
    """Generate SearchPanes options with both ``total`` and ``count`` per value.

    Uses a single ``$facet`` aggregation for all columns, reducing MongoDB
    round-trips from 2N to exactly 2 regardless of column count.

    DataTables SearchPanes server-side protocol requires two counts per option:
    - ``total``: count across the base dataset (custom_filter only, no search/pane filters)
    - ``count``: count with all current filters applied

    Returns:
        Dictionary mapping column names to their option lists
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
    except Exception as e:
        logger.error(f"Error generating SearchPanes options: {str(e)}")
        return {col_name: [] for col_name, _ in eligible}

    def _hashable(v):
        return str(v.to_decimal()) if isinstance(v, Decimal128) else v

    options = {}
    for col_name, _, __ in eligible:
        total_map = {_hashable(r["_id"]): r["count"] for r in total_result.get(col_name, [])}
        count_map = {_hashable(r["_id"]): r["count"] for r in count_result.get(col_name, [])}
        column_options = []
        for raw_value, total in sorted(total_map.items(), key=lambda x: -x[1])[:1000]:
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


def parse_searchpanes_filters(request_args: Dict[str, Any], field_mapper) -> Dict[str, Any]:
    """Parse SearchPanes filter parameters from request.

    Returns:
        MongoDB query conditions for SearchPanes filters
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

        db_field = field_mapper.get_db_field(column_name)
        field_type = field_mapper.get_field_type(column_name)

        # Convert values based on field type
        converted_values = []
        for value in selected_values:
            if field_type == "number":
                try:
                    converted_values.append(TypeConverter.to_number(value))
                except (ValueError, TypeError):
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

        if converted_values:
            conditions.append({db_field: {"$in": converted_values}})

    if conditions:
        return {"$and": conditions}
    return {}
