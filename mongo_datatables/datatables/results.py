"""Fetch and count DataTables results from MongoDB aggregation pipelines."""

import logging
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables.datatables.formatting import process_cursor

logger = logging.getLogger(__name__)


def filter_has_text(f: Dict[str, Any]) -> bool:
    """Return True if the filter contains a $text operator at any depth.

    f: MongoDB filter dict to inspect.
    Returns True if $text is present anywhere in the filter.
    """
    if "$text" in f:
        return True
    for v in f.values():
        if isinstance(v, list):
            if any(isinstance(item, dict) and filter_has_text(item) for item in v):
                return True
    return False


def build_pipeline(
    current_filter: Dict[str, Any],
    pipeline_stages: List[Dict[str, Any]],
    sort_specification: Dict[str, int],
    projection: Dict[str, int],
    start: int,
    limit: int,
    paginate: bool = True,
) -> List[Dict[str, Any]]:
    """Build the aggregation pipeline for results or export.

    current_filter: Combined active MongoDB filter.
    pipeline_stages: Pre-match stages (e.g. $lookup, $addFields).
    sort_specification: MongoDB sort dict.
    projection: MongoDB projection dict.
    start: Pagination offset.
    limit: Page size (0 means no limit).
    paginate: If True, include $skip and $limit stages.
    Returns list of MongoDB aggregation pipeline stages.
    """
    pipeline: List[Dict[str, Any]] = []
    if current_filter and filter_has_text(current_filter):
        pipeline.append({"$match": current_filter})
        pipeline.extend(pipeline_stages)
    else:
        pipeline.extend(pipeline_stages)
        if current_filter:
            pipeline.append({"$match": current_filter})
    pipeline.append({"$sort": sort_specification})
    if paginate:
        if start > 0:
            pipeline.append({"$skip": start})
        if limit and limit > 0:
            pipeline.append({"$limit": limit})
    pipeline.append({"$project": projection})
    return pipeline


def fetch_results(
    collection: Collection,
    pipeline: List[Dict[str, Any]],
    row_id: Optional[str],
    field_mapper: Any,
    row_class: Any,
    row_data: Any,
    row_attr: Any,
    allow_disk_use: bool,
) -> List[Dict[str, Any]]:
    """Execute an aggregation pipeline and return formatted rows.

    collection: MongoDB collection to query.
    pipeline: Aggregation pipeline stages.
    row_id: Optional field name for DT_RowId.
    field_mapper: FieldMapper for alias resolution.
    row_class: Static string or callable for DT_RowClass.
    row_data: Static dict or callable for DT_RowData.
    row_attr: Static dict or callable for DT_RowAttr.
    allow_disk_use: Whether to allow disk use in aggregation.
    Returns list of formatted document dicts.
    """
    try:
        cursor = collection.aggregate(pipeline, allowDiskUse=allow_disk_use)
    except PyMongoError as e:
        logger.error("MongoDB aggregation error in fetch_results(): %s", e, exc_info=True)
        return []
    except Exception as e:
        logger.error("Unexpected error in fetch_results(): %s", e, exc_info=True)
        return []
    try:
        return process_cursor(cursor, row_id, field_mapper, row_class, row_data, row_attr)
    except (ValueError, TypeError) as e:
        logger.error("Result formatting error in fetch_results(): %s", e, exc_info=True)
        return []
    except Exception as e:
        logger.error("Unexpected error in fetch_results(): %s", e, exc_info=True)
        return []


def count_total(
    collection: Collection,
    custom_filter: Dict[str, Any],
) -> int:
    """Count total records in the collection.

    Uses estimated_document_count() for large collections without a custom
    filter, and count_documents() for accuracy otherwise.

    collection: MongoDB collection.
    custom_filter: Base filter from DataTables kwargs.
    Returns total document count, or 0 on error.
    """
    try:
        estimated = collection.estimated_document_count()
        if estimated < 100000 or custom_filter:
            return collection.count_documents(custom_filter)
        return estimated
    except PyMongoError as e:
        logger.error("Error counting total records: %s", e, exc_info=True)
        try:
            return collection.count_documents(custom_filter)
        except PyMongoError:
            return 0


def count_filtered(
    collection: Collection,
    current_filter: Dict[str, Any],
    pipeline_stages: List[Dict[str, Any]],
    total_count: int,
    allow_disk_use: bool,
) -> int:
    """Count records after applying filters.

    collection: MongoDB collection.
    current_filter: Combined active MongoDB filter.
    pipeline_stages: Pre-match stages.
    total_count: Pre-computed total (used when filter is empty).
    allow_disk_use: Whether to allow disk use in aggregation.
    Returns filtered document count, or 0 on error.
    """
    if not current_filter:
        return total_count
    try:
        pipeline = list(pipeline_stages) + [{"$match": current_filter}, {"$count": "total"}]
        result = list(collection.aggregate(pipeline, allowDiskUse=allow_disk_use))
        return result[0]["total"] if result else 0
    except PyMongoError as e:
        logger.debug("Aggregation failed, using count_documents: %s", e)
        try:
            return collection.count_documents(current_filter)
        except PyMongoError:
            logger.error("count_documents also failed, returning 0", exc_info=True)
            return 0
    except (ValueError, TypeError) as e:
        logger.error("Invalid data in count_filtered(): %s", e, exc_info=True)
        return 0
    except Exception as e:
        logger.error("Unexpected error in count_filtered(): %s", e, exc_info=True)
        return 0


def _resolve_rowgroup_field(
    data_src: Any,
    columns: List[Dict[str, Any]],
    field_mapper: Any,
) -> Optional[str]:
    """Resolve a rowGroup dataSrc to a MongoDB field name.

    data_src: Column index (int) or field name (str) from rowGroup config.
    columns: Columns list from the DataTables request.
    field_mapper: FieldMapper for alias resolution.
    Returns the MongoDB field name, or None if it cannot be resolved.
    """
    if isinstance(data_src, int):
        if data_src >= len(columns):
            return None
        field_name = columns[data_src].get("data")
    else:
        field_name = data_src
    return field_mapper.get_db_field(field_name) if field_name else None


def get_rowgroup_data(
    collection: Collection,
    columns: List[Dict[str, Any]],
    field_mapper: Any,
    current_filter: Dict[str, Any],
    request_args: Dict[str, Any],
    allow_disk_use: bool,
) -> Optional[Dict[str, Any]]:
    """Generate RowGroup aggregation data using MongoDB pipeline.

    collection: MongoDB collection.
    columns: Columns list from request_args.
    field_mapper: FieldMapper for alias resolution.
    current_filter: Combined active MongoDB filter.
    request_args: Validated DataTables request parameters.
    allow_disk_use: Whether to allow disk use in aggregation.
    Returns dict with dataSrc and groups if rowGroup is configured and the
    field can be resolved; None otherwise. Callers should treat None as
    "no rowGroup data available" and omit the key from the response.
    """
    rowgroup_params = request_args.get("rowGroup")
    if not rowgroup_params:
        return None
    data_src = rowgroup_params.get("dataSrc")
    if not isinstance(data_src, (str, int)):
        return None

    mongo_field = _resolve_rowgroup_field(data_src, columns, field_mapper)
    if not mongo_field:
        return None

    try:
        pipeline: List[Dict[str, Any]] = []
        if current_filter:
            pipeline.append({"$match": current_filter})
        pipeline.append({"$group": {"_id": f"${mongo_field}", "count": {"$sum": 1}}})
        pipeline.append({"$sort": {"_id": 1}})
        groups = list(collection.aggregate(pipeline, allowDiskUse=allow_disk_use))
        group_data = {
            str(g["_id"]) if g["_id"] is not None else "null": {"count": g["count"]}
            for g in groups
        }
        return {"dataSrc": data_src, "groups": group_data}
    except PyMongoError as e:
        logger.error("Error generating RowGroup data: %s", e, exc_info=True)
        return None
