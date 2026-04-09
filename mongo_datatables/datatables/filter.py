"""Build MongoDB filters and sort specifications for DataTables queries."""

import logging
from typing import Any, Dict, List, Optional

from mongo_datatables.utils import FieldMapper, is_truthy
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.datatables.search.builder import parse_search_builder
from mongo_datatables.datatables.search.fixed import parse_search_fixed, parse_column_search_fixed
from mongo_datatables.datatables.search.panes import parse_searchpanes_filters

logger = logging.getLogger(__name__)


def build_filter(
    custom_filter: Dict[str, Any],
    query_builder: MongoQueryBuilder,
    request_args: Dict[str, Any],
    field_mapper: FieldMapper,
    columns: List[Dict[str, Any]],
    searchable_columns: List[str],
    search_terms_without_a_colon: List[str],
    search_terms_with_a_colon: List[str],
    search_value: str,
) -> Dict[str, Any]:
    """Build the combined MongoDB filter from all active conditions.

    custom_filter: Base filter from DataTables kwargs.
    query_builder: MongoQueryBuilder instance.
    request_args: Validated DataTables request parameters.
    field_mapper: FieldMapper for alias resolution.
    columns: Columns list from request_args.
    searchable_columns: List of searchable column data names.
    search_terms_without_a_colon: Global search terms.
    search_terms_with_a_colon: Field-specific colon-syntax terms.
    search_value: Raw global search string.
    Returns combined MongoDB query dict.
    """
    conditions = []
    search = request_args.get("search", {})
    case_insensitive = is_truthy(search.get("caseInsensitive", True))

    if custom_filter:
        conditions.append(custom_filter)

    sb = parse_search_builder(request_args, field_mapper)
    if sb:
        conditions.append(sb)

    sp = parse_searchpanes_filters(request_args, field_mapper)
    if sp:
        conditions.append(sp)

    sf = parse_search_fixed(request_args, query_builder, searchable_columns)
    if sf:
        conditions.append(sf)

    csf = parse_column_search_fixed(columns, field_mapper, query_builder)
    if csf:
        conditions.append(csf)

    global_search = query_builder.build_global_search(
        search_terms_without_a_colon,
        searchable_columns,
        original_search=search_value,
        search_regex=is_truthy(search.get("regex", False)),
        search_smart=is_truthy(search.get("smart", True)),
        case_insensitive=case_insensitive,
    )
    if global_search:
        conditions.append(global_search)

    col_search = query_builder.build_column_search(columns, case_insensitive=case_insensitive)
    if col_search:
        conditions.append(col_search)

    col_specific = query_builder.build_column_specific_search(
        search_terms_with_a_colon,
        searchable_columns,
        case_insensitive=case_insensitive,
    )
    if col_specific:
        conditions.append(col_specific)

    if len(conditions) > 1:
        return {"$and": conditions}
    if len(conditions) == 1:
        return conditions[0]
    return {}


def build_sort_specification(
    request_args: Dict[str, Any],
    columns: List[Dict[str, Any]],
    field_mapper: FieldMapper,
) -> Dict[str, int]:
    """Generate sort specification from the DataTables request.

    request_args: Validated DataTables request parameters.
    columns: Columns list from request_args.
    field_mapper: FieldMapper for alias resolution.
    Returns MongoDB sort specification dict.
    """
    sort_spec: Dict[str, int] = {}
    for order_info in request_args.get("order", []):
        if not isinstance(order_info, dict):
            continue
        try:
            col_idx = int(order_info["column"])
        except (KeyError, ValueError, TypeError):
            continue
        order_name = order_info.get("name", "")
        column = None
        if order_name:
            column = next(
                (c for c in columns if c.get("name") == order_name or c.get("data") == order_name),
                None,
            )
        if column is None and 0 <= col_idx < len(columns):
            column = columns[col_idx]
        if column is None:
            continue
        direction = 1 if order_info.get("dir", "asc") == "asc" else -1
        order_data = column.get("orderData")
        if order_data is not None:
            indices = [order_data] if isinstance(order_data, int) else list(order_data)
            for idx in indices:
                if 0 <= idx < len(columns):
                    target = columns[idx]
                    if is_truthy(target.get("orderable", True)):
                        field = target.get("data")
                        if field:
                            db_field = field_mapper.get_db_field(field)
                            if db_field not in sort_spec:
                                sort_spec[db_field] = direction
        else:
            ui_field = column.get("data")
            if ui_field and is_truthy(column.get("orderable", True)):
                db_field = field_mapper.get_db_field(ui_field)
                if db_field not in sort_spec:
                    sort_spec[db_field] = direction
    if "_id" not in sort_spec:
        sort_spec["_id"] = 1
    return sort_spec


def build_projection(
    columns: List[Dict[str, Any]],
    field_mapper: FieldMapper,
    row_id: Optional[str] = None,
) -> Dict[str, int]:
    """Generate projection specification to select fields.

    columns: Columns list from request_args.
    field_mapper: FieldMapper for alias resolution.
    row_id: Optional field name to use as DT_RowId.
    Returns MongoDB projection specification dict.
    """
    projection: Dict[str, int] = {"_id": 1}
    for column in columns:
        if isinstance(column, dict) and column.get("data"):
            projection[field_mapper.get_db_field(column["data"])] = 1
    if row_id:
        projection[field_mapper.get_db_field(row_id)] = 1
    return projection


