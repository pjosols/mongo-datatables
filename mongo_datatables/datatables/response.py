"""Build the DataTables JSON response dict."""

import logging
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables.utils import FieldMapper
from mongo_datatables.datatables.results import get_rowgroup_data

logger = logging.getLogger(__name__)


def build_response(
    draw: int,
    count_total_fn: Any,
    count_filtered_fn: Any,
    results_fn: Any,
    get_searchpanes_options_fn: Any,
    parse_extension_config_fn: Any,
    collection: Collection,
    columns: List[Dict[str, Any]],
    field_mapper: FieldMapper,
    filter_doc: Dict[str, Any],
    request_args: Dict[str, Any],
    allow_disk_use: bool,
) -> Dict[str, Any]:
    """Build the complete DataTables JSON response dict.

    draw: DataTables draw counter.
    count_total_fn: Callable returning total record count.
    count_filtered_fn: Callable returning filtered record count.
    results_fn: Callable returning data rows.
    get_searchpanes_options_fn: Callable returning SearchPanes options.
    parse_extension_config_fn: Callable(key) returning extension config or None.
    collection: MongoDB collection.
    columns: Columns config from request.
    field_mapper: FieldMapper instance.
    filter_doc: Active MongoDB filter.
    request_args: Validated request args.
    allow_disk_use: Whether to allow disk use in aggregation.
    Returns DataTables response dict.
    """
    search_return = request_args.get("search", {}).get("return", True)
    records_filtered = -1 if search_return in (False, "false") else count_filtered_fn()
    response: Dict[str, Any] = {
        "draw": draw,
        "recordsTotal": count_total_fn(),
        "recordsFiltered": records_filtered,
        "data": results_fn(),
    }
    if request_args.get("searchPanes"):
        response["searchPanes"] = {"options": get_searchpanes_options_fn()}
    for ext_key in ("fixedColumns", "responsive", "buttons", "select"):
        cfg = parse_extension_config_fn(ext_key)
        if cfg is not None:
            response[ext_key] = cfg
    rowgroup = get_rowgroup_data(
        collection, columns, field_mapper, filter_doc, request_args, allow_disk_use,
    )
    if rowgroup:
        response["rowGroup"] = rowgroup
    return response


def error_response(draw: int, error: Exception) -> Dict[str, Any]:
    """Build a DataTables error response dict.

    draw: DataTables draw counter.
    error: The exception that occurred.
    Returns error response dict.
    """
    return {"draw": draw, "error": str(error), "recordsTotal": 0, "recordsFiltered": 0, "data": []}


def normalize_draw(request_args: Dict[str, Any]) -> None:
    """Normalize the draw value in request_args in-place.

    request_args: Mutable request args dict. Modified in place.
    """
    if not (isinstance(request_args, dict) and "draw" in request_args):
        return
    draw_val = request_args.get("draw")
    if draw_val is None:
        request_args["draw"] = 1
    else:
        try:
            request_args["draw"] = max(1, int(draw_val))
        except (ValueError, TypeError):
            request_args["draw"] = 1
