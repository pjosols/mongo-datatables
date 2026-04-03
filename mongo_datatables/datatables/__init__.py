"""Provide DataTables query building and result fetching helpers."""

from mongo_datatables.datatables.filter import (
    build_filter,
    build_sort_specification,
    build_projection,
    get_searchpanes_options,
)
from mongo_datatables.datatables.results import (
    build_pipeline,
    fetch_results,
    get_rowgroup_data,
    count_total,
    count_filtered,
)

__all__ = [
    "build_filter",
    "build_sort_specification",
    "build_projection",
    "get_searchpanes_options",
    "build_pipeline",
    "fetch_results",
    "get_rowgroup_data",
    "count_total",
    "count_filtered",
]


def __getattr__(name: str) -> object:
    """Lazy-load DataTables and DataField to avoid circular imports."""
    if name == "DataTables":
        from mongo_datatables.datatables_core import DataTables
        return DataTables
    if name == "DataField":
        from mongo_datatables.data_field import DataField
        return DataField
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
