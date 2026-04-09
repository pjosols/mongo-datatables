"""Backward-compatible re-export. Use mongo_datatables.datatables.search.fixed instead."""
from mongo_datatables.datatables.search.fixed import (  # noqa: F401
    parse_search_fixed,
    parse_column_search_fixed,
)
