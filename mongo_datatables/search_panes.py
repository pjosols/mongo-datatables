"""Backward-compatible re-export. Use mongo_datatables.datatables.search.panes instead."""
from mongo_datatables.datatables.search.panes import (  # noqa: F401
    get_searchpanes_options,
    parse_searchpanes_filters,
)
