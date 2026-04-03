"""Backward-compatible re-export. Use mongo_datatables.datatables.formatting instead."""
from mongo_datatables.datatables.formatting import (
    format_result_values,
    remap_aliases,
    process_cursor,
)

__all__ = ["format_result_values", "remap_aliases", "process_cursor"]
