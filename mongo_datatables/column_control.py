"""Backward-compatible re-export. Use mongo_datatables.datatables.query.column_control instead."""
from mongo_datatables.datatables.query.column_control import build_column_control_conditions

__all__ = ["build_column_control_conditions"]
