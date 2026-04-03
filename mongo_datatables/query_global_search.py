"""Backward-compatible re-export. Use mongo_datatables.datatables.query.global_search instead."""
from mongo_datatables.datatables.query.global_search import build_global_search

__all__ = ["build_global_search"]
