"""Backward-compatible re-export. Use mongo_datatables.datatables.query.regex_utils instead."""
from mongo_datatables.datatables.query.regex_utils import validate_regex, safe_regex

__all__ = ["validate_regex", "safe_regex"]
