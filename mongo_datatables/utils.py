"""Utility classes for mongo-datatables library."""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from mongo_datatables.exceptions import FieldMappingError

# Re-exported for backward compatibility
from mongo_datatables.field_utils import FieldMapper, SearchTermParser

__all__ = [
    "is_truthy",
    "TypeConverter",
    "DateHandler",
    "FieldMapper",
    "SearchTermParser",
]

_TRUTHY = frozenset([True, "true", "True", 1])


def is_truthy(value: Any) -> bool:
    """Return True if value is a DataTables truthy boolean (True/"true"/"True"/1)."""
    return value in _TRUTHY


class TypeConverter:
    """Utilities for converting string values to appropriate Python types."""

    @staticmethod
    def to_number(value: str) -> Union[int, float]:
        """Convert a string to int or float.

        value: String representation of a number.
        Returns int if no decimal component, float otherwise.
        Raises FieldMappingError if the value cannot be converted.
        """
        try:
            if "." in value or "e" in value.lower():
                return float(value)
            return int(value)
        except (ValueError, TypeError) as e:
            raise FieldMappingError(f"Cannot convert '{value}' to number") from e

    @staticmethod
    def to_boolean(value: str) -> bool:
        """Convert a string to boolean.

        value: String representation of a boolean.
        Returns True if value is 'true', 'yes', '1', 't', 'y' (case-insensitive).
        """
        return value.lower() in ("true", "yes", "1", "t", "y")

    @staticmethod
    def to_array(value: str) -> List[Any]:
        """Convert a string to a list.

        value: String representation of an array.
        Returns parsed list, or single-element list if not valid JSON array.
        """
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            return [value]

    @staticmethod
    def parse_json(value: str) -> Any:
        """Parse a JSON string into a Python object.

        value: JSON string.
        Returns parsed Python object.
        Raises FieldMappingError if the value is not valid JSON.
        """
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise FieldMappingError(f"Cannot parse JSON: {value}") from e


class DateHandler:
    """Utilities for parsing and manipulating dates."""

    @staticmethod
    def parse_iso_date(date_str: str) -> datetime:
        """Parse an ISO format date string (YYYY-MM-DD).

        date_str: Date string in YYYY-MM-DD format.
        Returns datetime object at midnight.
        Raises FieldMappingError if the date string is invalid.
        """
        try:
            parts = date_str.split("-")
            if len(parts) != 3:
                raise ValueError("Date must be in YYYY-MM-DD format")
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
        except (ValueError, TypeError) as e:
            raise FieldMappingError(f"Cannot parse date '{date_str}': {e}") from e

    @staticmethod
    def parse_iso_datetime(datetime_str: str) -> datetime:
        """Parse an ISO format datetime string.

        datetime_str: ISO format datetime string (handles 'Z' suffix).
        Returns datetime object.
        Raises FieldMappingError if the datetime string is invalid.
        """
        try:
            return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            raise FieldMappingError(f"Cannot parse datetime '{datetime_str}': {e}") from e

    @staticmethod
    def get_next_day(date: datetime) -> datetime:
        """Get the next day after a given date.

        date: datetime object.
        Returns datetime at midnight of the following day.
        """
        return date + timedelta(days=1)

    @staticmethod
    def get_date_range_for_comparison(
        date_str: str,
        operator: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a MongoDB date comparison condition for a date string.

        date_str: Date string in YYYY-MM-DD format.
        operator: Comparison operator (>, <, >=, <=, =, or None for exact match).
        Returns MongoDB query condition dict.
        Raises FieldMappingError if the date string is invalid.
        """
        start_date = DateHandler.parse_iso_date(date_str)
        next_day = DateHandler.get_next_day(start_date)

        if operator == ">":
            return {"$gte": next_day}
        if operator == "<":
            return {"$lt": start_date}
        if operator == ">=":
            return {"$gte": start_date}
        if operator == "<=":
            return {"$lt": next_day}
        if operator in ("=", None):
            return {"$gte": start_date, "$lt": next_day}
        raise FieldMappingError(f"Invalid date comparison operator: {operator}")
