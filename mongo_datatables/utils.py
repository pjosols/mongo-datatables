"""Utility classes for mongo-datatables library.

This module provides reusable utility classes for type conversion, date handling,
field mapping, and search term parsing used throughout the mongo-datatables library.
"""

import json
import logging
import re
import shlex
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from mongo_datatables.exceptions import FieldMappingError

logger = logging.getLogger(__name__)


class TypeConverter:
    """Utilities for converting string values to appropriate Python types.

    This class provides static methods for converting string representations
    to numbers, booleans, dates, and JSON objects/arrays.
    """

    @staticmethod
    def to_number(value: str) -> Union[int, float]:
        """Convert a string to int or float.

        Args:
            value: String representation of a number

        Returns:
            int if the number has no decimal component, float otherwise

        Raises:
            FieldMappingError: If the value cannot be converted to a number
        """
        try:
            # Try to detect if it's a float or int
            # Check for decimal point or scientific notation (e/E)
            if '.' in value or 'e' in value.lower():
                return float(value)
            else:
                return int(value)
        except (ValueError, TypeError) as e:
            raise FieldMappingError(f"Cannot convert '{value}' to number") from e

    @staticmethod
    def to_boolean(value: str) -> bool:
        """Convert a string to boolean.

        Args:
            value: String representation of a boolean

        Returns:
            True if value is 'true', 'yes', '1', 't', 'y' (case-insensitive), False otherwise
        """
        return value.lower() in ('true', 'yes', '1', 't', 'y')

    @staticmethod
    def to_array(value: str) -> List[Any]:
        """Convert a string to array/list.

        Attempts to parse as JSON array. If that fails, returns a single-element list.

        Args:
            value: String representation of an array

        Returns:
            Parsed list or single-element list containing the value
        """
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            # If parsed but not a list, wrap it
            return [parsed]
        except json.JSONDecodeError:
            # If not valid JSON, use the value as a single element
            return [value]

    @staticmethod
    def parse_json(value: str) -> Any:
        """Parse a JSON string into a Python object.

        Args:
            value: JSON string

        Returns:
            Parsed Python object (dict, list, or scalar)

        Raises:
            FieldMappingError: If the value is not valid JSON
        """
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise FieldMappingError(f"Cannot parse JSON: {value}") from e


class DateHandler:
    """Utilities for parsing and manipulating dates.

    This class provides static methods for date parsing, formatting, and
    arithmetic operations, with proper handling of timezone and date boundaries.
    """

    @staticmethod
    def parse_iso_date(date_str: str) -> datetime:
        """Parse an ISO format date string (YYYY-MM-DD).

        Args:
            date_str: Date string in YYYY-MM-DD format

        Returns:
            datetime object at midnight (00:00:00)

        Raises:
            FieldMappingError: If the date string is not in valid ISO format
        """
        try:
            parts = date_str.split('-')
            if len(parts) != 3:
                raise ValueError("Date must be in YYYY-MM-DD format")
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
            return datetime(year, month, day)
        except (ValueError, TypeError) as e:
            raise FieldMappingError(f"Cannot parse date '{date_str}': {e}") from e

    @staticmethod
    def parse_iso_datetime(datetime_str: str) -> datetime:
        """Parse an ISO format datetime string.

        Handles both with and without timezone information, including 'Z' suffix.

        Args:
            datetime_str: ISO format datetime string

        Returns:
            datetime object

        Raises:
            FieldMappingError: If the datetime string is not valid
        """
        try:
            # Handle 'Z' suffix for UTC timezone
            clean_str = datetime_str.replace('Z', '+00:00')
            return datetime.fromisoformat(clean_str)
        except (ValueError, TypeError) as e:
            raise FieldMappingError(f"Cannot parse datetime '{datetime_str}': {e}") from e

    @staticmethod
    def get_next_day(date: datetime) -> datetime:
        """Get the next day after a given date.

        Uses timedelta to properly handle month and year boundaries.

        Args:
            date: datetime object

        Returns:
            datetime object representing the next day at midnight
        """
        return date + timedelta(days=1)

    @staticmethod
    def get_date_range_for_comparison(
        date_str: str,
        operator: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build a MongoDB date comparison condition for a date string.

        Handles comparison operators (>, <, >=, <=, =) properly by considering
        that dates in the database may have time components.

        Args:
            date_str: Date string in YYYY-MM-DD format
            operator: Comparison operator (>, <, >=, <=, =, or None for exact match)

        Returns:
            MongoDB query condition dict (e.g., {"$gte": date, "$lt": next_day})

        Raises:
            FieldMappingError: If the date string is invalid
        """
        start_date = DateHandler.parse_iso_date(date_str)
        next_day = DateHandler.get_next_day(start_date)

        if operator == '>':
            # Greater than: dates strictly after the specified date
            return {"$gte": next_day}
        elif operator == '<':
            # Less than: dates strictly before the specified date
            return {"$lt": start_date}
        elif operator == '>=':
            # Greater than or equal: dates on or after the specified date
            return {"$gte": start_date}
        elif operator == '<=':
            # Less than or equal: dates on or before the end of the specified date
            return {"$lt": next_day}
        elif operator == '=' or operator is None:
            # Exact match: the whole day
            return {"$gte": start_date, "$lt": next_day}
        else:
            raise FieldMappingError(f"Invalid date comparison operator: {operator}")


class FieldMapper:
    """Manages field name mappings between UI and database representations.

    This class handles:
    - Mapping UI field aliases to database field names
    - Reverse mapping from database to UI field names
    - Field type lookup for both UI and database field names
    """

    def __init__(self, data_fields: List[Any]):
        """Initialize field mapper with DataField objects.

        Args:
            data_fields: List of DataField objects defining field mappings
        """
        from mongo_datatables.datatables import DataField

        self.data_fields = data_fields or []

        # Build mapping dictionaries
        self.field_types: Dict[str, str] = {}
        self.ui_to_db: Dict[str, str] = {}
        self.db_to_ui: Dict[str, str] = {}

        for field in self.data_fields:
            if isinstance(field, DataField):
                # Store field type by database field name
                self.field_types[field.name] = field.data_type

                # Map UI alias to database field name
                self.ui_to_db[field.alias] = field.name

                # Reverse mapping
                self.db_to_ui[field.name] = field.alias

    def get_db_field(self, ui_field: str) -> str:
        """Map a UI field name to its database field name.

        Args:
            ui_field: UI field name or alias

        Returns:
            Database field name, or the original name if no mapping exists
        """
        return self.ui_to_db.get(ui_field, ui_field)

    def get_ui_field(self, db_field: str) -> str:
        """Map a database field name to its UI field name.

        Args:
            db_field: Database field name

        Returns:
            UI field name or alias, or the original name if no mapping exists
        """
        return self.db_to_ui.get(db_field, db_field)

    def get_field_type(self, field_name: str) -> Optional[str]:
        """Get the data type for a field.

        Tries both the field name as-is and as a database field name.

        Args:
            field_name: Field name (can be UI or database field name)

        Returns:
            Field type string, or None if not found
        """
        # Try direct lookup
        if field_name in self.field_types:
            return self.field_types[field_name]

        # Try mapping from UI to DB first
        db_field = self.get_db_field(field_name)
        return self.field_types.get(db_field)


class SearchTermParser:
    """Utilities for parsing search terms with quoted phrase support.

    This class handles search strings that may contain quoted phrases,
    treating quoted text as single search terms.
    """

    @staticmethod
    def parse(search_value: str) -> List[str]:
        """Extract search terms from a search string.

        Handles quoted phrases (both single and double quotes) as single terms.
        For example, 'Author:Robert "Jonathan Kennedy"' is parsed as two terms:
        ['Author:Robert', 'Jonathan Kennedy'].

        Args:
            search_value: Search string to parse

        Returns:
            List of search terms with quoted phrases preserved as single terms
        """
        if not search_value:
            return []

        try:
            # Use shlex to handle quoted strings properly
            # shlex naturally handles both single and double quotes
            return shlex.split(search_value)
        except ValueError as e:
            # If shlex fails (e.g., unclosed quotes), fall back to simple split
            logger.warning(f"Malformed search syntax '{search_value}': {e}. Using simple split.")
            return search_value.split()
