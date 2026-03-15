"""MongoDB query builder for DataTables requests.

This module provides the MongoQueryBuilder class that constructs MongoDB
queries from DataTables request parameters, handling global search, column
search, and field-specific search with comparison operators.
"""

import logging
import re
from typing import Any, Dict, List, Optional

from mongo_datatables.exceptions import QueryBuildError
from mongo_datatables.utils import TypeConverter, DateHandler, FieldMapper

logger = logging.getLogger(__name__)


class MongoQueryBuilder:
    """Builds MongoDB queries from DataTables request parameters.

    This class handles:
    - Global search across multiple columns (with text index support)
    - Column-specific searches
    - Field:value syntax for targeted searches
    - Comparison operators (>, <, >=, <=, =) for numbers and dates
    """

    def __init__(
        self,
        field_mapper: FieldMapper,
        use_text_index: bool = True,
        has_text_index: bool = False
    ):
        """Initialize the query builder.

        Args:
            field_mapper: FieldMapper instance for field name and type lookups
            use_text_index: Whether to use text indexes when available
            has_text_index: Whether the collection has a text index
        """
        self.field_mapper = field_mapper
        self.use_text_index = use_text_index
        self.has_text_index = has_text_index

    def build_column_search(
        self,
        columns: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Build search conditions for individual column searches.

        Args:
            columns: List of column configurations from DataTables request

        Returns:
            MongoDB query condition for column-specific searches
        """
        conditions = []

        for column in columns:
            column_search = column.get("search", {})
            search_value = column_search.get("value", "")

            if search_value and column.get("searchable", False):
                column_name = column["data"]
                field_type = self.field_mapper.get_field_type(column_name)

                if field_type == "number":
                    try:
                        numeric_value = TypeConverter.to_number(search_value)
                        conditions.append({column_name: numeric_value})
                    except Exception:
                        pass
                elif field_type == "date":
                    conditions.append(
                        {column_name: {"$regex": search_value, "$options": "i"}}
                    )
                else:
                    regex_flag = column_search.get("regex", False)
                    pattern = search_value if regex_flag else re.escape(search_value)
                    conditions.append({column_name: {"$regex": pattern, "$options": "i"}})

        if conditions:
            return {"$and": conditions}
        return {}

    def build_global_search(
        self,
        search_terms: List[str],
        searchable_columns: List[str],
        original_search: str = "",
        search_regex: bool = False
    ) -> Dict[str, Any]:
        """Build global search conditions.

        This method uses text indexes when available for better performance.
        For quoted terms, it performs exact phrase matching.
        For non-quoted terms, it uses OR semantics to match any of the terms.

        Args:
            search_terms: List of parsed search terms (without colons)
            searchable_columns: List of searchable column names
            original_search: Original search string before parsing (for quote detection)

        Returns:
            MongoDB query condition for global search
        """
        if not search_terms:
            return {}

        if not searchable_columns:
            return {}

        was_quoted = False
        if original_search:
            if re.match(r'^".*"$', original_search) or re.match(r"^'.*'$", original_search):
                was_quoted = True

        if was_quoted and len(search_terms) == 1:
            if self.use_text_index and self.has_text_index:
                return {"$text": {"$search": original_search}}

            or_conditions = []
            for column in searchable_columns:
                field_type = self.field_mapper.get_field_type(column)

                if field_type in ("date", "number"):
                    continue

                regex_term = search_terms[0] if search_regex else re.escape(search_terms[0])
                pattern = regex_term if search_regex else f"\\b{regex_term}\\b"
                or_conditions.append({column: {"$regex": pattern, "$options": "i"}})

            if or_conditions:
                return {"$or": or_conditions}
            return {}

        if self.use_text_index and self.has_text_index:
            text_search_query = " ".join(search_terms)
            return {"$text": {"$search": text_search_query}}

        or_conditions = []
        for term in search_terms:
            for column in searchable_columns:
                field_type = self.field_mapper.get_field_type(column)

                if field_type == "date":
                    continue

                if field_type == "number":
                    try:
                        numeric_value = TypeConverter.to_number(term)
                        or_conditions.append({column: numeric_value})
                    except Exception:
                        pass
                else:
                    pattern = term if search_regex else re.escape(term)
                    or_conditions.append({column: {"$regex": pattern, "$options": "i"}})

        if or_conditions:
            return {"$or": or_conditions}
        return {}

    def build_column_specific_search(
        self,
        colon_terms: List[str],
        searchable_columns: List[str]
    ) -> Dict[str, Any]:
        """Build search conditions for column-specific searches using colon syntax.

        Handles search terms in the format "field:value" for targeted column searching.
        Also supports comparison operators: >, <, >=, <=, = for numeric and date fields.

        Args:
            colon_terms: List of search terms containing exactly one colon
            searchable_columns: List of searchable column names

        Returns:
            MongoDB query condition for column-specific searches
        """
        and_conditions = []

        for term in colon_terms:
            field, value = term.split(":", 1)
            field = field.strip()
            value = value.strip()

            if not field or not value:
                continue

            db_field = self.field_mapper.get_db_field(field)

            if field not in searchable_columns and db_field not in searchable_columns:
                continue

            field_type = self.field_mapper.get_field_type(db_field)

            operator = None
            if value.startswith(">") and not value.startswith(">="):
                operator = ">"
                value = value[1:].strip()
            elif value.startswith("<") and not value.startswith("<="):
                operator = "<"
                value = value[1:].strip()
            elif value.startswith(">="):
                operator = ">="
                value = value[2:].strip()
            elif value.startswith("<="):
                operator = "<="
                value = value[2:].strip()
            elif value.startswith("="):
                operator = "="
                value = value[1:].strip()

            if field_type == "number":
                condition = self._build_number_condition(db_field, value, operator)
                if condition:
                    and_conditions.append(condition)
            elif field_type == "date":
                condition = self._build_date_condition(db_field, value, operator)
                if condition:
                    and_conditions.append(condition)
            else:
                and_conditions.append({db_field: {"$regex": value, "$options": "i"}})

        if and_conditions:
            return {"$and": and_conditions}
        return {}

    def _build_number_condition(
        self,
        field: str,
        value: str,
        operator: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Build a MongoDB condition for a number field.

        Args:
            field: Database field name
            value: String value to convert to number
            operator: Comparison operator (>, <, >=, <=, =, or None)

        Returns:
            MongoDB condition dict, or None if conversion fails
        """
        try:
            numeric_value = TypeConverter.to_number(value)

            if operator == ">":
                return {field: {"$gt": numeric_value}}
            elif operator == "<":
                return {field: {"$lt": numeric_value}}
            elif operator == ">=":
                return {field: {"$gte": numeric_value}}
            elif operator == "<=":
                return {field: {"$lte": numeric_value}}
            elif operator == "=":
                return {field: numeric_value}
            else:
                return {field: numeric_value}
        except Exception:
            return {field: {"$regex": value, "$options": "i"}}

    def _build_date_condition(
        self,
        field: str,
        value: str,
        operator: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Build a MongoDB condition for a date field.

        Args:
            field: Database field name
            value: Date string in YYYY-MM-DD format
            operator: Comparison operator (>, <, >=, <=, =, or None)

        Returns:
            MongoDB condition dict, or None if parsing fails
        """
        try:
            if '-' in value and len(value.split('-')) == 3:
                date_condition = DateHandler.get_date_range_for_comparison(value, operator)
                return {field: date_condition}
            else:
                return {field: {"$regex": value, "$options": "i"}}
        except Exception:
            return {field: {"$regex": value, "$options": "i"}}
