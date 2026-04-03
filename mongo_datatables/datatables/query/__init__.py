"""MongoDB query builder for DataTables requests."""

import re
from typing import Any, Dict, List, Optional

from mongo_datatables.utils import FieldMapper, TypeConverter, DateHandler, is_truthy
from mongo_datatables.datatables.query.conditions import (
    parse_operator,
    build_number_condition,
    build_date_condition,
    build_number_column_conditions,
    build_date_column_conditions,
    build_text_column_conditions,
)
from mongo_datatables.datatables.query.column_control import build_column_control_conditions
from mongo_datatables.datatables.query.global_search import build_global_search as _build_global_search

__all__ = ["MongoQueryBuilder", "TypeConverter", "DateHandler"]


class MongoQueryBuilder:
    """Builds MongoDB queries from DataTables request parameters.

    Handles global search across multiple columns (with text index support),
    column-specific searches, field:value colon syntax, and comparison operators
    (>, <, >=, <=, =) for numbers and dates.
    """

    def __init__(
        self,
        field_mapper: FieldMapper,
        use_text_index: bool = True,
        has_text_index: bool = False,
        stemming: bool = False,
    ) -> None:
        """Initialize the query builder.

        field_mapper: FieldMapper instance for field name and type lookups.
        use_text_index: Whether to use text indexes when available.
        has_text_index: Whether the collection has a text index.
        stemming: Allow morphological variants when using a text index.
        """
        self.field_mapper = field_mapper
        self.use_text_index = use_text_index
        self.has_text_index = has_text_index
        self.stemming = stemming

    def build_column_search(
        self,
        columns: List[Dict[str, Any]],
        case_insensitive: bool = True,
    ) -> Dict[str, Any]:
        """Build search conditions for individual column searches.

        columns: List of column configurations from DataTables request.
        case_insensitive: Whether to perform case-insensitive regex searches.
        Returns MongoDB query condition for column-specific searches.
        """
        conditions: List[Dict[str, Any]] = []

        for column in columns:
            column_search = column.get("search", {})
            search_value = column_search.get("value", "")
            cc = column.get("columnControl")
            has_cc = cc and isinstance(cc, dict)

            if (search_value and is_truthy(column.get("searchable"))) or has_cc:
                column_name = column.get("name") or column["data"]
                field_type = self.field_mapper.get_field_type(column_name)
                db_field = self.field_mapper.get_db_field(column_name)

                if search_value and is_truthy(column.get("searchable")):
                    conditions.extend(
                        _build_single_column_conditions(
                            db_field, field_type, search_value, column_search, case_insensitive
                        )
                    )

                if has_cc:
                    conditions.extend(build_column_control_conditions(db_field, field_type, cc))

        return {"$and": conditions} if conditions else {}

    def build_global_search(
        self,
        search_terms: List[str],
        searchable_columns: List[str],
        original_search: str = "",
        search_regex: bool = False,
        search_smart: bool = True,
        case_insensitive: bool = True,
    ) -> Dict[str, Any]:
        """Build global search conditions across all searchable columns.

        search_terms: Parsed search terms (without colons).
        searchable_columns: List of searchable column names.
        original_search: Original search string before parsing.
        search_regex: Whether to treat search terms as regex patterns.
        search_smart: Whether to use smart (AND) multi-term search.
        case_insensitive: Whether to perform case-insensitive regex searches.
        Returns MongoDB query condition for global search.
        """
        return _build_global_search(
            self.field_mapper, self.use_text_index, self.has_text_index, self.stemming,
            search_terms, searchable_columns, original_search, search_regex,
            search_smart, case_insensitive,
        )

    def build_column_specific_search(
        self,
        colon_terms: List[str],
        searchable_columns: List[str],
        case_insensitive: bool = True,
    ) -> Dict[str, Any]:
        """Build search conditions for field:value colon-syntax terms.

        colon_terms: List of search terms containing exactly one colon.
        searchable_columns: List of searchable column names.
        case_insensitive: Whether to use case-insensitive matching.
        Returns MongoDB query condition for column-specific searches.
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
            operator, value = parse_operator(value)

            if field_type == "number":
                cond = build_number_condition(db_field, value, operator)
                if cond:
                    and_conditions.append(cond)
            elif field_type == "date":
                cond = build_date_condition(db_field, value, operator)
                if cond:
                    and_conditions.append(cond)
            elif field_type == "keyword":
                and_conditions.append({db_field: value})
            else:
                opts = "i" if case_insensitive else ""
                and_conditions.append({db_field: {"$regex": re.escape(value), "$options": opts}})

        return {"$and": and_conditions} if and_conditions else {}

    def _build_number_condition(
        self, field: str, value: str, operator: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Build a number condition (backward-compatible shim).

        field: Database field name.
        value: String value to compare.
        operator: Comparison operator or None.
        Returns MongoDB condition dict or None if value is invalid.
        """
        return build_number_condition(field, value, operator)

    def _build_date_condition(
        self, field: str, value: str, operator: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Build a date condition (backward-compatible shim).

        field: Database field name.
        value: Date string.
        operator: Comparison operator or None.
        Returns MongoDB condition dict or None if value is invalid.
        """
        return build_date_condition(field, value, operator)

    def _build_column_control_condition(
        self, field: str, field_type: str, cc: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Build column control conditions (backward-compatible shim).

        field: Database field name.
        field_type: Field type string.
        cc: Column control configuration dict.
        Returns list of MongoDB condition dicts.
        """
        return build_column_control_conditions(field, field_type, cc)


def _build_single_column_conditions(
    db_field: str,
    field_type: str,
    search_value: str,
    column_search: Dict[str, Any],
    case_insensitive: bool,
) -> List[Dict[str, Any]]:
    """Build conditions for a single column's search value.

    db_field: Database field name.
    field_type: Field type string.
    search_value: The search value.
    column_search: The column search config dict.
    case_insensitive: Whether to use case-insensitive matching.
    Returns list of MongoDB condition dicts.
    """
    if field_type == "number":
        return build_number_column_conditions(db_field, search_value)
    if field_type == "keyword":
        return [{db_field: search_value}]
    if field_type == "date":
        return build_date_column_conditions(db_field, search_value)
    return build_text_column_conditions(db_field, search_value, column_search, case_insensitive)
