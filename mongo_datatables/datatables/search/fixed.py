"""searchFixed support — parses DataTables 2.x fixed/named searches into MongoDB filters."""
from typing import Any, Dict, List

from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.field_utils import FieldMapper
from mongo_datatables.utils import SearchTermParser, is_truthy


def parse_search_fixed(request_args: Dict[str, Any], query_builder: MongoQueryBuilder, searchable_columns: List[str]) -> Dict[str, Any]:
    """Parse searchFixed named searches (DataTables 2.0+) into a MongoDB filter.

    Supports both the DataTables 2.x wire format (``search.fixed`` array of
    ``{name, term}`` objects) and the legacy dict format (``searchFixed`` dict).
    Entries with ``term == "function"`` are skipped (client-side-only functions).

    Each named fixed search is ANDed with the main query. Values are treated
    as global search terms across all searchable columns.
    """
    # DataTables 2.x wire format: search.fixed is an array of {name, term}
    fixed_array = request_args.get("search", {}).get("fixed", [])
    # Legacy/custom format: top-level searchFixed dict
    legacy_dict = request_args.get("searchFixed", {})

    terms_to_apply = []
    if isinstance(fixed_array, list):
        for entry in fixed_array:
            term = entry.get("term") if isinstance(entry, dict) else None
            if term and term != "function":
                terms_to_apply.append(term)
    if isinstance(legacy_dict, dict):
        for value in legacy_dict.values():
            if value:
                terms_to_apply.append(str(value))

    search_config = request_args.get("search", {}) if isinstance(request_args.get("search"), dict) else {}
    search_regex = is_truthy(search_config.get("regex", False))
    case_insensitive = is_truthy(search_config.get("caseInsensitive", True))
    conditions = []
    for value in terms_to_apply:
        parsed = SearchTermParser.parse(str(value))
        cond = query_builder.build_global_search(
            parsed, searchable_columns, original_search=str(value),
            search_regex=search_regex, case_insensitive=case_insensitive
        )
        if cond:
            conditions.append(cond)
    if not conditions:
        return {}
    return {"$and": conditions} if len(conditions) > 1 else conditions[0]


def parse_column_search_fixed(columns: List[Dict[str, Any]], field_mapper: FieldMapper, query_builder: MongoQueryBuilder) -> Dict[str, Any]:
    """Parse per-column searchFixed (DataTables 2.0+) into a MongoDB filter.

    Supports both the DataTables 2.x wire format (``columns[i].search.fixed``
    array of ``{name, term}`` objects) and the legacy dict format
    (``columns[i].searchFixed`` dict). Entries with ``term == "function"`` are
    skipped. Returns MongoDB query condition, or ``{}`` if no column-level fixed searches exist.
    """
    conditions = []
    for col in columns:
        db_field = field_mapper.get_db_field(col.get("data", ""))
        if not db_field:
            continue

        # DataTables 2.x wire format: col.search.fixed array
        fixed_array = col.get("search", {}).get("fixed", []) if isinstance(col.get("search"), dict) else []
        # Legacy format: col.searchFixed dict
        legacy_dict = col.get("searchFixed", {})

        col_terms = []
        if isinstance(fixed_array, list):
            for entry in fixed_array:
                term = entry.get("term") if isinstance(entry, dict) else None
                if term and term != "function":
                    col_terms.append(term)
        if isinstance(legacy_dict, dict):
            for value in legacy_dict.values():
                if value:
                    col_terms.append(str(value))

        existing_search = col.get("search", {}) if isinstance(col.get("search"), dict) else {}
        for value in col_terms:
            cond = query_builder.build_column_search(
                [{**col, "search": {
                    "value": str(value),
                    "regex": False,
                    "smart": existing_search.get("smart", True),
                    "caseInsensitive": existing_search.get("caseInsensitive", True),
                }}]
            )
            if cond:
                conditions.append(cond)

    if not conditions:
        return {}
    return {"$and": conditions} if len(conditions) > 1 else conditions[0]
