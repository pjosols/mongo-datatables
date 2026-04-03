"""Global search query builder for DataTables requests."""

import re
from typing import Any, Dict, List

from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.utils import TypeConverter, FieldMapper
from mongo_datatables.datatables.query.regex_utils import safe_regex


def build_global_search(
    field_mapper: FieldMapper,
    use_text_index: bool,
    has_text_index: bool,
    stemming: bool,
    search_terms: List[str],
    searchable_columns: List[str],
    original_search: str = "",
    search_regex: bool = False,
    search_smart: bool = True,
    case_insensitive: bool = True,
) -> Dict[str, Any]:
    """Build global search conditions across all searchable columns.

    field_mapper: FieldMapper for field name and type lookups.
    use_text_index: Whether to use text indexes when available.
    has_text_index: Whether the collection has a text index.
    stemming: Allow morphological variants when using a text index.
    search_terms: Parsed search terms (without colons).
    searchable_columns: List of searchable column names.
    original_search: Original search string before parsing.
    search_regex: Whether to treat search terms as regex patterns.
    search_smart: Whether to use smart (AND) multi-term search.
    case_insensitive: Whether to perform case-insensitive regex searches.
    Returns MongoDB query condition for global search.
    """
    if not search_terms or not searchable_columns:
        return {}

    was_quoted = bool(
        original_search
        and (re.match(r'^".*"$', original_search) or re.match(r"^'.*'$", original_search))
    )

    if was_quoted and len(search_terms) == 1:
        return _build_quoted_search(
            field_mapper, use_text_index, has_text_index,
            search_terms[0], searchable_columns, original_search, search_regex, case_insensitive,
        )

    if use_text_index and has_text_index and not search_regex and case_insensitive:
        if stemming:
            text_query = " ".join(f"+{t}" for t in search_terms)
        else:
            text_query = " ".join(f'"{t}"' for t in search_terms)
        return {"$text": {"$search": text_query}}

    col_meta = []
    for c in searchable_columns:
        ft = field_mapper.get_field_type(c)
        db = field_mapper.get_db_field(c)
        if ft not in ("date", "keyword"):
            col_meta.append((db, ft))

    if search_smart and len(search_terms) > 1:
        return _build_smart_search(search_terms, col_meta, search_regex, case_insensitive)

    return _build_or_search(search_terms, col_meta, search_regex, case_insensitive)


def _build_quoted_search(
    field_mapper: FieldMapper,
    use_text_index: bool,
    has_text_index: bool,
    term: str,
    searchable_columns: List[str],
    original_search: str,
    search_regex: bool,
    case_insensitive: bool,
) -> Dict[str, Any]:
    """Build search for a single quoted term.

    field_mapper: FieldMapper for field lookups.
    use_text_index: Whether to use text indexes.
    has_text_index: Whether the collection has a text index.
    term: The unquoted search term.
    searchable_columns: List of searchable column names.
    original_search: The original quoted search string.
    search_regex: Whether to treat as regex.
    case_insensitive: Whether to use case-insensitive matching.
    Returns MongoDB query condition.
    """
    if use_text_index and has_text_index and not search_regex and case_insensitive:
        return {"$text": {"$search": original_search}}

    or_conditions = []
    for column in searchable_columns:
        if field_mapper.get_field_type(column) in ("date", "number", "keyword"):
            continue
        try:
            safe = safe_regex(term, search_regex)
        except ValueError:
            continue
        pattern = safe if search_regex else f"\\b{safe}\\b"
        opts = "i" if case_insensitive else ""
        or_conditions.append(
            {field_mapper.get_db_field(column): {"$regex": pattern, "$options": opts}}
        )
    return {"$or": or_conditions} if or_conditions else {}


def _build_smart_search(
    search_terms: List[str],
    col_meta: List[tuple],
    search_regex: bool,
    case_insensitive: bool,
) -> Dict[str, Any]:
    """Build AND-of-OR conditions for multi-term smart search.

    search_terms: List of search terms.
    col_meta: List of (db_field, field_type) tuples.
    search_regex: Whether to treat terms as regex.
    case_insensitive: Whether to use case-insensitive matching.
    Returns MongoDB query condition.
    """
    per_term = []
    for term in search_terms:
        term_conds = _term_or_conditions(term, col_meta, search_regex, case_insensitive)
        if term_conds:
            per_term.append({"$or": term_conds} if len(term_conds) > 1 else term_conds[0])
    return {"$and": per_term} if per_term else {}


def _build_or_search(
    search_terms: List[str],
    col_meta: List[tuple],
    search_regex: bool,
    case_insensitive: bool,
) -> Dict[str, Any]:
    """Build OR conditions across all terms and columns.

    search_terms: List of search terms.
    col_meta: List of (db_field, field_type) tuples.
    search_regex: Whether to treat terms as regex.
    case_insensitive: Whether to use case-insensitive matching.
    Returns MongoDB query condition.
    """
    or_conditions = []
    for term in search_terms:
        or_conditions.extend(_term_or_conditions(term, col_meta, search_regex, case_insensitive))
    return {"$or": or_conditions} if or_conditions else {}


def _term_or_conditions(
    term: str,
    col_meta: List[tuple],
    search_regex: bool,
    case_insensitive: bool,
) -> List[Dict[str, Any]]:
    """Build OR conditions for a single term across columns.

    term: The search term.
    col_meta: List of (db_field, field_type) tuples.
    search_regex: Whether to treat as regex.
    case_insensitive: Whether to use case-insensitive matching.
    Returns list of MongoDB condition dicts.
    """
    conds = []
    for db_field, field_type in col_meta:
        if field_type == "number":
            try:
                conds.append({db_field: TypeConverter.to_number(term)})
            except (ValueError, TypeError, FieldMappingError):
                pass
        else:
            try:
                pattern = safe_regex(term, search_regex)
            except ValueError:
                continue
            opts = "i" if case_insensitive else ""
            conds.append({db_field: {"$regex": pattern, "$options": opts}})
    return conds
