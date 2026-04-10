"""Provide backward-compatible shim methods and re-exports for the datatables subpackage."""

from typing import Any, Dict, List, Optional

from mongo_datatables.utils import is_truthy

from mongo_datatables.datatables.results import (
    count_total,
    count_filtered,
)
from mongo_datatables.datatables.search.panes import get_searchpanes_options

__all__ = [
    "count_total",
    "count_filtered",
    "get_searchpanes_options",
    "DataTablesMixin",
]

_ROWGROUP_STRIP_KEYS = {"startRender", "endRender"}


class DataTablesMixin:
    """Backward-compatible shim methods for DataTables.

    Provides legacy underscore-prefixed methods and properties that delegate
    to the current public API. Inherit before DataTables core logic.
    """

    def _process_cursor(self, cursor: Any) -> List[Dict[str, Any]]:
        """Format cursor rows for backward compatibility.
        
        cursor: MongoDB cursor or list of documents.
        Returns list of formatted row dicts.
        """
        from mongo_datatables.datatables.formatting import process_cursor
        return process_cursor(cursor, self.row_id, self.field_mapper,
                              self.row_class, self.row_data, self.row_attr)

    def _remap_aliases(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Remap aliased fields in a document for backward compatibility.
        
        doc: document with aliased field names.
        Returns document with original field names.
        """
        from mongo_datatables.datatables.formatting import remap_aliases
        return remap_aliases(doc, self.field_mapper)

    def _parse_search_builder(self) -> Dict[str, Any]:
        """Parse SearchBuilder payload for backward compatibility.
        
        Returns MongoDB filter dict from SearchBuilder criteria.
        """
        from mongo_datatables.datatables.search.builder import parse_search_builder
        return parse_search_builder(self.request_args, self.field_mapper)

    def _parse_searchpanes_filters(self) -> Dict[str, Any]:
        """Parse SearchPanes filters for backward compatibility.
        
        Returns MongoDB filter dict from SearchPanes selections.
        """
        from mongo_datatables.datatables.search.panes import parse_searchpanes_filters
        return parse_searchpanes_filters(self.request_args, self.field_mapper)

    @staticmethod
    def _filter_has_text(f: Dict[str, Any]) -> bool:
        """Return True if filter contains a $text operator for backward compatibility.
        
        f: MongoDB filter dict.
        Returns True if filter includes $text search.
        """
        from mongo_datatables.datatables.results import filter_has_text
        return filter_has_text(f)

    @property
    def column_specific_search_condition(self) -> Dict[str, Any]:
        """Build column-specific search condition for backward compatibility.
        
        Returns MongoDB filter from colon-syntax terms or per-column search.
        """
        colon_result = self.query_builder.build_column_specific_search(
            self.search_terms_with_a_colon, self.searchable_columns
        )
        if colon_result:
            return colon_result
        return self.query_builder.build_column_search(self.columns)

    def _format_result_values(self, doc: Dict[str, Any]) -> None:
        """Format result values in-place for backward compatibility.
        
        doc: document to format (modified in-place).
        """
        from mongo_datatables.datatables.formatting import format_result_values
        format_result_values(doc)

    @property
    def column_search_conditions(self) -> Dict[str, Any]:
        """Build per-column search conditions for backward compatibility.
        
        Returns MongoDB filter from per-column search inputs.
        """
        return self.query_builder.build_column_search(self.columns)

    @property
    def global_search_condition(self) -> Dict[str, Any]:
        """Global search condition built from the current request (compat shim).
        
        Returns MongoDB filter from global search box.
        """
        search = self.request_args.get("search", {})
        return self.query_builder.build_global_search(
            self.search_terms_without_a_colon,
            self.searchable_columns,
            original_search=self.search_value,
            search_regex=is_truthy(search.get("regex", False)),
            search_smart=is_truthy(search.get("smart", True)),
            case_insensitive=is_truthy(search.get("caseInsensitive", True)),
        )

    def _parse_search_fixed(self) -> Dict[str, Any]:
        """Parse searchFixed into a MongoDB filter for backward compatibility.
        
        Returns MongoDB filter from fixed search criteria.
        """
        from mongo_datatables.datatables.search.fixed import parse_search_fixed
        return parse_search_fixed(self.request_args, self.query_builder, self.searchable_columns)

    def _parse_column_search_fixed(self) -> Dict[str, Any]:
        """Parse per-column searchFixed into a MongoDB filter for backward compatibility.
        
        Returns MongoDB filter from per-column fixed search criteria.
        """
        from mongo_datatables.datatables.search.fixed import parse_column_search_fixed
        return parse_column_search_fixed(self.columns, self.field_mapper, self.query_builder)

    def _parse_rowgroup_config(self) -> Optional[Dict[str, Any]]:
        """Parse rowGroup config from request args, excluding render callbacks.

        Returns dict with dataSrc and other keys (except startRender/endRender),
        or None if rowGroup is absent or has no dataSrc.
        """
        config = self._parse_extension_config("rowGroup")
        if config is None or "dataSrc" not in config:
            return None
        return {k: v for k, v in config.items() if k not in _ROWGROUP_STRIP_KEYS}
