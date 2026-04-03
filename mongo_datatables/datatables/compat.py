"""Backward-compatible re-exports and shim mixin for the datatables subpackage."""

from typing import Any, Dict, List, Optional

from mongo_datatables.datatables.results import (
    count_total,
    count_filtered,
    get_rowgroup_data,
)
from mongo_datatables.datatables.filter import (
    build_filter,
    get_searchpanes_options,
)
from mongo_datatables.datatables.results import build_pipeline, fetch_results

__all__ = [
    "count_total",
    "count_filtered",
    "get_searchpanes_options",
    "DataTablesMixin",
]


class DataTablesMixin:
    """Backward-compatible shim methods for DataTables.

    Provides legacy underscore-prefixed methods and properties that delegate
    to the current public API. Inherit before DataTables core logic.
    """

    def _build_pipeline(self, paginate: bool = True) -> List[Dict[str, Any]]:
        """Build the aggregation pipeline (backward-compatible shim)."""
        return build_pipeline(
            self.filter, self.pipeline_stages, self.sort_specification,
            self.projection, self.start, self.limit, paginate=paginate,
        )

    def _build_filter(self) -> Dict[str, Any]:
        """Build the combined MongoDB filter (backward-compatible shim)."""
        return build_filter(
            self.custom_filter, self.query_builder, self.request_args,
            self.field_mapper, self.columns, self.searchable_columns,
            self.search_terms_without_a_colon, self.search_terms_with_a_colon,
            self.search_value,
        )

    def _process_cursor(self, cursor: Any) -> List[Dict[str, Any]]:
        """Process a cursor into formatted rows (backward-compatible shim)."""
        from mongo_datatables.datatables.formatting import process_cursor
        return process_cursor(cursor, self.row_id, self.field_mapper,
                              self.row_class, self.row_data, self.row_attr)

    def _remap_aliases(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Remap aliased fields in a document (backward-compatible shim)."""
        from mongo_datatables.datatables.formatting import remap_aliases
        return remap_aliases(doc, self.field_mapper)

    def _get_rowgroup_data(self) -> Optional[Dict[str, Any]]:
        """Get RowGroup aggregation data (backward-compatible shim)."""
        return get_rowgroup_data(
            self.collection, self.columns, self.field_mapper,
            self.filter, self.request_args, self.allow_disk_use,
        )

    def _parse_search_builder(self) -> Dict[str, Any]:
        """Parse SearchBuilder payload (backward-compatible shim)."""
        from mongo_datatables.search_builder import parse_search_builder
        return parse_search_builder(self.request_args, self.field_mapper)

    def _parse_searchpanes_filters(self) -> Dict[str, Any]:
        """Parse SearchPanes filters (backward-compatible shim)."""
        from mongo_datatables.search_panes import parse_searchpanes_filters
        return parse_searchpanes_filters(self.request_args, self.field_mapper)

    @staticmethod
    def _filter_has_text(f: Dict[str, Any]) -> bool:
        """Return True if filter contains a $text operator (backward-compatible shim)."""
        from mongo_datatables.datatables.results import filter_has_text
        return filter_has_text(f)

    @property
    def global_search_condition(self) -> Dict[str, Any]:
        """Global search condition (backward-compatible property)."""
        return self.query_builder.build_global_search(
            self.search_terms_without_a_colon,
            self.searchable_columns,
            original_search=self.search_value,
        )

    @property
    def column_specific_search_condition(self) -> Dict[str, Any]:
        """Column-specific search condition (backward-compatible property)."""
        colon_result = self.query_builder.build_column_specific_search(
            self.search_terms_with_a_colon, self.searchable_columns
        )
        if colon_result:
            return colon_result
        return self.query_builder.build_column_search(self.columns)

    def _format_result_values(self, doc: Dict[str, Any]) -> None:
        """Format result values in-place (backward-compatible shim)."""
        from mongo_datatables.datatables.formatting import format_result_values
        format_result_values(doc)
