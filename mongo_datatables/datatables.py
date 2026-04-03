"""Server-side processing for jQuery DataTables with MongoDB."""

import logging
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

from mongo_datatables.data_field import DataField
from mongo_datatables.utils import FieldMapper, SearchTermParser, is_truthy
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.request_validator import validate_request_args
from mongo_datatables.dt_filter import build_filter, build_sort_specification, build_projection
from mongo_datatables.dt_filter import get_searchpanes_options as _get_searchpanes_options
from mongo_datatables.dt_results import build_pipeline, fetch_results, get_rowgroup_data
from mongo_datatables.dt_results import count_total as _count_total, count_filtered as _count_filtered

logger = logging.getLogger(__name__)

__all__ = ["DataTables", "DataField"]


class DataTables:
    """Server-side processor for jQuery DataTables with MongoDB integration.

    Translates a DataTables Ajax request into MongoDB aggregation pipelines and
    returns the standard DataTables JSON response. Handles pagination, sorting,
    global/column/colon-syntax search, SearchPanes, SearchBuilder, and fixed searches.
    Pass extra kwargs as a custom MongoDB filter (e.g. ``status="active"``).
    """

    def __init__(
        self,
        pymongo_object: Any,
        collection_name: str,
        request_args: Dict[str, Any],
        data_fields: Optional[List[DataField]] = None,
        use_text_index: bool = True,
        stemming: bool = False,
        allow_disk_use: bool = False,
        row_class: Any = None,
        row_data: Any = None,
        row_attr: Any = None,
        row_id: Optional[str] = None,
        pipeline_stages: Optional[List[Dict[str, Any]]] = None,
        **custom_filter: Any,
    ) -> None:
        """Initialize the DataTables processor.

        pymongo_object: PyMongo client or Flask-PyMongo instance.
        collection_name: MongoDB collection name.
        request_args: DataTables request parameters.
        data_fields: DataField list defining schema.
        use_text_index: Use MongoDB text index when available (default True).
        stemming: Allow morphological variants with text index (default False).
        allow_disk_use: Allow aggregation pipelines to spill to disk.
        row_class/row_data/row_attr: Per-row DT_RowClass/DT_RowData/DT_RowAttr.
        row_id: Field name for DT_RowId (default None uses _id).
        pipeline_stages: Aggregation stages injected before $match.
        **custom_filter: Additional MongoDB filter criteria.
        """
        self.collection = self._get_collection(pymongo_object, collection_name)
        self.request_args = validate_request_args(request_args)
        self.data_fields = data_fields or []
        self.field_mapper = FieldMapper(self.data_fields)
        self.use_text_index = use_text_index
        self.stemming = stemming
        self.allow_disk_use = allow_disk_use
        self.row_class = row_class
        self.row_data = row_data
        self.row_attr = row_attr
        self.row_id = row_id
        self.pipeline_stages = list(pipeline_stages) if pipeline_stages else []
        self.custom_filter = custom_filter

        self._results = None
        self._recordsTotal = None
        self._recordsFiltered = None
        self._filter_cache = None
        self._search_terms_cache = None
        self._has_text_index = None

        self._check_text_index()

        self.query_builder = MongoQueryBuilder(
            field_mapper=self.field_mapper,
            use_text_index=self.use_text_index,
            has_text_index=self.has_text_index,
            stemming=self.stemming,
        )

    @staticmethod
    def _get_collection(pymongo_object: Any, collection_name: str) -> Collection:
        """Resolve a MongoDB collection from a PyMongo client or Flask-PyMongo instance."""
        if isinstance(pymongo_object, Database):
            return pymongo_object[collection_name]
        if hasattr(pymongo_object, "db"):
            return pymongo_object.db[collection_name]
        if hasattr(pymongo_object, "get_database"):
            return pymongo_object.get_database()[collection_name]
        return pymongo_object[collection_name]

    def _check_text_index(self) -> None:
        """Check if the collection has a text index and cache the result."""
        if not self.use_text_index:
            self._has_text_index = False
            return
        try:
            self._has_text_index = any(
                "textIndexVersion" in idx for idx in self.collection.list_indexes()
            )
        except PyMongoError:
            self._has_text_index = False

    @property
    def has_text_index(self) -> bool:
        """True if a text index exists on the collection."""
        return self._has_text_index

    @property
    def columns(self) -> List[Dict[str, Any]]:
        """Columns configuration from the request."""
        cols = self.request_args.get("columns", [])
        return cols if isinstance(cols, list) else []

    @property
    def searchable_columns(self) -> List[str]:
        """List of searchable column data names."""
        return [
            col["data"]
            for col in self.columns
            if isinstance(col, dict) and col.get("data") and is_truthy(col.get("searchable"))
        ]

    @property
    def search_value(self) -> str:
        """Global search value from the request."""
        return self.request_args.get("search", {}).get("value", "")

    @property
    def search_terms(self) -> List[str]:
        """Search terms parsed from the global search value, preserving quoted phrases."""
        if self._search_terms_cache is None:
            self._search_terms_cache = SearchTermParser.parse(self.search_value)
        return self._search_terms_cache

    @property
    def search_terms_without_a_colon(self) -> List[str]:
        """Global search terms (no colon)."""
        return [t for t in self.search_terms if ":" not in t]

    @property
    def search_terms_with_a_colon(self) -> List[str]:
        """Field-specific colon-syntax search terms."""
        return [t for t in self.search_terms if ":" in t]

    @property
    def filter(self) -> Dict[str, Any]:
        """Combined MongoDB filter from all active conditions (cached)."""
        if self._filter_cache is None:
            self._filter_cache = build_filter(
                self.custom_filter, self.query_builder, self.request_args,
                self.field_mapper, self.columns, self.searchable_columns,
                self.search_terms_without_a_colon, self.search_terms_with_a_colon,
                self.search_value,
            )
        return self._filter_cache

    def get_sort_specification(self) -> Dict[str, int]:
        """Generate sort specification from the request."""
        return build_sort_specification(self.request_args, self.columns, self.field_mapper)

    @property
    def sort_specification(self) -> Dict[str, int]:
        """Sort specification (property alias for get_sort_specification)."""
        return self.get_sort_specification()

    @property
    def projection(self) -> Dict[str, int]:
        """MongoDB projection specification."""
        return build_projection(self.columns, self.field_mapper, self.row_id)

    @property
    def start(self) -> int:
        """Pagination start index."""
        try:
            return max(0, int(self.request_args.get("start", 0)))
        except (ValueError, TypeError):
            return 0

    @property
    def limit(self) -> int:
        """Page size."""
        try:
            return int(self.request_args.get("length", 10))
        except (ValueError, TypeError):
            return 10

    @property
    def draw(self) -> int:
        """Draw counter echoed from the request."""
        try:
            return max(1, int(self.request_args.get("draw", 1)))
        except (ValueError, TypeError):
            return 1

    def get_searchpanes_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return option counts for each SearchPanes column."""
        return _get_searchpanes_options(
            self.columns, self.field_mapper, self.custom_filter,
            self.filter, self.collection, self.allow_disk_use,
        )

    def results(self) -> List[Dict[str, Any]]:
        """Execute the MongoDB query and return formatted rows (cached)."""
        if self._results is None:
            pipeline = build_pipeline(
                self.filter, self.pipeline_stages, self.sort_specification,
                self.projection, self.start, self.limit, paginate=True,
            )
            self._results = fetch_results(
                self.collection, pipeline, self.row_id, self.field_mapper,
                self.row_class, self.row_data, self.row_attr, self.allow_disk_use,
            )
        return self._results

    def count_total(self) -> int:
        """Count total records in the collection (cached)."""
        if self._recordsTotal is None:
            self._recordsTotal = _count_total(self.collection, self.custom_filter)
        return self._recordsTotal

    def count_filtered(self) -> int:
        """Count records after applying filters (cached)."""
        if self._recordsFiltered is None:
            self._recordsFiltered = _count_filtered(
                self.collection, self.filter, self.pipeline_stages,
                self.count_total(), self.allow_disk_use,
            )
        return self._recordsFiltered

    def get_export_data(self) -> List[Dict[str, Any]]:
        """Get all data for export without pagination limits."""
        pipeline = build_pipeline(
            self.filter, self.pipeline_stages, self.sort_specification,
            self.projection, self.start, self.limit, paginate=False,
        )
        return fetch_results(
            self.collection, pipeline, self.row_id, self.field_mapper,
            self.row_class, self.row_data, self.row_attr, self.allow_disk_use,
        )

    def _parse_extension_config(self, key: str) -> Optional[Dict[str, Any]]:
        """Return extension config dict for the given request key, or None."""
        val = self.request_args.get(key)
        if not val:
            return None
        if val is True:
            return {}
        return val if isinstance(val, dict) else None

    def get_rows(self) -> Dict[str, Any]:
        """Get the complete DataTables JSON response.

        Returns dict with draw, recordsTotal, recordsFiltered, data, and optional
        extension keys. Includes 'error' key if an unhandled exception occurs.
        """
        try:
            search_return = self.request_args.get("search", {}).get("return", True)
            records_filtered = -1 if search_return in (False, "false") else self.count_filtered()
            response: Dict[str, Any] = {
                "draw": self.draw,
                "recordsTotal": self.count_total(),
                "recordsFiltered": records_filtered,
                "data": self.results(),
            }
            if self.request_args.get("searchPanes"):
                response["searchPanes"] = {"options": self.get_searchpanes_options()}
            for ext_key in ("fixedColumns", "responsive", "buttons", "select"):
                cfg = self._parse_extension_config(ext_key)
                if cfg is not None:
                    response[ext_key] = cfg
            rowgroup = get_rowgroup_data(
                self.collection, self.columns, self.field_mapper,
                self.filter, self.request_args, self.allow_disk_use,
            )
            if rowgroup:
                response["rowGroup"] = rowgroup
            return response
        except PyMongoError as e:
            logger.error("DataTables get_rows MongoDB error: %s", e)
            return {"draw": self.draw, "error": str(e), "recordsTotal": 0, "recordsFiltered": 0, "data": []}
        except (ValueError, TypeError, KeyError) as e:
            logger.error("DataTables get_rows invalid data: %s", e)
            return {"draw": self.draw, "error": str(e), "recordsTotal": 0, "recordsFiltered": 0, "data": []}
