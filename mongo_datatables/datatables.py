"""Server-side processing for jQuery DataTables with MongoDB."""

import logging

from typing import Dict, List, Any, Optional
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables.utils import FieldMapper, SearchTermParser, is_truthy
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.search_builder import parse_search_builder
from mongo_datatables.search_fixed import parse_search_fixed, parse_column_search_fixed
from mongo_datatables.formatting import format_result_values, remap_aliases, process_cursor
from mongo_datatables.search_panes import get_searchpanes_options as _get_searchpanes_options_fn, parse_searchpanes_filters

logger = logging.getLogger(__name__)


class DataField:
    """Represents a data field with MongoDB and DataTables column mapping.
    
    This class defines a field name in MongoDB with its full path (including parent objects),
    a column alias mapping, and type information for proper data handling and optimized 
    searching. It servers two purposes:
    
    1. Maps an alias name to a fully nested field name in MongoDB. For example for a nested
    field PublisherInfo.Date, with an alias of 'Published', a search like "Published:2001-12-12"
    results in query for {"PublisherInfo.Date": ... }
    
    2. Specified a field type for optimized searching, enabling comparison operators on 
    numeric and date fields. All other types are treated as text. For keys where indexes
    are available, an optimized text search is performed. Without indexes, a case-insensitive
    regex search is performed.
    
    Data type handling:
        - 'number': Supports exact matching and comparison operators (>, <, >=, <=, =)
        - 'date': Supports date parsing and comparison operators for ISO format dates
        - All other types: Treated as text with case-insensitive regex search
    
    Attributes:
        name: The full field path in the database (e.g., 'PublisherInfo.Date')
        data_type: The type of data stored in this field (e.g., 'string', 'number', 'date')
        alias: The name to display in the UI (defaults to the last part of the field path)
    """
    
    # Valid MongoDB data types used in this application
    # all other types are treated as text with regex search
    VALID_TYPES = frozenset(['string', 'keyword', 'number', 'date', 'boolean', 'array', 'object', 'objectid', 'null'])
    
    def __init__(self, name: str, data_type: str, alias: str = None):
        """Initialize a DataField.
        
        Args:
            name: The full field path in MongoDB (e.g., 'Title' or 'PublisherInfo.Date')
            data_type: The data type of the field (must be a valid MongoDB type)
                       'number' and 'date' have special search handling with comparison operators
                       'objectid' has special formatting for output
                       All other types are treated as text with regex search
            alias: Optional UI display name (defaults to the field name if not provided)
        
        Raises:
            ValueError: If data_type is not a valid MongoDB type
        """
        if not name or not name.strip():
            raise ValueError("DataField name must be a non-empty string")
        self.name = name
        
        # Validate data type
        data_type = data_type.lower()
        if data_type not in self.VALID_TYPES:
            raise ValueError(f"Invalid data_type '{data_type}'. Must be one of: {sorted(self.VALID_TYPES)}")
        self.data_type = data_type
        
        # Use the last part of the field path as the default alias if none provided
        default_alias = name.split('.')[-1] if '.' in name else name
        self.alias = alias or default_alias
        
    def __repr__(self):
        alias_str = f", alias='{self.alias}'" if self.alias != self.name.split('.')[-1] else ""
        return f"DataField(name='{self.name}'{alias_str}, data_type='{self.data_type}')"
    
 
class DataTables:
    """Server-side processor for jQuery DataTables with MongoDB integration.

    Handles pagination, sorting, filtering, and search operations for DataTables
    using MongoDB as the backend. Supports text indexes for efficient searching
    and provides detailed query statistics.

    Attributes:
        collection: MongoDB collection to query
        request_args: DataTables request parameters
        data_fields: List of DataField objects defining the data schema
        use_text_index: Whether to use text indexes when available
    """

    def __init__(
        self,
        pymongo_object: Any,
        collection_name: str,
        request_args: Dict[str, Any],
        data_fields: Optional[List['DataField']] = None,
        use_text_index: bool = True,
        allow_disk_use: bool = False,
        row_class=None,
        row_data=None,
        row_attr=None,
        row_id: Optional[str] = None,
        pipeline_stages: Optional[List[Dict[str, Any]]] = None,
        **custom_filter: Any,
    ) -> None:
        """Initialize the DataTables processor.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: DataTables request parameters
            data_fields: List of DataField objects defining database fields with UI mappings
            use_text_index: Whether to use text indexes when available (default: True).
                            When True, uses MongoDB text index for fast whole-word search.
                            Set False to use regex for substring matching.
            allow_disk_use: Pass allowDiskUse=True to all aggregation pipelines (default: False).
                            Enables MongoDB to write temporary files when the 100 MB in-memory
                            aggregation limit is exceeded. Useful for large datasets with complex
                            SearchBuilder or SearchPanes filters.
            row_class: Static string or callable(row) -> str for DT_RowClass per-row CSS class
            row_data: Static dict or callable(row) -> dict for DT_RowData per-row data attributes
            row_attr: Static dict or callable(row) -> dict for DT_RowAttr per-row HTML attributes
            row_id: Field name to use as DT_RowId (default: None uses MongoDB _id).
                    When set, the specified field's value is used as DT_RowId instead of _id.
                    The _id field is still included in the projection for internal use.
            pipeline_stages: Optional list of MongoDB aggregation stages to inject before
                    the $match stage in all pipelines (e.g. $lookup, $addFields).
            **custom_filter: Additional filtering criteria
        """
        self.collection = self._get_collection(pymongo_object, collection_name)
        self.request_args = request_args
        self.data_fields = data_fields or []
        self.field_mapper = FieldMapper(self.data_fields)
        self.use_text_index = use_text_index
        self.allow_disk_use = allow_disk_use
        self.row_class = row_class
        self.row_data = row_data
        self.row_attr = row_attr
        self.row_id = row_id
        self.pipeline_stages = list(pipeline_stages) if pipeline_stages else []

        if 'data_fields' in custom_filter:
            del custom_filter['data_fields']

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
            has_text_index=self.has_text_index
        )

    def _get_collection(self, pymongo_object: Any, collection_name: str) -> Collection:
        """Get a MongoDB collection from a PyMongo object.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection

        Returns:
            MongoDB collection object
        """
        if isinstance(pymongo_object, Database):
            db = pymongo_object
        elif hasattr(pymongo_object, "db"):
            db = pymongo_object.db
        elif hasattr(pymongo_object, "get_database"):
            db = pymongo_object.get_database()
        else:
            return pymongo_object[collection_name]

        return db[collection_name]

    def _check_text_index(self) -> None:
        """Check if the collection has a text index and store the result.

        Skips the database round-trip when use_text_index is False.
        """
        if not self.use_text_index:
            self._has_text_index = False
            return
        try:
            self._has_text_index = any("textIndexVersion" in idx for idx in self.collection.list_indexes())
        except PyMongoError:
            self._has_text_index = False

    @property
    def has_text_index(self) -> bool:
        """Check if the collection has a text index.

        Returns:
            True if a text index exists, False otherwise
        """
        return self._has_text_index

    @property
    def columns(self) -> List[Dict[str, Any]]:
        """Get the columns configuration from the request.

        Returns:
            List of column configurations
        """
        return self.request_args.get("columns", [])

    @property
    def searchable_columns(self) -> List[str]:
        """Get a list of searchable column names.

        Returns:
            List of column names that are searchable
        """
        return [
            column["data"] for column in self.columns if is_truthy(column.get("searchable"))
        ]

    @property
    def search_value(self) -> str:
        """Get the global search value from the request.

        Returns:
            Global search value as a string
        """
        return self.request_args.get("search", {}).get("value", "")

    @property
    def search_terms(self) -> List[str]:
        """Extract search terms from the DataTables request.

        Handles quoted terms as single search strings. For example,
        a search for 'Author:Robert "Jonathan Kennedy"' would be parsed as
        two terms: 'Author:Robert' and 'Jonathan Kennedy'.

        Returns:
            List of search terms with quoted phrases preserved as single terms
        """
        if self._search_terms_cache is None:
            self._search_terms_cache = SearchTermParser.parse(self.search_value)
        return self._search_terms_cache

    @property
    def search_terms_without_a_colon(self) -> List[str]:
        """Get search terms that don't contain a colon (global search).

        Returns:
            List of search terms without colons
        """
        return [term for term in self.search_terms if ":" not in term]

    @property
    def search_terms_with_a_colon(self) -> List[str]:
        """Get search terms that contain exactly one colon (field-specific search).

        These terms are in format "field:value" for targeted column searching.

        Returns:
            List of search terms with exactly one colon
        """
        return [term for term in self.search_terms if ":" in term]

    @property
    def column_search_conditions(self) -> Dict[str, Any]:
        """Build filter conditions from per-column search inputs.

        Reads ``columns[i][search][value]`` for each column and matches it
        against that column's field only.  Respects the ``smart``, ``regex``,
        and ``caseInsensitive`` flags on each column's search object.  For
        ``number`` and ``date`` fields, a pipe-delimited ``min|max`` value is
        treated as an inclusive range.

        Returns:
            ``{"$and": [...]}`` combining all active column conditions, or
            ``{}`` if no column searches are active.
        """
        search = self.request_args.get("search", {})
        case_insensitive = is_truthy(search.get("caseInsensitive", True))
        return self.query_builder.build_column_search(self.columns, case_insensitive=case_insensitive)

    @property
    def global_search_condition(self) -> Dict[str, Any]:
        """Build the filter condition for the global search box.

        Uses ``$text`` when a text index is available and the ``regex`` and
        ``caseInsensitive`` flags allow it; otherwise falls back to per-column
        ``$regex``.  Respects ``search[smart]``, ``search[regex]``, and
        ``search[caseInsensitive]`` from the request.  Colon-syntax terms
        (``field:value``) are excluded here — see
        :attr:`column_specific_search_condition`.

        Returns:
            A MongoDB query dict, or ``{}`` if the search value is empty.
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

    @property
    def column_specific_search_condition(self) -> Dict[str, Any]:
        """Build filter conditions from ``field:value`` colon-syntax terms.

        Extracts colon-syntax terms from the global search value and targets
        each against its named field.  Supports plain values, quoted phrases,
        and comparison operators (``>``, ``>=``, ``<``, ``<=``, ``=``) for
        ``number`` and ``date`` fields.  All terms are ANDed.  Respects
        ``search[caseInsensitive]``.

        Returns:
            ``{"$and": [...]}`` combining all colon-term conditions, or ``{}``
            if no colon-syntax terms are present.
        """
        colon_terms = self.search_terms_with_a_colon
        search = self.request_args.get("search", {})
        case_insensitive = is_truthy(search.get("caseInsensitive", True))
        return self.query_builder.build_column_specific_search(
            colon_terms,
            self.searchable_columns,
            case_insensitive=case_insensitive,
        )

    def get_searchpanes_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return option counts for each SearchPanes column.

        Queries the collection to compute, for each searchable column, the
        distinct values present along with two counts per value:

        - ``total`` — how many documents have that value in the full collection
          (respecting any ``custom_filter`` but ignoring the current search).
        - ``count`` — how many documents have that value after the current
          search/filter is applied.

        Returns:
            Dict mapping each column's ``data`` name to a list of
            ``{"label": value, "total": int, "count": int}`` dicts.
        """
        return _get_searchpanes_options_fn(
            self.columns, self.field_mapper, self.custom_filter, self.filter,
            self.collection, self.allow_disk_use,
        )

    def _parse_searchpanes_filters(self) -> Dict[str, Any]:
        return parse_searchpanes_filters(self.request_args, self.field_mapper)

    def _parse_search_fixed(self) -> Dict[str, Any]:
        return parse_search_fixed(self.request_args, self.query_builder, self.searchable_columns)

    def _parse_column_search_fixed(self) -> Dict[str, Any]:
        return parse_column_search_fixed(self.columns, self.field_mapper, self.query_builder)

    def _parse_search_builder(self) -> Dict[str, Any]:
        return parse_search_builder(self.request_args, self.field_mapper)

    @property
    def filter(self) -> Dict[str, Any]:
        """Combine all filter conditions into a single MongoDB query.

        Returns:
            MongoDB query with all filter conditions
        """
        if self._filter_cache is None:
            self._filter_cache = self._build_filter()
        return self._filter_cache

    def _build_filter(self) -> Dict[str, Any]:
        """Build the combined MongoDB filter from all active conditions.

        Returns:
            MongoDB query with all filter conditions
        """
        conditions = []

        if self.custom_filter:
            conditions.append(self.custom_filter)

        # Add SearchBuilder filters
        search_builder_filter = self._parse_search_builder()
        if search_builder_filter:
            conditions.append(search_builder_filter)

        # Add SearchPanes filters
        searchpanes_filter = self._parse_searchpanes_filters()
        if searchpanes_filter:
            conditions.append(searchpanes_filter)

        search_fixed_filter = self._parse_search_fixed()
        if search_fixed_filter:
            conditions.append(search_fixed_filter)

        col_search_fixed_filter = self._parse_column_search_fixed()
        if col_search_fixed_filter:
            conditions.append(col_search_fixed_filter)

        global_search = self.global_search_condition
        if global_search:
            conditions.append(global_search)

        column_search = self.column_search_conditions
        if column_search:
            conditions.append(column_search)

        column_specific = self.column_specific_search_condition
        if column_specific:
            conditions.append(column_specific)

        if len(conditions) > 1:
            return {"$and": conditions}
        elif len(conditions) == 1:
            return conditions[0]
        else:
            return {}

    def get_sort_specification(self) -> Dict[str, int]:
        """Generate sort specification from the request.

        Supports multi-column sorting by iterating over all entries in the
        ``order`` array. Non-orderable columns (``orderable == "false"``) are
        skipped, and the first occurrence of a field wins (matching DataTables
        behaviour).

        Returns:
            MongoDB sort specification
        """
        sort_spec = {}
        for order_info in self.request_args.get("order", []):
            col_idx = int(order_info["column"])
            order_name = order_info.get("name", "")
            column = None
            if order_name:
                column = next(
                    (c for c in self.columns if c.get("name") == order_name or c.get("data") == order_name),
                    None
                )
            if column is None and 0 <= col_idx < len(self.columns):
                column = self.columns[col_idx]
            if column is None:
                continue
            direction = 1 if order_info["dir"] == "asc" else -1
            order_data = column.get("orderData")
            if order_data is not None:
                indices = [order_data] if isinstance(order_data, int) else list(order_data)
                for idx in indices:
                    if 0 <= idx < len(self.columns):
                        target = self.columns[idx]
                        if is_truthy(target.get("orderable", True)):
                            field = target.get("data")
                            if field:
                                db_field = self.field_mapper.get_db_field(field)
                                if db_field not in sort_spec:
                                    sort_spec[db_field] = direction
            else:
                ui_field_name = column.get("data")
                if ui_field_name and is_truthy(column.get("orderable", True)):
                    db_field_name = self.field_mapper.get_db_field(ui_field_name)
                    if db_field_name not in sort_spec:
                        sort_spec[db_field_name] = direction
        if "_id" not in sort_spec:
            sort_spec["_id"] = 1
        return sort_spec
    
    # For backward compatibility, keep the property interface
    @property
    def sort_specification(self) -> Dict[str, int]:
        return self.get_sort_specification()

    @property
    def start(self) -> int:
        """Get the start parameter for pagination.

        Returns:
            Start index for pagination
        """
        try:
            return max(0, int(self.request_args.get("start", 0)))
        except (ValueError, TypeError):
            return 0

    @property
    def limit(self) -> int:
        """Get the length parameter for pagination.

        Returns:
            Number of records to return
        """
        try:
            return int(self.request_args.get("length", 10))
        except (ValueError, TypeError):
            return 10

    @property
    def draw(self) -> int:
        """The ``draw`` counter from the request, echoed unchanged in the response.

        Returns:
            Request ``draw`` value coerced to a positive int; defaults to ``1``.
        """
        try:
            return max(1, int(self.request_args.get("draw", 1)))
        except (ValueError, TypeError):
            return 1

    @property
    def projection(self) -> Dict[str, int]:
        """Generate projection specification to select fields.

        Returns:
            MongoDB projection specification
        """
        projection = {"_id": 1}

        for column in self.columns:
            if "data" in column and column["data"]:
                projection[self.field_mapper.get_db_field(column["data"])] = 1

        if self.row_id:
            projection[self.field_mapper.get_db_field(self.row_id)] = 1

        return projection

    def _format_result_values(self, result_dict: Dict[str, Any], parent_key: str = "") -> None:
        format_result_values(result_dict, parent_key)

    def _process_cursor(self, cursor) -> List[Dict[str, Any]]:
        return process_cursor(cursor, self.row_id, self.field_mapper, self.row_class, self.row_data, self.row_attr)

    def _remap_aliases(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        return remap_aliases(doc, self.field_mapper)

    @staticmethod
    def _filter_has_text(f: dict) -> bool:
        """Return True if the filter contains a $text operator at any depth."""
        if "$text" in f:
            return True
        for v in f.values():
            if isinstance(v, list):
                if any(isinstance(item, dict) and DataTables._filter_has_text(item) for item in v):
                    return True
        return False

    def _build_pipeline(self, paginate: bool = True) -> list:
        """Build the aggregation pipeline for results or export.

        Args:
            paginate: If True, include $skip and $limit stages.

        Returns:
            List of MongoDB aggregation pipeline stages.
        """
        pipeline = []
        current_filter = self.filter
        if current_filter and self._filter_has_text(current_filter):
            # $text match must be the first pipeline stage — prepend before pipeline_stages
            pipeline.append({"$match": current_filter})
            pipeline.extend(self.pipeline_stages)
        else:
            pipeline.extend(self.pipeline_stages)
            if current_filter:
                pipeline.append({"$match": current_filter})
        if self.sort_specification:
            pipeline.append({"$sort": self.sort_specification})
        if paginate:
            if self.start > 0:
                pipeline.append({"$skip": self.start})
            if self.limit and self.limit > 0:
                pipeline.append({"$limit": self.limit})
        pipeline.append({"$project": self.projection})
        return pipeline

    def results(self) -> List[Dict[str, Any]]:
        """Execute the MongoDB query with optimized pipeline.

        Returns:
            List of documents formatted for DataTables response
        """
        if self._results is not None:
            return self._results

        try:
            self._results = self._process_cursor(
                self.collection.aggregate(self._build_pipeline(paginate=True), allowDiskUse=self.allow_disk_use)
            )
            return self._results
        except PyMongoError as e:
            logger.error(f"Error executing MongoDB query: {str(e)}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error in results(): {str(e)}", exc_info=True)
            return []

    def count_total(self) -> int:
        """Count total records in the collection with optimized performance.

        Uses estimated_document_count() for large collections (>100k docs) and
        count_documents({}) for smaller collections to maintain accuracy.

        Returns:
            Total number of records, or 0 if an error occurs
        """
        if self._recordsTotal is None:
            try:
                # Try estimated count first for performance
                estimated_count = self.collection.estimated_document_count()
                
                # Use exact count for small collections or when custom filter exists
                if estimated_count < 100000 or self.custom_filter:
                    self._recordsTotal = self.collection.count_documents(self.custom_filter)
                    logger.debug(f"Using exact count: {self._recordsTotal}")
                else:
                    self._recordsTotal = estimated_count
                    logger.debug(f"Using estimated count: {self._recordsTotal}")
                    
            except PyMongoError as e:
                logger.error(f"Error counting total records: {str(e)}", exc_info=True)
                # Fallback to basic count
                try:
                    self._recordsTotal = self.collection.count_documents(self.custom_filter)
                except PyMongoError:
                    self._recordsTotal = 0
        return self._recordsTotal

    def count_filtered(self) -> int:
        """Count records after applying filters.

        Uses aggregation pipeline with fallback to count_documents.

        Returns:
            Number of filtered records, or 0 if an error occurs
        """
        if self._recordsFiltered is None:
            if not self.filter:
                self._recordsFiltered = self.count_total()
            else:
                try:
                    pipeline = list(self.pipeline_stages) + [{"$match": self.filter}, {"$count": "total"}]
                    result = list(self.collection.aggregate(pipeline, allowDiskUse=self.allow_disk_use))
                    self._recordsFiltered = result[0]["total"] if result else 0
                    logger.debug(f"Filtered count via aggregation: {self._recordsFiltered}")
                except Exception as e:
                    logger.debug(f"Aggregation failed, using count_documents: {str(e)}")
                    try:
                        self._recordsFiltered = self.collection.count_documents(self.filter)
                    except Exception:
                        logger.error("count_documents also failed, returning 0", exc_info=True)
                        self._recordsFiltered = 0
        return self._recordsFiltered

    def _parse_extension_config(self, key: str) -> Optional[Dict[str, Any]]:
        """Return the extension config dict from request_args for the given key, or None.

        Args:
            key: The request_args key for the extension (e.g. ``"fixedColumns"``).

        Returns:
            The config dict if present, ``{}`` if the value is ``True``, or ``None``.
        """
        val = self.request_args.get(key)
        if not val:
            return None
        if val is True:
            return {}
        return val if isinstance(val, dict) else None

    def _parse_rowgroup_config(self) -> Optional[Dict[str, Any]]:
        """Parse RowGroup extension configuration from request parameters.

        Returns:
            ``{"dataSrc": <value>}`` if a valid dataSrc is present, else ``None``.
        """
        rowgroup_params = self.request_args.get("rowGroup")
        if not rowgroup_params:
            return None
        data_src = rowgroup_params.get("dataSrc")
        if isinstance(data_src, (str, int)):
            return {"dataSrc": data_src}
        return None

    def _get_rowgroup_data(self) -> Optional[Dict[str, Any]]:
        """Generate RowGroup aggregation data using MongoDB pipeline.
        
        Returns:
            Dictionary containing group summaries or None if RowGroup not configured
        """
        rowgroup_config = self._parse_rowgroup_config()
        if not rowgroup_config or "dataSrc" not in rowgroup_config:
            return None
            
        data_src = rowgroup_config["dataSrc"]
        
        # Map column index to field name if dataSrc is numeric
        if isinstance(data_src, int):
            if data_src < len(self.columns):
                field_name = self.columns[data_src].get("data")
            else:
                return None
        else:
            field_name = data_src
            
        # Get the actual MongoDB field name
        mongo_field = self.field_mapper.get_db_field(field_name) if field_name else None
        if not mongo_field:
            return None
            
        try:
            # Build aggregation pipeline for group summaries
            pipeline = []
            
            # Apply filters
            if self.filter:
                pipeline.append({"$match": self.filter})
                
            # Group by the specified field and calculate summaries
            group_stage = {
                "$group": {
                    "_id": f"${mongo_field}",
                    "count": {"$sum": 1}
                }
            }
            
            pipeline.append(group_stage)
            pipeline.append({"$sort": {"_id": 1}})
            
            cursor = self.collection.aggregate(pipeline, allowDiskUse=self.allow_disk_use)
            groups = list(cursor)
            
            group_data = {
                str(group["_id"]) if group["_id"] is not None else "null": {"count": group["count"]}
                for group in groups
            }
                            
            return {
                "dataSrc": data_src,
                "groups": group_data
            }
            
        except PyMongoError as e:
            logger.error(f"Error generating RowGroup data: {str(e)}", exc_info=True)
            return None

    def get_export_data(self) -> List[Dict[str, Any]]:
        """Get all data for export without pagination limits.

        Returns:
            List of all documents matching current filters, formatted for export
        """
        try:
            return self._process_cursor(
                self.collection.aggregate(self._build_pipeline(paginate=False), allowDiskUse=self.allow_disk_use)
            )
        except PyMongoError as e:
            logger.error(f"Error executing export query: {str(e)}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_export_data(): {str(e)}", exc_info=True)
            return []

    def get_rows(self) -> Dict[str, Any]:
        """Get the complete formatted response for DataTables.

        Returns:
            Dictionary containing all required DataTables response fields.
            Includes an 'error' key if an unhandled exception occurs.
        """
        try:
            search_return = self.request_args.get("search", {}).get("return", True)
            records_filtered = -1 if search_return in (False, "false") else self.count_filtered()
            response = {
                "draw": self.draw,
                "recordsTotal": self.count_total(),
                "recordsFiltered": records_filtered,
                "data": self.results(),
            }

            # Add SearchPanes options if requested
            if self.request_args.get("searchPanes"):
                response["searchPanes"] = {"options": self.get_searchpanes_options()}

            for ext_key in ("fixedColumns", "responsive", "buttons", "select"):
                cfg = self._parse_extension_config(ext_key)
                if cfg is not None:
                    response[ext_key] = cfg

            # Add RowGroup data if requested
            rowgroup_data = self._get_rowgroup_data()
            if rowgroup_data:
                response["rowGroup"] = rowgroup_data

            return response
        except Exception as e:
            logger.error("DataTables get_rows failed: %s", e)
            return {
                "draw": self.draw,
                "error": str(e),
                "recordsTotal": 0,
                "recordsFiltered": 0,
                "data": [],
            }
