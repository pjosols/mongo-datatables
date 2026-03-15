"""Server-side processing for jQuery DataTables with MongoDB."""

import logging
import math
import re
import uuid
from datetime import timedelta
from typing import Dict, List, Any, Optional
from bson import Binary, Decimal128, ObjectId, Regex
from bson.errors import InvalidId as ObjectIdError
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables.utils import FieldMapper, SearchTermParser, TypeConverter, DateHandler, is_truthy
from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.query_builder import MongoQueryBuilder

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
    VALID_TYPES = frozenset(['string', 'number', 'date', 'boolean', 'array', 'object', 'objectid', 'null'])
    
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
        **custom_filter: Any,
    ) -> None:
        """Initialize the DataTables processor.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: DataTables request parameters
            data_fields: List of DataField objects defining database fields with UI mappings
            use_text_index: Whether to use text indexes when available (default: True)
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
        if hasattr(pymongo_object, "db"):
            db = pymongo_object.db
        elif hasattr(pymongo_object, "get_database"):
            db = pymongo_object.get_database()
        elif isinstance(pymongo_object, Database):
            db = pymongo_object
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
        """Generate search conditions for individual column searches.

        Returns:
            MongoDB query condition for column-specific searches
        """
        search = self.request_args.get("search", {})
        case_insensitive = is_truthy(search.get("caseInsensitive", True))
        return self.query_builder.build_column_search(self.columns, case_insensitive=case_insensitive)

    @property
    def global_search_condition(self) -> Dict[str, Any]:
        """Generate search conditions for the global search value.

        This method uses text indexes when available for better performance.
        For quoted terms, it performs exact phrase matching.
        For non-quoted terms, it uses OR semantics to match any of the terms.

        Returns:
            MongoDB query condition for global search
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
        """Generate search conditions for column-specific searches using the colon syntax.

        Handles search terms in the format "field:value" for targeted column searching.
        Also supports comparison operators: >, <, >=, <=, = for numeric and date fields.

        Returns:
            MongoDB query condition for column-specific searches
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
        """Generate SearchPanes options with both ``total`` and ``count`` per value.

        Uses a single ``$facet`` aggregation for all columns, reducing MongoDB
        round-trips from 2N to exactly 2 regardless of column count.

        DataTables SearchPanes server-side protocol requires two counts per option:
        - ``total``: count across the base dataset (custom_filter only, no search/pane filters)
        - ``count``: count with all current filters applied

        Returns:
            Dictionary mapping column names to their option lists
        """
        eligible = [
            (col.get("data"), self.field_mapper.get_db_field(col.get("data")))
            for col in self.columns
            if is_truthy(col.get("searchable"))
            and col.get("data")
            and self.field_mapper.get_field_type(col.get("data")) not in ("object", "array")
        ]
        if not eligible:
            return {}

        facet_branches = {
            col_name: [
                {"$group": {"_id": f"${db_field}", "count": {"$sum": 1}}},
                {"$match": {"_id": {"$ne": None}}},
            ]
            for col_name, db_field in eligible
        }
        total_pipeline = ([{"$match": self.custom_filter}] if self.custom_filter else []) + [{"$facet": facet_branches}]
        count_pipeline = ([{"$match": self.filter}] if self.filter else []) + [{"$facet": facet_branches}]

        try:
            total_docs = list(self.collection.aggregate(total_pipeline, allowDiskUse=self.allow_disk_use))
            total_result = total_docs[0] if total_docs else {}
            count_docs = list(self.collection.aggregate(count_pipeline, allowDiskUse=self.allow_disk_use))
            count_result = count_docs[0] if count_docs else {}
        except Exception as e:
            logger.error(f"Error generating SearchPanes options: {str(e)}")
            return {col_name: [] for col_name, _ in eligible}

        def _hashable(v):
            return str(v.to_decimal()) if isinstance(v, Decimal128) else v

        options = {}
        for col_name, _ in eligible:
            total_map = {_hashable(r["_id"]): r["count"] for r in total_result.get(col_name, [])}
            count_map = {_hashable(r["_id"]): r["count"] for r in count_result.get(col_name, [])}
            column_options = []
            for raw_value, total in sorted(total_map.items(), key=lambda x: -x[1])[:1000]:
                if isinstance(raw_value, ObjectId):
                    display_value = str(raw_value)
                elif hasattr(raw_value, 'isoformat'):
                    display_value = raw_value.isoformat()
                else:
                    display_value = str(raw_value) if raw_value is not None else ""
                column_options.append({
                    "label": display_value,
                    "value": display_value,
                    "total": total,
                    "count": count_map.get(raw_value, 0),
                })
            options[col_name] = column_options
        return options

    def _parse_searchpanes_filters(self) -> Dict[str, Any]:
        """Parse SearchPanes filter parameters from request.

        Returns:
            MongoDB query conditions for SearchPanes filters
        """
        conditions = []
        
        # Check for searchPanes parameter in request
        searchpanes = self.request_args.get("searchPanes", {})
        
        # If searchPanes is just a boolean flag, no filters to apply
        if not isinstance(searchpanes, dict):
            return {}
        
        for column_name, selected_values in searchpanes.items():
            if not selected_values:
                continue
                
            db_field = self.field_mapper.get_db_field(column_name)
            field_type = self.field_mapper.get_field_type(column_name)
            
            # Convert values based on field type
            converted_values = []
            for value in selected_values:
                if field_type == "number":
                    try:
                        converted_values.append(TypeConverter.to_number(value))
                    except (ValueError, TypeError):
                        converted_values.append(value)
                elif field_type == "objectid":
                    try:
                        converted_values.append(ObjectId(value))
                    except (ObjectIdError, ValueError):
                        converted_values.append(value)
                elif field_type == "date":
                    try:
                        converted_values.append(DateHandler.parse_iso_date(value.split('T')[0]))
                    except FieldMappingError:
                        converted_values.append(value)
                else:
                    converted_values.append(value)
            
            if converted_values:
                conditions.append({db_field: {"$in": converted_values}})
        
        if conditions:
            return {"$and": conditions}
        return {}

    def _parse_search_fixed(self) -> Dict[str, Any]:
        """Parse searchFixed named searches (DataTables 2.0+) into a MongoDB filter.

        Each named fixed search is ANDed with the main query. Values are treated
        as global search terms across all searchable columns.
        """
        search_fixed = self.request_args.get("searchFixed", {})
        if not isinstance(search_fixed, dict) or not search_fixed:
            return {}
        conditions = []
        for value in search_fixed.values():
            if not value:
                continue
            terms = SearchTermParser.parse(str(value))
            cond = self.query_builder.build_global_search(
                terms, self.searchable_columns, original_search=str(value),
                search_regex=False
            )
            if cond:
                conditions.append(cond)
        if not conditions:
            return {}
        return {"$and": conditions} if len(conditions) > 1 else conditions[0]

    def _parse_column_search_fixed(self) -> Dict[str, Any]:
        """Parse per-column searchFixed dicts (DataTables 2.0+) into a MongoDB filter.

        Each column may carry a ``searchFixed`` dict of named fixed searches.
        Values are applied as column-scoped searches using ``build_column_search``.

        Returns:
            MongoDB query condition, or ``{}`` if no column-level fixed searches exist.
        """
        conditions = []
        for col in self.columns:
            col_fixed = col.get("searchFixed", {})
            if not isinstance(col_fixed, dict) or not col_fixed:
                continue
            db_field = self.field_mapper.get_db_field(col.get("data", ""))
            if not db_field:
                continue
            for value in col_fixed.values():
                if not value:
                    continue
                cond = self.query_builder.build_column_search(
                    [{**col, "search": {"value": str(value), "regex": False}}]
                )
                if cond:
                    conditions.append(cond)
        if not conditions:
            return {}
        return {"$and": conditions} if len(conditions) > 1 else conditions[0]

    def _parse_search_builder(self) -> Dict[str, Any]:
        """Translate a SearchBuilder criteria tree into a MongoDB query.

        The DataTables SearchBuilder extension sends a nested ``searchBuilder``
        parameter when ``serverSide: true`` is enabled.  Each leaf criterion has
        the shape::

            {
                "condition": "=",
                "origData": "salary",
                "type": "num",
                "value": ["50000"]
            }

        Groups are nested via a ``criteria`` list and a ``logic`` key
        (``"AND"`` or ``"OR"``).

        Returns:
            MongoDB query dict, or ``{}`` if no SearchBuilder data is present.
        """
        sb = self.request_args.get("searchBuilder")
        if not sb or not isinstance(sb, dict):
            return {}
        return self._sb_group(sb)

    def _sb_group(self, group: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively convert a SearchBuilder group to a MongoDB condition."""
        logic = group.get("logic", "AND").upper()
        mongo_op = "$and" if logic == "AND" else "$or"
        parts = []
        for criterion in group.get("criteria", []):
            if "criteria" in criterion:
                # nested group
                sub = self._sb_group(criterion)
                if sub:
                    parts.append(sub)
            else:
                cond = self._sb_criterion(criterion)
                if cond:
                    parts.append(cond)
        if not parts:
            return {}
        return {mongo_op: parts} if len(parts) > 1 else parts[0]

    def _sb_criterion(self, criterion: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single SearchBuilder leaf criterion to a MongoDB condition."""
        condition = criterion.get("condition", "")
        orig_data = criterion.get("origData") or criterion.get("data", "")
        values = criterion.get("value", [])
        sb_type = criterion.get("type", "string")

        if not orig_data or not condition:
            return {}

        db_field = self.field_mapper.get_db_field(orig_data)
        v0 = values[0] if values else None
        v1 = values[1] if len(values) > 1 else None

        # null / not-null — type-aware: num/date only check None; string/html also check empty string
        if condition == "null":
            if sb_type in ("num", "num-fmt", "html-num", "html-num-fmt", "date", "moment", "luxon"):
                return {db_field: None}
            return {db_field: {"$in": [None, ""]}}
        if condition == "!null":
            if sb_type in ("num", "num-fmt", "html-num", "html-num-fmt", "date", "moment", "luxon"):
                return {db_field: {"$ne": None}}
            return {db_field: {"$nin": [None, ""]}}

        if sb_type in ("num", "num-fmt", "html-num", "html-num-fmt"):
            return self._sb_number(db_field, condition, v0, v1)
        if sb_type in ("date", "moment", "luxon"):
            return self._sb_date(db_field, condition, v0, v1)
        # string / html / array / default
        return self._sb_string(db_field, condition, v0)

    def _sb_number(self, field: str, condition: str, v0, v1) -> Dict[str, Any]:
        """Build a MongoDB condition for a numeric SearchBuilder criterion."""
        def _n(v):
            return TypeConverter.to_number(v)
        try:
            if condition == "=":   return {field: _n(v0)}
            if condition == "!=":  return {field: {"$ne": _n(v0)}}
            if condition == "<":   return {field: {"$lt": _n(v0)}}
            if condition == "<=":  return {field: {"$lte": _n(v0)}}
            if condition == ">":   return {field: {"$gt": _n(v0)}}
            if condition == ">=":  return {field: {"$gte": _n(v0)}}
            if condition == "between":  return {field: {"$gte": _n(v0), "$lte": _n(v1)}}
            if condition == "!between": return {"$or": [{field: {"$lt": _n(v0)}}, {field: {"$gt": _n(v1)}}]}
        except (ValueError, TypeError, FieldMappingError):
            pass
        return {}

    def _sb_date(self, field: str, condition: str, v0, v1) -> Dict[str, Any]:
        """Build a MongoDB condition for a date SearchBuilder criterion."""
        def _d(v):
            return DateHandler.parse_iso_date(v.split('T')[0])
        try:
            if condition == "=":
                d = _d(v0)
                return {field: {"$gte": d, "$lt": d + timedelta(days=1)}}
            if condition == "!=":
                d = _d(v0)
                return {"$or": [{field: {"$lt": d}}, {field: {"$gte": d + timedelta(days=1)}}]}
            if condition == "<":   return {field: {"$lt": _d(v0)}}
            if condition == "<=":  return {field: {"$lt": _d(v0) + timedelta(days=1)}}
            if condition == ">":   return {field: {"$gt": _d(v0)}}
            if condition == ">=":  return {field: {"$gte": _d(v0)}}
            if condition == "between":  return {field: {"$gte": _d(v0), "$lt": _d(v1) + timedelta(days=1)}}
            if condition == "!between": return {"$or": [{field: {"$lt": _d(v0)}}, {field: {"$gte": _d(v1) + timedelta(days=1)}}]}
        except (ValueError, TypeError, FieldMappingError):
            pass
        return {}

    def _sb_string(self, field: str, condition: str, v0) -> Dict[str, Any]:
        """Build a MongoDB condition for a string SearchBuilder criterion."""
        if v0 is None:
            return {}
        s = re.escape(v0)
        if condition == "=":        return {field: {"$regex": f"^{s}$", "$options": "i"}}
        if condition == "!=":       return {field: {"$not": {"$regex": f"^{s}$", "$options": "i"}}}
        if condition == "contains":  return {field: {"$regex": s, "$options": "i"}}
        if condition == "!contains": return {field: {"$not": {"$regex": s, "$options": "i"}}}
        if condition == "starts":    return {field: {"$regex": f"^{s}", "$options": "i"}}
        if condition == "!starts":   return {field: {"$not": {"$regex": f"^{s}", "$options": "i"}}}
        if condition == "ends":      return {field: {"$regex": f"{s}$", "$options": "i"}}
        if condition == "!ends":     return {field: {"$not": {"$regex": f"{s}$", "$options": "i"}}}
        return {}

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
        """Get the draw counter for DataTables response sequencing."""
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
        """Recursively format values in result dictionary for JSON serialization.

        Args:
            result_dict: Dictionary to process
            parent_key: Key of parent for nested dictionaries
        """
        if not result_dict:
            return
            
        items = list(result_dict.items())
        for key, val in items:
            full_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(val, dict):
                self._format_result_values(val, full_key)
            elif isinstance(val, list):
                for i, item in enumerate(val):
                    if isinstance(item, dict):
                        self._format_result_values(item, f"{full_key}[{i}]")
                    elif isinstance(item, ObjectId):
                        val[i] = str(item)
                    elif hasattr(item, 'isoformat'):
                        val[i] = item.isoformat()
                    elif isinstance(item, Decimal128):
                        val[i] = float(item.to_decimal())
                    elif isinstance(item, Binary):
                        val[i] = str(uuid.UUID(bytes=bytes(item))) if item.subtype in (3, 4) else item.hex()
                    elif isinstance(item, Regex):
                        flags = ''.join(v for k, v in ((re.IGNORECASE, 'i'), (re.MULTILINE, 'm'), (re.DOTALL, 's'), (re.VERBOSE, 'x')) if int(item.flags) & int(k))
                        val[i] = f'/{item.pattern}/{flags}'
                    elif isinstance(item, float) and not math.isfinite(item):
                        val[i] = None
            elif isinstance(val, ObjectId):
                result_dict[key] = str(val)
            elif hasattr(val, 'isoformat'):
                result_dict[key] = val.isoformat()
            elif isinstance(val, float) and not math.isfinite(val):
                result_dict[key] = None
            elif isinstance(val, Decimal128):
                result_dict[key] = float(val.to_decimal())
            elif isinstance(val, Binary):
                result_dict[key] = str(uuid.UUID(bytes=bytes(val))) if val.subtype in (3, 4) else val.hex()
            elif isinstance(val, Regex):
                flags = ''.join(v for k, v in ((re.IGNORECASE, 'i'), (re.MULTILINE, 'm'), (re.DOTALL, 's'), (re.VERBOSE, 'x')) if int(val.flags) & int(k))
                result_dict[key] = f'/{val.pattern}/{flags}'

    def _process_cursor(self, cursor) -> List[Dict[str, Any]]:
        """Convert aggregation cursor to DataTables-formatted list."""
        processed = []
        for result in cursor:
            d = dict(result)
            if self.row_id and self.row_id in d:
                d["DT_RowId"] = str(d[self.row_id])
            elif "_id" in d:
                d["DT_RowId"] = str(d.pop("_id"))
            self._format_result_values(d)
            d = self._remap_aliases(d)
            if self.row_class is not None:
                d["DT_RowClass"] = self.row_class(d) if callable(self.row_class) else self.row_class
            if self.row_data is not None:
                d["DT_RowData"] = self.row_data(d) if callable(self.row_data) else self.row_data
            if self.row_attr is not None:
                d["DT_RowAttr"] = self.row_attr(d) if callable(self.row_attr) else self.row_attr
            processed.append(d)
        return processed

    def _remap_aliases(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Remap DB field names to UI aliases in a result document.

        For DataFields with dot-notation names (e.g. 'PublisherInfo.Date'),
        MongoDB returns nested dicts. This method extracts the value and
        stores it under the UI alias key, removing the intermediate nesting
        when no other fields from that parent are needed.
        """
        if not self.field_mapper.db_to_ui:
            return doc
        for db_field, ui_alias in self.field_mapper.db_to_ui.items():
            if db_field == ui_alias:
                continue  # no remapping needed
            if '.' in db_field:
                # Extract value from nested structure
                parts = db_field.split('.')
                val = doc
                for part in parts:
                    if isinstance(val, dict) and part in val:
                        val = val[part]
                    else:
                        val = None
                        break
                if val is not None:
                    doc[ui_alias] = val
                    # Remove top-level parent key only if it's no longer needed
                    top = parts[0]
                    if top in doc and isinstance(doc[top], dict):
                        # Check if any other db_field uses this same top-level key
                        other_uses = any(
                            f != db_field and f.startswith(top + '.')
                            for f in self.field_mapper.db_to_ui
                        )
                        if not other_uses:
                            del doc[top]
            else:
                # Simple rename: db_field key -> ui_alias key
                if db_field in doc:
                    doc[ui_alias] = doc.pop(db_field)
        return doc

    def _build_pipeline(self, paginate: bool = True) -> list:
        """Build the aggregation pipeline for results or export.

        Args:
            paginate: If True, include $skip and $limit stages.

        Returns:
            List of MongoDB aggregation pipeline stages.
        """
        pipeline = []
        if self.filter:
            pipeline.append({"$match": self.filter})
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
                    pipeline = [{"$match": self.filter}, {"$count": "total"}]
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
