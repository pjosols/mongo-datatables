"""Server-side processing for jQuery DataTables with MongoDB."""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from bson.objectid import ObjectId
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables.exceptions import DatabaseOperationError, QueryBuildError
from mongo_datatables.utils import FieldMapper, SearchTermParser, TypeConverter, DateHandler
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
    VALID_TYPES = ['string', 'number', 'date', 'boolean', 'array', 'object', 'objectid', 'null']
    
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
        self.name = name
        
        # Validate data type
        data_type = data_type.lower()
        if data_type not in self.VALID_TYPES:
            raise ValueError(f"Invalid data type: {data_type}. Must be one of: {', '.join(self.VALID_TYPES)}")
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
        **custom_filter: Any,
    ) -> None:
        """Initialize the DataTables processor.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: DataTables request parameters
            data_fields: List of DataField objects defining database fields with UI mappings
            use_text_index: Whether to use text indexes when available (default: True)
            **custom_filter: Additional filtering criteria
        """
        self.collection = self._get_collection(pymongo_object, collection_name)
        self.request_args = request_args
        self.data_fields = data_fields or []
        self.field_mapper = FieldMapper(self.data_fields)
        self.use_text_index = use_text_index

        if 'data_fields' in custom_filter:
            del custom_filter['data_fields']

        self.custom_filter = custom_filter

        self._results = None
        self._recordsTotal = None
        self._recordsFiltered = None
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
        """Check if the collection has a text index and store the result."""
        indexes = list(self.collection.list_indexes())
        text_indexes = [idx for idx in indexes if "textIndexVersion" in idx]

        if text_indexes:
            self._has_text_index = True
        else:
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
            column["data"] for column in self.columns if column.get("searchable", False)
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
        return SearchTermParser.parse(self.search_value)

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
        return [term for term in self.search_terms if term.count(":") == 1]

    @property
    def column_search_conditions(self) -> Dict[str, Any]:
        """Generate search conditions for individual column searches.

        Returns:
            MongoDB query condition for column-specific searches
        """
        return self.query_builder.build_column_search(self.columns)

    @property
    def global_search_condition(self) -> Dict[str, Any]:
        """Generate search conditions for the global search value.

        This method uses text indexes when available for better performance.
        For quoted terms, it performs exact phrase matching.
        For non-quoted terms, it uses OR semantics to match any of the terms.

        Returns:
            MongoDB query condition for global search
        """
        search_terms = self.search_terms_without_a_colon
        return self.query_builder.build_global_search(
            search_terms,
            self.searchable_columns,
            original_search=self.search_value,
            search_regex=bool(self.request_args.get("search", {}).get("regex", False))
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
        return self.query_builder.build_column_specific_search(
            colon_terms,
            self.searchable_columns
        )

    def get_searchpanes_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate SearchPanes options data for all searchable columns.

        Returns:
            Dictionary mapping column names to their option lists
        """
        options = {}
        
        for column in self.columns:
            if not column.get("searchable", False):
                continue
                
            column_name = column.get("data")
            if not column_name:
                continue
                
            db_field = self.field_mapper.get_db_field(column_name)
            field_type = self.field_mapper.get_field_type(column_name)
            
            # Skip complex types that don't work well with SearchPanes
            if field_type in ["object", "array"]:
                continue
                
            try:
                pipeline = []
                
                # Apply base filter if exists
                if self.custom_filter:
                    pipeline.append({"$match": self.custom_filter})
                
                # Group by field value and count occurrences
                pipeline.extend([
                    {"$group": {
                        "_id": f"${db_field}",
                        "count": {"$sum": 1}
                    }},
                    {"$match": {"_id": {"$ne": None}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 1000}  # Limit options for performance
                ])
                
                cursor = self.collection.aggregate(pipeline)
                column_options = []
                
                for result in cursor:
                    value = result["_id"]
                    count = result["count"]
                    
                    # Format value for display
                    if isinstance(value, ObjectId):
                        display_value = str(value)
                    elif hasattr(value, 'isoformat'):
                        display_value = value.isoformat()
                    else:
                        display_value = str(value) if value is not None else ""
                    
                    column_options.append({
                        "label": display_value,
                        "value": display_value,
                        "count": count
                    })
                
                options[column_name] = column_options
                
            except Exception as e:
                logger.error(f"Error generating SearchPanes options for {column_name}: {str(e)}")
                options[column_name] = []
        
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
                    except:
                        converted_values.append(value)
                elif field_type == "objectid":
                    try:
                        converted_values.append(ObjectId(value))
                    except:
                        converted_values.append(value)
                else:
                    converted_values.append(value)
            
            if converted_values:
                conditions.append({db_field: {"$in": converted_values}})
        
        if conditions:
            return {"$and": conditions}
        return {}

    @property
    def filter(self) -> Dict[str, Any]:
        """Combine all filter conditions into a single MongoDB query.

        Returns:
            MongoDB query with all filter conditions
        """
        conditions = []

        if self.custom_filter:
            conditions.append(self.custom_filter)

        # Add SearchPanes filters
        searchpanes_filter = self._parse_searchpanes_filters()
        if searchpanes_filter:
            conditions.append(searchpanes_filter)

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

        Returns:
            MongoDB sort specification
        """
        sort_spec = {}

        if self.request_args.get("order"):
            order_info = self.request_args.get("order")[0]
            col_idx = int(order_info["column"])

            if 0 <= col_idx < len(self.columns):
                column = self.columns[col_idx]
                ui_field_name = column.get("data")

                if ui_field_name:
                    db_field_name = self.field_mapper.get_db_field(ui_field_name)
                    direction = order_info["dir"]
                    sort_spec[db_field_name] = 1 if direction == "asc" else -1

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
        return int(self.request_args.get("start", 0))

    @property
    def limit(self) -> int:
        """Get the length parameter for pagination.

        Returns:
            Number of records to return
        """
        return int(self.request_args.get("length", 10))

    @property
    def projection(self) -> Dict[str, int]:
        """Generate projection specification to select fields.

        Returns:
            MongoDB projection specification
        """
        projection = {"_id": 1}

        for column in self.columns:
            if "data" in column and column["data"]:
                projection[column["data"]] = 1

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
            elif isinstance(val, ObjectId):
                result_dict[key] = str(val)
            elif hasattr(val, 'isoformat'):
                result_dict[key] = val.isoformat()
            elif isinstance(val, float):
                result_dict[key] = val

    def results(self) -> List[Dict[str, Any]]:
        """Execute the MongoDB query with optimized pipeline.

        Returns:
            List of documents formatted for DataTables response
        """
        if self._results is not None:
            return self._results

        try:
            pipeline = []

            if self.filter:
                pipeline.append({"$match": self.filter})

            if self.sort_specification:
                pipeline.append({"$sort": self.sort_specification})

            if self.start > 0:
                pipeline.append({"$skip": self.start})

            if self.limit:
                pipeline.append({"$limit": self.limit})

            pipeline.append({"$project": self.projection})

            cursor = self.collection.aggregate(pipeline)
            results = list(cursor)

            processed_results = []
            for result in results:
                result_dict = dict(result)

                if "_id" in result_dict:
                    result_dict["DT_RowId"] = str(result_dict["_id"])
                    del result_dict["_id"]

                self._format_result_values(result_dict)
                processed_results.append(result_dict)

            self._results = processed_results
            return processed_results

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
                
                # Convert to int in case it's a mock object
                try:
                    estimated_count = int(estimated_count)
                except (TypeError, ValueError):
                    # If conversion fails, fall back to exact count
                    self._recordsTotal = self.collection.count_documents({})
                    logger.debug(f"Using exact count (fallback): {self._recordsTotal}")
                    return self._recordsTotal
                
                # Use exact count for small collections or when custom filter exists
                if estimated_count < 100000 or self.custom_filter:
                    self._recordsTotal = self.collection.count_documents({})
                    logger.debug(f"Using exact count: {self._recordsTotal}")
                else:
                    self._recordsTotal = estimated_count
                    logger.debug(f"Using estimated count: {self._recordsTotal}")
                    
            except PyMongoError as e:
                logger.error(f"Error counting total records: {str(e)}", exc_info=True)
                # Fallback to basic count
                try:
                    self._recordsTotal = self.collection.count_documents({})
                except PyMongoError:
                    self._recordsTotal = 0
        return self._recordsTotal

    def count_filtered(self) -> int:
        """Count records after applying filters with optimized performance.

        Uses aggregation pipeline for complex filters to improve performance
        on large datasets.

        Returns:
            Number of filtered records, or 0 if an error occurs
        """
        if self._recordsFiltered is None:
            try:
                if not self.filter:
                    self._recordsFiltered = self.count_total()
                else:
                    # Try aggregation pipeline for better performance on large datasets
                    try:
                        pipeline = [
                            {"$match": self.filter},
                            {"$count": "total"}
                        ]
                        
                        result = list(self.collection.aggregate(pipeline))
                        self._recordsFiltered = result[0]["total"] if result else 0
                        logger.debug(f"Filtered count via aggregation: {self._recordsFiltered}")
                    except (PyMongoError, Exception) as e:
                        # Fallback to traditional count_documents
                        logger.debug(f"Aggregation failed, using count_documents: {str(e)}")
                        self._recordsFiltered = self.collection.count_documents(self.filter)
                    
            except PyMongoError as e:
                logger.error(f"Error counting filtered records: {str(e)}", exc_info=True)
                # Final fallback
                try:
                    if self.filter:
                        self._recordsFiltered = self.collection.count_documents(self.filter)
                    else:
                        self._recordsFiltered = self.count_total()
                except PyMongoError:
                    self._recordsFiltered = 0
        return self._recordsFiltered

    def _parse_fixed_columns_config(self) -> Optional[Dict[str, Any]]:
        """Parse FixedColumns configuration from request parameters.
        
        Returns:
            FixedColumns configuration dict or None if not requested
        """
        fixed_columns = self.request_args.get("fixedColumns")
        if not fixed_columns:
            return None
            
        config = {}
        
        # Parse left fixed columns
        if "left" in fixed_columns:
            try:
                config["left"] = int(fixed_columns["left"])
            except (ValueError, TypeError):
                config["left"] = 0
                
        # Parse right fixed columns  
        if "right" in fixed_columns:
            try:
                config["right"] = int(fixed_columns["right"])
            except (ValueError, TypeError):
                config["right"] = 0
                
        return config if config else None

    def _parse_responsive_config(self) -> Optional[Dict[str, Any]]:
        """Parse Responsive extension configuration from request parameters.
        
        Returns:
            Dictionary containing responsive configuration or None if not requested
        """
        responsive_params = self.request_args.get("responsive")
        if not responsive_params:
            return None
            
        config = {}
        
        # Parse breakpoints configuration
        if "breakpoints" in responsive_params:
            breakpoints = responsive_params["breakpoints"]
            if isinstance(breakpoints, dict):
                config["breakpoints"] = breakpoints
                
        # Parse display configuration
        if "display" in responsive_params:
            display = responsive_params["display"]
            if isinstance(display, dict):
                config["display"] = display
                
        # Parse column priorities
        if "priorities" in responsive_params:
            priorities = responsive_params["priorities"]
            if isinstance(priorities, dict):
                config["priorities"] = priorities
                
        return config if config else None

    def _parse_buttons_config(self) -> Optional[Dict[str, Any]]:
        """Parse Buttons extension configuration from request parameters.
        
        Returns:
            Dictionary containing buttons configuration or None if not requested
        """
        buttons_params = self.request_args.get("buttons")
        if not buttons_params:
            return None
            
        config = {}
        
        # Parse export configuration
        if "export" in buttons_params:
            export_config = buttons_params["export"]
            if isinstance(export_config, dict):
                config["export"] = export_config
                
        # Parse column visibility configuration
        if "colvis" in buttons_params:
            colvis_config = buttons_params["colvis"]
            if isinstance(colvis_config, dict):
                config["colvis"] = colvis_config
                
        # Parse print configuration
        if "print" in buttons_params:
            print_config = buttons_params["print"]
            if isinstance(print_config, dict):
                config["print"] = print_config
                
        # Parse copy configuration
        if "copy" in buttons_params:
            copy_config = buttons_params["copy"]
            if isinstance(copy_config, dict):
                config["copy"] = copy_config
                
        return config if config else None

    def _parse_select_config(self) -> Optional[Dict[str, Any]]:
        """Parse Select extension configuration from request parameters.
        
        Returns:
            Dictionary containing select configuration or None if not requested
        """
        select_params = self.request_args.get("select")
        if not select_params:
            return None
            
        # Handle boolean true case (default configuration)
        if select_params is True:
            return {"style": "os"}
            
        config = {}
        
        # Parse selection style
        if isinstance(select_params, dict):
            style = select_params.get("style", "os")
            if style in ["os", "single", "multi", "multi+shift"]:
                config["style"] = style
            else:
                config["style"] = "os"
                
        return config if config else None

    def _parse_rowgroup_config(self) -> Optional[Dict[str, Any]]:
        """Parse RowGroup extension configuration from request parameters.
        
        Returns:
            Dictionary containing rowGroup configuration or None if not requested
        """
        rowgroup_params = self.request_args.get("rowGroup")
        if not rowgroup_params:
            return None
            
        config = {}
        
        # Parse data source for grouping
        if "dataSrc" in rowgroup_params:
            data_src = rowgroup_params["dataSrc"]
            if isinstance(data_src, (str, int)):
                config["dataSrc"] = data_src
                
        # Parse start render function indicator
        if "startRender" in rowgroup_params:
            config["startRender"] = bool(rowgroup_params["startRender"])
            
        # Parse end render function indicator  
        if "endRender" in rowgroup_params:
            config["endRender"] = bool(rowgroup_params["endRender"])
            
        return config if config else None

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
            
            # Add sum and avg for numeric fields
            for field in self.data_fields:
                if field.data_type == "number":
                    group_stage["$group"][f"{field.alias}_sum"] = {"$sum": f"${field.name}"}
                    group_stage["$group"][f"{field.alias}_avg"] = {"$avg": f"${field.name}"}
                    
            pipeline.append(group_stage)
            pipeline.append({"$sort": {"_id": 1}})
            
            cursor = self.collection.aggregate(pipeline)
            groups = list(cursor)
            
            # Format group data
            group_data = {}
            for group in groups:
                group_key = str(group["_id"]) if group["_id"] is not None else "null"
                group_data[group_key] = {
                    "count": group["count"]
                }
                
                # Add numeric summaries
                for field in self.data_fields:
                    if field.data_type == "number":
                        sum_key = f"{field.alias}_sum"
                        avg_key = f"{field.alias}_avg"
                        if sum_key in group:
                            group_data[group_key][sum_key] = group[sum_key]
                        if avg_key in group:
                            group_data[group_key][avg_key] = group[avg_key]
                            
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
            pipeline = []

            if self.filter:
                pipeline.append({"$match": self.filter})

            if self.sort_specification:
                pipeline.append({"$sort": self.sort_specification})

            pipeline.append({"$project": self.projection})

            cursor = self.collection.aggregate(pipeline)
            results = list(cursor)

            processed_results = []
            for result in results:
                result_dict = dict(result)

                if "_id" in result_dict:
                    result_dict["DT_RowId"] = str(result_dict["_id"])
                    del result_dict["_id"]

                self._format_result_values(result_dict)
                processed_results.append(result_dict)

            return processed_results

        except PyMongoError as e:
            logger.error(f"Error executing export query: {str(e)}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_export_data(): {str(e)}", exc_info=True)
            return []

    def get_rows(self) -> Dict[str, Any]:
        """Get the complete formatted response for DataTables.

        Returns:
            Dictionary containing all required DataTables response fields
        """
        response = {
            "draw": int(self.request_args.get("draw", 1)),
            "recordsTotal": self.count_total(),
            "recordsFiltered": self.count_filtered(),
            "data": self.results(),
        }
        
        # Add SearchPanes options if requested
        if self.request_args.get("searchPanes"):
            response["searchPanes"] = {"options": self.get_searchpanes_options()}
            
        # Add FixedColumns configuration if requested
        fixed_columns_config = self._parse_fixed_columns_config()
        if fixed_columns_config:
            response["fixedColumns"] = fixed_columns_config
            
        # Add Responsive configuration if requested
        responsive_config = self._parse_responsive_config()
        if responsive_config:
            response["responsive"] = responsive_config
            
        # Add Buttons configuration if requested
        buttons_config = self._parse_buttons_config()
        if buttons_config:
            response["buttons"] = buttons_config
            
        # Add Select configuration if requested
        select_config = self._parse_select_config()
        if select_config:
            response["select"] = select_config
            
        # Add RowGroup data if requested
        rowgroup_data = self._get_rowgroup_data()
        if rowgroup_data:
            response["rowGroup"] = rowgroup_data
        
        return response
