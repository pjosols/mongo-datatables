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
        preserve_id: bool = False,
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
        # Initialize database connection
        self.collection = self._get_collection(pymongo_object, collection_name)
        self.request_args = request_args

        # Store data fields
        self.data_fields = data_fields or []

        # Create field mapper for UI <-> DB field mapping
        self.field_mapper = FieldMapper(self.data_fields)

        self.use_text_index = use_text_index

        # Remove data_fields from custom filter if it was passed as a keyword argument
        if 'data_fields' in custom_filter:
            del custom_filter['data_fields']

        # Custom filter for additional query criteria
        self.custom_filter = custom_filter

        # Cache for results
        self._results = None
        self._recordsTotal = None
        self._recordsFiltered = None
        self._has_text_index = None

        # Check for text indexes
        self._check_text_index()

        # Initialize query builder
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
        # Handle different PyMongo object types
        if hasattr(pymongo_object, "db"):
            # Flask-PyMongo or similar with .db attribute
            db = pymongo_object.db
        elif hasattr(pymongo_object, "get_database"):
            # PyMongo client
            db = pymongo_object.get_database()
        elif isinstance(pymongo_object, Database):
            # PyMongo database
            db = pymongo_object
        else:
            # Assume it's a dict-like object with the collection as a value
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
            self.search_value
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

    @property
    def filter(self) -> Dict[str, Any]:
        """Combine all filter conditions into a single MongoDB query.

        Returns:
            MongoDB query with all filter conditions
        """
        conditions = []

        # Add custom filter if provided
        if self.custom_filter:
            conditions.append(self.custom_filter)

        # Add global search condition if present
        global_search = self.global_search_condition
        if global_search:
            conditions.append(global_search)

        # Add column-specific search conditions if present
        column_search = self.column_search_conditions
        if column_search:
            conditions.append(column_search)

        # Add column-specific search conditions using colon syntax
        column_specific = self.column_specific_search_condition
        if column_specific:
            conditions.append(column_specific)

        # Combine all conditions with $and
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
        # Default sort field and direction
        default_sort_field = "Title"
        default_sort_dir = 1  # 1 for ascending, -1 for descending
        
        # Initialize the sort specification
        sort_spec = {}
        
        # If there's no order in the request, use the default
        if not self.request_args.get("order"):
            sort_spec[default_sort_field] = default_sort_dir
        else:
            # Get the order column index and direction
            order_info = self.request_args.get("order")[0]
            col_idx = int(order_info["column"])
            direction = order_info["dir"]
            
            # Map direction to MongoDB sort value
            dir_value = 1 if direction == "asc" else -1
            
            # Get the column data from the columns array
            columns = self.columns
            if 0 <= col_idx < len(columns):
                column = columns[col_idx]
                ui_field_name = column["data"]
                
                # If the field name is valid, use it; otherwise fall back to default
                if ui_field_name:
                    # Map UI field name to DB field name
                    db_field_name = self.field_mapper.get_db_field(ui_field_name)
                    sort_spec[db_field_name] = dir_value
                else:
                    # Invalid field name, use default
                    sort_spec[default_sort_field] = default_sort_dir
            else:
                # Invalid column index, use default
                sort_spec[default_sort_field] = default_sort_dir
        
        # Always add _id as a secondary sort for stability
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
        # Always include _id for row identification
        projection = {"_id": 1}

        # Include all column data fields
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
                # Recursively process nested dictionaries
                self._format_result_values(val, full_key)
            elif isinstance(val, list):
                # Process lists - may contain dictionaries or other complex types
                for i, item in enumerate(val):
                    if isinstance(item, dict):
                        self._format_result_values(item, f"{full_key}[{i}]")
                    elif isinstance(item, ObjectId):
                        val[i] = str(item)
                    elif hasattr(item, 'isoformat'):
                        val[i] = item.isoformat()
            elif isinstance(val, ObjectId):
                result_dict[key] = str(val)
            elif hasattr(val, 'isoformat'):  # Handle date objects
                result_dict[key] = val.isoformat()
            elif isinstance(val, float):
                # Format floats to 2 decimal places by default
                result_dict[key] = val

    def results(self) -> List[Dict[str, Any]]:
        """Execute the MongoDB query with optimized pipeline.

        Returns:
            List of documents formatted for DataTables response
        """
        if self._results is not None:
            return self._results

        try:
            # Build an efficient aggregation pipeline
            pipeline = []

            # Add match stage first for better performance
            if self.filter:
                pipeline.append({"$match": self.filter})

            # Add sort stage
            if self.sort_specification:
                pipeline.append({"$sort": self.sort_specification})

            # Add pagination
            if self.start > 0:
                pipeline.append({"$skip": self.start})

            if self.limit:
                pipeline.append({"$limit": self.limit})

            # Add projection at the end
            pipeline.append({"$project": self.projection})

            # Execute the query
            cursor = self.collection.aggregate(pipeline)
            results = list(cursor)

            # Process the results
            processed_results = []
            for result in results:
                # Convert to dict if it's not already
                result_dict = dict(result)

                # Handle ObjectId for row identifier - create DT_RowId and remove _id
                if "_id" in result_dict:
                    result_dict["DT_RowId"] = str(result_dict["_id"])
                    # Remove the _id field as it's not needed in the client-side representation
                    del result_dict["_id"]

                # Format complex values and handle nested objects
                self._format_result_values(result_dict)

                processed_results.append(result_dict)

            self._results = processed_results
            return processed_results

        except PyMongoError as e:
            # Log the error and return empty results
            logger.error(f"Error executing MongoDB query: {str(e)}", exc_info=True)
            return []
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Unexpected error in results(): {str(e)}", exc_info=True)
            return []

    def count_total(self) -> int:
        """Count total records in the collection.

        Returns:
            Total number of records, or 0 if an error occurs
        """
        if self._recordsTotal is None:
            try:
                self._recordsTotal = self.collection.count_documents({})
            except PyMongoError as e:
                logger.error(f"Error counting total records: {str(e)}", exc_info=True)
                self._recordsTotal = 0
        return self._recordsTotal

    def count_filtered(self) -> int:
        """Count records after applying filters.

        Returns:
            Number of filtered records, or 0 if an error occurs
        """
        if self._recordsFiltered is None:
            try:
                if self.filter:
                    self._recordsFiltered = self.collection.count_documents(self.filter)
                else:
                    self._recordsFiltered = self.count_total()
            except PyMongoError as e:
                logger.error(f"Error counting filtered records: {str(e)}", exc_info=True)
                self._recordsFiltered = 0
        return self._recordsFiltered

    def get_rows(self) -> Dict[str, Any]:
        """Get the complete formatted response for DataTables.

        Returns:
            Dictionary containing all required DataTables response fields
        """
        return {
            "draw": int(self.request_args.get("draw", 1)),
            "recordsTotal": self.count_total(),
            "recordsFiltered": self.count_filtered(),
            "data": self.results(),
        }
