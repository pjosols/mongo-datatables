"""Server-side processing for jQuery DataTables with MongoDB."""

import json
from typing import Dict, List, Any, Optional, Tuple, Set
from bson.objectid import ObjectId
from pymongo.database import Database
from pymongo.collection import Collection


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
        debug_mode: bool = False,
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
            debug_mode: Whether to collect and return debug information (default: False)
            **custom_filter: Additional filtering criteria
        """
        # Initialize database connection
        self.collection = self._get_collection(pymongo_object, collection_name)
        self.request_args = request_args
        self.debug_mode = debug_mode
        
        # Store data fields
        self.data_fields = data_fields or []
        
        # Create internal mappings from data_fields
        self.field_types = {field.name: field.data_type for field in self.data_fields} if self.data_fields else {}
        self.ui_to_db_field_map = {field.alias: field.name for field in self.data_fields} if self.data_fields else {}
            
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

        # Query statistics (only collected if debug_mode is True)
        self._query_stats = {}
        if self.debug_mode:
            self._query_stats = {
                "used_text_index": False,
                "used_standard_index": False,
                "search_type": "none",
            }
        
        # Check for text indexes
        self._check_text_index()

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
        search_value = self.search_value
        if not search_value:
            return []
        
        # Use regular expression to handle quoted terms
        import re
        
        # Process the search value to handle quoted terms
        processed_terms = []
        
        # First, find all quoted phrases (both double and single quotes)
        # and replace them with placeholders
        quote_patterns = [(r'"([^"]*)"', '"{}"'), (r"'([^']*)'",'"{}"')]
        placeholders = {}
        placeholder_count = 0
        
        modified_search = search_value
        for pattern, format_str in quote_patterns:
            matches = re.findall(pattern, modified_search)
            for match in matches:
                # Create a unique placeholder
                placeholder = f"__QUOTED_TERM_{placeholder_count}__"
                placeholder_count += 1
                
                # Store the original quoted term
                placeholders[placeholder] = match
                
                # Replace the quoted term with the placeholder in the search string
                quoted_term = format_str.format(match)
                modified_search = modified_search.replace(quoted_term, placeholder)
        
        # Split the modified search string by whitespace
        split_terms = modified_search.split()
        
        # Replace placeholders with their original terms
        for term in split_terms:
            if term in placeholders:
                processed_terms.append(placeholders[term])
            else:
                processed_terms.append(term)
                
        return processed_terms

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
        conditions = []

        for column in self.columns:
            column_search = column.get("search", {})
            search_value = column_search.get("value", "")

            if search_value and column.get("searchable", False):
                column_name = column["data"]
                field_type = self.field_types.get(column_name)

                # Handle different field types
                if field_type == "number":
                    try:
                        numeric_value = float(search_value)
                        conditions.append({column_name: numeric_value})
                    except (ValueError, TypeError):
                        pass
                elif field_type == "date":
                    # Date search could be implemented here
                    conditions.append(
                        {column_name: {"$regex": search_value, "$options": "i"}}
                    )
                else:
                    # Default to case-insensitive regex for text
                    conditions.append(
                        {column_name: {"$regex": search_value, "$options": "i"}}
                    )

        if conditions:
            return {"$and": conditions}
        return {}

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
        if not search_terms:
            return {}

        # Get searchable columns
        searchable_columns = self.searchable_columns
        if not searchable_columns:
            return {}
            
        # Get the original search value to check if it was quoted
        original_search = self.search_value
        was_quoted = False
        if original_search:
            # Check if the original search was surrounded by quotes
            import re
            if re.match(r'^".*"$', original_search) or re.match(r"^'.*'$", original_search):
                was_quoted = True

        # If the search was originally quoted, we want exact phrase matching
        if was_quoted and len(search_terms) == 1:
            if self.debug_mode:
                self._query_stats["search_type"] = "exact_phrase"
                self._query_stats["search_terms"] = search_terms
            
            # For a quoted search, try to use text search if available
            if self.use_text_index and self.has_text_index:
                # MongoDB text search handles quoted phrases natively
                # The original quotes will be preserved in the search value
                self._query_stats["used_text_index"] = True
                self._query_stats["search_type"] = "text_exact_phrase"
                
                # Use the original quoted search value for text search
                # This preserves the quotes which MongoDB uses for exact phrase matching
                return {"$text": {"$search": original_search}}
            
            # Fall back to regex if text search is not available
            if self.debug_mode:
                self._query_stats["used_text_index"] = False
                self._query_stats["search_type"] = "regex_exact_phrase"
            
            # For a quoted search, create conditions for exact phrase matching
            or_conditions = []
            for column in searchable_columns:
                field_type = self.field_types.get(column)
                
                # Skip date and number fields for exact phrase matching
                if field_type in ("date", "number"):
                    continue
                    
                # Use word boundaries for exact phrase matching
                import re
                # Remove the quotes from the search term
                clean_term = search_terms[0]
                regex_term = re.escape(clean_term)  # Escape special regex characters
                or_conditions.append({column: {"$regex": f"\\b{regex_term}\\b", "$options": "i"}})
                
            if or_conditions:
                return {"$or": or_conditions}
            return {}
            
        # For non-quoted searches with text index available, use text search with OR semantics
        if self.use_text_index and self.has_text_index:
            # Combine all terms with spaces for OR semantics in text search
            text_search_query = " ".join(search_terms)
            if self.debug_mode:
                self._query_stats["used_text_index"] = True
                self._query_stats["search_type"] = "text_or"
                self._query_stats["search_terms"] = search_terms

            # Return a text search query that will use the index with OR semantics
            return {"$text": {"$search": text_search_query}}

        # For cases when text index is not available, use regex search with OR semantics
        if self.debug_mode:
            self._query_stats["used_text_index"] = False
            self._query_stats["search_type"] = "regex_or"
            self._query_stats["search_terms"] = search_terms

        # Create a single $or condition for all terms
        or_conditions = []
        for term in search_terms:
            for column in searchable_columns:
                field_type = self.field_types.get(column)

                # Skip date fields for text search
                if field_type == "date":
                    continue

                # For number fields, try to convert the term to a number
                if field_type == "number":
                    try:
                        numeric_value = float(term)
                        or_conditions.append({column: numeric_value})
                    except (ValueError, TypeError):
                        # Not a valid number, skip this field entirely
                        pass
                else:
                    # For text fields, use case-insensitive regex
                    or_conditions.append({column: {"$regex": term, "$options": "i"}})

        if or_conditions:
            return {"$or": or_conditions}
        return {}

    @property
    def column_specific_search_condition(self) -> Dict[str, Any]:
        """Generate search conditions for column-specific searches using the colon syntax.

        Handles search terms in the format "field:value" for targeted column searching.
        Also supports comparison operators: >, <, >=, <=, = for numeric and date fields.

        Returns:
            MongoDB query condition for column-specific searches
        """
        colon_terms = self.search_terms_with_a_colon
        if not colon_terms:
            return {}

        and_conditions = []
        for term in colon_terms:
            field, value = term.split(":", 1)
            field = field.strip()
            value = value.strip()

            if not field or not value:
                continue
                
            # Map UI field name to database field name if it exists in the mapping
            db_field = self.ui_to_db_field_map.get(field, field)

            # Check if the field is searchable - either the UI field name or DB field name should be searchable
            # This handles cases where the search term uses the UI field name (like 'Published')
            # but the actual searchable column might be using the DB field name (like 'PublisherInfo.Date')
            if field not in self.searchable_columns and db_field not in self.searchable_columns:
                continue

            # Handle different field types - use the database field name for lookup
            field_type = self.field_types.get(db_field)

            # Check for comparison operators
            operator = None
            if value.startswith(">") and not value.startswith(">="):
                operator = ">"
                value = value[1:].strip()
            elif value.startswith("<") and not value.startswith("<="):
                operator = "<"
                value = value[1:].strip()
            elif value.startswith(">="):
                operator = ">="
                value = value[2:].strip()
            elif value.startswith("<="):
                operator = "<="
                value = value[2:].strip()
            elif value.startswith("="):
                operator = "="
                value = value[1:].strip()

            if field_type == "number":
                # For numeric fields, try to apply comparison operators
                try:
                    numeric_value = float(value)

                    if operator == ">":
                        and_conditions.append({db_field: {"$gt": numeric_value}})
                    elif operator == "<":
                        and_conditions.append({db_field: {"$lt": numeric_value}})
                    elif operator == ">=":
                        and_conditions.append({db_field: {"$gte": numeric_value}})
                    elif operator == "<=":
                        and_conditions.append({db_field: {"$lte": numeric_value}})
                    elif operator == "=":
                        and_conditions.append({db_field: numeric_value})
                    else:
                        # No operator, exact match
                        and_conditions.append({db_field: numeric_value})
                except (ValueError, TypeError):
                    # Not a valid number, use regex search
                    and_conditions.append({db_field: {"$regex": value, "$options": "i"}})
            elif field_type == "date":
                # Handle date comparisons for MongoDB ISODate objects
                from datetime import datetime
                
                # Use the operator already detected above
                date_value = value
                
                # Try to parse the date value
                try:
                    # Handle ISO format dates (YYYY-MM-DD)
                    if '-' in date_value and len(date_value.split('-')) == 3:
                        # Parse the date and create an ISODate-compatible format
                        # MongoDB ISODate objects are stored with time component
                        year, month, day = date_value.split('-')
                        
                        # For exact date matches, we need to match the entire day
                        start_date = datetime(int(year), int(month), int(day))
                        
                        # Apply the appropriate comparison operator
                        if operator == '>':
                            # For greater than, we want dates strictly after the specified date
                            next_day = datetime(int(year), int(month), int(day) + 1)
                            condition = {db_field: {"$gte": next_day}}
                        elif operator == '<':
                            # For less than, we want dates strictly before the specified date
                            condition = {db_field: {"$lt": start_date}}
                        elif operator == '>=':
                            # For greater than or equal, we want dates on or after the specified date
                            condition = {db_field: {"$gte": start_date}}
                        elif operator == '<=':
                            # For less than or equal, we want dates on or before the end of the specified date
                            next_day = datetime(int(year), int(month), int(day) + 1)
                            condition = {db_field: {"$lt": next_day}}
                        elif operator == '=':
                            # For exact date match, match the whole day
                            next_day = datetime(int(year), int(month), int(day) + 1)
                            condition = {db_field: {"$gte": start_date, "$lt": next_day}}
                        else:
                            # For no operator, also match the whole day
                            next_day = datetime(int(year), int(month), int(day) + 1)
                            condition = {db_field: {"$gte": start_date, "$lt": next_day}}
                        
                        and_conditions.append(condition)
                    else:
                        # Not in ISO format, fall back to regex search
                        condition = {db_field: {"$regex": value, "$options": "i"}}
                        and_conditions.append(condition)
                except Exception as e:
                    # If date parsing fails, fall back to regex search
                    condition = {db_field: {"$regex": value, "$options": "i"}}
                    and_conditions.append(condition)
            else:
                # Default to case-insensitive regex for text
                and_conditions.append({db_field: {"$regex": value, "$options": "i"}})

        if and_conditions:
            return {"$and": and_conditions}
        return {}

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
            if self.debug_mode:
                self._query_stats["sort_column_ui"] = default_sort_field
                self._query_stats["sort_column_db"] = default_sort_field
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
                    db_field_name = self.ui_to_db_field_map.get(ui_field_name, ui_field_name)
                    sort_spec[db_field_name] = dir_value
                    
                    # Add debug information if enabled
                    if self.debug_mode:
                        self._query_stats["sort_column_ui"] = ui_field_name
                        self._query_stats["sort_column_db"] = db_field_name
                else:
                    # Invalid field name, use default
                    sort_spec[default_sort_field] = default_sort_dir
                    if self.debug_mode:
                        self._query_stats["sort_column_ui"] = default_sort_field
                        self._query_stats["sort_column_db"] = default_sort_field
                        self._query_stats["sort_error"] = f"Invalid column index {col_idx} - no data field"
            else:
                # Invalid column index, use default
                sort_spec[default_sort_field] = default_sort_dir
                if self.debug_mode:
                    self._query_stats["sort_column_ui"] = default_sort_field
                    self._query_stats["sort_column_db"] = default_sort_field
                    self._query_stats["sort_error"] = f"Invalid column index {col_idx}"
        
        # Always add _id as a secondary sort for stability
        if "_id" not in sort_spec:
            sort_spec["_id"] = 1
        
        # Track sorted fields for diagnostics if debug mode is enabled
        if self.debug_mode:
            self._query_stats["sorted_fields"] = list(sort_spec.keys())
            self._query_stats["sort_spec"] = sort_spec
        
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

        except Exception as e:
            # Log the error and return empty results
            # Use proper logging instead of print
            import logging
            logging.error(f"Error executing MongoDB query: {str(e)}")
            return []

    def count_total(self) -> int:
        """Count total records in the collection.

        Returns:
            Total number of records, or 0 if an error occurs
        """
        if self._recordsTotal is None:
            try:
                self._recordsTotal = self.collection.count_documents({})
            except Exception as e:
                import logging
                logging.error(f"Error counting total records: {str(e)}")
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
            except Exception as e:
                import logging
                logging.error(f"Error counting filtered records: {str(e)}")
                self._recordsFiltered = 0
        return self._recordsFiltered

    def get_rows(self) -> Dict[str, Any]:
        """Get the complete formatted response for DataTables with query stats.

        Returns:
            Dictionary containing all required DataTables response fields plus query stats
        """
        response = {
            "draw": int(self.request_args.get("draw", 1)),
            "recordsTotal": self.count_total(),
            "recordsFiltered": self.count_filtered(),
            "data": self.results(),
        }

        # Add query stats for debugging only if debug mode is enabled
        if self.debug_mode:
            response["query_stats"] = self._query_stats

        return response
