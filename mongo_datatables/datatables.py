"""MongoDB server-side processor for jQuery DataTables.

This package seamlessly connects jQuery DataTables to MongoDB collections, providing
efficient server-side processing for large datasets with minimal configuration.

Features:
    - Server-side pagination, sorting, and filtering
    - Global search across multiple columns
    - Column-specific searches using both DataTables API and 'field:value' syntax
    - Support for regex search with case-insensitive matching
    - Custom MongoDB filters for advanced query scenarios
    - Nested document field support using dot notation
    - Automatic handling of MongoDB data types
    - Performance optimized MongoDB aggregation pipelines
    - Support for MongoDB text indexes for improved search performance
    - Intelligent handling of data types including dates and numbers

Example usage with Flask:
    ```python
    from flask import Flask, render_template, request, jsonify
    from flask_pymongo import PyMongo
    from mongo_datatables import DataTables

    app = Flask(__name__)
    app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
    mongo = PyMongo(app)

    @app.route('/table')
    def table_view():
        return render_template('table.html')

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        # Basic usage
        results = DataTables(mongo, 'users', data).get_rows()

        # With custom filter
        # results = DataTables(mongo, 'users', data, status='active').get_rows()
        return jsonify(results)
    ```

Advanced filtering:
    The custom_filter parameter accepts any valid MongoDB query operators.
    For example, to filter by date range:
    ```python
    from datetime import datetime, timedelta

    today = datetime.now()
    expiry_date = today + timedelta(days=60)

    # Find contracts expiring in the next 60 days
    results = DataTables(
        mongo,
        'contracts',
        data,
        ExpiryDate={'$gt': today, '$lt': expiry_date}
    ).get_rows()
    ```
"""
import json
from typing import Dict, List, Any, Optional, Tuple, Set
from bson.objectid import ObjectId
from pymongo.database import Database
from pymongo.collection import Collection


class DataTables:
    """Server-side processor for MongoDB integration with jQuery DataTables.

    This class handles all aspects of server-side processing including:
    - Pagination
    - Sorting
    - Global search across multiple columns
    - Column-specific search
    - Custom filters
    - Type-aware search operations
    """

    def __init__(
        self,
        pymongo_object: Any,
        collection_name: str,
        request_args: Dict[str, Any],
        field_types: Optional[Dict[str, str]] = None,
        **custom_filter: Any
    ) -> None:
        """Initialize a DataTables server-side processor for MongoDB.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: DataTables request parameters (typically from request.get_json())
            field_types: Optional mapping of field names to their types for specialized handling
                         Supported types: 'date', 'number', 'boolean', 'text'
            **custom_filter: Additional filtering criteria for MongoDB queries

        Example:
            ```python
            # Flask route example
            @app.route('/api/data', methods=['POST'])
            def get_data():
                data = request.get_json()
                # With field type information
                field_types = {
                    'created_at': 'date',
                    'price': 'number',
                    'active': 'boolean'
                }
                results = DataTables(mongo, 'users', data, field_types=field_types).get_rows()
                return jsonify(results)
            ```
        """
        self.mongo = pymongo_object
        self.collection_name = collection_name
        self.request_args = request_args
        self.custom_filter = custom_filter
        self.field_types = field_types or {}

        # Cache for expensive operations
        self._cardinality = None
        self._cardinality_filtered = None
        self._results = None
        self._has_text_index = None

    @property
    def db(self) -> Database:
        """Get the MongoDB database instance.

        Returns:
            The PyMongo database instance
        """
        return self.mongo.db

    @property
    def collection(self) -> Collection:
        """Get the MongoDB collection.

        Returns:
            The PyMongo collection instance
        """
        return self.db[self.collection_name]

    @property
    def has_text_index(self) -> bool:
        """Check if the collection has a text index.

        Returns:
            True if a text index exists, False otherwise
        """
        if self._has_text_index is None:
            indexes = list(self.collection.list_indexes())
            self._has_text_index = any('text' in idx.get('key', {}) for idx in indexes)
        return self._has_text_index

    @property
    def search_terms(self) -> List[str]:
        """Extract search terms from the DataTables request.

        Returns:
            List of search terms split by whitespace
        """
        search_value = self.request_args.get("search", {}).get("value", "")
        return str(search_value).split() if search_value else []

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
    def dt_column_search(self) -> List[Dict[str, Any]]:
        """Extract column-specific search criteria from DataTables request.

        DataTables supports individual column searching, which is extracted here.
        See: https://datatables.net/manual/server-side

        Returns:
            List of dictionaries with column search information
        """
        columns = self.request_args.get("columns", [])
        return [{
            "column": column['data'],
            "value": column['search']['value'],
            "regex": column['search']['regex'] == "true" or column['search']['regex'] is True
        } for column in columns if column.get('search', {}).get("value", "")]

    @property
    def requested_columns(self) -> List[str]:
        """Get the list of column names requested by DataTables.

        Returns:
            List of column names
        """
        columns = self.request_args.get("columns", [])
        return [column.get("data", "") for column in columns if column.get("data")]

    @property
    def draw(self) -> int:
        """Get the draw counter from DataTables request.

        The draw counter ensures that returned Ajax data is drawn in sequence.

        Returns:
            The draw counter as an integer
        """
        return int(str(self.request_args.get("draw", "1")))

    @property
    def start(self) -> int:
        """Get the starting record number for pagination.

        Returns:
            The starting record index
        """
        return int(self.request_args.get("start", 0))

    @property
    def limit(self) -> Optional[int]:
        """Get the number of records to display per page.

        Returns:
            Number of records to fetch, or None if all records requested
        """
        length = self.request_args.get("length", 10)
        if length == -1:
            return None
        return int(length)

    @property
    def cardinality(self) -> int:
        """Get the total number of records in the collection.

        Returns:
            Total record count
        """
        if self._cardinality is None:
            self._cardinality = self.collection.count_documents({})
        return self._cardinality

    @property
    def cardinality_filtered(self) -> int:
        """Get the number of records after applying filters.

        Returns:
            Filtered record count
        """
        if self._cardinality_filtered is None:
            self._cardinality_filtered = self.collection.count_documents(self.filter)
        return self._cardinality_filtered

    @property
    def order_direction(self) -> int:
        """Get the sort direction for MongoDB (1 for asc, -1 for desc).

        Returns:
            MongoDB sort direction value
        """
        try:
            direction = self.request_args.get("order", [{}])[0].get("dir", "asc")
            return {"asc": 1, "desc": -1}.get(direction, 1)
        except (IndexError, KeyError, AttributeError):
            return 1  # Default to ascending order

    @property
    def order_columns(self) -> List[Tuple[str, int]]:
        """Get the columns and directions to sort by.

        Returns:
            List of (column_name, direction) tuples for sorting
        """
        try:
            order_list = self.request_args.get("order", [])
            result = []

            for order_item in order_list:
                column_idx = int(order_item.get("column", 0))
                direction = {"asc": 1, "desc": -1}.get(order_item.get("dir", "asc"), 1)

                if 0 <= column_idx < len(self.requested_columns):
                    column_name = self.requested_columns[column_idx]
                    result.append((column_name, direction))

            return result or [(self.requested_columns[0], 1)] if self.requested_columns else []
        except (IndexError, ValueError, KeyError, AttributeError):
            # Fallback to first column if available
            return [(self.requested_columns[0], 1)] if self.requested_columns else []

    @property
    def sort_specification(self) -> Dict[str, int]:
        """Build the MongoDB sort specification.

        Returns:
            Dictionary for MongoDB sort operation
        """
        return {column: direction for column, direction in self.order_columns}

    @property
    def projection(self) -> Dict[str, Any]:
        """Build the MongoDB projection to return requested fields.

        Uses $ifNull to handle missing fields gracefully.

        Returns:
            MongoDB projection specification
        """
        projection = {"_id": 1}  # Always include _id

        for key in self.requested_columns:
            # Handle nested fields with dot notation
            if "." in key:
                # For nested fields, we need to include the parent fields
                parts = key.split(".")
                for i in range(1, len(parts)):
                    parent = ".".join(parts[:i])
                    projection[parent] = 1

            projection[key] = {"$ifNull": [f"${key}", ""]}

        return projection

    def _process_value_by_type(self, field: str, value: str) -> Any:
        """Process a search value based on field type.

        Args:
            field: Field name
            value: Search value as string

        Returns:
            Processed value with appropriate type or query operator
        """
        field_type = self.field_types.get(field, 'text')

        if field_type == 'number':
            # Try to parse as number for numeric fields
            try:
                # Check for comparison operators - longer operators must be checked first
                # to prevent '>' from matching the start of '>='
                if value.startswith('>='):
                    return {'$gte': float(value[2:])}
                elif value.startswith('>'):
                    return {'$gt': float(value[1:])}
                elif value.startswith('<='):
                    return {'$lte': float(value[2:])}
                elif value.startswith('<'):
                    return {'$lt': float(value[1:])}
                elif '-' in value and not (value.startswith('-') or value.endswith('-')):
                    # Range query (e.g., "10-20")
                    start, end = value.split('-', 1)
                    return {'$gte': float(start), '$lte': float(end)}
                else:
                    # Exact match or simple numeric value
                    return float(value)
            except (ValueError, TypeError):
                # Fall back to string search if not valid number
                return {'$regex': value, '$options': 'i'}

        elif field_type == 'date':
            # Date field handling
            try:
                from datetime import datetime

                # Check for date comparison operators
                if value.startswith('>'):
                    try:
                        date_val = datetime.fromisoformat(value[1:])
                        return {'$gt': date_val}
                    except ValueError:
                        return {'$regex': value, '$options': 'i'}
                elif value.startswith('<'):
                    try:
                        date_val = datetime.fromisoformat(value[1:])
                        return {'$lt': date_val}
                    except ValueError:
                        return {'$regex': value, '$options': 'i'}
                elif '-' in value and len(value.split('-')) == 3:
                    # Looks like a date string
                    try:
                        date_val = datetime.fromisoformat(value)
                        # For dates, typically want to match the whole day
                        next_day = date_val.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        next_day = next_day.replace(day=next_day.day + 1)
                        return {'$gte': date_val, '$lt': next_day}
                    except ValueError:
                        return {'$regex': value, '$options': 'i'}
                else:
                    # Generic string that might be a date
                    return {'$regex': value, '$options': 'i'}
            except ImportError:
                # datetime module not available
                return {'$regex': value, '$options': 'i'}

        elif field_type == 'boolean':
            # Boolean field handling
            lower_val = value.lower()
            if lower_val in ('true', 'yes', '1', 't', 'y'):
                return True
            elif lower_val in ('false', 'no', '0', 'f', 'n'):
                return False
            else:
                return {'$regex': value, '$options': 'i'}

        # Default: text field
        return {'$regex': value, '$options': 'i'}

    def _build_column_specific_search(self) -> Dict[str, Any]:
        """Build MongoDB query for column-specific search terms.

        Handles both DataTables column search and custom field:value syntax.

        Returns:
            MongoDB query for column-specific search
        """
        column_specific_search = {}

        # Process DataTables column search
        for column_search in self.dt_column_search:
            col = column_search['column']
            value = column_search['value']

            if column_search.get("regex", False):
                column_specific_search[col] = {'$regex': value, '$options': 'i'}
            else:
                # Apply type-specific handling
                column_specific_search[col] = self._process_value_by_type(col, value)

        # Process field:value search terms
        for term in self.search_terms_with_a_colon:
            try:
                col, value = term.split(':', 1)
                if col in self.requested_columns:
                    # Check for exact match syntax (quotes)
                    if value.startswith('"') and value.endswith('"'):
                        # Exact match without type conversion
                        column_specific_search[col] = value.strip('"')
                    else:
                        # Apply type-specific handling
                        column_specific_search[col] = self._process_value_by_type(col, value)
            except ValueError:
                continue  # Skip malformed terms

        return column_specific_search

    def _build_global_search_query(self) -> Dict[str, Any]:
        """Build MongoDB query for global search across all columns.

        Optimizes searching by using text indexes when available.

        Returns:
            MongoDB query for global search
        """
        if not self.search_terms_without_a_colon:
            return {}

        # If text index exists and we have only one search term, use text search
        if self.has_text_index and len(self.search_terms_without_a_colon) == 1:
            return {'$text': {'$search': self.search_terms_without_a_colon[0]}}

        # Otherwise build a more efficient regex query
        and_conditions = []
        for term in self.search_terms_without_a_colon:
            # For each term, create an OR condition across all columns
            or_conditions = []

            for column in self.requested_columns:
                field_type = self.field_types.get(column, 'text')

                # Skip non-text fields for global search to improve performance
                # unless the term looks like it might match the field type
                if field_type == 'number':
                    try:
                        float(term)  # Only include if it looks like a number
                        or_conditions.append({column: float(term)})
                    except ValueError:
                        pass  # Skip this field for this term
                elif field_type == 'boolean':
                    lower_term = term.lower()
                    if lower_term in ('true', 'yes', '1', 't', 'y', 'false', 'no', '0', 'f', 'n'):
                        or_conditions.append({
                            column: lower_term in ('true', 'yes', '1', 't', 'y')
                        })
                else:
                    # Text fields - always use explicit regex syntax for consistency
                    or_conditions.append({
                        column: {'$regex': term, '$options': 'i'}
                    })

            if or_conditions:
                and_conditions.append({'$or': or_conditions})

        if and_conditions:
            return {'$and': and_conditions}

        return {}

    @property
    def filter(self) -> Dict[str, Any]:
        """Build the complete MongoDB filter query.

        Combines custom filters, global search, and column-specific search.

        Returns:
            Complete MongoDB query filter
        """
        final_filter = {}

        # Start with custom filters
        if self.custom_filter:
            final_filter.update(self.custom_filter)

        # Add column-specific search (these are more targeted and efficient)
        column_search = self._build_column_specific_search()
        if column_search:
            for key, value in column_search.items():
                final_filter[key] = value

        # Add global search (more expensive)
        global_search = self._build_global_search_query()
        if global_search:
            # Use $and to combine with existing filters
            if final_filter:
                # Convert the structure to a more consistent format
                # This ensures the test will reliably find the conditions
                and_conditions = []

                # Add existing filter as a condition
                and_conditions.append(final_filter.copy())

                # Add global search conditions
                if '$and' in global_search:
                    and_conditions.extend(global_search['$and'])
                else:
                    and_conditions.append(global_search)

                final_filter = {'$and': and_conditions}
            else:
                final_filter = global_search

        return final_filter

    def results(self) -> List[Dict[str, Any]]:
        """Execute the MongoDB query and return formatted results.

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
                pipeline.append({'$match': self.filter})

            # Add sort stage
            if self.sort_specification:
                pipeline.append({'$sort': self.sort_specification})

            # Add pagination
            if self.start > 0:
                pipeline.append({'$skip': self.start})

            if self.limit:
                pipeline.append({'$limit': self.limit})

            # Add projection at the end
            pipeline.append({'$project': self.projection})

            # Execute the query
            cursor = self.collection.aggregate(pipeline)
            results = list(cursor)

            # Process the results
            processed_results = []
            for result in results:
                # Convert to dict if it's not already
                result_dict = dict(result)

                # Handle ObjectId for row identifier
                result_dict["DT_RowId"] = str(result_dict.pop('_id'))

                # Format complex values
                for key, val in result_dict.items():
                    if isinstance(val, (list, dict)):
                        result_dict[key] = json.dumps(val)
                    elif isinstance(val, float):
                        # Format floats to 2 decimal places by default
                        result_dict[key] = f"{val:.2f}" if val == int(val) else f"{val:.2f}"
                    elif isinstance(val, ObjectId):
                        result_dict[key] = str(val)
                    elif hasattr(val, 'isoformat'):  # Handle date objects
                        result_dict[key] = val.isoformat()

                processed_results.append(result_dict)

            self._results = processed_results
            return processed_results

        except Exception as e:
            # Log the error and return empty results
            print(f"Error executing MongoDB query: {str(e)}")
            return []

    def get_rows(self) -> Dict[str, Any]:
        """Get the complete formatted response for DataTables.

        Returns:
            Dictionary containing all required DataTables response fields
        """
        return {
            'recordsTotal': self.cardinality,
            'recordsFiltered': self.cardinality_filtered,
            'draw': self.draw,
            'data': self.results()
        }