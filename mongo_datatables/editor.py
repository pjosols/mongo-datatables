"""MongoDB server-side processor for DataTables Editor.

This module provides server-side implementation for the DataTables Editor extension,
enabling CRUD (Create, Read, Update, Delete) operations on MongoDB collections.

DataTables Editor (https://editor.datatables.net/) is a commercial extension for
DataTables that provides end users with the ability to create, edit and delete
entries in a DataTable. This implementation translates Editor requests into
MongoDB operations.

Features:
    - Create new documents in MongoDB collections
    - Edit existing documents with field-level updates
    - Delete documents individually or in batches
    - Automatic handling of MongoDB ObjectIds
    - Support for complex data types (arrays, objects, dates)
    - Error handling and validation

Example usage with Flask:
    ```python
    @app.route('/api/editor/<collection>', methods=['POST'])
    def editor_endpoint(collection):
        data = request.get_json()
        doc_id = request.args.get('id', '')
        result = Editor(mongo, collection, data, doc_id).process()
        return jsonify(result)
    ```
"""
from typing import Dict, List, Any, Optional
from bson.objectid import ObjectId
import json
from pymongo.database import Database
from pymongo.collection import Collection
from datetime import datetime

# Import DataField from datatables module
from mongo_datatables.datatables import DataField


class Editor:
    """Server-side processor for DataTables Editor with MongoDB.

    This class handles CRUD operations from DataTables Editor, translating them
    into appropriate MongoDB operations.
    """

    def __init__(
        self,
        pymongo_object: Any,
        collection_name: str,
        request_args: Dict[str, Any],
        doc_id: Optional[str] = None,
        data_fields: Optional[List[DataField]] = None
    ) -> None:
        """Initialize the Editor processor.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: Editor request parameters (from request.get_json())
            doc_id: Comma-separated list of document IDs for edit/remove operations
            data_fields: List of DataField objects defining database fields with UI mappings
        """
        self.mongo = pymongo_object
        self.collection_name = collection_name
        self.request_args = request_args or {}
        self.doc_id = doc_id or ""
        
        # Store data fields
        self.data_fields = data_fields or []
        
        # Create internal mappings from data_fields
        self.field_types = {field.name: field.data_type for field in self.data_fields} if self.data_fields else {}
        self.ui_to_db_field_map = {field.alias: field.name for field in self.data_fields} if self.data_fields else {}

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
        
    def map_ui_field_to_db_field(self, field_name: str) -> str:
        """Map a UI field name to its corresponding database field name.
        
        Args:
            field_name: The UI field name to map
            
        Returns:
            The corresponding database field name, or the original field name if no mapping exists
        """
        return self.ui_to_db_field_map.get(field_name, field_name)

    @property
    def action(self) -> str:
        """Get the Editor action type.

        Returns:
            Action type (create, edit, remove)
        """
        return self.request_args.get("action", "")

    @property
    def data(self) -> Dict[str, Any]:
        """Get the data payload from the request.

        Returns:
            Dictionary containing the submitted data
        """
        return self.request_args.get("data", {})

    @property
    def list_of_ids(self) -> List[str]:
        """Get list of document IDs for batch operations.

        Returns:
            List of document ID strings
        """
        if not self.doc_id:
            return []
        return self.doc_id.split(",")

    def _format_response_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format document for response to Editor.

        - Converts ObjectId to string DT_RowId
        - Formats complex types for JSON

        Args:
            doc: Document from MongoDB

        Returns:
            Formatted document for Editor response
        """
        response_doc = dict(doc)  # Create a copy to avoid modifying the original

        # Handle ObjectId
        if '_id' in response_doc:
            response_doc['DT_RowId'] = str(response_doc.pop('_id'))

        # Format complex types for JSON response
        for key, val in response_doc.items():
            if isinstance(val, ObjectId):
                response_doc[key] = str(val)
            elif isinstance(val, datetime):
                response_doc[key] = val.isoformat()

        return response_doc

    def remove(self) -> Dict[str, Any]:
        """Remove one or more documents from the collection.

        Returns:
            Empty dict on success

        Raises:
            ValueError: If no document IDs are provided
        """
        if not self.list_of_ids:
            raise ValueError("Document ID is required for remove operation")

        try:
            for doc_id in self.list_of_ids:
                self.collection.delete_one({"_id": ObjectId(doc_id)})
            return {}
        except Exception as e:
            return {"error": str(e)}

    def create(self) -> Dict[str, Any]:
        """Create a new document in the collection."""
        if not self.data or '0' not in self.data:
            raise ValueError("Data is required for create operation")

        try:
            # Process the document using our updated method
            main_data, dot_notation_data = self._preprocess_document(self.data['0'])

            # Build the combined document for creation
            data_obj = main_data.copy()

            # Handle nested fields by parsing dot notation
            for dot_key, value in dot_notation_data.items():
                parts = dot_key.split('.')
                current = data_obj

                # Build nested structure
                for i, part in enumerate(parts[:-1]):
                    if part not in current:
                        current[part] = {}
                    current = current[part]

                # Set the final value
                current[parts[-1]] = value

            # Insert the document
            result = self.collection.insert_one(data_obj)

            # Get the inserted document for the response
            created_doc = self.collection.find_one({"_id": result.inserted_id})

            # Format the response
            response_data = self._format_response_document(created_doc)

            return {"data": [response_data]}
        except Exception as e:
            print(f"Error in create operation: {e}")
            return {"error": str(e)}

    def _preprocess_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process document data before database operations.

        - Converts string JSON to Python objects
        - Handles special data types including dates
        - Properly structures nested fields using dot notation
        - Removes empty values

        Args:
            doc: Document data from Editor

        Returns:
            Processed document ready for database operation
        """
        # Remove empty values to avoid overwriting with nulls
        processed_doc = {k: v for k, v in doc.items() if v is not None}

        # Store dot notation fields for MongoDB update operations
        dot_notation_updates = {}

        # Process each field
        for key, val in processed_doc.items():
            # Try to parse JSON strings into objects/arrays
            if isinstance(val, str):
                try:
                    parsed_val = json.loads(val)
                    if '.' in key:
                        # Store with dot notation for MongoDB update
                        dot_notation_updates[key] = parsed_val
                    else:
                        processed_doc[key] = parsed_val
                    continue  # Skip further processing for this field
                except json.JSONDecodeError:
                    # Not valid JSON, continue with other processing
                    pass

            # Handle date strings - check both the key itself and the final segment after a dot
            is_date_field = (
                    key.lower().endswith(('date', 'time', 'at')) or
                    (key.split('.')[-1].lower().endswith(('date', 'time', 'at')))
            )

            if isinstance(val, str) and is_date_field and val.strip():
                try:
                    # Convert to datetime object for MongoDB
                    date_obj = datetime.fromisoformat(val.replace('Z', '+00:00'))

                    if '.' in key:
                        # Store with dot notation for MongoDB update
                        dot_notation_updates[key] = date_obj
                    else:
                        processed_doc[key] = date_obj
                except (ValueError, TypeError):
                    # If date parsing fails, keep as string but still handle dot notation
                    if '.' in key:
                        dot_notation_updates[key] = val
            elif '.' in key:
                # Non-date field with dot notation
                dot_notation_updates[key] = val

        # Remove all dot notation fields from the main doc since we'll handle them separately
        for key in list(processed_doc.keys()):
            if '.' in key:
                del processed_doc[key]

        # Return both the processed document and dot notation fields
        return processed_doc, dot_notation_updates

    def edit(self) -> Dict[str, Any]:
        """Edit one or more documents in the collection."""
        if not self.list_of_ids:
            raise ValueError("Document ID is required for edit operation")

        try:
            data = []

            for doc_id in self.list_of_ids:
                if doc_id not in self.data:
                    continue

                # Get the update data for this document
                update_data = self.data[doc_id]

                # Process document with recursive function
                updates = {}
                self._process_updates(update_data, updates, "")

                # If we have updates, apply them all at once
                if updates:
                    self.collection.update_one(
                        {"_id": ObjectId(doc_id)},
                        {"$set": updates}
                    )

                # Get the updated document
                updated_doc = self.collection.find_one({"_id": ObjectId(doc_id)})

                # Format response
                response_data = self._format_response_document(updated_doc)
                data.append(response_data)

            return {"data": data}
        except Exception as e:
            print(f"Edit error: {e}")
            return {"error": str(e)}

    def _process_updates(self, data, updates, prefix=""):
        """Recursively process updates at any nesting level.

        Args:
            data: Data to process (dict or value)
            updates: Updates dict to populate (will be modified in-place)
            prefix: Dot notation prefix for the current nesting level
        """
        if not isinstance(data, dict):
            return

        for key, value in data.items():
            if value is None:
                continue

            # Build the full key path with dot notation
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                # Recurse into nested dictionaries
                self._process_updates(value, updates, full_key)
            else:
                # Process leaf value
                field_type = self.field_types.get(full_key, 'string')

                if field_type == 'date' and isinstance(value, str):
                    try:
                        # Convert to datetime
                        if 'T' in value:
                            date_obj = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        else:
                            date_obj = datetime.fromisoformat(f"{value}T00:00:00")

                        updates[full_key] = date_obj
                    except Exception as e:
                        print(f"Date conversion error for {full_key}: {e}")
                        updates[full_key] = value
                elif field_type == 'number' and isinstance(value, str):
                    try:
                        # Convert string to number
                        if '.' in value:
                            updates[full_key] = float(value)
                        else:
                            updates[full_key] = int(value)
                    except ValueError:
                        updates[full_key] = value
                elif field_type == 'boolean' and isinstance(value, str):
                    # Convert string to boolean
                    updates[full_key] = value.lower() in ('true', 'yes', '1', 't', 'y')
                elif field_type == 'array' and isinstance(value, str):
                    try:
                        # Try to parse JSON array
                        updates[full_key] = json.loads(value)
                    except json.JSONDecodeError:
                        # If not valid JSON, use as single value
                        updates[full_key] = [value]
                else:
                    # Standard field
                    updates[full_key] = value

    def process(self) -> Dict[str, Any]:
        """Process the Editor request based on the action.

        Returns:
            Response data for the Editor client

        Raises:
            ValueError: If action is not supported
        """
        actions = {
            "create": self.create,
            "edit": self.edit,
            "remove": self.remove
        }

        if self.action not in actions:
            raise ValueError(f"Unsupported action: {self.action}")

        return actions[self.action]()