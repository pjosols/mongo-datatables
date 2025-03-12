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
        doc_id: Optional[str] = None
    ) -> None:
        """Initialize the Editor processor.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: Editor request parameters (from request.get_json())
            doc_id: Comma-separated list of document IDs for edit/remove operations

        Example:
            ```python
            # Flask route example
            @app.route('/api/editor', methods=['POST'])
            def editor_endpoint():
                data = request.get_json()
                doc_id = request.args.get('id', '')
                result = Editor(mongo, 'users', data, doc_id).process()
                return jsonify(result)
            ```
        """
        self.mongo = pymongo_object
        self.collection_name = collection_name
        self.request_args = request_args or {}
        self.doc_id = doc_id or ""

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

    def _preprocess_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process document data before database operations.

        - Converts string JSON to Python objects
        - Handles special data types
        - Removes empty values

        Args:
            doc: Document data from Editor

        Returns:
            Processed document ready for database operation
        """
        # Remove empty values to avoid overwriting with nulls
        processed_doc = {k: v for k, v in doc.items() if v is not None}

        # Process each field
        for key, val in processed_doc.items():
            # Try to parse JSON strings into objects/arrays
            if isinstance(val, str):
                try:
                    processed_doc[key] = json.loads(val)
                except json.JSONDecodeError:
                    # Not valid JSON, keep as string
                    pass

            # Handle date strings with ISO format
            if isinstance(val, str) and key.lower().endswith(('date', 'time', 'at')):
                try:
                    processed_doc[key] = datetime.fromisoformat(val.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    # Not a valid date string, keep as is
                    pass

        return processed_doc

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
        """Create a new document in the collection.

        Returns:
            Dict with 'data' key containing the created document

        Raises:
            ValueError: If no data is provided
        """
        if not self.data or '0' not in self.data:
            raise ValueError("Data is required for create operation")

        try:
            # Process the document
            data_obj = self._preprocess_document(self.data['0'])

            # Insert the document
            result = self.collection.insert_one(data_obj)

            # Get the inserted document for the response
            created_doc = self.collection.find_one({"_id": result.inserted_id})

            # Format the response
            response_data = self._format_response_document(created_doc)

            return {"data": [response_data]}
        except Exception as e:
            return {"error": str(e)}

    def edit(self) -> Dict[str, Any]:
        """Edit one or more documents in the collection.

        Returns:
            Dict with 'data' key containing the updated documents

        Raises:
            ValueError: If no document IDs are provided
        """
        if not self.list_of_ids:
            raise ValueError("Document ID is required for edit operation")

        try:
            data = []

            for doc_id in self.list_of_ids:
                if doc_id not in self.data:
                    continue

                # Process the document
                update_data = self._preprocess_document(self.data[doc_id])

                # Update the document
                self.collection.update_one(
                    {"_id": ObjectId(doc_id)},
                    {"$set": update_data}
                )

                # Get the updated document for the response
                updated_doc = self.collection.find_one({"_id": ObjectId(doc_id)})

                # Format the response
                response_data = self._format_response_document(updated_doc)

                data.append(response_data)

            return {"data": data}
        except Exception as e:
            return {"error": str(e)}

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
