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
import json
import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from bson.objectid import ObjectId
from bson.errors import InvalidId as ObjectIdError
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from datetime import datetime

# Import DataField from datatables module
from mongo_datatables.datatables import DataField
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError, FieldMappingError
from mongo_datatables.utils import FieldMapper, TypeConverter, DateHandler

logger = logging.getLogger(__name__)


class StorageAdapter:
    """Pluggable storage backend for Editor file uploads.

    Subclass and implement :meth:`store` to persist uploaded files.
    Optionally implement :meth:`retrieve` and ``files_for_field`` to support
    additional features.

    **Optional protocol method** — ``files_for_field(field: str) -> dict``:
    If defined on a subclass, it is called after ``create`` and ``edit``
    operations (when ``file_fields`` is configured on :class:`Editor`) and
    after ``upload`` operations.  The returned dict is included in the Editor
    response as ``files[field]``, letting the client display thumbnails or
    filenames without a separate request.  Expected return shape::

        {"<file_id>": {"filename": "photo.jpg", "web_path": "/uploads/photo.jpg"}}

    If the method is absent, the ``files`` key is omitted from those responses
    (the existing ``hasattr`` check is preserved intentionally).
    """

    def store(self, field: str, filename: str, content_type: str, data: bytes) -> str:
        """Persist an uploaded file and return a unique identifier.

        Args:
            field: The Editor field name the upload belongs to.
            filename: Original filename as reported by the browser.
            content_type: MIME type of the uploaded file.
            data: Raw file bytes.

        Returns:
            A unique string ID that can later be passed to :meth:`retrieve`.

        Raises:
            NotImplementedError: Subclasses must implement this method.
        """
        raise NotImplementedError

    def retrieve(self, file_id: str) -> bytes:
        """Return the raw bytes for a previously stored file.

        Args:
            file_id: The ID string returned by :meth:`store`.

        Returns:
            Raw file bytes.

        Raises:
            NotImplementedError: Subclasses must implement this method.
        """
        raise NotImplementedError


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
        data_fields: Optional[List[DataField]] = None,
        validators: Optional[Dict[str, Any]] = None,
        storage_adapter: Optional["StorageAdapter"] = None,
        options=None,
        hooks: Optional[Dict[str, Any]] = None,
        row_class=None,
        row_data=None,
        row_attr=None,
        file_fields=None,
        dependent_handlers: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the Editor processor.

        Args:
            pymongo_object: PyMongo client connection or Flask-PyMongo instance
            collection_name: Name of the MongoDB collection
            request_args: Editor request parameters (from request.get_json())
            doc_id: Comma-separated list of document IDs for edit/remove operations
            data_fields: List of DataField objects defining database fields with UI mappings
            validators: Optional dict mapping field names to callables for field-level validation
            storage_adapter: Optional StorageAdapter instance for file upload handling
            options: Optional options dict or callable returning options dict
            hooks: Optional dict of pre-action hooks keyed by 'pre_create', 'pre_edit', 'pre_remove'.
                Each hook is callable(row_id, row_data) -> bool; falsy return cancels the row.
            row_class: Optional string or callable(row) -> str to set DT_RowClass on each response row.
            row_data: Optional dict or callable(row) -> dict to set DT_RowData on each response row.
            row_attr: Optional dict or callable(row) -> dict to set DT_RowAttr on each response row.
            file_fields: Optional list of field names that are upload fields. When set and
                storage_adapter has files_for_field(), 'files' is included in create/edit responses.
            dependent_handlers: Optional dict mapping field names to callables for dependent field
                Ajax requests. Each callable receives (field, values, rows) and returns a response
                dict with any of: options, values, messages, errors, labels, show, hide, enable, disable.
        """
        self.mongo = pymongo_object
        self.collection_name = collection_name
        self.request_args = request_args or {}
        self.doc_id = doc_id or ""
        self.data_fields = data_fields or []
        self.field_mapper = FieldMapper(self.data_fields)
        self.fields: Dict[str, DataField] = {f.alias: f for f in self.data_fields if isinstance(f, DataField)}
        self.validators = validators or {}
        self.storage_adapter = storage_adapter
        self._options = options
        self.hooks = hooks or {}
        self.row_class = row_class
        self.row_data = row_data
        self.row_attr = row_attr
        self.file_fields = file_fields or []
        self.dependent_handlers = dependent_handlers or {}
        self._collection = self._resolve_collection(pymongo_object, collection_name)

    def _resolve_options(self):
        if self._options is None:
            return None
        return self._options() if callable(self._options) else self._options

    @staticmethod
    def _resolve_collection(pymongo_object: Any, collection_name: str) -> Collection:
        """Resolve a MongoDB collection from various pymongo object types.

        Args:
            pymongo_object: Flask-PyMongo instance, MongoClient, Database, or dict-like object
            collection_name: Name of the collection to resolve

        Returns:
            The resolved PyMongo collection instance
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

    @property
    def db(self) -> Optional[Database]:
        """Get the MongoDB database instance.

        Returns:
            The PyMongo database instance, or None if not resolvable
        """
        if hasattr(self.mongo, "db"):
            return self.mongo.db
        elif hasattr(self.mongo, "get_database"):
            return self.mongo.get_database()
        elif isinstance(self.mongo, Database):
            return self.mongo
        return None

    @property
    def collection(self) -> Collection:
        """Get the MongoDB collection.

        Returns:
            The PyMongo collection instance
        """
        return self._collection
        
    def map_ui_field_to_db_field(self, field_name: str) -> str:
        """Map a UI field name to its corresponding database field name.

        Args:
            field_name: The UI field name to map

        Returns:
            The corresponding database field name, or the original field name if no mapping exists
        """
        return self.field_mapper.get_db_field(field_name)

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
        return [id_.strip() for id_ in self.doc_id.split(",") if id_.strip()]

    def _format_response_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format document for response to Editor.

        - Converts ObjectId to string DT_RowId
        - Formats complex types for JSON

        Args:
            doc: Document from MongoDB

        Returns:
            Formatted document for Editor response
        """
        response_doc = dict(doc)

        if '_id' in response_doc:
            response_doc['DT_RowId'] = str(response_doc.pop('_id'))

        for key, val in response_doc.items():
            if isinstance(val, ObjectId):
                response_doc[key] = str(val)
            elif isinstance(val, datetime):
                response_doc[key] = val.isoformat()

        if self.row_class is not None:
            response_doc['DT_RowClass'] = self.row_class(response_doc) if callable(self.row_class) else self.row_class
        if self.row_data is not None:
            response_doc['DT_RowData'] = self.row_data(response_doc) if callable(self.row_data) else self.row_data
        if self.row_attr is not None:
            response_doc['DT_RowAttr'] = self.row_attr(response_doc) if callable(self.row_attr) else self.row_attr

        return response_doc

    def _run_pre_hook(self, action: str, row_id: str, row_data: dict) -> bool:
        """Run a pre-action hook if registered; returns False to cancel the row.

        Args:
            action: Action name ('create', 'edit', 'remove')
            row_id: Row key or document ID
            row_data: Row data dict passed to the hook

        Returns:
            True to proceed, False to cancel this row
        """
        hook = self.hooks.get(f"pre_{action}")
        return bool(hook(row_id, row_data)) if hook else True

    def remove(self) -> Dict[str, Any]:
        """Remove one or more documents from the collection.

        Returns:
            Empty dict on success

        Raises:
            InvalidDataError: If no document IDs are provided or ID format is invalid
            DatabaseOperationError: If database operation fails
        """
        if not self.list_of_ids:
            raise InvalidDataError("Document ID is required for remove operation")

        try:
            cancelled = []
            for doc_id in self.list_of_ids:
                if not self._run_pre_hook("remove", doc_id, {}):
                    cancelled.append(doc_id)
                    continue
                try:
                    self.collection.delete_one({"_id": ObjectId(doc_id)})
                except (ObjectIdError, ValueError) as e:
                    raise InvalidDataError(f"Invalid document ID format: {doc_id}") from e
            return {"cancelled": cancelled} if cancelled else {}
        except InvalidDataError:
            raise
        except PyMongoError as e:
            raise DatabaseOperationError(f"Failed to delete documents: {str(e)}") from e

    def search(self) -> Dict[str, Any]:
        """Handle ``action=search`` for ``autocomplete`` and ``tags`` field types.

        When ``search`` is in the request, performs a case-insensitive prefix
        regex match and returns up to 100 deduplicated results.  When
        ``values[]`` is present instead, performs an exact ``$in`` lookup to
        resolve stored values back to display labels.

        Returns:
            ``{"data": [{"label": str, "value": any}, ...]}``
        """
        field = self.request_args.get("field", "")
        search_term = self.request_args.get("search", None)
        values = self.request_args.get("values", [])
        db_field = self.field_mapper.get_db_field(field) or field

        if search_term is not None:
            query = {db_field: {"$regex": re.escape(search_term), "$options": "i"}}
        elif values:
            query = {db_field: {"$in": self._coerce_values(field, values)}}
        else:
            return {"data": []}

        docs = self.collection.find(query, {db_field: 1}).limit(100)
        seen = set()
        results = []
        for doc in docs:
            val = doc.get(db_field)
            if val is None:
                continue
            key = str(val)
            if key not in seen:
                seen.add(key)
                results.append({"label": str(val), "value": val})
        return {"data": results}

    def _coerce_values(self, field: str, values: list) -> list:
        """Coerce string values from the request to the field's declared type."""
        data_field = self.fields.get(field)
        if data_field is None:
            return values
        field_type = getattr(data_field, "data_type", None)
        if field_type == "number":
            coerced = []
            for v in values:
                try:
                    coerced.append(int(v) if isinstance(v, str) and "." not in v else float(v))
                except (ValueError, TypeError):
                    coerced.append(v)
            return coerced
        if field_type == "boolean":
            return [v if isinstance(v, bool) else str(v).lower() in ("true", "1") for v in values]
        return values

    def dependent(self) -> Dict[str, Any]:
        """Handle dependent field Ajax requests.

        Called when a field configured with Editor's dependent() method triggers
        an Ajax call. Dispatches to a registered handler for the triggering field.

        Returns:
            Response dict with any of: options, values, messages, errors, labels,
            show, hide, enable, disable keys.

        Raises:
            InvalidDataError: If no handler is registered for the triggering field.
        """
        field = self.request_args.get("field", "")
        handler = self.dependent_handlers.get(field)
        if not handler:
            raise InvalidDataError(f"No dependent handler registered for field: {field}")
        values = self.request_args.get("values", {})
        rows = self.request_args.get("rows", [])
        return handler(field, values, rows)

    def upload(self) -> Dict[str, Any]:
        """Handle action=upload — store a file via the pluggable storage adapter.

        Expects request_args to contain:
            uploadField: name of the Editor field this file belongs to
            upload: dict with keys filename, content_type, data (bytes)

        Returns:
            {"upload": {"id": "<file_id>"}, "files": {<field>: {<id>: {...}} or {}}}

        Raises:
            InvalidDataError: if adapter, uploadField, or upload data is missing
        """
        if not self.storage_adapter:
            raise InvalidDataError("No storage adapter configured for file uploads")
        field = self.request_args.get("uploadField", "")
        if not field:
            raise InvalidDataError("uploadField is required for upload action")
        upload = self.request_args.get("upload")
        if not upload:
            raise InvalidDataError("No file data provided for upload")
        file_id = self.storage_adapter.store(
            field,
            upload.get("filename", ""),
            upload.get("content_type", ""),
            upload.get("data", b""),
        )
        files = {}
        if hasattr(self.storage_adapter, 'files_for_field'):
            files[field] = self.storage_adapter.files_for_field(field)
        return {"upload": {"id": file_id}, "files": files}

    def _collect_files(self) -> Optional[Dict[str, Any]]:
        """Collect file metadata from the storage adapter for all configured upload fields.

        Returns:
            Dict of {field: {file_id: metadata}} if adapter has files_for_field and file_fields
            are configured, otherwise None.
        """
        if not self.file_fields or not self.storage_adapter:
            return None
        if not hasattr(self.storage_adapter, 'files_for_field'):
            return None
        files = {}
        for field in self.file_fields:
            field_files = self.storage_adapter.files_for_field(field)
            if field_files:
                files[field] = field_files
        return files if files else None

    def create(self) -> Dict[str, Any]:
        """Create one or more new documents in the collection.

        Returns:
            Dictionary with created document data

        Raises:
            InvalidDataError: If data is missing or malformed
            DatabaseOperationError: If database operation fails
        """
        if not self.data:
            raise InvalidDataError("Data is required for create operation")
        try:
            results = []
            cancelled = []
            for key in sorted(self.data.keys(), key=lambda k: int(k) if k.isdigit() else k):
                row = self.data[key]
                if not self._run_pre_hook("create", key, row):
                    cancelled.append(key)
                    continue
                main_data, dot_notation_data = self._preprocess_document(row)
                data_obj = main_data.copy()
                for dot_key, value in dot_notation_data.items():
                    parts = dot_key.split('.')
                    current = data_obj
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = value
                result = self.collection.insert_one(data_obj)
                created_doc = self.collection.find_one({"_id": result.inserted_id})
                results.append(self._format_response_document(created_doc))
            response = {"data": results}
            if cancelled:
                response["cancelled"] = cancelled
            files = self._collect_files()
            if files is not None:
                response["files"] = files
            return response
        except (InvalidDataError, FieldMappingError):
            raise
        except PyMongoError as e:
            logger.error(f"Error in create operation: {e}", exc_info=True)
            raise DatabaseOperationError(f"Failed to create document: {str(e)}") from e
        except Exception as e:
            logger.error(f"Unexpected error in create operation: {e}", exc_info=True)
            raise DatabaseOperationError(f"Unexpected error creating document: {str(e)}") from e

    def _preprocess_document(self, doc: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Process document data before database operations.

        - Converts string JSON to Python objects
        - Handles special data types including dates
        - Properly structures nested fields using dot notation
        - Removes empty values

        Args:
            doc: Document data from Editor

        Returns:
            Tuple of (processed_document, dot_notation_updates)
        """
        processed_doc = {k: v for k, v in doc.items() if v is not None}
        dot_notation_updates = {}

        for key, val in processed_doc.items():
            if isinstance(val, str):
                try:
                    parsed_val = json.loads(val)
                    if '.' in key:
                        dot_notation_updates[key] = parsed_val
                    else:
                        processed_doc[key] = parsed_val
                    continue
                except json.JSONDecodeError:
                    pass

            is_date_field = (
                    key.lower().endswith(('date', 'time', 'at')) or
                    (key.split('.')[-1].lower().endswith(('date', 'time', 'at')))
            )

            if isinstance(val, str) and is_date_field and val.strip():
                try:
                    date_obj = DateHandler.parse_iso_datetime(val)

                    if '.' in key:
                        dot_notation_updates[key] = date_obj
                    else:
                        processed_doc[key] = date_obj
                except FieldMappingError:
                    if '.' in key:
                        dot_notation_updates[key] = val
            elif '.' in key:
                dot_notation_updates[key] = val

        for key in list(processed_doc.keys()):
            if '.' in key:
                del processed_doc[key]

        return processed_doc, dot_notation_updates

    def edit(self) -> Dict[str, Any]:
        """Edit one or more documents in the collection.

        Returns:
            Dictionary with updated document data

        Raises:
            InvalidDataError: If document IDs are missing or invalid
            DatabaseOperationError: If database operation fails
        """
        if not self.list_of_ids:
            raise InvalidDataError("Document ID is required for edit operation")

        try:
            data = []
            cancelled = []

            for doc_id in self.list_of_ids:
                if doc_id not in self.data:
                    continue

                update_data = {k: v for k, v in self.data[doc_id].items()
                               if not k.startswith("DT_Row")}

                if not self._run_pre_hook("edit", doc_id, update_data):
                    cancelled.append(doc_id)
                    continue

                updates = {}
                self._process_updates(update_data, updates, "")

                if updates:
                    try:
                        self.collection.update_one(
                            {"_id": ObjectId(doc_id)},
                            {"$set": updates}
                        )
                    except (ObjectIdError, ValueError) as e:
                        raise InvalidDataError(f"Invalid document ID format: {doc_id}") from e

                updated_doc = self.collection.find_one({"_id": ObjectId(doc_id)})
                response_data = self._format_response_document(updated_doc)
                data.append(response_data)

            response = {"data": data}
            if cancelled:
                response["cancelled"] = cancelled
            files = self._collect_files()
            if files is not None:
                response["files"] = files
            return response
        except (InvalidDataError, FieldMappingError):
            raise
        except PyMongoError as e:
            logger.error(f"Edit error: {e}", exc_info=True)
            raise DatabaseOperationError(f"Failed to update documents: {str(e)}") from e
        except Exception as e:
            logger.error(f"Unexpected error in edit operation: {e}", exc_info=True)
            raise DatabaseOperationError(f"Unexpected error updating documents: {str(e)}") from e

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

            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                self._process_updates(value, updates, full_key)
            else:
                field_type = self.field_mapper.get_field_type(full_key)
                if not field_type:
                    field_type = 'string'

                if field_type == 'date' and isinstance(value, str):
                    try:
                        if 'T' in value:
                            date_obj = DateHandler.parse_iso_datetime(value)
                        else:
                            date_obj = DateHandler.parse_iso_datetime(f"{value}T00:00:00")

                        updates[full_key] = date_obj
                    except FieldMappingError as e:
                        logger.warning(f"Date conversion error for {full_key}: {e}")
                        updates[full_key] = value
                elif field_type == 'number' and isinstance(value, str):
                    try:
                        updates[full_key] = TypeConverter.to_number(value)
                    except FieldMappingError:
                        updates[full_key] = value
                elif field_type == 'boolean' and isinstance(value, str):
                    updates[full_key] = TypeConverter.to_boolean(value)
                elif field_type == 'array' and isinstance(value, str):
                    updates[full_key] = TypeConverter.to_array(value)
                else:
                    updates[full_key] = value

    def _run_validators(self, row_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Run field validators against a data row.

        Args:
            row_data: The row data dict to validate

        Returns:
            List of {"name": field, "status": message} dicts for failed fields
        """
        errors = []
        for field, validator in self.validators.items():
            value = row_data.get(field)
            result = validator(value)
            if result:
                errors.append({"name": field, "status": result})
        return errors

    def process(self) -> Dict[str, Any]:
        """Process the Editor request based on the action.

        Catches exceptions and returns Editor protocol error/fieldErrors JSON
        instead of raising, so the Editor client can display errors inline.

        Returns:
            Response data for the Editor client, or error dict on failure
        """
        actions = {
            "create": self.create,
            "edit": self.edit,
            "remove": self.remove,
            "search": self.search,
            "upload": self.upload,
            "dependent": self.dependent,
        }

        if self.action not in actions:
            return {"error": f"Unsupported action: {self.action}"}

        # Run validators for write operations that have data rows
        if self.validators and self.action in ("create", "edit"):
            field_errors = []
            if self.action == "edit":
                rows_to_validate = {k: v for k, v in self.data.items() if k in self.list_of_ids}
            else:
                rows_to_validate = self.data
            for row_data in rows_to_validate.values():
                field_errors.extend(self._run_validators(row_data))
            if field_errors:
                return {"fieldErrors": field_errors}

        try:
            response = actions[self.action]()
            opts = self._resolve_options()
            if opts is not None:
                response["options"] = opts
            return response
        except (InvalidDataError, FieldMappingError) as e:
            return {"error": str(e)}
        except DatabaseOperationError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error in process: {e}", exc_info=True)
            return {"error": str(e)}