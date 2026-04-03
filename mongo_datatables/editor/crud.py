"""Create, edit, and remove operations for Editor."""
import logging
from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId
from bson.errors import InvalidId as ObjectIdError
from pymongo.errors import PyMongoError

from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError, FieldMappingError
from mongo_datatables.editor.document import (
    format_response_document,
    collect_files,
    preprocess_document,
    build_updates,
)

logger = logging.getLogger(__name__)


def run_create(
    data: Dict[str, Any],
    collection: Any,
    fields: Dict[str, Any],
    data_fields: list,
    field_mapper: Any,
    file_fields: List[str],
    storage_adapter: Any,
    row_class: Any,
    row_data: Any,
    row_attr: Any,
    pre_hook: Any,
) -> Dict[str, Any]:
    """Create one or more new documents in the collection.

    data: Editor data payload keyed by row key.
    collection: PyMongo collection.
    fields: Dict of alias -> DataField.
    data_fields: List of DataField objects.
    field_mapper: FieldMapper instance.
    file_fields: Upload field names.
    storage_adapter: StorageAdapter instance or None.
    row_class/row_data/row_attr: DT_Row* metadata providers.
    pre_hook: callable(action, row_id, row_data) -> bool.
    Returns dict with created document data.
    Raises InvalidDataError if data is missing or malformed.
    Raises DatabaseOperationError if the database operation fails.
    """
    if not data:
        raise InvalidDataError("Data is required for create operation")

    def _fmt(doc: Dict[str, Any]) -> Dict[str, Any]:
        return format_response_document(doc, row_class, row_data, row_attr)

    try:
        results = []
        cancelled = []
        for key in sorted(data.keys(), key=lambda k: int(k) if k.isdigit() else k):
            row = data[key]
            if not pre_hook("create", key, row):
                cancelled.append(key)
                continue
            main_data, dot_data = preprocess_document(row, fields, data_fields, field_mapper)
            doc_obj = main_data.copy()
            for dot_key, value in dot_data.items():
                parts = dot_key.split(".")
                cur = doc_obj
                for part in parts[:-1]:
                    cur = cur.setdefault(part, {})
                cur[parts[-1]] = value
            result = collection.insert_one(doc_obj)
            created = collection.find_one({"_id": result.inserted_id})
            results.append(_fmt(created))
        response: Dict[str, Any] = {"data": results}
        if cancelled:
            response["cancelled"] = cancelled
        files = collect_files(file_fields, storage_adapter)
        if files is not None:
            response["files"] = files
        return response
    except (InvalidDataError, FieldMappingError):
        raise
    except PyMongoError as e:
        logger.error("Error in create operation: %s", e, exc_info=True)
        raise DatabaseOperationError(f"Failed to create document: {e}") from e
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        logger.error("Unexpected error in create operation: %s", e, exc_info=True)
        raise DatabaseOperationError(f"Unexpected error creating document: {e}") from e


def run_edit(
    list_of_ids: List[str],
    data: Dict[str, Any],
    collection: Any,
    fields: Dict[str, Any],
    data_fields: list,
    field_mapper: Any,
    file_fields: List[str],
    storage_adapter: Any,
    row_class: Any,
    row_data: Any,
    row_attr: Any,
    pre_hook: Any,
) -> Dict[str, Any]:
    """Edit one or more documents in the collection.

    list_of_ids: Document IDs to update.
    data: Editor data payload keyed by document ID.
    collection: PyMongo collection.
    fields: Dict of alias -> DataField.
    data_fields: List of DataField objects.
    field_mapper: FieldMapper instance.
    file_fields: Upload field names.
    storage_adapter: StorageAdapter instance or None.
    row_class/row_data/row_attr: DT_Row* metadata providers.
    pre_hook: callable(action, row_id, row_data) -> bool.
    Returns dict with updated document data.
    Raises InvalidDataError if document IDs are missing or invalid.
    Raises DatabaseOperationError if the database operation fails.
    """
    if not list_of_ids:
        raise InvalidDataError("Document ID is required for edit operation")

    def _fmt(doc: Dict[str, Any]) -> Dict[str, Any]:
        return format_response_document(doc, row_class, row_data, row_attr)

    try:
        result_data = []
        cancelled = []
        for doc_id in list_of_ids:
            if doc_id not in data:
                continue
            update_data = {k: v for k, v in data[doc_id].items() if not k.startswith("DT_Row")}
            if not pre_hook("edit", doc_id, update_data):
                cancelled.append(doc_id)
                continue
            updates: Dict[str, Any] = {}
            build_updates(update_data, field_mapper, fields, data_fields, updates)
            if updates:
                try:
                    collection.update_one({"_id": ObjectId(doc_id)}, {"$set": updates})
                except (ObjectIdError, ValueError) as e:
                    raise InvalidDataError(f"Invalid document ID format: {doc_id}") from e
            updated = collection.find_one({"_id": ObjectId(doc_id)})
            result_data.append(_fmt(updated))
        response: Dict[str, Any] = {"data": result_data}
        if cancelled:
            response["cancelled"] = cancelled
        files = collect_files(file_fields, storage_adapter)
        if files is not None:
            response["files"] = files
        return response
    except (InvalidDataError, FieldMappingError):
        raise
    except PyMongoError as e:
        logger.error("Edit error: %s", e, exc_info=True)
        raise DatabaseOperationError(f"Failed to update documents: {e}") from e
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        logger.error("Unexpected error in edit operation: %s", e, exc_info=True)
        raise DatabaseOperationError(f"Unexpected error updating documents: {e}") from e


def run_remove(
    list_of_ids: List[str],
    collection: Any,
    pre_hook: Any,
) -> Dict[str, Any]:
    """Remove one or more documents from the collection.

    list_of_ids: Document IDs to delete.
    collection: PyMongo collection.
    pre_hook: callable(action, row_id, row_data) -> bool.
    Returns empty dict on success, or {"cancelled": [...]} if hooks fired.
    Raises InvalidDataError if no IDs provided or ID format is invalid.
    Raises DatabaseOperationError if the database operation fails.
    """
    if not list_of_ids:
        raise InvalidDataError("Document ID is required for remove operation")
    try:
        cancelled = []
        for doc_id in list_of_ids:
            if not pre_hook("remove", doc_id, {}):
                cancelled.append(doc_id)
                continue
            try:
                collection.delete_one({"_id": ObjectId(doc_id)})
            except (ObjectIdError, ValueError) as e:
                raise InvalidDataError(f"Invalid document ID format: {doc_id}") from e
        return {"cancelled": cancelled} if cancelled else {}
    except InvalidDataError:
        raise
    except PyMongoError as e:
        raise DatabaseOperationError(f"Failed to delete documents: {e}") from e


def run_validators(
    validators: Dict[str, Any],
    row_data: Dict[str, Any],
) -> List[Dict[str, str]]:
    """Run field validators against a data row.

    validators: Dict of field -> callable(value) -> str|None.
    row_data: The row data dict to validate.
    Returns list of {"name": field, "status": message} for failed fields.
    """
    errors = []
    for field, validator in validators.items():
        result = validator(row_data.get(field))
        if result:
            errors.append({"name": field, "status": result})
    return errors


def resolve_collection(pymongo_object: Any, collection_name: str) -> Any:
    """Resolve a MongoDB collection from various pymongo object types.

    pymongo_object: Flask-PyMongo instance, MongoClient, Database, or dict-like.
    collection_name: Name of the collection to resolve.
    Returns the resolved PyMongo Collection.
    """
    from pymongo.database import Database as _Database

    if hasattr(pymongo_object, "db"):
        db = pymongo_object.db
    elif hasattr(pymongo_object, "get_database"):
        db = pymongo_object.get_database()
    elif isinstance(pymongo_object, _Database):
        db = pymongo_object
    else:
        return pymongo_object[collection_name]
    return db[collection_name]


def resolve_db(pymongo_object: Any) -> Optional[Any]:
    """Get the MongoDB database instance from various pymongo object types.

    pymongo_object: Flask-PyMongo instance, MongoClient, or Database.
    Returns the PyMongo Database, or None if not resolvable.
    """
    from pymongo.database import Database as _Database

    if hasattr(pymongo_object, "db"):
        return pymongo_object.db
    if hasattr(pymongo_object, "get_database"):
        return pymongo_object.get_database()
    if isinstance(pymongo_object, _Database):
        return pymongo_object
    return None
