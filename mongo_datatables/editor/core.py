"""Process DataTables Editor requests with MongoDB."""
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables.data_field import DataField
from mongo_datatables.exceptions import InvalidDataError, FieldMappingError
from mongo_datatables.utils import FieldMapper, TypeConverter
from mongo_datatables.editor.validators import (
    validate_collection_name,
    validate_doc_id,
    _validate_request_args_structure,
)
from mongo_datatables.editor.storage import StorageAdapter
from mongo_datatables.editor.crud import (
    run_create,
    run_edit,
    run_remove,
    resolve_collection,
    resolve_db,
)
from mongo_datatables.editor.document import (
    format_response_document,
    preprocess_document,
    build_updates,
)
from mongo_datatables.editor.search import handle_search, handle_dependent, handle_upload
from mongo_datatables.editor.dispatch import process_request

logger = logging.getLogger(__name__)

__all__ = ["Editor", "StorageAdapter"]


class Editor:
    """Server-side processor for DataTables Editor with MongoDB."""

    def __init__(
        self,
        pymongo_object: Any,
        collection_name: str,
        request_args: Dict[str, Any],
        doc_id: Optional[str] = None,
        data_fields: Optional[List[DataField]] = None,
        validators: Optional[Dict[str, Any]] = None,
        storage_adapter: Optional[StorageAdapter] = None,
        options: Optional[Union[Dict[str, Any], Callable[[], Dict[str, Any]]]] = None,
        hooks: Optional[Dict[str, Any]] = None,
        row_class: Optional[Union[str, Callable[..., str]]] = None,
        row_data: Optional[Union[Dict[str, Any], Callable[..., Dict[str, Any]]]] = None,
        row_attr: Optional[Union[Dict[str, Any], Callable[..., Dict[str, Any]]]] = None,
        file_fields: Optional[List[str]] = None,
        dependent_handlers: Optional[Dict[str, Any]] = None,
        virus_scanner: Optional[Any] = None,
    ) -> None:
        """Initialize the Editor processor.

        pymongo_object: PyMongo client, Flask-PyMongo instance, or Database.
        collection_name: Name of the MongoDB collection.
        request_args: Editor request parameters (from request.get_json()).
        doc_id: Comma-separated document IDs for edit/remove operations.
        data_fields: DataField objects defining field mappings and write whitelist.
        validators: Dict of field -> callable(value) -> str|None for field validation.
        storage_adapter: StorageAdapter instance for file uploads.
        options: Options dict or zero-arg callable returning one.
        hooks: Dict of pre_create, pre_edit, pre_remove callables; return falsy to cancel.
        row_class: String or callable(row) -> str for DT_RowClass.
        row_data: Dict or callable(row) -> dict for DT_RowData.
        row_attr: Dict or callable(row) -> dict for DT_RowAttr.
        file_fields: List of field names that are upload fields.
        dependent_handlers: Dict of field -> callable(field, values, rows).
        virus_scanner: Optional scanner implementing scan(filename, data) -> bool; False rejects.
        """
        self._init_error: Optional[str] = None
        _validate_request_args_structure(request_args if request_args is not None else {})
        validate_collection_name(collection_name)
        self.mongo = pymongo_object
        self.collection_name = collection_name
        self.request_args = request_args if isinstance(request_args, dict) else {}
        self.doc_id = doc_id or ""
        try:
            validate_doc_id(self.doc_id)
        except InvalidDataError as e:
            if self._init_error is None:
                self._init_error = str(e)
        self.data_fields = data_fields or []
        self.field_mapper = FieldMapper(self.data_fields)
        self.fields: Dict[str, DataField] = {
            f.alias: f for f in self.data_fields if isinstance(f, DataField)
        }
        self.validators = validators or {}
        self.storage_adapter = storage_adapter
        self._options = options
        self.hooks = hooks or {}
        self.row_class = row_class
        self.row_data = row_data
        self.row_attr = row_attr
        self.file_fields = file_fields or []
        self.dependent_handlers = dependent_handlers or {}
        self.virus_scanner = virus_scanner
        self._collection = resolve_collection(pymongo_object, collection_name)

    @property
    def db(self) -> Optional[Database]:
        """Get the MongoDB database instance."""
        return resolve_db(self.mongo)

    @property
    def collection(self) -> Collection:
        """Get the MongoDB collection."""
        return self._collection

    def map_ui_field_to_db_field(self, field_name: str) -> str:
        """Map a UI field name to its database field name.

        field_name: The UI field name to map.
        Returns the database field name, or the original if no mapping exists.
        """
        return self.field_mapper.get_db_field(field_name)

    @property
    def action(self) -> str:
        """Get the Editor action type (create, edit, remove, …)."""
        return self.request_args.get("action", "")

    @property
    def data(self) -> Dict[str, Any]:
        """Get the data payload from the request."""
        return self.request_args.get("data", {})

    @property
    def list_of_ids(self) -> List[str]:
        """Get list of document IDs for batch operations."""
        if not self.doc_id:
            return []
        return [id_.strip() for id_ in self.doc_id.split(",") if id_.strip()]

    def _resolve_options(self) -> Optional[Any]:
        """Resolve options dict, calling if callable.

        Returns the options dict, or None if not configured.
        """
        if self._options is None:
            return None
        return self._options() if callable(self._options) else self._options

    def _pre_hook(self, action: str, row_id: str, row_data: Dict[str, Any]) -> bool:
        """Run a pre-action hook; return False to cancel the row.

        action: The action type (create, edit, remove).
        row_id: The row identifier.
        row_data: The row data dict.
        Returns True to proceed, False to cancel.
        """
        hook = self.hooks.get(f"pre_{action}")
        return bool(hook(row_id, row_data)) if hook else True

    def _crud_kwargs(self) -> Dict[str, Any]:
        """Common keyword arguments forwarded to CRUD helpers."""
        return dict(
            fields=self.fields,
            data_fields=self.data_fields,
            field_mapper=self.field_mapper,
            file_fields=self.file_fields,
            storage_adapter=self.storage_adapter,
            row_class=self.row_class,
            row_data=self.row_data,
            row_attr=self.row_attr,
            pre_hook=self._pre_hook,
        )

    def create(self) -> Dict[str, Any]:
        """Create one or more new documents. See editor.crud.run_create."""
        return run_create(self.data, self.collection, **self._crud_kwargs())

    def edit(self) -> Dict[str, Any]:
        """Edit one or more documents. See editor.crud.run_edit."""
        return run_edit(self.list_of_ids, self.data, self.collection, **self._crud_kwargs())

    def remove(self) -> Dict[str, Any]:
        """Remove one or more documents. See editor.crud.run_remove."""
        return run_remove(self.list_of_ids, self.collection, self._pre_hook)

    def search(self) -> Dict[str, Any]:
        """Handle action=search for autocomplete and tags field types."""
        return handle_search(self.request_args, self.collection, self.field_mapper, self.fields)

    def dependent(self) -> Dict[str, Any]:
        """Handle dependent field Ajax requests."""
        return handle_dependent(self.request_args, self.dependent_handlers)

    def upload(self) -> Dict[str, Any]:
        """Handle action=upload via the pluggable storage adapter."""
        return handle_upload(self.request_args, self.storage_adapter, self.virus_scanner)

    def _preprocess_document(self, doc: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Preprocess a document before insert/update.

        doc: Raw document data from Editor.
        Returns (processed_document, dot_notation_updates).
        """
        return preprocess_document(doc, self.fields, self.data_fields)

    def _process_updates(self, data: Any, updates: Dict[str, Any]) -> None:
        """Build $set updates dict from nested Editor data in-place.

        data: Data to process (dict or scalar).
        updates: Dict to populate in-place.
        """
        build_updates(data, self.field_mapper, self.fields, self.data_fields, updates)

    def _format_response_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format a MongoDB document for the Editor response.

        doc: Document from MongoDB.
        Returns formatted document dict.
        """
        return format_response_document(doc, self.row_class, self.row_data, self.row_attr)

    def _coerce_values(self, field: str, values: List[Any]) -> List[Any]:
        """Coerce a list of values to the type declared for field.

        field: Field name to look up in field_mapper.
        values: List of raw values to coerce.
        Returns list of coerced values.
        Raises InvalidDataError if any value cannot be coerced to the declared type.
        """
        field_type = self.field_mapper.get_field_type(field) or "string"
        result = []
        for v in values:
            if field_type == "number":
                try:
                    result.append(TypeConverter.to_number(str(v)))
                except (ValueError, TypeError, FieldMappingError):
                    result.append(v)
            elif field_type == "boolean":
                result.append(TypeConverter.to_boolean(str(v)) if not isinstance(v, bool) else v)
            else:
                result.append(v)
        return result

    def process(self) -> Dict[str, Any]:
        """Process the Editor request based on the action.

        Returns protocol-compliant JSON response. Catches exceptions and returns
        error dict so the client can display errors inline.
        """
        return process_request(self)
