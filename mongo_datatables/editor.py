"""MongoDB server-side processor for DataTables Editor.

CRUD operations for DataTables Editor backed by MongoDB.
Helpers live in editor_storage, editor_document, editor_search, editor_crud.
"""
import logging
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

from mongo_datatables.datatables import DataField
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError, FieldMappingError
from mongo_datatables.utils import FieldMapper
from mongo_datatables.editor_validator import (
    validate_editor_request_args,
    validate_doc_id,
    validate_data_fields_whitelist,
)
from mongo_datatables.editor_storage import StorageAdapter
from mongo_datatables.editor_crud import (
    run_create,
    run_edit,
    run_remove,
    run_validators,
    resolve_collection,
    resolve_db,
)
from mongo_datatables.editor_search import handle_search, handle_dependent, handle_upload

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
        options=None,
        hooks: Optional[Dict[str, Any]] = None,
        row_class=None,
        row_data=None,
        row_attr=None,
        file_fields=None,
        dependent_handlers: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialise the Editor processor.

        pymongo_object: PyMongo client, Flask-PyMongo instance, or Database.
        collection_name: Name of the MongoDB collection.
        request_args: Editor request parameters (from request.get_json()).
        doc_id: Comma-separated document IDs for edit/remove operations.
        data_fields: DataField objects defining field mappings.
        validators: Dict of field -> callable for field-level validation.
        storage_adapter: StorageAdapter instance for file uploads.
        options: Options dict or callable returning one.
        hooks: Dict of pre-action hooks keyed by 'pre_create', 'pre_edit', 'pre_remove'.
        row_class: String or callable(row) -> str for DT_RowClass.
        row_data: Dict or callable(row) -> dict for DT_RowData.
        row_attr: Dict or callable(row) -> dict for DT_RowAttr.
        file_fields: List of field names that are upload fields.
        dependent_handlers: Dict of field -> callable(field, values, rows).
        """
        self.mongo = pymongo_object
        self.collection_name = collection_name
        self.request_args = request_args or {}
        self.doc_id = doc_id or ""
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
        self._collection = resolve_collection(pymongo_object, collection_name)

        validate_editor_request_args(self.request_args)
        validate_doc_id(self.doc_id)

    @property
    def db(self) -> Optional[Database]:
        """Get the MongoDB database instance."""
        return resolve_db(self.mongo)

    @property
    def collection(self) -> Collection:
        """Get the MongoDB collection."""
        return self._collection

    def map_ui_field_to_db_field(self, field_name: str) -> str:
        """Map a UI field name to its corresponding database field name.

        field_name: The UI field name to map.
        Returns the db field name, or the original if no mapping exists.
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

    def _resolve_options(self):
        if self._options is None:
            return None
        return self._options() if callable(self._options) else self._options

    def _pre_hook(self, action: str, row_id: str, row_data: dict) -> bool:
        """Run a pre-action hook; returns False to cancel the row."""
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
        """Create one or more new documents. See editor_crud.run_create."""
        for row in self.data.values():
            validate_data_fields_whitelist(row, self.fields, self.data_fields)
        return run_create(self.data, self.collection, **self._crud_kwargs())

    def edit(self) -> Dict[str, Any]:
        """Edit one or more documents. See editor_crud.run_edit."""
        for row in self.data.values():
            validate_data_fields_whitelist(row, self.fields, self.data_fields)
        return run_edit(self.list_of_ids, self.data, self.collection, **self._crud_kwargs())

    def remove(self) -> Dict[str, Any]:
        """Remove one or more documents. See editor_crud.run_remove."""
        return run_remove(self.list_of_ids, self.collection, self._pre_hook)

    def search(self) -> Dict[str, Any]:
        """Handle action=search for autocomplete and tags field types."""
        return handle_search(self.request_args, self.collection, self.field_mapper, self.fields)

    def dependent(self) -> Dict[str, Any]:
        """Handle dependent field Ajax requests."""
        return handle_dependent(self.request_args, self.dependent_handlers)

    def upload(self) -> Dict[str, Any]:
        """Handle action=upload via the pluggable storage adapter."""
        return handle_upload(self.request_args, self.storage_adapter)

    def process(self) -> Dict[str, Any]:
        """Process the Editor request based on the action.

        Catches exceptions and returns Editor protocol error JSON so the
        client can display errors inline rather than raising.
        Returns response data for the Editor client, or error dict on failure.
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

        if self.validators and self.action in ("create", "edit"):
            rows = (
                {k: v for k, v in self.data.items() if k in self.list_of_ids}
                if self.action == "edit"
                else self.data
            )
            field_errors = [
                err for row in rows.values() for err in run_validators(self.validators, row)
            ]
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
        except PyMongoError as e:
            logger.error("Unexpected PyMongo error in process: %s", e, exc_info=True)
            return {"error": f"Database error: {e}"}
        except KeyError as e:
            # Malformed request data missing expected keys (e.g. bad row IDs or field names)
            logger.error("Missing key in process: %s", e, exc_info=True)
            return {"error": f"Missing field: {e}"}
        except (TypeError, ValueError) as e:
            # Invalid data types or values from external input that passed earlier validation
            logger.error("Invalid data in process: %s", e, exc_info=True)
            return {"error": f"Invalid data: {e}"}
        except AttributeError as e:
            # Unexpected None or wrong type on internal objects — likely a bug; log prominently
            logger.error("AttributeError in process (possible bug): %s", e, exc_info=True)
            return {"error": str(e)}
