"""Request dispatch and validation for the Editor processor."""
import logging
from typing import TYPE_CHECKING, Any, Dict, List

from pymongo.errors import PyMongoError

from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError, FieldMappingError
from mongo_datatables.editor.crud import run_validators
from mongo_datatables.editor.validators import validate_data_fields_whitelist

if TYPE_CHECKING:
    from mongo_datatables.editor.core import Editor

logger = logging.getLogger(__name__)

_REQUIRED_KEYS: Dict[str, List[str]] = {
    "create": ["data"],
    "edit": ["data"],
    "remove": ["data"],
    "search": ["field"],
    "upload": ["upload"],
    "dependent": ["field"],
}


def process_request(editor: "Editor") -> Dict[str, Any]:
    """Dispatch an Editor request to the appropriate handler.

    editor: Fully initialised Editor instance.
    Returns protocol-compliant response dict, or error dict on failure.
    """
    if editor._init_error is not None:
        return {"error": editor._init_error}

    actions = {
        "create": editor.create,
        "edit": editor.edit,
        "remove": editor.remove,
        "search": editor.search,
        "upload": editor.upload,
        "dependent": editor.dependent,
    }

    if editor.action not in actions:
        return {"error": f"Unsupported action: {editor.action}"}

    missing = [
        k for k in _REQUIRED_KEYS.get(editor.action, [])
        if k not in editor.request_args
    ]
    if missing:
        return {"error": f"Missing required keys for action '{editor.action}': {missing}"}

    rows = _extract_rows(editor)

    if (editor.fields or editor.data_fields) and editor.action in ("create", "edit"):
        try:
            for row in rows.values():
                validate_data_fields_whitelist(row, editor.fields, editor.data_fields)
        except InvalidDataError as e:
            return {"error": str(e)}

    if editor.validators and editor.action in ("create", "edit"):
        field_errors = [
            err for row in rows.values() for err in run_validators(editor.validators, row)
        ]
        if field_errors:
            return {"fieldErrors": field_errors}

    try:
        response = actions[editor.action]()
        opts = editor._resolve_options()
        if opts is not None:
            response["options"] = opts
        return response
    except (InvalidDataError, FieldMappingError) as e:
        return {"error": str(e)}
    except DatabaseOperationError:
        return {"error": "A database error occurred. Please try again."}
    except PyMongoError as e:
        logger.error("Unexpected PyMongo error in process: %s", e, exc_info=True)
        return {"error": "A database error occurred. Please try again."}
    except (KeyError, TypeError, ValueError) as e:
        logger.error(
            "Unexpected error in process action=%r: %s", editor.action, e, exc_info=True
        )
        return {"error": "An error occurred processing your request."}
    except BaseException:
        logger.error("Unhandled error in process action=%r", editor.action, exc_info=True)
        raise


def _extract_rows(editor: "Editor") -> Dict[str, Any]:
    """Extract the relevant row subset from editor.data for validation.

    editor: Editor instance.
    Returns dict of rows to validate.
    """
    if editor.action == "edit":
        return {k: v for k, v in editor.data.items() if k in editor.list_of_ids}
    if editor.action == "create":
        return editor.data
    return {}
