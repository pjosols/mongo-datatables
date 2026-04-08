"""Test Editor subpackage structure and public API re-exports."""
import importlib
import pytest
from unittest.mock import MagicMock
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database


# ---------------------------------------------------------------------------
# Public API re-exports
# ---------------------------------------------------------------------------

def test_editor_importable_from_top_level():
    from mongo_datatables import Editor
    assert Editor is not None


def test_storage_adapter_importable_from_top_level():
    from mongo_datatables import StorageAdapter
    assert StorageAdapter is not None


def test_editor_importable_from_editor_package():
    from mongo_datatables.editor import Editor
    assert Editor is not None


def test_storage_adapter_importable_from_editor_package():
    from mongo_datatables.editor import StorageAdapter
    assert StorageAdapter is not None


def test_top_level_and_subpackage_editor_are_same_class():
    from mongo_datatables import Editor as E1
    from mongo_datatables.editor import Editor as E2
    assert E1 is E2


def test_top_level_and_subpackage_storage_adapter_are_same_class():
    from mongo_datatables import StorageAdapter as S1
    from mongo_datatables.editor import StorageAdapter as S2
    assert S1 is S2


# ---------------------------------------------------------------------------
# Submodule imports (new names without editor_ prefix)
# ---------------------------------------------------------------------------

def test_editor_core_module_importable():
    mod = importlib.import_module("mongo_datatables.editor.core")
    assert hasattr(mod, "Editor")


def test_editor_validator_module_importable():
    mod = importlib.import_module("mongo_datatables.editor.validators")
    assert hasattr(mod, "validate_editor_request_args")
    assert hasattr(mod, "validate_doc_id")
    assert hasattr(mod, "validate_upload_data")
    assert hasattr(mod, "validate_data_fields_whitelist")


def test_editor_crud_module_importable():
    mod = importlib.import_module("mongo_datatables.editor.crud")
    assert hasattr(mod, "run_create")
    assert hasattr(mod, "run_edit")
    assert hasattr(mod, "run_remove")
    assert hasattr(mod, "run_validators")
    assert hasattr(mod, "resolve_collection")
    assert hasattr(mod, "resolve_db")


def test_editor_document_module_importable():
    mod = importlib.import_module("mongo_datatables.editor.document")
    assert hasattr(mod, "format_response_document")
    assert hasattr(mod, "collect_files")
    assert hasattr(mod, "preprocess_document")
    assert hasattr(mod, "build_updates")


def test_editor_search_module_importable():
    mod = importlib.import_module("mongo_datatables.editor.search")
    assert hasattr(mod, "handle_search")
    assert hasattr(mod, "handle_dependent")
    assert hasattr(mod, "handle_upload")


def test_editor_storage_module_importable():
    mod = importlib.import_module("mongo_datatables.editor.storage")
    assert hasattr(mod, "StorageAdapter")


# ---------------------------------------------------------------------------
# Editor class is the same object from core and from package __init__
# ---------------------------------------------------------------------------

def test_editor_core_class_is_reexported():
    from mongo_datatables.editor.core import Editor as CoreEditor
    from mongo_datatables.editor import Editor as PkgEditor
    assert CoreEditor is PkgEditor


# ---------------------------------------------------------------------------
# Functional smoke test: Editor instantiation via new subpackage
# ---------------------------------------------------------------------------

def _make_editor(action: str = "create", doc_id: str = ""):
    from mongo_datatables.editor import Editor
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = collection
    request_args = {"action": action, "data": {}}
    return Editor(mongo, "col", request_args, doc_id), collection


def test_editor_instantiation_via_subpackage():
    editor, _ = _make_editor()
    assert editor.action == "create"


def test_editor_action_property():
    editor, _ = _make_editor("edit")
    assert editor.action == "edit"


def test_editor_list_of_ids_empty_when_no_doc_id():
    editor, _ = _make_editor()
    assert editor.list_of_ids == []


def test_editor_list_of_ids_parsed_from_doc_id():
    from mongo_datatables.editor import Editor
    oid1 = str(ObjectId())
    oid2 = str(ObjectId())
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    mongo.db.__getitem__.return_value = MagicMock(spec=Collection)
    editor = Editor(mongo, "col", {"action": "remove", "data": {}}, f"{oid1},{oid2}")
    assert editor.list_of_ids == [oid1, oid2]


# ---------------------------------------------------------------------------
# __all__ on editor package exposes expected names
# ---------------------------------------------------------------------------

def test_editor_package_all():
    import mongo_datatables.editor as pkg
    assert "Editor" in pkg.__all__
    assert "StorageAdapter" in pkg.__all__
