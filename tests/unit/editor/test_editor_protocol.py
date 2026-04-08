"""Editor tests — protocol correctness (pymongo types, IDs, validators)."""
import unittest
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult
from pymongo.errors import PyMongoError

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField
from mongo_datatables.editor import StorageAdapter
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError

class TestEditorPymongoTypes(unittest.TestCase):
    def _make_collection(self):
        col = MagicMock()
        col.insert_one.return_value = MagicMock(inserted_id="id1")
        col.find_one.return_value = {"_id": "id1", "name": "Test"}
        return col

    def test_flask_pymongo_object(self):
        col = self._make_collection()
        mongo = MagicMock()
        mongo.db.__getitem__ = MagicMock(return_value=col)
        editor = Editor(mongo, "items", {})
        self.assertIs(editor.collection, col)
        mongo.db.__getitem__.assert_called_once_with("items")

    def test_mongo_client_object(self):
        col = self._make_collection()
        client = MagicMock(spec=["get_database"])
        db_mock = MagicMock()
        db_mock.__getitem__ = MagicMock(return_value=col)
        client.get_database.return_value = db_mock
        editor = Editor(client, "items", {})
        self.assertIs(editor.collection, col)
        client.get_database.assert_called_once()

    def test_raw_database_object(self):
        col = self._make_collection()
        db = MagicMock(spec=Database)
        db.__getitem__ = MagicMock(return_value=col)
        editor = Editor(db, "items", {})
        self.assertIs(editor.collection, col)
        db.__getitem__.assert_called_once_with("items")

    def test_dict_style_fallback(self):
        col = self._make_collection()
        obj = {"items": col}
        editor = Editor(obj, "items", {})
        self.assertIs(editor.collection, col)

    def test_db_property_flask_pymongo(self):
        mongo = MagicMock()
        editor = Editor(mongo, "items", {})
        self.assertIs(editor.db, mongo.db)

    def test_db_property_mongo_client(self):
        client = MagicMock(spec=["get_database"])
        client.get_database.return_value = MagicMock(spec=Database)
        editor = Editor(client, "items", {})
        self.assertIs(editor.db, client.get_database())

    def test_db_property_raw_database(self):
        db = MagicMock(spec=Database)
        editor = Editor(db, "items", {})
        self.assertIs(editor.db, db)




class TestListOfIdsWhitespaceStripping:
    """list_of_ids must strip whitespace from comma-separated IDs."""

    def test_strips_spaces_around_ids(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        oid = str(ObjectId())
        editor = Editor(mongo, 'c', {}, f" {oid} ")
        assert editor.list_of_ids == [oid]

    def test_strips_spaces_in_multi_id_string(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        oid1, oid2 = str(ObjectId()), str(ObjectId())
        editor = Editor(mongo, 'c', {}, f"{oid1} , {oid2}")
        assert editor.list_of_ids == [oid1, oid2]

    def test_filters_empty_segments(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        oid = str(ObjectId())
        editor = Editor(mongo, 'c', {}, f"{oid},")
        assert editor.list_of_ids == [oid]

    def test_empty_doc_id_returns_empty_list(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        assert Editor(mongo, 'c', {}, "").list_of_ids == []

    def test_whitespace_only_doc_id_returns_empty_list(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        assert Editor(mongo, 'c', {}, "   ").list_of_ids == []

    def test_remove_uses_stripped_ids(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        oid = str(ObjectId())
        col.delete_one.return_value = MagicMock(spec=DeleteResult)
        editor = Editor(mongo, 'c', {'action': 'remove'}, f" {oid} ")
        result = editor.remove()
        col.delete_one.assert_called_once_with({'_id': ObjectId(oid)})
        assert result == {}




class TestUploadHasattr:
    """upload() must use hasattr so inherited files_for_field methods work."""

    def test_inherited_files_for_field_is_called(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)

        class BaseAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data):
                return 'file-1'
            def files_for_field(self, field):
                return {'file-1': {'filename': 'test.png'}}

        class ConcreteAdapter(BaseAdapter):
            pass

        adapter = ConcreteAdapter()
        editor = Editor(mongo, 'c', {
            'action': 'upload',
            'uploadField': 'avatar',
            'upload': {'filename': 'test.png', 'content_type': 'image/png', 'data': b'\x89PNG\r\n\x1a\n'}
        }, storage_adapter=adapter)
        result = editor.upload()
        assert result['upload']['id'] == 'file-1'
        assert result['files']['avatar'] == {'file-1': {'filename': 'test.png'}}

    def test_adapter_without_files_for_field_returns_empty_files(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)

        class MinimalAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data):
                return 'file-2'

        adapter = MinimalAdapter()
        editor = Editor(mongo, 'c', {
            'action': 'upload',
            'uploadField': 'doc',
            'upload': {'filename': 'doc.pdf', 'content_type': 'application/pdf', 'data': b'%PDF-'}
        }, storage_adapter=adapter)
        result = editor.upload()
        assert result['upload']['id'] == 'file-2'
        assert result['files'] == {}




class TestEditValidatorScope:
    """process() must only validate rows in list_of_ids for edit actions."""

    def test_only_validates_rows_being_edited(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        oid1, oid2 = str(ObjectId()), str(ObjectId())
        validated_fields = []

        def name_validator(value):
            validated_fields.append(value)
            return None

        col.update_one.return_value = MagicMock(spec=UpdateResult)
        col.find_one.return_value = {'_id': ObjectId(oid1), 'name': 'Alice'}

        editor = Editor(mongo, 'c', {
            'action': 'edit',
            'data': {oid1: {'name': 'Alice'}, oid2: {'name': 'Bob'}}
        }, doc_id=oid1, validators={'name': name_validator})
        editor.process()
        assert validated_fields == ['Alice']

    def test_edit_validator_error_only_for_edited_rows(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        oid1, oid2 = str(ObjectId()), str(ObjectId())

        def strict_validator(value):
            return 'Value is invalid' if value == 'invalid' else None

        editor = Editor(mongo, 'c', {
            'action': 'edit',
            'data': {oid1: {'name': 'valid'}, oid2: {'name': 'invalid'}}
        }, doc_id=oid1, validators={'name': strict_validator})
        col.update_one.return_value = MagicMock(spec=UpdateResult)
        col.find_one.return_value = {'_id': ObjectId(oid1), 'name': 'valid'}
        result = editor.process()
        assert 'fieldErrors' not in result
        assert 'data' in result

    def test_create_validates_all_rows(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        validated_fields = []

        def name_validator(value):
            validated_fields.append(value)
            return None

        insert_result = MagicMock()
        insert_result.inserted_id = ObjectId()
        col.insert_one.return_value = insert_result
        col.find_one.return_value = {'_id': insert_result.inserted_id, 'name': 'Alice'}

        editor = Editor(mongo, 'c', {
            'action': 'create',
            'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}}
        }, validators={'name': name_validator})
        editor.process()
        assert sorted(validated_fields) == ['Alice', 'Bob']


