"""Editor tests — file upload and storage adapter."""
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

class TestEditorUpload(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def _make_editor(self, request_args, storage=None):
        return Editor(self.mongo, 'files', request_args, storage_adapter=storage)

    def test_storage_adapter_interface(self):
        adapter = StorageAdapter()
        with self.assertRaises(NotImplementedError):
            adapter.store('avatar', 'photo.png', 'image/png', b'data')

    def test_storage_adapter_retrieve_not_implemented(self):
        adapter = StorageAdapter()
        with self.assertRaises(NotImplementedError):
            adapter.retrieve('some-id')

    def test_upload_no_adapter_returns_error(self):
        editor = self._make_editor({'action': 'upload', 'uploadField': 'avatar'}, storage=None)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_no_field_returns_error(self):
        adapter = MagicMock()
        editor = self._make_editor({'action': 'upload'}, storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_no_file_returns_error(self):
        adapter = MagicMock()
        editor = self._make_editor({'action': 'upload', 'uploadField': 'avatar', 'upload': None},
                                   storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_calls_adapter_store(self):
        adapter = MagicMock(spec=StorageAdapter)
        adapter.store.return_value = 'file-id-123'
        file_data = {'filename': 'photo.png', 'content_type': 'image/png', 'data': b'imgdata'}
        editor = self._make_editor({'action': 'upload', 'uploadField': 'avatar', 'upload': file_data},
                                   storage=adapter)
        result = editor.upload()
        adapter.store.assert_called_once_with('avatar', 'photo.png', 'image/png', b'imgdata')
        self.assertEqual(result, {'upload': {'id': 'file-id-123'}, 'files': {}})

    def test_upload_adapter_store_exception_returns_error(self):
        adapter = MagicMock()
        adapter.store.side_effect = Exception('storage failure')
        file_data = {'filename': 'x.pdf', 'content_type': 'application/pdf', 'data': b'pdf'}
        editor = self._make_editor({'action': 'upload', 'uploadField': 'doc', 'upload': file_data},
                                   storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_via_process_dispatch(self):
        adapter = MagicMock()
        adapter.store.return_value = 'abc'
        file_data = {'filename': 'f.jpg', 'content_type': 'image/jpeg', 'data': b'jpg'}
        editor = self._make_editor({'action': 'upload', 'uploadField': 'img', 'upload': file_data},
                                   storage=adapter)
        result = editor.process()
        self.assertIn('upload', result)
        self.assertEqual(result['upload']['id'], 'abc')

    def test_upload_files_dict_from_adapter(self):
        class RichAdapter:
            def store(self, field, filename, content_type, data): return 'rich-id'
            def retrieve(self, file_id): raise NotImplementedError
            def files_for_field(self, field):
                return {'rich-id': {'filename': 'photo.png', 'web_path': '/uploads/photo.png'}}

        file_data = {'filename': 'photo.png', 'content_type': 'image/png', 'data': b'x'}
        editor = Editor(self.mongo, 'files', {'action': 'upload', 'uploadField': 'avatar',
                                               'upload': file_data}, storage_adapter=RichAdapter())
        result = editor.upload()
        self.assertEqual(result['upload']['id'], 'rich-id')
        self.assertIn('avatar', result['files'])
        self.assertIn('rich-id', result['files']['avatar'])

    def test_existing_actions_unaffected(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Test'}
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Test'}}},
                        storage_adapter=None)
        result = editor.process()
        self.assertIn('data', result)


@pytest.fixture
def mock_collection_upload():
    return MagicMock()




class TestEditorFilesInResponse:
    def _make_adapter_with_files(self, files_data):
        class Adapter(StorageAdapter):
            def store(self, field, filename, content_type, data): return "1"
            def retrieve(self, file_id): return b""
            def files_for_field(self, field): return files_data.get(field, {})
        return Adapter()

    def test_create_includes_files_when_file_fields_configured(self, mock_collection_upload):
        files_data = {"photo": {"1": {"filename": "a.png", "web_path": "/uploads/a.png"}}}
        adapter = self._make_adapter_with_files(files_data)
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice", "photo": "1"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice", "photo": "1"}}},
                        storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" in result
        assert result["files"]["photo"] == {"1": {"filename": "a.png", "web_path": "/uploads/a.png"}}

    def test_edit_includes_files_when_file_fields_configured(self, mock_collection_upload):
        doc_id = str(ObjectId())
        files_data = {"photo": {"1": {"filename": "b.png", "web_path": "/uploads/b.png"}}}
        adapter = self._make_adapter_with_files(files_data)
        mock_collection_upload.update_one.return_value = MagicMock()
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(doc_id), "name": "Bob", "photo": "1"}
        editor = Editor(MagicMock(), "col",
                        {"action": "edit", "data": {doc_id: {"name": "Bob", "photo": "1"}}},
                        doc_id=doc_id, storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.edit()
        assert "files" in result
        assert result["files"]["photo"] == {"1": {"filename": "b.png", "web_path": "/uploads/b.png"}}

    def test_create_no_files_without_file_fields(self, mock_collection_upload):
        adapter = self._make_adapter_with_files({"photo": {"1": {}}})
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=adapter)
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_create_no_files_without_adapter(self, mock_collection_upload):
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_create_no_files_when_adapter_lacks_files_for_field(self, mock_collection_upload):
        class BasicAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data): return "1"
            def retrieve(self, file_id): return b""
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=BasicAdapter(), file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_files_empty_when_no_files_stored(self, mock_collection_upload):
        adapter = self._make_adapter_with_files({})
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_multiple_file_fields(self, mock_collection_upload):
        files_data = {
            "photo": {"1": {"web_path": "/uploads/1.png"}},
            "doc": {"2": {"web_path": "/uploads/2.pdf"}},
        }
        adapter = self._make_adapter_with_files(files_data)
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "photo": "1", "doc": "2"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"photo": "1", "doc": "2"}}},
                        storage_adapter=adapter, file_fields=["photo", "doc"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert result["files"]["photo"] == {"1": {"web_path": "/uploads/1.png"}}
        assert result["files"]["doc"] == {"2": {"web_path": "/uploads/2.pdf"}}


