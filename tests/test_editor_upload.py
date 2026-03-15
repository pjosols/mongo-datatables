import unittest
from unittest.mock import MagicMock, patch
from io import BytesIO
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import Editor


class TestEditorUpload(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def _make_editor(self, request_args, storage=None):
        return Editor(self.mongo, 'files', request_args, storage_adapter=storage)

    # --- StorageAdapter protocol ---

    def test_storage_adapter_interface(self):
        """StorageAdapter.store(field, filename, content_type, data) returns an id string."""
        from mongo_datatables.editor import StorageAdapter
        adapter = StorageAdapter()
        with self.assertRaises(NotImplementedError):
            adapter.store('avatar', 'photo.png', 'image/png', b'data')

    def test_storage_adapter_retrieve_not_implemented(self):
        from mongo_datatables.editor import StorageAdapter
        adapter = StorageAdapter()
        with self.assertRaises(NotImplementedError):
            adapter.retrieve('some-id')

    # --- upload action dispatching ---

    def test_upload_no_adapter_returns_error(self):
        """Without a storage adapter, upload returns an error."""
        args = {'action': 'upload', 'uploadField': 'avatar'}
        editor = self._make_editor(args, storage=None)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_no_field_returns_error(self):
        """Missing uploadField returns an error."""
        adapter = MagicMock()
        args = {'action': 'upload'}
        editor = self._make_editor(args, storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_no_file_returns_error(self):
        """Missing upload file data returns an error."""
        adapter = MagicMock()
        args = {'action': 'upload', 'uploadField': 'avatar', 'upload': None}
        editor = self._make_editor(args, storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_calls_adapter_store(self):
        """upload() calls adapter.store with correct args and returns protocol response."""
        adapter = MagicMock()
        adapter.store.return_value = 'file-id-123'
        file_data = {'filename': 'photo.png', 'content_type': 'image/png', 'data': b'imgdata'}
        args = {'action': 'upload', 'uploadField': 'avatar', 'upload': file_data}
        editor = self._make_editor(args, storage=adapter)
        result = editor.upload()
        adapter.store.assert_called_once_with('avatar', 'photo.png', 'image/png', b'imgdata')
        self.assertEqual(result, {'upload': {'id': 'file-id-123'}, 'files': {}})

    def test_upload_adapter_store_exception_returns_error(self):
        """If adapter.store raises, process() returns error dict."""
        adapter = MagicMock()
        adapter.store.side_effect = Exception('storage failure')
        file_data = {'filename': 'x.pdf', 'content_type': 'application/pdf', 'data': b'pdf'}
        args = {'action': 'upload', 'uploadField': 'doc', 'upload': file_data}
        editor = self._make_editor(args, storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_via_process_dispatch(self):
        """process() dispatches action=upload to upload()."""
        adapter = MagicMock()
        adapter.store.return_value = 'abc'
        file_data = {'filename': 'f.jpg', 'content_type': 'image/jpeg', 'data': b'jpg'}
        args = {'action': 'upload', 'uploadField': 'img', 'upload': file_data}
        editor = self._make_editor(args, storage=adapter)
        result = editor.process()
        self.assertIn('upload', result)
        self.assertEqual(result['upload']['id'], 'abc')

    def test_upload_files_dict_from_adapter(self):
        """If adapter returns files metadata, it is included in response."""
        class RichAdapter:
            def store(self, field, filename, content_type, data):
                return 'rich-id'
            def retrieve(self, file_id):
                raise NotImplementedError
            def files_for_field(self, field):
                return {'rich-id': {'filename': 'photo.png', 'web_path': '/uploads/photo.png'}}
        file_data = {'filename': 'photo.png', 'content_type': 'image/png', 'data': b'x'}
        args = {'action': 'upload', 'uploadField': 'avatar', 'upload': file_data}
        editor = Editor(self.mongo, 'files', args, storage_adapter=RichAdapter())
        result = editor.upload()
        self.assertEqual(result['upload']['id'], 'rich-id')
        self.assertIn('avatar', result['files'])
        self.assertIn('rich-id', result['files']['avatar'])

    def test_existing_actions_unaffected(self):
        """Adding storage_adapter param does not break existing create/edit/remove."""
        from bson.objectid import ObjectId
        from unittest.mock import MagicMock
        from pymongo.results import InsertOneResult
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Test'}
        args = {'action': 'create', 'data': {'0': {'name': 'Test'}}}
        editor = Editor(self.mongo, 'users', args, storage_adapter=None)
        result = editor.process()
        self.assertIn('data', result)


if __name__ == '__main__':
    unittest.main(verbosity=2)

import pytest
from bson.objectid import ObjectId
from unittest.mock import MagicMock
from mongo_datatables.editor import StorageAdapter


@pytest.fixture
def mock_collection():
    return MagicMock()


class TestEditorFilesInResponse:
    """Gap #8: files metadata in create/edit responses."""

    def _make_adapter_with_files(self, files_data):
        class Adapter(StorageAdapter):
            def store(self, field, filename, content_type, data): return "1"
            def retrieve(self, file_id): return b""
            def files_for_field(self, field): return files_data.get(field, {})
        return Adapter()

    def test_create_includes_files_when_file_fields_configured(self, mock_collection):
        files_data = {"photo": {"1": {"filename": "a.png", "web_path": "/uploads/a.png"}}}
        adapter = self._make_adapter_with_files(files_data)
        mock_collection.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection.find_one.return_value = {"_id": ObjectId(), "name": "Alice", "photo": "1"}
        editor = Editor(MagicMock(), "col", {"action": "create", "data": {"0": {"name": "Alice", "photo": "1"}}},
                        storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection
        result = editor.create()
        assert "files" in result
        assert result["files"]["photo"] == {"1": {"filename": "a.png", "web_path": "/uploads/a.png"}}

    def test_edit_includes_files_when_file_fields_configured(self, mock_collection):
        doc_id = str(ObjectId())
        files_data = {"photo": {"1": {"filename": "b.png", "web_path": "/uploads/b.png"}}}
        adapter = self._make_adapter_with_files(files_data)
        mock_collection.update_one.return_value = MagicMock()
        mock_collection.find_one.return_value = {"_id": ObjectId(doc_id), "name": "Bob", "photo": "1"}
        editor = Editor(MagicMock(), "col", {"action": "edit", "data": {doc_id: {"name": "Bob", "photo": "1"}}},
                        doc_id=doc_id, storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection
        result = editor.edit()
        assert "files" in result
        assert result["files"]["photo"] == {"1": {"filename": "b.png", "web_path": "/uploads/b.png"}}

    def test_create_no_files_without_file_fields(self, mock_collection):
        adapter = self._make_adapter_with_files({"photo": {"1": {}}})
        mock_collection.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col", {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=adapter)  # no file_fields
        editor._collection = mock_collection
        result = editor.create()
        assert "files" not in result

    def test_create_no_files_without_adapter(self, mock_collection):
        mock_collection.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col", {"action": "create", "data": {"0": {"name": "Alice"}}},
                        file_fields=["photo"])  # no adapter
        editor._collection = mock_collection
        result = editor.create()
        assert "files" not in result

    def test_create_no_files_when_adapter_lacks_files_for_field(self, mock_collection):
        class BasicAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data): return "1"
            def retrieve(self, file_id): return b""
        mock_collection.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col", {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=BasicAdapter(), file_fields=["photo"])
        editor._collection = mock_collection
        result = editor.create()
        assert "files" not in result

    def test_files_empty_when_no_files_stored(self, mock_collection):
        adapter = self._make_adapter_with_files({})  # files_for_field returns {}
        mock_collection.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col", {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection
        result = editor.create()
        # files_for_field returns {} for 'photo', so files dict is empty -> not included
        assert "files" not in result

    def test_multiple_file_fields(self, mock_collection):
        files_data = {
            "photo": {"1": {"web_path": "/uploads/1.png"}},
            "doc": {"2": {"web_path": "/uploads/2.pdf"}},
        }
        adapter = self._make_adapter_with_files(files_data)
        mock_collection.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection.find_one.return_value = {"_id": ObjectId(), "photo": "1", "doc": "2"}
        editor = Editor(MagicMock(), "col", {"action": "create", "data": {"0": {"photo": "1", "doc": "2"}}},
                        storage_adapter=adapter, file_fields=["photo", "doc"])
        editor._collection = mock_collection
        result = editor.create()
        assert result["files"]["photo"] == {"1": {"web_path": "/uploads/1.png"}}
        assert result["files"]["doc"] == {"2": {"web_path": "/uploads/2.pdf"}}
