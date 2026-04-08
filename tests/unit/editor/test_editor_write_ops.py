"""Editor tests — write operations — batch CRUD, multi-row create, DT_Row* field stripping."""
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

class TestEditorCRUD(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.sample_id = str(ObjectId())
        self.sample_id2 = str(ObjectId())

    def test_create_simple_document(self):
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId()
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {
            "_id": insert_result.inserted_id, "name": "Test User",
            "email": "test@example.com", "age": 30
        }
        editor = Editor(self.mongo, 'users', {
            "action": "create",
            "data": {"0": {"name": "Test User", "email": "test@example.com", "age": "30"}}
        })
        result = editor.create()
        self.collection.insert_one.assert_called_once()
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["name"], "Test User")

    def test_edit_simple_document(self):
        doc_id = self.sample_id
        self.collection.find_one.return_value = {
            "_id": ObjectId(doc_id), "name": "Original Name",
            "email": "original@example.com", "age": 25
        }
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        editor = Editor(self.mongo, 'users', {
            "action": "edit",
            "data": {doc_id: {"DT_RowId": doc_id, "name": "Updated Name",
                               "email": "updated@example.com", "age": "35"}}
        }, doc_id=doc_id)
        result = editor.edit()
        self.collection.update_one.assert_called_once()
        args, _ = self.collection.update_one.call_args
        self.assertEqual(args[0]["_id"], ObjectId(doc_id))
        self.assertIn("$set", args[1])
        self.assertEqual(args[1]["$set"]["name"], "Updated Name")

    def test_remove_document(self):
        doc_id = self.sample_id
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result
        editor = Editor(self.mongo, 'users', {
            "action": "remove",
            "data": {doc_id: {"DT_RowId": doc_id, "id": doc_id}}
        }, doc_id=doc_id)
        result = editor.remove()
        self.collection.delete_one.assert_called_once()
        args, _ = self.collection.delete_one.call_args
        self.assertEqual(args[0]["_id"], ObjectId(doc_id))
        self.assertEqual(result, {})

    def test_batch_edit(self):
        doc_id1, doc_id2 = self.sample_id, self.sample_id2
        doc1 = {"_id": ObjectId(doc_id1), "name": "User 1", "status": "active"}
        doc2 = {"_id": ObjectId(doc_id2), "name": "User 2", "status": "inactive"}

        def mock_find_one(filter_dict):
            if filter_dict["_id"] == ObjectId(doc_id1):
                return doc1
            elif filter_dict["_id"] == ObjectId(doc_id2):
                return doc2
            return None

        self.collection.find_one.side_effect = mock_find_one
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        editor = Editor(self.mongo, 'users', {
            "action": "edit",
            "data": {
                doc_id1: {"DT_RowId": doc_id1, "status": "approved"},
                doc_id2: {"DT_RowId": doc_id2, "status": "approved"}
            }
        }, doc_id=f"{doc_id1},{doc_id2}")
        result = editor.edit()
        self.assertEqual(self.collection.update_one.call_count, 2)
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 2)




class TestEditorMultiRowCreate(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def _make_insert_mock(self, oid):
        m = MagicMock(spec=InsertOneResult)
        m.inserted_id = oid
        return m

    def test_single_row_create_still_works(self):
        oid = ObjectId()
        self.collection.insert_one.return_value = self._make_insert_mock(oid)
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Alice'}
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {'0': {'name': 'Alice'}}})
        result = editor.create()
        self.assertEqual(len(result['data']), 1)
        self.collection.insert_one.assert_called_once()

    def test_multi_row_create_inserts_all_rows(self):
        oids = [ObjectId(), ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'Alice'}, {'_id': oids[1], 'name': 'Bob'},
            {'_id': oids[2], 'name': 'Carol'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}, '2': {'name': 'Carol'}}
        })
        result = editor.create()
        self.assertEqual(len(result['data']), 3)
        self.assertEqual(self.collection.insert_one.call_count, 3)

    def test_multi_row_create_returns_all_dt_row_ids(self):
        oids = [ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'Alice'}, {'_id': oids[1], 'name': 'Bob'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create', 'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}}
        })
        result = editor.create()
        row_ids = [r['DT_RowId'] for r in result['data']]
        self.assertEqual(row_ids, [str(oids[0]), str(oids[1])])

    def test_multi_row_create_sorted_order(self):
        oids = [ObjectId(), ObjectId(), ObjectId()]
        names_inserted = []

        def capture_insert(doc):
            names_inserted.append(doc.get('name'))
            idx = len(names_inserted) - 1
            m = MagicMock(spec=InsertOneResult)
            m.inserted_id = oids[idx]
            return m

        self.collection.insert_one.side_effect = capture_insert
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'First'}, {'_id': oids[1], 'name': 'Second'},
            {'_id': oids[2], 'name': 'Third'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {'2': {'name': 'Third'}, '0': {'name': 'First'}, '1': {'name': 'Second'}}
        })
        editor.create()
        self.assertEqual(names_inserted, ['First', 'Second', 'Third'])

    def test_empty_data_raises_invalid_data_error(self):
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {}})
        with self.assertRaises(InvalidDataError):
            editor.create()

    def test_multi_row_via_process(self):
        oids = [ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'X'}, {'_id': oids[1], 'name': 'Y'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create', 'data': {'0': {'name': 'X'}, '1': {'name': 'Y'}}
        })
        result = editor.process()
        self.assertIn('data', result)
        self.assertEqual(len(result['data']), 2)




class TestEditDTRowFieldsStripped(unittest.TestCase):
    """DT_Row* keys submitted by the Editor client must never reach MongoDB."""

    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.doc_id = str(ObjectId())
        self.collection.update_one.return_value = MagicMock(spec=UpdateResult)
        self.collection.find_one.return_value = {
            "_id": ObjectId(self.doc_id), "name": "Alice"
        }

    def _edit(self, row_data):
        editor = Editor(
            self.mongo, "users",
            {"action": "edit", "data": {self.doc_id: row_data}},
            doc_id=self.doc_id,
        )
        return editor.edit()

    def _set_keys(self):
        args, _ = self.collection.update_one.call_args
        return set(args[1]["$set"].keys())

    def test_dt_row_id_not_in_set(self):
        self._edit({"DT_RowId": self.doc_id, "name": "Alice"})
        self.assertNotIn("DT_RowId", self._set_keys())

    def test_dt_row_class_not_in_set(self):
        self._edit({"DT_RowClass": "highlight", "name": "Alice"})
        self.assertNotIn("DT_RowClass", self._set_keys())

    def test_dt_row_data_not_in_set(self):
        self._edit({"DT_RowData": {"pkey": 1}, "name": "Alice"})
        self.assertNotIn("DT_RowData", self._set_keys())

    def test_dt_row_attr_not_in_set(self):
        self._edit({"DT_RowAttr": {"data-id": "x"}, "name": "Alice"})
        self.assertNotIn("DT_RowAttr", self._set_keys())

    def test_normal_fields_still_in_set(self):
        self._edit({"DT_RowId": self.doc_id, "name": "Alice", "status": "active"})
        keys = self._set_keys()
        self.assertIn("name", keys)
        self.assertIn("status", keys)

    def test_all_dt_row_variants_stripped(self):
        self._edit({
            "DT_RowId": self.doc_id,
            "DT_RowClass": "x",
            "DT_RowData": {},
            "DT_RowAttr": {},
            "name": "Alice",
        })
        keys = self._set_keys()
        self.assertFalse(any(k.startswith("DT_Row") for k in keys))
        self.assertIn("name", keys)

    def test_no_update_when_only_dt_row_fields(self):
        """If the client only sends DT_Row* fields, no $set should be issued."""
        self._edit({"DT_RowId": self.doc_id})
        self.collection.update_one.assert_not_called()

    def test_pre_hook_receives_stripped_data(self):
        """pre_edit hook should not see DT_Row* keys."""
        received = {}
        hook = MagicMock(side_effect=lambda row_id, row_data: received.update(row_data) or True)
        editor = Editor(
            self.mongo, "users",
            {"action": "edit", "data": {self.doc_id: {"DT_RowId": self.doc_id, "name": "Alice"}}},
            doc_id=self.doc_id,
            hooks={"pre_edit": hook},
        )
        editor.edit()
        self.assertNotIn("DT_RowId", received)
        self.assertIn("name", received)


