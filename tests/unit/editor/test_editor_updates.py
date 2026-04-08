"""Test Editor update operations and mutations."""
import unittest
from unittest.mock import MagicMock
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField


class TestProcessUpdates(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_process_updates_with_nested_data(self):
        data_fields = [
            DataField("profile.joined_date", "date"),
            DataField("stats.visits", "number"),
            DataField("settings.notifications", "boolean"),
            DataField("tags", "array")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        data = {
            "profile": {"name": "John Doe", "joined_date": "2023-05-15"},
            "stats": {"visits": "42", "last_seen": "2023-06-01T10:30:00"},
            "settings": {"notifications": "true", "theme": "dark"},
            "tags": "[\"member\", \"premium\"]"
        }
        updates = {}
        editor._process_updates(data, updates)
        self.assertIsInstance(updates["profile.joined_date"], datetime)
        self.assertEqual(updates["profile.joined_date"].day, 15)
        self.assertEqual(updates["stats.visits"], 42)
        self.assertTrue(updates["settings.notifications"])
        self.assertEqual(updates["tags"], ["member", "premium"])

    def test_process_updates_with_type_conversions(self):
        data_fields = [
            DataField("age", "number"), DataField("active", "boolean"),
            DataField("scores", "array"), DataField("birthday", "date")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        data = {"age": "30", "active": "yes", "scores": "[90, 85, 95]", "birthday": "1993-08-20"}
        updates = {}
        editor._process_updates(data, updates)
        self.assertEqual(updates["age"], 30)
        self.assertTrue(updates["active"])
        self.assertEqual(updates["scores"], [90, 85, 95])
        self.assertIsInstance(updates["birthday"], datetime)

    def test_process_updates_with_invalid_values(self):
        data_fields = [
            DataField("age", "number"), DataField("joined_date", "date"), DataField("tags", "array")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        data = {"age": "not-a-number", "joined_date": "invalid-date", "tags": "not-valid-json"}
        updates = {}
        editor._process_updates(data, updates)
        self.assertEqual(updates["age"], "not-a-number")
        self.assertEqual(updates["joined_date"], "invalid-date")
        self.assertEqual(updates["tags"], ["not-valid-json"])

    def test_process_updates_non_dict_is_noop(self):
        editor = Editor(self.mongo, 'test', {})
        updates = {}
        editor._process_updates("a string", updates)
        self.assertEqual(updates, {})

    def test_process_updates_skips_none_values(self):
        editor = Editor(self.mongo, 'test', {})
        updates = {}
        editor._process_updates({"name": None, "age": "30"}, updates)
        self.assertNotIn("name", updates)
        self.assertIn("age", updates)

    def test_process_updates_date_with_T(self):
        data_fields = [DataField('birthday', 'date')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        updates = {}
        editor._process_updates({"birthday": "1993-08-20T00:00:00"}, updates)
        self.assertIsInstance(updates["birthday"], datetime)


class TestEditorMutations(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.sample_id = str(ObjectId())

    def test_edit_with_complex_nested_updates(self):
        doc_id = self.sample_id
        self.collection.find_one.return_value = {
            "_id": ObjectId(doc_id), "name": "Original Name",
            "profile": {"bio": "Original Bio"}, "contact": {"email": "original@example.com"}
        }
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        request_args = {"action": "edit", "data": {doc_id: {
            "DT_RowId": doc_id, "name": "Updated Name",
            "profile.bio": "Updated Bio", "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "updated@example.com", "contact.phone": "987-654-3210"
        }}}
        editor = Editor(self.mongo, 'users', request_args, doc_id=doc_id,
                        data_fields=[DataField('profile.skills', 'array')])
        result = editor.edit()
        self.collection.update_one.assert_called_once()
        args, _ = self.collection.update_one.call_args
        self.assertEqual(args[0]["_id"], ObjectId(doc_id))
        set_updates = args[1]["$set"]
        self.assertEqual(set_updates["name"], "Updated Name")
        self.assertEqual(set_updates["profile.skills"], ["Python", "MongoDB"])
        self.assertIn("data", result)

    def test_create_with_complex_nested_structure(self):
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId()
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {
            "_id": insert_result.inserted_id, "name": "New User",
            "profile": {"bio": "Developer", "skills": ["Python", "MongoDB"]},
            "contact": {"email": "new@example.com", "phone": "123-456-7890"},
            "created_at": datetime(2023, 6, 15, 10, 30, 0)
        }
        request_args = {"action": "create", "data": {"0": {
            "name": "New User", "profile.bio": "Developer",
            "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "new@example.com", "contact.phone": "123-456-7890",
            "created_at": "2023-06-15T10:30:00"
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        result = editor.create()
        self.collection.insert_one.assert_called_once()
        args, _ = self.collection.insert_one.call_args
        inserted_doc = args[0]
        self.assertEqual(inserted_doc["name"], "New User")
        self.assertIn("profile", inserted_doc)
        self.assertEqual(inserted_doc["profile"]["bio"], "Developer")
        self.assertIn("data", result)
