"""Tests for Editor multi-pymongo-type support (MongoClient, Database, Flask-PyMongo, dict-style)."""
import unittest
from unittest.mock import MagicMock, patch
from pymongo.database import Database
from mongo_datatables import Editor


class TestEditorPymongoTypes(unittest.TestCase):
    def _make_collection(self):
        col = MagicMock()
        col.insert_one.return_value = MagicMock(inserted_id="id1")
        col.find_one.return_value = {"_id": "id1", "name": "Test"}
        return col

    def test_flask_pymongo_object(self):
        """Flask-PyMongo: obj.db[collection_name]"""
        col = self._make_collection()
        mongo = MagicMock()
        mongo.db.__getitem__ = MagicMock(return_value=col)
        editor = Editor(mongo, "items", {})
        self.assertIs(editor.collection, col)
        mongo.db.__getitem__.assert_called_once_with("items")

    def test_mongo_client_object(self):
        """MongoClient: obj.get_database()[collection_name]"""
        col = self._make_collection()
        client = MagicMock(spec=["get_database"])
        db_mock = MagicMock()
        db_mock.__getitem__ = MagicMock(return_value=col)
        client.get_database.return_value = db_mock
        editor = Editor(client, "items", {})
        self.assertIs(editor.collection, col)
        client.get_database.assert_called_once()

    def test_raw_database_object(self):
        """Raw pymongo Database: obj[collection_name]"""
        col = self._make_collection()
        db = MagicMock(spec=Database)
        db.__getitem__ = MagicMock(return_value=col)
        editor = Editor(db, "items", {})
        self.assertIs(editor.collection, col)
        db.__getitem__.assert_called_once_with("items")

    def test_dict_style_fallback(self):
        """Dict-style fallback: obj[collection_name]"""
        col = self._make_collection()
        obj = {"items": col}
        editor = Editor(obj, "items", {})
        self.assertIs(editor.collection, col)

    def test_db_property_flask_pymongo(self):
        """db property returns mongo.db for Flask-PyMongo."""
        mongo = MagicMock()
        editor = Editor(mongo, "items", {})
        self.assertIs(editor.db, mongo.db)

    def test_db_property_mongo_client(self):
        """db property returns get_database() for MongoClient."""
        client = MagicMock(spec=["get_database"])
        client.get_database.return_value = MagicMock(spec=Database)
        editor = Editor(client, "items", {})
        self.assertIs(editor.db, client.get_database())

    def test_db_property_raw_database(self):
        """db property returns the Database itself for raw Database."""
        db = MagicMock(spec=Database)
        editor = Editor(db, "items", {})
        self.assertIs(editor.db, db)


if __name__ == "__main__":
    unittest.main()
