"""Tests for DataTables initialization and basic properties."""
import unittest
from unittest.mock import MagicMock, patch
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo import MongoClient

from mongo_datatables import DataTables
from tests.base_test import BaseDataTablesTest


class TestInitialization(BaseDataTablesTest):
    """Test cases for DataTables initialization and basic properties."""

    def test_initialization(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.collection, self.collection)
        self.assertEqual(datatables.request_args, self.request_args)
        self.assertEqual(datatables.custom_filter, {})

    def test_initialization_with_custom_filter(self):
        custom_filter = {"status": "active"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
        self.assertEqual(datatables.custom_filter, custom_filter)

    def test_collection_property(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.collection, self.collection)
        self.mongo.db.__getitem__.assert_called_once_with('users')

    def test_initialization_with_mongo_client(self):
        mongo_client = MagicMock(spec=MongoClient)
        db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        mongo_client.get_database.return_value = db
        db.__getitem__.return_value = collection
        datatables = DataTables(mongo_client, 'test_collection', self.request_args)
        mongo_client.get_database.assert_called_once()
        db.__getitem__.assert_called_once_with('test_collection')
        self.assertEqual(datatables.collection, collection)

    def test_initialization_with_database(self):
        db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        db.__getitem__.return_value = collection
        datatables = DataTables(db, 'test_collection', self.request_args)
        db.__getitem__.assert_called_once_with('test_collection')
        self.assertEqual(datatables.collection, collection)

    def test_initialization_with_dict_like(self):
        dict_like = {}
        collection = MagicMock(spec=Collection)
        dict_like['test_collection'] = collection
        datatables = DataTables(dict_like, 'test_collection', self.request_args)
        self.assertEqual(datatables.collection, collection)

    def test_start(self):
        self.request_args["start"] = 20
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 20)

    def test_limit(self):
        self.request_args["length"] = 25
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.limit, 25)

        self.request_args["length"] = -1
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.limit, -1)

    def test_count_total(self):
        self.collection.count_documents.return_value = 100
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.count_total(), 100)
        self.collection.count_documents.assert_called_once_with({})

    def test_count_filtered(self):
        custom_filter = {"status": "active"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
        self.collection.aggregate.return_value = [{"total": 50}]
        self.collection.count_documents.return_value = 50
        result = datatables.count_filtered()
        self.assertEqual(result, 50)
        self.collection.aggregate.assert_called_once()

    def test_projection(self):
        datatables = DataTables(self.mongo, 'users', self.request_args)
        projection = datatables.projection
        self.assertEqual(projection["_id"], 1)
        for column in ["name", "email", "status"]:
            self.assertEqual(projection[column], 1)

    def test_projection_with_nested_fields(self):
        self.request_args["columns"].append(
            {"data": "address.city", "name": "", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}}
        )
        datatables = DataTables(self.mongo, 'users', self.request_args)
        projection = datatables.projection
        self.assertIn("address.city", projection)
        self.assertEqual(projection["address.city"], 1)
        self.assertNotIn("address", projection)

    def test_use_text_index_false_skips_list_indexes(self):
        self.collection.list_indexes.reset_mock()
        dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=False)
        self.collection.list_indexes.assert_not_called()
        self.assertFalse(dt.has_text_index)

    def test_use_text_index_true_calls_list_indexes(self):
        self.collection.list_indexes.return_value = iter([])
        self.collection.list_indexes.reset_mock()
        DataTables(self.mongo, 'users', self.request_args, use_text_index=True)
        self.collection.list_indexes.assert_called_once()

    def test_has_text_index_true_when_index_present(self):
        self.collection.list_indexes.return_value = iter([{"textIndexVersion": 3, "key": {"$**": "text"}}])
        dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=True)
        self.assertTrue(dt.has_text_index)

    def test_has_text_index_false_when_no_index(self):
        self.collection.list_indexes.return_value = iter([{"key": {"_id": 1}}])
        dt = DataTables(self.mongo, 'users', self.request_args, use_text_index=True)
        self.assertFalse(dt.has_text_index)


class TestGetCollectionBranches(unittest.TestCase):
    """Tests for _get_collection branch ordering."""

    ARGS = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "name", "searchable": True, "orderable": True,
                      "search": {"value": "", "regex": False}}],
    }

    def _make_fake_db(self):
        col = MagicMock(spec=Collection)
        col.list_indexes = MagicMock(return_value=[])
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)

        class FakeDatabase(Database):
            def __init__(self):
                pass

            def __getitem__(self, name):
                return col

            def __getattr__(self, name):
                return col

        return FakeDatabase(), col

    def test_database_instance_uses_db_directly(self):
        from mongo_datatables import DataField
        fake_db, expected_col = self._make_fake_db()
        dt = DataTables(fake_db, "books", self.ARGS, [DataField("name", "string")])
        assert dt.collection is expected_col

    def test_flask_pymongo_db_attribute_path(self):
        from mongo_datatables import DataField
        col = MagicMock(spec=Collection)
        col.list_indexes = MagicMock(return_value=[])
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)
        flask_pymongo = MagicMock()
        flask_pymongo.db = MagicMock(spec=Database)
        flask_pymongo.db.__getitem__ = MagicMock(return_value=col)
        dt = DataTables(flask_pymongo, "books", self.ARGS, [DataField("name", "string")])
        flask_pymongo.db.__getitem__.assert_called_once_with("books")


if __name__ == '__main__':
    unittest.main()
