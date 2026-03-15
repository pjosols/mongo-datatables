import unittest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult
from pymongo.errors import PyMongoError

from mongo_datatables import Editor
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError


class TestEditorErrorResponseFormat(unittest.TestCase):
    """Gap #1: process() returns Editor protocol error JSON instead of raising."""

    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_unsupported_action_returns_error_dict(self):
        editor = Editor(self.mongo, 'users', {'action': 'invalid'})
        result = editor.process()
        self.assertIn('error', result)
        self.assertNotIn('data', result)

    def test_db_error_returns_error_dict(self):
        self.collection.insert_one.side_effect = PyMongoError('db down')
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {'0': {'name': 'x'}}})
        result = editor.process()
        self.assertIn('error', result)

    def test_invalid_data_returns_error_dict(self):
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {}})
        result = editor.process()
        self.assertIn('error', result)

    def test_invalid_id_on_remove_returns_error_dict(self):
        self.collection.delete_one.side_effect = PyMongoError('fail')
        editor = Editor(self.mongo, 'users', {'action': 'remove'}, doc_id='not-an-objectid')
        result = editor.process()
        self.assertIn('error', result)

    def test_validators_pass_allows_create(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Alice'}

        validators = {'name': lambda v: None}  # always passes
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                        validators=validators)
        result = editor.process()
        self.assertIn('data', result)
        self.assertNotIn('fieldErrors', result)

    def test_validators_fail_returns_field_errors(self):
        validators = {'name': lambda v: 'Name is required' if not v else None}
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': ''}}},
                        validators=validators)
        result = editor.process()
        self.assertIn('fieldErrors', result)
        self.assertNotIn('data', result)
        self.assertEqual(result['fieldErrors'][0]['name'], 'name')
        self.assertEqual(result['fieldErrors'][0]['status'], 'Name is required')

    def test_validators_multiple_field_errors(self):
        validators = {
            'name': lambda v: 'Required' if not v else None,
            'email': lambda v: 'Invalid email' if v and '@' not in v else None,
        }
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': '', 'email': 'bad'}}},
                        validators=validators)
        result = editor.process()
        self.assertIn('fieldErrors', result)
        self.assertEqual(len(result['fieldErrors']), 2)

    def test_validators_not_run_for_remove(self):
        """Validators should not block remove operations."""
        self.collection.delete_one.return_value = MagicMock()
        doc_id = str(ObjectId())
        validators = {'name': lambda v: 'Required'}
        editor = Editor(self.mongo, 'users', {'action': 'remove'},
                        doc_id=doc_id, validators=validators)
        result = editor.process()
        # remove should succeed (no fieldErrors)
        self.assertNotIn('fieldErrors', result)

    def test_no_validators_no_field_errors(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Bob'}

        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Bob'}}})
        result = editor.process()
        self.assertIn('data', result)
        self.assertNotIn('fieldErrors', result)


class TestEditorMultiRowCreate(unittest.TestCase):
    """Gap #2: create() handles multiple rows (data[0], data[1], ...)."""

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

        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Alice'}}})
        result = editor.create()
        self.assertEqual(len(result['data']), 1)
        self.collection.insert_one.assert_called_once()

    def test_multi_row_create_inserts_all_rows(self):
        oids = [ObjectId(), ObjectId(), ObjectId()]
        insert_calls = [self._make_insert_mock(o) for o in oids]
        self.collection.insert_one.side_effect = insert_calls
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'Alice'},
            {'_id': oids[1], 'name': 'Bob'},
            {'_id': oids[2], 'name': 'Carol'},
        ]

        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {
                '0': {'name': 'Alice'},
                '1': {'name': 'Bob'},
                '2': {'name': 'Carol'},
            }
        })
        result = editor.create()
        self.assertEqual(len(result['data']), 3)
        self.assertEqual(self.collection.insert_one.call_count, 3)

    def test_multi_row_create_returns_all_dt_row_ids(self):
        oids = [ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'Alice'},
            {'_id': oids[1], 'name': 'Bob'},
        ]

        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}}
        })
        result = editor.create()
        row_ids = [r['DT_RowId'] for r in result['data']]
        self.assertEqual(row_ids, [str(oids[0]), str(oids[1])])

    def test_multi_row_create_sorted_order(self):
        """Rows should be inserted in numeric key order (0, 1, 2, ...)."""
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
            {'_id': oids[0], 'name': 'First'},
            {'_id': oids[1], 'name': 'Second'},
            {'_id': oids[2], 'name': 'Third'},
        ]

        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {
                '2': {'name': 'Third'},
                '0': {'name': 'First'},
                '1': {'name': 'Second'},
            }
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
            {'_id': oids[0], 'name': 'X'},
            {'_id': oids[1], 'name': 'Y'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {'0': {'name': 'X'}, '1': {'name': 'Y'}}
        })
        result = editor.process()
        self.assertIn('data', result)
        self.assertEqual(len(result['data']), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
