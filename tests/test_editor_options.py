import unittest
from unittest.mock import MagicMock, call
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult

from mongo_datatables import Editor


class TestEditorOptions(unittest.TestCase):
    """Gap #5: options key in Editor process() response."""

    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Alice'}
        self.collection.update_one.return_value = MagicMock()
        self.collection.delete_one.return_value = MagicMock()

    def _make_editor(self, request_args, doc_id='', options=None):
        return Editor(self.mongo, 'users', request_args, doc_id=doc_id, options=options)

    def test_no_options_key_absent_by_default(self):
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}})
        result = editor.process()
        self.assertNotIn('options', result)

    def test_options_dict_included_in_create_response(self):
        opts = {'status': [{'label': 'Active', 'value': 1}, {'label': 'Inactive', 'value': 0}]}
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}}, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_callable_called_and_result_included(self):
        opts = {'city': [{'label': 'Edinburgh', 'value': 1}]}
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}}, options=lambda: opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_on_edit_action(self):
        oid = str(self.collection.find_one.return_value['_id'])
        opts = {'role': [{'label': 'Admin', 'value': 'admin'}]}
        editor = self._make_editor(
            {'action': 'edit', 'data': {oid: {'name': 'Bob'}}},
            doc_id=oid,
            options=opts,
        )
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_on_remove_action(self):
        oid = str(ObjectId())
        opts = {'type': [{'label': 'X', 'value': 'x'}]}
        editor = self._make_editor({'action': 'remove'}, doc_id=oid, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_callable_called_fresh_each_process_call(self):
        call_count = {'n': 0}

        def dynamic_opts():
            call_count['n'] += 1
            return {'field': [{'label': str(call_count['n']), 'value': call_count['n']}]}

        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}}, options=dynamic_opts)
        editor.process()
        editor.process()
        self.assertEqual(call_count['n'], 2)
