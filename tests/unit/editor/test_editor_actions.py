"""Editor tests — editor actions (dependent, search, options, error responses)."""
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

def _make_dependent_editor(request_args, dependent_handlers=None):
    mongo = MagicMock()
    mongo.db = MagicMock()
    mongo.db.__getitem__ = MagicMock(return_value=MagicMock())
    return Editor(mongo, "test_collection", request_args, dependent_handlers=dependent_handlers)


class TestEditorDependent(unittest.TestCase):
    def test_dependent_calls_handler_with_field_values_rows(self):
        handler = MagicMock(return_value={"options": {"city": [{"label": "NYC", "value": 1}]}})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {"country": "US"}, "rows": [{"country": "CA"}]},
            dependent_handlers={"country": handler},
        )
        result = editor.dependent()
        handler.assert_called_once_with("country", {"country": "US"}, [{"country": "CA"}])
        self.assertEqual(result, {"options": {"city": [{"label": "NYC", "value": 1}]}})

    def test_dependent_no_handler_raises_invalid_data_error(self):
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
            dependent_handlers={},
        )
        with self.assertRaises(InvalidDataError):
            editor.dependent()

    def test_dependent_no_handlers_configured_raises(self):
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
        )
        with self.assertRaises(InvalidDataError):
            editor.dependent()

    def test_dependent_via_process_dispatches_correctly(self):
        handler = MagicMock(return_value={"values": {"city": "London"}})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {"country": "UK"}, "rows": []},
            dependent_handlers={"country": handler},
        )
        result = editor.process()
        self.assertEqual(result, {"values": {"city": "London"}})

    def test_dependent_via_process_unknown_field_returns_error(self):
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "unknown", "values": {}, "rows": []},
            dependent_handlers={"country": MagicMock()},
        )
        result = editor.process()
        self.assertIn("error", result)

    def test_dependent_missing_values_defaults_to_empty_dict(self):
        handler = MagicMock(return_value={})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country"},
            dependent_handlers={"country": handler},
        )
        editor.dependent()
        handler.assert_called_once_with("country", {}, [])

    def test_dependent_missing_rows_defaults_to_empty_list(self):
        handler = MagicMock(return_value={})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {"country": "US"}},
            dependent_handlers={"country": handler},
        )
        editor.dependent()
        handler.assert_called_once_with("country", {"country": "US"}, [])

    def test_dependent_response_can_contain_all_protocol_keys(self):
        full_response = {
            "options": {"city": [{"label": "NYC", "value": 1}]},
            "values": {"zip": "10001"}, "messages": {"city": "Select a city"},
            "errors": {"zip": "Invalid zip"}, "labels": {"city": "City/Town"},
            "show": ["zip"], "hide": "region", "enable": ["city"], "disable": "country",
        }
        handler = MagicMock(return_value=full_response)
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
            dependent_handlers={"country": handler},
        )
        self.assertEqual(editor.dependent(), full_response)

    def test_dependent_multiple_handlers_dispatches_to_correct_one(self):
        handler_a = MagicMock(return_value={"values": {"b": "from_a"}})
        handler_b = MagicMock(return_value={"values": {"a": "from_b"}})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "field_b", "values": {}, "rows": []},
            dependent_handlers={"field_a": handler_a, "field_b": handler_b},
        )
        result = editor.dependent()
        handler_a.assert_not_called()
        handler_b.assert_called_once()
        self.assertEqual(result, {"values": {"a": "from_b"}})

    def test_dependent_bypasses_validators(self):
        validator_called = []
        def validator(val):
            validator_called.append(True)
            return None
        handler = MagicMock(return_value={})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
            dependent_handlers={"country": handler},
        )
        editor.validators = {"country": validator}
        editor.process()
        self.assertEqual(validator_called, [])




class TestEditorErrorResponseFormat(unittest.TestCase):
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
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                        validators={'name': lambda v: None},
                        data_fields=[DataField('name', 'string')])
        result = editor.process()
        self.assertIn('data', result)
        self.assertNotIn('fieldErrors', result)

    def test_validators_fail_returns_field_errors(self):
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': ''}}},
                        validators={'name': lambda v: 'Name is required' if not v else None})
        result = editor.process()
        self.assertIn('fieldErrors', result)
        self.assertNotIn('data', result)
        self.assertEqual(result['fieldErrors'][0]['name'], 'name')
        self.assertEqual(result['fieldErrors'][0]['status'], 'Name is required')

    def test_validators_multiple_field_errors(self):
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': '', 'email': 'bad'}}},
                        validators={
                            'name': lambda v: 'Required' if not v else None,
                            'email': lambda v: 'Invalid email' if v and '@' not in v else None,
                        })
        result = editor.process()
        self.assertIn('fieldErrors', result)
        self.assertEqual(len(result['fieldErrors']), 2)

    def test_validators_not_run_for_remove(self):
        self.collection.delete_one.return_value = MagicMock()
        doc_id = str(ObjectId())
        editor = Editor(self.mongo, 'users', {'action': 'remove'},
                        doc_id=doc_id, validators={'name': lambda v: 'Required'})
        result = editor.process()
        self.assertNotIn('fieldErrors', result)

    def test_no_validators_no_field_errors(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Bob'}
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {'0': {'name': 'Bob'}}},
                        data_fields=[DataField('name', 'string')])
        result = editor.process()
        self.assertIn('data', result)
        self.assertNotIn('fieldErrors', result)




class TestEditorOptions(unittest.TestCase):
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
        return Editor(self.mongo, 'users', request_args, doc_id=doc_id, options=options,
                      data_fields=[DataField('name', 'string')])

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
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                                   options=lambda: opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_on_edit_action(self):
        oid = str(self.collection.find_one.return_value['_id'])
        opts = {'role': [{'label': 'Admin', 'value': 'admin'}]}
        editor = self._make_editor(
            {'action': 'edit', 'data': {oid: {'name': 'Bob'}}}, doc_id=oid, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_on_remove_action(self):
        oid = str(ObjectId())
        opts = {'type': [{'label': 'X', 'value': 'x'}]}
        editor = self._make_editor({'action': 'remove', 'data': {}}, doc_id=oid, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_callable_called_fresh_each_process_call(self):
        call_count = {'n': 0}

        def dynamic_opts():
            call_count['n'] += 1
            return {'field': [{'label': str(call_count['n']), 'value': call_count['n']}]}

        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                                   options=dynamic_opts)
        editor.process()
        editor.process()
        self.assertEqual(call_count['n'], 2)




# ---------------------------------------------------------------------------
# test_editor_search_action.py — TestEditorSearchAction
# ---------------------------------------------------------------------------

def _make_search_editor(request_args):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = collection
    editor = Editor(mongo, "countries", request_args)
    return editor, collection


class TestEditorSearchAction(unittest.TestCase):
    def test_search_by_term_returns_matching_labels(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": "u"})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        self.assertEqual(result["data"], [{"label": "Uganda", "value": "Uganda"},
                                          {"label": "Ukraine", "value": "Ukraine"}])

    def test_search_by_values_returns_exact_matches(self):
        editor, col = _make_search_editor({"action": "search", "field": "country",
                                           "values": ["Uganda", "Ukraine"]})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        self.assertEqual(len(result["data"]), 2)
        self.assertEqual(result["data"][0]["value"], "Uganda")

    def test_search_unknown_field_returns_empty(self):
        editor, col = _make_search_editor({"action": "search", "field": "nonexistent", "search": "x"})
        col.find.return_value.limit.return_value = []
        result = editor.search()
        self.assertEqual(result["data"], [])

    def test_search_empty_term_returns_results(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": ""})
        col.find.return_value.limit.return_value = [{"country": "France"}, {"country": "Germany"}]
        result = editor.search()
        self.assertEqual(len(result["data"]), 2)

    def test_search_deduplicates_values(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": "u"})
        col.find.return_value.limit.return_value = [
            {"country": "Uganda"}, {"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        values = [d["value"] for d in result["data"]]
        self.assertEqual(values, ["Uganda", "Ukraine"])

    def test_search_no_params_returns_empty(self):
        editor, col = _make_search_editor({"action": "search", "field": "country"})
        result = editor.search()
        self.assertEqual(result, {"data": []})
        col.find.assert_not_called()

    def test_search_registered_in_process(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": "u"})
        with patch.object(Editor, "search", return_value={"data": []}) as mock_search:
            result = editor.process()
            mock_search.assert_called_once()
            self.assertEqual(result, {"data": []})

    def test_search_by_values_coerces_number_type(self):
        """values list with string numbers should be coerced to int for number fields."""

        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        col.find.return_value.limit.return_value = [{"_id": "x", "age": 30}]
        editor = Editor(mongo, "items", {"action": "search", "field": "age", "values": ["30"]},
                        data_fields=[DataField("age", "number")])
        result = editor.search()
        col.find.assert_called_once_with({"age": {"$in": [30]}}, {"age": 1})
        assert result == {"data": [{"label": "30", "value": 30}]}

    def test_search_by_values_coerces_boolean_type(self):
        """values list with string booleans should be coerced to bool for boolean fields."""

        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        col.find.return_value.limit.return_value = [{"_id": "x", "active": True}]
        editor = Editor(mongo, "items", {"action": "search", "field": "active", "values": ["true"]},
                        data_fields=[DataField("active", "boolean")])
        result = editor.search()
        col.find.assert_called_once_with({"active": {"$in": [True]}}, {"active": 1})
        assert result == {"data": [{"label": "True", "value": True}]}

    def test_search_by_values_no_field_type_passes_through(self):
        """values without a declared field type are passed through unchanged."""
        editor, col = _make_search_editor({"action": "search", "field": "name", "values": ["Alice"]})
        col.find.return_value.limit.return_value = [{"_id": "x", "name": "Alice"}]
        result = editor.search()
        col.find.assert_called_once_with({"name": {"$in": ["Alice"]}}, {"name": 1})
        assert result == {"data": [{"label": "Alice", "value": "Alice"}]}

    def test_search_value_preserves_original_type_in_response(self):
        """value in response dict should be the original MongoDB type, not a string."""
        editor, col = _make_search_editor({"action": "search", "field": "score", "search": "42"})
        col.find.return_value.limit.return_value = [{"_id": "x", "score": 42}]
        result = editor.search()
        assert result["data"][0]["value"] == 42
        assert isinstance(result["data"][0]["value"], int)


