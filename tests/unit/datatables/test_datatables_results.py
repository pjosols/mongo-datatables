"""Test DataTables results: pipeline construction, data formatting, complex types."""
import unittest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from datetime import datetime
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import DataTables, DataField
from tests.unit.base_test import BaseDataTablesTest


class TestResults(BaseDataTablesTest):
    """Test results method and query pipeline."""

    def test_results_method(self):
        self.collection.aggregate.return_value = self.sample_docs
        self.collection.count_documents.return_value = len(self.sample_docs)
        datatables = DataTables(self.mongo, 'users', self.request_args)
        result = datatables.results()
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), len(self.sample_docs))
        for doc in result:
            self.assertIn('name', doc)
            self.assertIn('email', doc)
            self.assertIn('status', doc)
            self.assertIn('DT_RowId', doc)

    def test_results_with_empty_data(self):
        self.collection.aggregate.return_value = []
        self.collection.count_documents.return_value = 0
        datatables = DataTables(self.mongo, 'users', self.request_args)
        result = datatables.results()
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_results_with_objectid_conversion(self):
        doc_id = ObjectId()
        doc_with_id = {"_id": doc_id, "name": "Test User"}
        self.collection.aggregate.return_value = [doc_with_id]
        self.collection.count_documents.return_value = 1
        datatables = DataTables(self.mongo, 'users', self.request_args)
        result = datatables.results()
        self.assertEqual(len(result), 1)
        self.assertNotIn('_id', result[0])
        self.assertIn('DT_RowId', result[0])
        self.assertEqual(result[0]['DT_RowId'], str(doc_id))
        self.assertEqual(result[0]['name'], "Test User")

    def test_results_error_handling(self):
        from pymongo.errors import PyMongoError
        self.collection.aggregate.side_effect = PyMongoError("Test exception")
        datatables = DataTables(self.mongo, 'users', self.request_args)
        result = datatables.results()
        self.assertEqual(result, [])

    def test_query_pipeline(self):
        custom_filter = {'name': 'test'}
        request_args = {
            'draw': 1, 'start': 10, 'length': 10,
            'search': {'value': '', 'regex': False},
            'order': [{'column': 0, 'dir': 'asc'}],
            'columns': [{'data': 'name', 'searchable': True, 'orderable': True,
                         'search': {'value': '', 'regex': False}}],
        }
        datatables = DataTables(self.mongo, 'users', request_args, **custom_filter)
        with patch.object(datatables.collection, 'aggregate') as mock_aggregate:
            mock_aggregate.return_value = []
            datatables.results()
            args, _ = mock_aggregate.call_args
            pipeline = args[0]
            self.assertTrue(len(pipeline) > 0)
            self.assertTrue(any('$project' in stage for stage in pipeline))
            if datatables.filter:
                self.assertTrue(any('$match' in stage for stage in pipeline))
            if datatables.sort_specification:
                self.assertTrue(any('$sort' in stage for stage in pipeline))
            if datatables.start > 0:
                self.assertTrue(any('$skip' in stage for stage in pipeline))
            if datatables.limit:
                self.assertTrue(any('$limit' in stage for stage in pipeline))


class TestDataTablesQueryPipeline(BaseDataTablesTest):
    """Test cases for query pipeline construction and results processing."""

    def setUp(self):
        super().setUp()
        self.data_fields = [
            DataField('title', 'string'),
            DataField('author', 'string'),
            DataField('year', 'number'),
            DataField('rating', 'number'),
            DataField('published_date', 'date'),
            DataField('tags', 'array'),
            DataField('metadata', 'object'),
            DataField('_id', 'objectid'),
        ]
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'author', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'year', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'rating', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'published_date', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'tags', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'metadata', 'searchable': False, 'orderable': False, 'search': {'value': '', 'regex': False}},
            {'data': '_id', 'searchable': False, 'orderable': True, 'search': {'value': '', 'regex': False}},
        ]

    def test_complete_query_pipeline(self):
        self.request_args['search']['value'] = 'test query'
        self.request_args['order'] = [{'column': 0, 'dir': 'asc'}]
        self.request_args['start'] = 10
        self.request_args['length'] = 25
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields, debug_mode=True)
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        datatables.results()
        args, _ = datatables.collection.aggregate.call_args
        pipeline = args[0]
        self.assertIsInstance(pipeline, list)
        self.assertGreaterEqual(len([s for s in pipeline if '$match' in s]), 1)
        self.assertEqual(len([s for s in pipeline if '$sort' in s]), 1)
        skip_stages = [s for s in pipeline if '$skip' in s]
        self.assertEqual(len(skip_stages), 1)
        self.assertEqual(skip_stages[0]['$skip'], 10)
        limit_stages = [s for s in pipeline if '$limit' in s]
        self.assertEqual(len(limit_stages), 1)
        self.assertEqual(limit_stages[0]['$limit'], 25)

    def test_results_with_complex_data_types(self):
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        mock_data = [{
            '_id': ObjectId('5f50c31e8a91e8c9c8d5c5d5'),
            'title': 'Test Title', 'author': 'Test Author',
            'year': 2020, 'rating': 4.5,
            'published_date': datetime(2020, 1, 1),
            'tags': ['fiction', 'bestseller'],
            'metadata': {'publisher': 'Test Publisher', 'edition': 1},
        }]
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter(mock_data)
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        results = datatables.results()
        self.assertEqual(len(results), 1)
        self.assertIn('DT_RowId', results[0])
        self.assertIsInstance(results[0]['DT_RowId'], str)
        self.assertIsInstance(results[0]['published_date'], str)
        self.assertNotIn('_id', results[0])
        self.assertIsInstance(results[0]['year'], int)
        self.assertIsInstance(results[0]['rating'], float)
        self.assertIsInstance(results[0]['tags'], list)
        self.assertIsInstance(results[0]['metadata'], dict)

    def test_empty_results(self):
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        self.assertEqual(datatables.results(), [])

    def test_custom_filter_in_pipeline(self):
        custom_filter = {'status': 'active', 'category': {'$in': ['book', 'magazine']}}
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields, **custom_filter)
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        datatables.results()
        args, _ = datatables.collection.aggregate.call_args
        pipeline = args[0]
        match_stages = [s for s in pipeline if '$match' in s]
        self.assertGreaterEqual(len(match_stages), 1)
        custom_filter_found = any(
            'status' in s['$match'] and 'category' in s['$match']
            for s in match_stages
        )
        self.assertTrue(custom_filter_found)

    def test_projection_in_pipeline(self):
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter([])
        datatables.collection.aggregate = MagicMock(return_value=mock_cursor)
        datatables.results()
        args, _ = datatables.collection.aggregate.call_args
        pipeline = args[0]
        project_stages = [s for s in pipeline if '$project' in s]
        self.assertEqual(len(project_stages), 1)
        projection = project_stages[0]['$project']
        for field in ['title', 'author', 'year', 'rating', 'published_date', 'tags', 'metadata', '_id']:
            self.assertEqual(projection.get(field), 1)

    def test_projection_with_alias_uses_db_field_name(self):
        data_fields = [DataField('author.fullName', 'string', alias='Author')]
        request_args = {
            'draw': 1,
            'columns': [{'data': 'Author', 'searchable': True, 'orderable': True,
                         'search': {'value': '', 'regex': False}}],
            'search': {'value': '', 'regex': False},
            'order': [], 'start': 0, 'length': 10,
        }
        dt = DataTables(self.mongo, 'test_collection', request_args, data_fields=data_fields)
        self.assertIn('author.fullName', dt.projection)
        self.assertNotIn('Author', dt.projection)

    def test_projection_without_alias_unchanged(self):
        data_fields = [DataField('title', 'string')]
        request_args = {
            'draw': 1,
            'columns': [{'data': 'title', 'searchable': True, 'orderable': True,
                         'search': {'value': '', 'regex': False}}],
            'search': {'value': '', 'regex': False},
            'order': [], 'start': 0, 'length': 10,
        }
        dt = DataTables(self.mongo, 'test_collection', request_args, data_fields=data_fields)
        self.assertIn('title', dt.projection)

    def test_projection_mixed_aliased_and_plain(self):
        data_fields = [
            DataField('author.fullName', 'string', alias='Author'),
            DataField('title', 'string'),
        ]
        request_args = {
            'draw': 1,
            'columns': [
                {'data': 'Author', 'searchable': True, 'orderable': True,
                 'search': {'value': '', 'regex': False}},
                {'data': 'title', 'searchable': True, 'orderable': True,
                 'search': {'value': '', 'regex': False}},
            ],
            'search': {'value': '', 'regex': False},
            'order': [], 'start': 0, 'length': 10,
        }
        dt = DataTables(self.mongo, 'test_collection', request_args, data_fields=data_fields)
        self.assertIn('author.fullName', dt.projection)
        self.assertIn('title', dt.projection)
        self.assertNotIn('Author', dt.projection)


if __name__ == '__main__':
    unittest.main()
