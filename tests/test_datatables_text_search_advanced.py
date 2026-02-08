import unittest
from unittest.mock import patch, MagicMock
from tests.base_test import BaseDataTablesTest
from mongo_datatables.datatables import DataTables, DataField


class TestDataTablesTextSearchAdvanced(BaseDataTablesTest):
    """Test cases for advanced text search functionality in DataTables"""

    def setUp(self):
        super().setUp()
        # Set up data fields for testing
        self.data_fields = [
            DataField('title', 'string'),
            DataField('author', 'string'),
            DataField('year', 'number'),
            DataField('rating', 'number'),
            DataField('published_date', 'date'),
            DataField('is_bestseller', 'boolean'),
            DataField('tags', 'array'),
            DataField('metadata', 'object'),
            DataField('publisher_id', 'objectid')
        ]

    def test_text_search_with_text_index(self):
        """Test text search when text index is available"""
        # Set up a search
        self.request_args['search']['value'] = 'test search'

        # Add columns to request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True},
            {'data': 'author', 'searchable': True},
            {'data': 'year', 'searchable': True}
        ]

        # Mock has_text_index to return True
        with patch.object(DataTables, 'has_text_index', return_value=True):
            # Create DataTables instance
            datatables = DataTables(
                self.mongo,
                'test_collection',
                self.request_args,
                data_fields=self.data_fields,
                use_text_index=True
            )

            # Get the global search condition
            condition = datatables.global_search_condition

            # Verify the condition uses text search
            self.assertIn('$text', condition)
            self.assertIn('$search', condition['$text'])

    def test_exact_phrase_search_with_text_index(self):
        """Test exact phrase search with text index available"""
        # Set up a quoted search
        self.request_args['search']['value'] = '"exact phrase"'

        # Add columns to request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True},
            {'data': 'author', 'searchable': True}
        ]

        # Mock has_text_index to return True
        with patch.object(DataTables, 'has_text_index', return_value=True):
            # Create DataTables instance
            datatables = DataTables(
                self.mongo,
                'test_collection',
                self.request_args,
                data_fields=self.data_fields,
                use_text_index=True
            )

            # Get the global search condition
            condition = datatables.global_search_condition

            # Verify the condition uses text search with the exact phrase
            self.assertIn('$text', condition)
            self.assertEqual(condition['$text']['$search'], '"exact phrase"')

    def test_quoted_search_without_text_index(self):
        """Test quoted search when text index is not available"""
        # Set up a quoted search
        self.request_args['search']['value'] = '"no text index"'

        # Add columns to request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True},
            {'data': 'author', 'searchable': True}
        ]

        # Create DataTables instance
        datatables = DataTables(
            self.mongo,
            'test_collection',
            self.request_args,
            data_fields=self.data_fields,
            use_text_index=True  # Even though we want to use it, it's not available
        )

        # Directly set has_text_index to False
        datatables._has_text_index = False

        # Get the global search condition
        condition = datatables.global_search_condition

        # Verify the condition uses regex instead of text search
        self.assertIn('$or', condition)
        self.assertNotIn('$text', condition)

    def test_text_search_disabled(self):
        """Test when text search is explicitly disabled"""
        # Set up a search
        self.request_args['search']['value'] = 'disabled text search'

        # Add columns to request args
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True},
            {'data': 'author', 'searchable': True}
        ]

        # Mock has_text_index to return True, but we'll disable text search
        with patch.object(DataTables, 'has_text_index', return_value=True):
            # Create DataTables instance with text search disabled
            datatables = DataTables(
                self.mongo,
                'test_collection',
                self.request_args,
                data_fields=self.data_fields,
                use_text_index=False  # Explicitly disable text search
            )

            # Get the global search condition
            condition = datatables.global_search_condition

            # Verify the condition uses regex instead of text search
            self.assertIn('$or', condition)
            self.assertNotIn('$text', condition)


if __name__ == '__main__':
    unittest.main()
