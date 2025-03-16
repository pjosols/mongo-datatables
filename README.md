# mongo-datatables

[![PyPI version](https://badge.fury.io/py/mongo-datatables.svg)](https://badge.fury.io/py/mongo-datatables)
[![Downloads](https://static.pepy.tech/badge/mongo-datatables)](https://pepy.tech/project/mongo-datatables)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/pjosols/mongo-datatables/branch/main/graph/badge.svg)](https://codecov.io/gh/pjosols/mongo-datatables)
[![Tests](https://github.com/pjosols/mongo-datatables/actions/workflows/python-tests.yml/badge.svg)](https://github.com/pjosols/mongo-datatables/actions/workflows/python-tests.yml)

Server-side processing for jQuery DataTables with MongoDB.

## Support
If you find this project helpful, consider buying me a coffee!

<a href="https://www.buymeacoffee.com/pjosols"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" width="150" alt="Buy Me A Coffee"></a>


## Overview

This package provides an elegant bridge between jQuery DataTables and MongoDB databases, handling the translation of DataTables server-side requests into optimized MongoDB queries. It supports both read operations and full CRUD functionality when paired with DataTables Editor.

## Key Capabilities

- Server-side processing for efficient handling of large datasets
- Advanced search functionality with column-specific filtering
- Multi-column sorting with MongoDB optimization
- Complete Editor integration for create, read, update, and delete operations
- Framework-agnostic design compatible with Flask, Django, and other Python web frameworks

## Installation

```bash
pip install mongo-datatables
```

## Basic Implementation

```python
from flask import request, jsonify
from mongo_datatables import DataTables

@app.route('/data/<collection>', methods=['POST'])
def get_data(collection):
    data = request.get_json()
    results = DataTables(mongo, collection, data).get_rows()
    return jsonify(results)
```

## Documentation

For comprehensive documentation, visit [mongo-datatables.readthedocs.io](https://mongo-datatables.readthedocs.io/)

## Performance Optimization

### Importance of Indexes for Large Collections

When working with large MongoDB collections, creating proper indexes is **critical** for performance. Without appropriate indexes, queries can become extremely slow or timeout entirely.

#### Text Indexes for Search Performance

The DataTables class is designed to leverage MongoDB text indexes for efficient search operations:

```python
# Create a text index in MongoDB (do this once in your setup script)
db.your_collection.create_index([("field1", "text"), ("field2", "text")])

# DataTables will automatically use the text index when available
datatables = DataTables(mongo, 'your_collection', request_args)
```

Benefits of text indexes:

- **Dramatically faster search** on large collections
- **Better relevance scoring** for search results
- **Language-aware stemming** for more natural search
- **Support for exact phrase queries** using quotes

If a text index is not available, the library will fall back to regex-based search, which is significantly slower for large collections.

#### Regular Indexes for Sorting and Filtering

In addition to text indexes, create regular indexes for fields used in sorting and filtering:

```python
# Create indexes for commonly sorted/filtered fields
db.your_collection.create_index("created_at")
db.your_collection.create_index("status")
```

> **Note:** MongoDB has a limit of one text index per collection, but you can include multiple fields in a single text index.

## Development

### Testing

Run the tests:

```bash
python run_tests.py
```

Generate coverage report:

```bash
python run_coverage.py
```

Coverage reports are available in the `htmlcov` directory.

## License

Released under the MIT License.