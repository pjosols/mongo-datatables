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

## Search Functionality

### How Search Works in mongo-datatables

mongo-datatables provides powerful search capabilities that adapt based on your MongoDB configuration and search syntax. Understanding how search works can help you optimize performance, especially for large collections.

#### Search Types and Performance

<table style="font-size: 0.35em; border-collapse: collapse;">
  <tr>
    <th style="padding: 2px 4px;">Type</th>
    <th style="padding: 2px 4px;">Example</th>
    <th style="padding: 2px 4px;">Performance</th>
    <th style="padding: 2px 4px;">Description</th>
    <th style="padding: 2px 4px;">MongoDB Query</th>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Text Index</b></td>
    <td style="padding: 2px 4px;"><code>George Orwell</code></td>
    <td style="padding: 2px 4px;">Very Fast<br>S: &lt;50ms<br>L: 100-300ms</td>
    <td style="padding: 2px 4px;">MongoDB text search with indexes</td>
    <td style="padding: 2px 4px;"><code>{$text:{$search:"George Orwell"}}</code></td>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Phrase</b></td>
    <td style="padding: 2px 4px;"><code>"Margaret Atwood"</code></td>
    <td style="padding: 2px 4px;">Fast<br>S: &lt;50ms<br>L: 100-300ms</td>
    <td style="padding: 2px 4px;">Exact phrase matching</td>
    <td style="padding: 2px 4px;"><code>{$text:{$search:"\"Margaret Atwood\""}}</code></td>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Field</b></td>
    <td style="padding: 2px 4px;"><code>Author:Bradbury</code></td>
    <td style="padding: 2px 4px;">Moderate<br>S: 20-50ms<br>L: 1-2s</td>
    <td style="padding: 2px 4px;">Field-specific queries</td>
    <td style="padding: 2px 4px;"><code>{Author:{$regex:"Bradbury",$options:"i"}}</code></td>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Comparison</b></td>
    <td style="padding: 2px 4px;"><code>Pages:>100</code></td>
    <td style="padding: 2px 4px;">Fast<br>S: &lt;50ms<br>L: 200-500ms</td>
    <td style="padding: 2px 4px;">Numeric/date comparisons</td>
    <td style="padding: 2px 4px;"><code>{Pages:{$gt:100}}</code></td>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Combined</b></td>
    <td style="padding: 2px 4px;"><code>Author:"Huxley" Year:>2000</code></td>
    <td style="padding: 2px 4px;">Moderate<br>S: 50-100ms<br>L: 500ms-1s</td>
    <td style="padding: 2px 4px;">Multiple search types</td>
    <td style="padding: 2px 4px;">Complex multi-condition query</td>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Regex</b></td>
    <td style="padding: 2px 4px;"><code>George Orwell</code> (no index)</td>
    <td style="padding: 2px 4px;">Slow<br>S: 50-100ms<br>L: 5-10s+</td>
    <td style="padding: 2px 4px;">Fallback when no text index</td>
    <td style="padding: 2px 4px;"><code>{$or:[{field:{$regex:"George"...}}]}</code></td>
  </tr>
  <tr>
    <td style="padding: 2px 4px;"><b>Mixed</b></td>
    <td style="padding: 2px 4px;"><code>Title:"Ski" Ishiguro</code></td>
    <td style="padding: 2px 4px;">Moderate<br>S: 50-100ms<br>L: 300-700ms</td>
    <td style="padding: 2px 4px;">Phrase + text search</td>
    <td style="padding: 2px 4px;">Complex phrase + text query</td>
  </tr>
</table>

<p style="font-size: 0.35em;"><i>Performance: S = Small collection (&lt;100K docs), L = Large collection (&gt;2M docs)</i></p>

### Performance Optimization

#### Importance of Indexes for Large Collections

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