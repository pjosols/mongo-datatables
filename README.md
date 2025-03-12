# mongo-datatables

[![PyPI version](https://badge.fury.io/py/mongo-datatables.svg)](https://badge.fury.io/py/mongo-datatables)
[![Downloads](https://static.pepy.tech/badge/mongo-datatables)](https://pepy.tech/project/mongo-datatables)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/pjosols/mongo-datatables/branch/main/graph/badge.svg)](https://codecov.io/gh/pjosols/mongo-datatables)
[![Tests](https://github.com/pjosols/mongo-datatables/actions/workflows/python-tests.yml/badge.svg)](https://github.com/pjosols/mongo-datatables/actions/workflows/python-tests.yml)

Server-side processing for jQuery DataTables with MongoDB.

## Support
If you find this project helpful, consider buying me a coffee!

<p>
  <a href="https://www.buymeacoffee.com/yourusername" target="_blank">
    <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" width="160">
  </a>
</p>

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