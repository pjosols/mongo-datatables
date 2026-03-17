======
Editor
======

Overview
========

The ``Editor`` class provides server-side implementation for the DataTables Editor extension,
enabling CRUD (Create, Read, Update, Delete) operations on MongoDB collections.

DataTables Editor is a commercial extension for DataTables that provides end users with
the ability to create, edit and delete entries in a DataTable. This implementation
translates Editor requests into MongoDB operations.

Class Documentation
===================

.. autoclass:: mongo_datatables.editor.StorageAdapter
   :members:

.. autoclass:: mongo_datatables.editor.Editor
   :members:
   :undoc-members:
   :show-inheritance:

Editor Actions
==============

The Editor class handles three main actions:

1. **create** — Add a new document to the MongoDB collection
2. **edit** — Update one or more existing documents
3. **remove** — Delete one or more documents from the collection

The action is determined by the ``action`` parameter in the request payload sent by
DataTables Editor.

Example Usage
=============

Basic usage with Flask:

.. code-block:: python

    from flask import Flask, request, jsonify
    from flask_pymongo import PyMongo
    from mongo_datatables import Editor

    app = Flask(__name__)
    app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
    mongo = PyMongo(app)

    @app.route('/api/editor', methods=['POST'])
    def editor_endpoint():
        data = request.get_json()
        doc_id = request.args.get('id', '')
        result = Editor(mongo, 'users', data, doc_id).process()
        return jsonify(result)

Supporting multiple collections:

.. code-block:: python

    @app.route('/api/editor/<collection>', methods=['POST'])
    def editor_endpoint(collection):
        data = request.get_json()
        doc_id = request.args.get('id', '')
        result = Editor(mongo, collection, data, doc_id).process()
        return jsonify(result)
