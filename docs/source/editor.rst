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

.. py:class:: mongo_datatables.editor.Editor(pymongo_object, collection_name, request_args, doc_id=None)

   Server-side processor for DataTables Editor with MongoDB.

   This class handles CRUD operations from DataTables Editor, translating them
   into appropriate MongoDB operations.

   :param pymongo_object: PyMongo client connection or Flask-PyMongo instance
   :param collection_name: Name of the MongoDB collection
   :param request_args: Editor request parameters (from request.get_json())
   :param doc_id: Comma-separated list of document IDs for edit/remove operations

   .. py:method:: process()

      Process the Editor request based on the action.

      :return: Response data for the Editor client
      :rtype: dict
      :raises ValueError: If action is not supported

Key Properties
==============

.. py:attribute:: db

   Get the MongoDB database instance.

   :return: The PyMongo database instance

.. py:attribute:: collection

   Get the MongoDB collection.

   :return: The PyMongo collection instance

.. py:attribute:: action

   Get the Editor action type (create, edit, remove).

   :return: Action type string

.. py:attribute:: data

   Get the data payload from the request.

   :return: Dictionary containing the submitted data

.. py:attribute:: list_of_ids

   Get list of document IDs for batch operations.

   :return: List of document ID strings

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

Editor Actions
==============

The Editor class handles three main actions:

1. **create** - Add a new document to the MongoDB collection
2. **edit** - Update one or more existing documents
3. **remove** - Delete one or more documents from the collection

The action is determined by the `action` parameter in the request payload sent by DataTables Editor.