======
Editor
======

The ``Editor`` class provides server-side CRUD for `DataTables Editor
<https://editor.datatables.net/>`_ (a commercial DataTables extension).  It
receives the JSON payload that Editor posts, dispatches to the appropriate
action, and returns the response Editor expects.

.. autoclass:: mongo_datatables.editor.StorageAdapter
   :members:

.. autoclass:: mongo_datatables.editor.Editor
   :members:
   :undoc-members:
   :show-inheritance:


Basic Setup
===========

A single endpoint handles all Editor actions (``create``, ``edit``,
``remove``).  Editor sends an ``action`` field in the request body;
``process()`` dispatches automatically.

.. code-block:: python

    from mongo_datatables import Editor, DataField

    @app.route("/api/editor", methods=["POST"])
    def editor_endpoint():
        data = request.get_json()
        result = Editor(
            db,
            "albums",
            data,
            doc_id=request.args.get("id"),
            data_fields=data_fields,
        ).process()
        return jsonify(result)

The ``doc_id`` parameter carries the document ID(s) for ``edit`` and
``remove`` operations; DataTables Editor appends them to the request URL as
``?id=<id>``.


Validators
==========

Pass a ``validators`` dict to enforce field-level rules before any write.
Each value is a callable that receives the submitted field value and returns
an error string, or ``None`` to pass.

.. code-block:: python

    Editor(
        db, "albums", data,
        doc_id=request.args.get("id"),
        data_fields=data_fields,
        validators={
            "year": lambda v: "Must be a valid year" if not (1900 < int(v) < 2100) else None,
            "title": lambda v: "Title is required" if not v.strip() else None,
        },
    ).process()

Validation errors are returned to Editor and displayed inline in the form
without saving any data.


Hooks
=====

Pre-action hooks run before each create, edit, or remove operation.  Return a
falsy value to cancel that row's operation (the row is added to a
``"cancelled"`` list in the response).

.. code-block:: python

    Editor(
        db, "albums", data,
        doc_id=request.args.get("id"),
        data_fields=data_fields,
        hooks={
            "pre_edit": lambda row_id, row: False if row.get("locked") else True,
            "pre_remove": lambda row_id, row: False,  # prevent all deletes
        },
    ).process()

Hook callables receive the document ID and the submitted row data dict.


Options
=======

Pass ``options`` to include a server-driven options dict in every Editor
response — useful for keeping select/radio/checkbox choices in sync with the
database without hardcoding them client-side.

.. code-block:: python

    Editor(
        db, "albums", data,
        doc_id=request.args.get("id"),
        data_fields=data_fields,
        options=lambda: {
            "genre": [{"label": g, "value": g} for g in db.albums.distinct("genre")],
        },
    ).process()

``options`` can be a plain dict or a zero-argument callable (called on each
request, so the values stay fresh).


Dependent Fields
================

Editor's ``dependent`` Ajax requests (used to update field options or values
when another field changes) are handled by ``dependent_handlers``:

.. code-block:: python

    def handle_artist_change(field, values, rows):
        artist = values.get("artist", "")
        albums = list(db.albums.find({"artist": artist}, {"title": 1}))
        return {"options": {"title": [{"label": a["title"], "value": str(a["_id"])} for a in albums]}}

    Editor(
        db, "albums", data,
        doc_id=request.args.get("id"),
        data_fields=data_fields,
        dependent_handlers={"artist": handle_artist_change},
    ).process()

The callable receives the triggering field name, the submitted values dict,
and the current rows list, and returns a dict with any of: ``options``,
``values``, ``messages``, ``errors``, ``labels``, ``show``, ``hide``,
``enable``, ``disable``.


Autocomplete and Tags Search
=============================

Editor's ``autocomplete`` and ``tags`` field types issue a separate Ajax
search request.  Route it to ``Editor.search()``:

.. code-block:: python

    @app.route("/api/editor/search", methods=["POST"])
    def editor_search():
        return jsonify(
            Editor(db, "albums", request.get_json(), data_fields=data_fields).search()
        )

The search action supports prefix search (``search`` param) and exact value
lookup (``values[]`` param) for resolving stored values back to display labels.


File Uploads
============

File uploads require a ``StorageAdapter`` subclass and the ``file_fields``
list:

.. code-block:: python

    from mongo_datatables import StorageAdapter

    class MyStorage(StorageAdapter):
        def store(self, field, filename, content_type, data):
            # save to S3, GridFS, filesystem, etc.
            return unique_file_id

        def retrieve(self, file_id):
            # return raw bytes
            ...

    Editor(
        db, "albums", data,
        doc_id=request.args.get("id"),
        data_fields=data_fields,
        file_fields=["cover_image"],
        storage_adapter=MyStorage(),
    ).process()
