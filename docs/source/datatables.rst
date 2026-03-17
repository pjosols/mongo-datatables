==========
DataTables
==========

The ``DataTables`` class translates a DataTables server-side Ajax request into
MongoDB aggregation pipelines and returns the standard DataTables JSON response.
It handles pagination, sorting, all search modes, SearchPanes, SearchBuilder,
and named fixed searches automatically.

.. autoclass:: mongo_datatables.datatables.DataField
   :members:
   :undoc-members:

.. autoclass:: mongo_datatables.datatables.DataTables
   :members:
   :undoc-members:
   :show-inheritance:


DataField
=========

``DataField`` maps a MongoDB document field to a DataTables column.  You need
it whenever your field names differ from your column names, you have nested
fields, or you want type-aware search (numeric comparisons, date ranges).

.. code-block:: python

    from mongo_datatables import DataTables, DataField

    fields = [
        DataField("title", "string"),
        DataField("PublisherInfo.Date", "date", alias="published"),
        DataField("pages", "number"),
        DataField("_id", "objectid"),
    ]

    dt = DataTables(db, "books", request.get_json(), data_fields=fields)

The ``alias`` must match the ``data`` name in your DataTables column
definition.  It defaults to the last segment of the field path, so
``PublisherInfo.Date`` becomes ``Date`` unless you set an alias.

Valid types: ``string``, ``number``, ``date``, ``boolean``, ``array``,
``object``, ``objectid``, ``null``.  Types ``number`` and ``date`` unlock
comparison operators in search; ``objectid`` values are serialised as strings
in the response.


Search
======

Search Types and Performance
-----------------------------

The search mode used depends on the request flags and whether a MongoDB text
index exists on the collection.

.. list-table::
   :header-rows: 1
   :widths: 15 30 20 35

   * - Type
     - Example
     - Performance (large collection)
     - Notes
   * - Text index
     - ``radiohead``
     - Fast (100–300 ms)
     - Requires a text index; whole-word, OR semantics
   * - Phrase
     - ``"ok computer"``
     - Fast (100–300 ms)
     - Exact phrase; uses ``$text`` phrase or word-boundary regex
   * - Smart / AND
     - ``radiohead 1997``
     - Fast with index
     - Each word must match somewhere in the row (DataTables default)
   * - Field-specific
     - ``artist:Yorke``
     - Moderate (1–2 s)
     - Colon syntax; targets one field
   * - Comparison
     - ``year:>1994``
     - Fast (200–500 ms)
     - ``>``, ``>=``, ``<``, ``<=``, ``=`` on number/date fields
   * - Regex
     - ``^ok``
     - Slow without index (5–10 s+)
     - Raw MongoDB regex; bypasses text index
   * - Fallback regex
     - ``radiohead`` (no text index)
     - Slow (5–10 s+)
     - Per-column ``$regex`` when no text index exists

*Timings based on collections > 2 M documents.*

Global Search
-------------

The DataTables search box drives the global search.  Multi-word terms use AND
semantics by default (``search[smart]=true``): each word must independently
match at least one searchable column.

Wrap a term in double quotes for exact phrase matching::

    "ok computer"

Colon Syntax
------------

Target a specific field from the global search box::

    artist:Yorke                  # field contains value
    artist:"Thom Yorke"           # exact phrase in field
    year:1997                     # equality (number/date fields)
    year:>1994                    # greater than
    year:>=1994 year:<2003        # multiple conditions, all ANDed
    release_date:>2020-01-01      # ISO date comparison

Column Search with Ranges
--------------------------

Per-column search inputs accept a pipe-delimited ``min|max`` for inclusive
range queries on ``number`` and ``date`` fields::

    1990|2000
    2020-01-01|2020-12-31

Search Flags
------------

The standard DataTables search flags are all respected:

- ``search[smart]`` (default ``true``) — AND semantics for multi-word global search.
- ``search[regex]`` (default ``false``) — treat search value as a raw MongoDB regex.
- ``search[caseInsensitive]`` (default ``true``) — case-insensitive matching.
  Per-column override via ``columns[i][search][caseInsensitive]``.

.. note::
   When ``regex=true`` or ``caseInsensitive=false``, the text index is bypassed
   and a ``$regex`` pipeline is used regardless of ``use_text_index``.


SearchPanes
===========

SearchPanes option counts are computed automatically.  Call
``get_searchpanes_options()`` from a dedicated endpoint to populate panes on
page load; the method is also called automatically inside ``get_rows()`` when
``searchPanes`` is present in the request.

.. code-block:: python

    @app.route("/searchpanes", methods=["POST"])
    def searchpanes():
        dt = DataTables(db, "albums", request.get_json(), data_fields)
        return jsonify(dt.get_searchpanes_options())

Each pane value gets a ``total`` count (unfiltered) and a ``count`` (after
the current search is applied), satisfying the full SearchPanes server-side
protocol.


SearchBuilder
=============

Full server-side SearchBuilder support is built in — nested AND/OR criteria
trees with ``string``, ``number``, ``date``, ``html-num``, and
``html-num-fmt`` column types are all handled.  No extra configuration is
required; ``get_rows()`` applies SearchBuilder criteria automatically when
present in the request.


Named Fixed Searches (``search.fixed``)
========================================

DataTables 2.x ``search.fixed`` named searches are applied as additional AND
conditions.  Both the DataTables 2.x wire format (``search.fixed`` array of
``{name, term}`` objects) and the legacy ``searchFixed`` top-level dict are
supported.  Per-column fixed searches via ``columns[i].search.fixed`` are
also handled.


Performance & Indexes
=====================

Indexes are critical for large collections — every ``get_rows()`` call runs
an aggregation pipeline.

Text index
----------

Create one text index covering all searchable fields:

.. code-block:: python

    db.albums.create_index([
        ("title", "text"),
        ("artist", "text"),
        ("genre", "text"),
    ])

With a text index, global search runs in ~100–300 ms on multi-million-row
collections.  Without one, the fallback regex scan can take 5–10+ seconds.

.. note::
   MongoDB allows only one text index per collection, but it can span any
   number of fields.

To disable text index use and always use regex (for substring matching):

.. code-block:: python

    DataTables(db, "albums", args, data_fields, use_text_index=False)

Regular indexes
---------------

Create indexes for fields used in sorting, column search, or custom filters:

.. code-block:: python

    db.albums.create_index("year")
    db.albums.create_index("artist")
    db.albums.create_index([("artist", 1), ("year", -1)])

Large-dataset options
---------------------

For complex SearchBuilder or SearchPanes queries on large collections that
exceed MongoDB's 100 MB in-memory aggregation limit:

.. code-block:: python

    DataTables(db, "albums", args, data_fields, allow_disk_use=True)
