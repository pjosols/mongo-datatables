==========
Exceptions
==========

All exceptions raised by mongo-datatables inherit from ``MongoDataTablesError``,
so you can catch any library error with a single ``except`` clause:

.. code-block:: python

    from mongo_datatables import MongoDataTablesError

    try:
        result = Editor(db, "albums", data, doc_id).process()
    except MongoDataTablesError as e:
        # handle any library error
        ...

Or catch specific exceptions for finer-grained handling:

.. code-block:: python

    from mongo_datatables import InvalidDataError, DatabaseOperationError

    try:
        result = Editor(db, "albums", data, doc_id).process()
    except InvalidDataError as e:
        return {"error": str(e)}, 400
    except DatabaseOperationError as e:
        return {"error": str(e)}, 503

.. autoclass:: mongo_datatables.exceptions.MongoDataTablesError
   :members:
   :show-inheritance:

.. autoclass:: mongo_datatables.exceptions.InvalidDataError
   :members:
   :show-inheritance:

.. autoclass:: mongo_datatables.exceptions.DatabaseOperationError
   :members:
   :show-inheritance:

.. autoclass:: mongo_datatables.exceptions.FieldMappingError
   :members:
   :show-inheritance:

.. autoclass:: mongo_datatables.exceptions.QueryBuildError
   :members:
   :show-inheritance:
