============
Installation
============

Requirements
============

The mongo-datatables package requires:

* Python 3.6+
* pymongo 3.6+
* A MongoDB database (version 3.6+ recommended)

Installation Methods
====================

Using pip
---------

The recommended way to install mongo-datatables is using pip:

.. code-block:: bash

    pip install mongo-datatables

This will install the latest stable version from PyPI.

From Source
-----------

You can also install directly from the source code:

.. code-block:: bash

    git clone https://github.com/pjosols/mongo-datatables.git
    cd mongo-datatables
    pip install -e .

Verifying Installation
======================

To verify that mongo-datatables is installed correctly, you can import it in Python:

.. code-block:: python

    >>> from mongo_datatables import DataTables
    >>> # No errors means successful installation
