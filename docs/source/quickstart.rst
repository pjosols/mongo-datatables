==========
Quickstart
==========

This guide will help you quickly integrate mongo-datatables with your web application.

Installation
============

Install the package using pip:

.. code-block:: bash

    pip install mongo-datatables

Performance Optimization
========================

Creating Indexes for Large Collections
--------------------------------------

Before setting up your application, it's important to create appropriate indexes in your MongoDB collections, especially if you're working with large datasets. This step is **critical** for ensuring good performance with DataTables server-side processing.

.. code-block:: python

    # Create a text index for efficient text search
    db.users.create_index([("name", "text"), ("email", "text")])
    
    # Create regular indexes for fields used in sorting and filtering
    db.users.create_index("created_at")
    db.users.create_index("status")

.. note::
   Without proper indexes, queries on large collections can become extremely slow or timeout entirely.

Basic Setup with Flask
======================

Create a basic Flask application with MongoDB integration:

.. code-block:: python

    from flask import Flask, render_template, request, jsonify
    from flask_pymongo import PyMongo
    from mongo_datatables import DataTables

    app = Flask(__name__)
    app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
    mongo = PyMongo(app)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        results = DataTables(mongo, 'users', data).get_rows()
        
        # Optional: Access query statistics for performance monitoring
        # query_stats = results['_query_stats']
        # app.logger.info(f"Search using text index: {query_stats['used_text_index']}")
        
        return jsonify(results)

HTML Template
=============

Create a template that includes jQuery, DataTables, and the necessary JavaScript setup:

.. code-block:: html

    <!DOCTYPE html>
    <html>
    <head>
        <title>DataTables Example</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
    </head>
    <body>
        <table id="example" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Email</th>
                    <th>Age</th>
                </tr>
            </thead>
        </table>

        <script>
            $(document).ready(function() {
                $('#example').DataTable({
                    processing: true,
                    serverSide: true,
                    ajax: {
                        url: '/api/data',
                        type: 'POST',
                        contentType: 'application/json',
                        data: function(d) {
                            return JSON.stringify(d);
                        }
                    },
                    columns: [
                        { data: 'name' },
                        { data: 'email' },
                        { data: 'age' }
                    ]
                });
            });
        </script>
    </body>
    </html>

Adding DataTables Editor
========================

To add DataTables Editor for CRUD operations:

1. Purchase and include the Editor library:

   .. code-block:: html

       <!-- DataTables Editor CSS -->
       <link rel="stylesheet" type="text/css" href="editor/css/editor.dataTables.min.css">
       
       <!-- DataTables Editor JS -->
       <script src="editor/js/dataTables.editor.min.js"></script>

2. Create the Editor endpoint in Flask:

   .. code-block:: python

       @app.route('/api/editor', methods=['POST'])
       def editor_endpoint():
           data = request.get_json()
           doc_id = request.args.get('id', '')
           result = Editor(mongo, 'users', data, doc_id).process()
           return jsonify(result)

3. Initialize Editor in your JavaScript:

   .. code-block:: javascript

       $(document).ready(function() {
           var editor = new $.fn.dataTable.Editor({
               ajax: {
                   url: '/api/editor',
                   type: 'POST',
                   contentType: 'application/json',
                   data: function(d) {
                       return JSON.stringify(d);
                   }
               },
               table: '#example',
               fields: [
                   { label: 'Name', name: 'name' },
                   { label: 'Email', name: 'email' },
                   { label: 'Age', name: 'age', type: 'number' }
               ]
           });

           $('#example').DataTable({
               dom: 'Bfrtip',
               processing: true,
               serverSide: true,
               ajax: {
                   url: '/api/data',
                   type: 'POST',
                   contentType: 'application/json',
                   data: function(d) {
                       return JSON.stringify(d);
                   }
               },
               columns: [
                   { data: 'name' },
                   { data: 'email' },
                   { data: 'age' }
               ],
               select: true,
               buttons: [
                   { extend: 'create', editor: editor },
                   { extend: 'edit', editor: editor },
                   { extend: 'remove', editor: editor }
               ]
           });
       });
