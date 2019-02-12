================
mongo_datatables
================
A script for using the jQuery plug-in DataTables server-side processing (and DataTables Editor) with MongoDB.

Works with Flask and Django. Supports column sorting and filtering by multiple search terms and/or column specific
searches like column:keyword.

See an example of Django and mongo-datatables on `GitHub`_.

.. _GitHub: https://github.com/pauljolsen/django-and-mongo-datatables

See below for examples using Flask.

|Downloads|

.. |Downloads| image:: http://pepy.tech/badge/mongo-datatables
   :target: http://pepy.tech/project/mongo-datatables

----


Install
=======
You can install with pip::

    pip install mongo-datatables

..

Basic Usage (Flask)
===================

In your ``views.py``::

    import json
    from flask import request, render_template
    from mongo_datatables import DataTables
    from app import mongo
    from . import main


    @main.route('/table-view')
    def table_view():
        return render_template('main/table_view.html')


    @main.route('/mongo/<collection>')
    def api_db(collection):
        request_args = json.loads(request.values.get("args"))
        results = DataTables(mongo, collection, request_args).get_rows()
        return json.dumps(results)


..

In your ``table_view.html``::

    {% extends "base.html" %}


    {% block content %}
        {{ super() }}

        <div class="container">

            <h1>
                Contracts
            </h1>

            <table id="dt_table" class="table table-striped table-responsive">
                <thead>
                <tr>
                    <th>ExpiryDate</th>
                    <th>ContractId</th>
                    <th>Vendor</th>
                    <th>Note</th>
                </tr>
                </thead>
            </table>


        </div>
    {% endblock %}

    {% block scripts %}
        {{ super() }} // DataTables, jQuery, Bootstrap loaded here

        <script>
            $(function () {
                $('#dt_table').DataTable({
                    serverSide: true,
                    ajax: {
                        url: '{{ url_for('main.api_db', collection='contracts') }}',
                        dataSrc: 'data',
                        type: 'GET',
                        data: function (args) {
                            //args.qString = getQuerystring(); //add in querystring args, or anything else you want
                            return {
                                "args": JSON.stringify(args)
                            };
                        }
                    },
                    columns: [
                        {data: 'ExpiryDate'},
                        {data: 'ContractId'},
                        {data: 'Vendor'},
                        {data: 'Note'}
                    ]
                });

            });

            // in case you want to pass the querystring along with the request
            function getQuerystring() {
                var $qItems = $('#qItems');
                $qItems.empty();
                var hash;
                var filters = {};
                var q = document.URL.split('?')[1];
                if (q != undefined) {
                    q = q.split('&');
                    for (var i = 0; i < q.length; i++) {
                        hash = q[i].split('=');
                        filters[hash[0]] = hash[1];
                    }
                }
                return filters
            }
        </script>

    {% endblock %}

..

Advanced Usage, With A Custom Filter (Flask)
============================================

In your ``views.py``::

    import json
    from datetime import datetime, timedelta
    from mongo_datatables import Editor, DataTables
    from flask import request
    from app import mongo
    from . import main


    @main.route('/support-expiry', methods=['GET'])
    def support_expiry():
        """This examples receives a 'daysToExpiry' value and translates it to an Expiration Date, which can be looked
        up in the Mongo collection.
        """

        request_args = json.loads(request.values.get("args"))
        custom_filter = {}

        # translate daysToExpiry into a filter for the ExpiryDate Mongo key
        if 'daysToExpiry' in request_args['qString']:
            days_to_expiry = request_args['qString'].pop('daysToExpiry', None)  # remove daysToExpiry, leave the rest
            t = datetime.utcnow()
            ts = t.strftime("%Y-%m-%d")
            if days_to_expiry == 'Expired':
                custom_filter.update({
                    'ExpiryDate': {'$lt': ts, '$ne': ''}  # ExpiryDate is before today but not equal to ''
                })
            else:
                d = t + timedelta(days=int(days_to_expiry))
                ds = d.strftime("%Y-%m-%d")
                custom_filter.update({
                    'ExpiryDate': {'$gt': ts, '$lt': ds}  # ExpiryDate is between now and daysToExpiry from now
                })

        # add the rest of the query string to the custom filter
        custom_filter.update(request_args['qString'])

        collection = 'HardwareInventory'
        results = DataTables(mongo, collection, request_args, **custom_filter).get_rows()
        return json.dumps(results)

..


DataTables Editor Usage (Flask)
===============================

In your ``views.py``::

    import json
    from flask import request
    from mongo_datatables import DataTables, Editor
    from . import main
    from app import mongo

    # include the table_view and api_db views from above

    @main.route('/mongo/edit/<collection>/<doc_id>', methods=['POST'])
    def api_editor(collection, doc_id):
        request_args = json.loads(request.values.get("args"))
        results = Editor(mongo, collection, request_args, doc_id).update_rows()
        return json.dumps(results)

..

In your ``table-view.html``::

    {% extends "base.html" %}


    {% block content %}
        {{ super() }}

        <div class="container">

            <table id="dt_table" class="table table-striped table-responsive">
                <thead>
                <tr>
                    <th>ExpiryDate</th>
                    <th>ContractId</th>
                    <th>Vendor</th>
                    <th>Note</th>
                </tr>
                </thead>
            </table>


        </div>
    {% endblock %}

    {% block scripts %}
        {{ super() }}  // DataTables, Editor, jQuery, Bootstrap, Buttons loaded here

        <script>

            $(function () {

                // DataTables
                var table = $('#dt_table').DataTable({
                    serverSide: true,
                    ajax: {
                        url: '{{ url_for('main.api_db', collection='contracts') }}',
                        dataSrc: 'data',
                        type: 'GET',
                        data: function (args) {
                            return {
                                "args": JSON.stringify(args)
                            };
                        }
                    },
                    select: true,
                    columns: [
                        {data: 'ExpiryDate'},
                        {data: 'ContractId'},
                        {data: 'Vendor'},
                        {data: 'Note'}
                    ]
                });

                // Editor
                var editor = new $.fn.dataTable.Editor({
                    ajax: {
                        //Editor replaces _id_ with the row ID(s) (the Mongo _id(s))
                        url: '{{ url_for('main.api_editor', collection='contracts', doc_id='_id_') }}',
                        type: 'POST',
                        data: function (args) {
                            return {
                                "args": JSON.stringify(args)
                            };
                        }
                    },
                    table: "#dt_table",
                    fields: [
                        {name: 'ExpiryDate', value: 'Expiry Date'},
                        {name: 'ContractId', value: 'Contract ID'},
                        {name: 'Vendor', value: 'Vendor'},
                        {name: 'Note', value: 'Note'}
                    ]
                });

                // Buttons
                new $.fn.dataTable.Buttons(table, [
                    {extend: "create", editor: editor},
                    {extend: "edit", editor: editor},
                    {extend: "remove", editor: editor}
                ]);

                table.buttons().container()
                        .appendTo($(table.table().container(), '.col-sm-6:eq(0)'));

            });
        </script>

    {% endblock %}

