.. image:: assets/databender.png
   :width: 320px
   :align: right
   :alt: dbtk - The Data Bender

dbtk - Data Benders ToolKit
=================================

The dbtk library is a lightweight database toolkit for ETL and data integration, designed to get
data to and from your databases with minimal hassle.

**Perfect for:**

* Data integration and ELT jobs
* Record-level transformations (cleaning, parsing, validation)
* Simple CRUD operations with multiple database types
* Moving data between systems

**Not designed for:**

* Heavy in-memory data transformations (use Pandas/polars)
* Complex aggregations (leverage your database's strengths)
* Data analysis and statistics

Philosophy
----------

Modern databases excel at aggregating and transforming data at scale. dbtk embraces
this by focusing on what Python does well: flexible record-by-record transformations,
connecting disparate systems, and orchestrating data movement.

If you need to pivot, aggregate, or perform complex SQL operations - write SQL and let
your database handle it. If you need dataframes and heavy analytics - reach for Pandas
or polars. dbtk sits in between: getting your data where it needs to be, cleaned and
validated along the way.

Quick Start
-----------

.. code-block:: python

    import dbtk
    from dbtk.etl.transforms import parse_date, email_validate

    # Read data, transform records, load to database
    with dbtk.connect('my_db') as db:
        table = Table('users', columns={
            'email': {'field': 'email', 'fn': email_validate},
            'signup_date': {'field': 'date', 'fn': parse_date}
        }, cursor=db.cursor())

        for record in dbtk.readers.CSVReader('users.csv'):
            table.set_values(record)
            table.exec_insert()

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`