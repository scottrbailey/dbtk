.. image:: assets/databender.png
   :width: 320px
   :align: right
   :alt: DBTK - The Data Bender

DBTK - Data Benders ToolKit
=================================

Like the elemental benders of Avatar, this library gives you precise control over data, the world's most rapidly growing element.
Extract data from various sources, transform it through powerful operations, and load it exactly where it needs to go.
This library is designed by and for data integrators.

**Perfect for:**

* Data integration and ELT jobs
* Record-level transformations (cleaning, parsing, validation)
* Simple CRUD operations with multiple database types
* Moving data between systems

**Not designed for:**

* Heavy in-memory data transformations (use Pandas/polars)
* Complex aggregations (leverage your database's strengths)
* Data analysis and statistics

**Features:**

* Universal Database Connectivity - Unified interface across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite with intelligent driver auto-detection
* Portable SQL Queries - Write SQL once with named parameters, runs on any database regardless of parameter style
* Smart Cursors - All cursors and readers return Record objects with the speed and efficiency of tuples and the flexibility of dicts
* Flexible File Reading - CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, DataFrame and fixed-width text files with consistent API
* Transparent Compression - Automatic decompression of .gz, .bz2, .xz, and .zip files with smart member selection
* Multiple Export Formats - Write to CSV, Excel, JSON, NDJSON, XML, fixed-width text, or directly between databases
* Advanced ETL Framework - Full-featured Table class for complex data transformations, validations, and merging
* Data Transformations - Built-in functions for dates, phones, emails, and custom data cleaning with international support
* High-Performance Bulk Operations - DataSurge for blazing-fast batch operations; BulkSurge for even faster direct loading when supported
* Integrated Logging - Timestamped log files with automatic cleanup, split error logs, and zero-config setup
* Encrypted Configuration - YAML-based config with password encryption and environment variable support

**Speed and Memory** The primary objective of DBTK is to give data integrators an elegant toolkit to speed up your development.
But DBTK's throughput and memory usage are very good. BulkSurge streaming from a polars and doing direct loads to PostgreSQL will
process 1M rows in 3-4 seconds. But even with a standard Python csv reader and numerous column transforms, DataSurge is able to
write 1M rows to every supported database in 5-10 seconds.

Philosophy
----------

Modern databases excel at aggregating and transforming data at scale. DBTK embraces
this by focusing on what Python does well: flexible record-by-record transformations,
connecting disparate systems, and orchestrating data movement.

If you need to pivot, aggregate, or perform complex SQL operations - write SQL and let
your database handle it. If you need dataframes and heavy analytics - reach for Pandas
or polars. DBTK sits in between: getting your data where it needs to be, cleaned and
validated along the way.


Quick Start
-----------

.. code-block:: python

    import dbtk
    from dbtk.etl import Table
    from dbtk.etl.transforms import parse_date, email_validate

    # Read data, transform records, load to database
    with dbtk.connect('my_db') as db:
        cursor = db.cursor()
        table = Table('users', columns={
            'email': {'field': 'email', 'fn': email_validate},
            'signup_date': {'field': 'date', 'fn': parse_date}
        }, cursor=cursor)

        with dbtk.readers.CSVReader(open('users.csv')) as reader:
            for record in reader:
                table.set_values(record)
                # Use raise_error=False to track incomplete records
                table.execute('insert', raise_error=False)

        print(f"Inserted: {table.counts['insert']}")
        print(f"Skipped: {table.counts['incomplete']}")

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   01-getting-started
   02-configuration
   03-database-connections
   04-record
   05-readers
   06-writers
   07-table
   08-datasurge
   09-etl-tools
   10-advanced
   12-troubleshooting
   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`