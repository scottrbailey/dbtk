API Reference
=============

Configuration
-------------

.. automodule:: dbtk.config
   :members:
   :undoc-members:
   :show-inheritance:

Database Connections
--------------------

.. automodule:: dbtk.database
   :members:
   :undoc-members:
   :show-inheritance:

Database Dialects
------------------

Per-database SQL generation used internally by ``Table``, ``DataSurge``, and
``BulkSurge`` (upsert/merge syntax, type mapping, etc.). Subclass
``DatabaseDialect`` to add support for a new database engine.

.. automodule:: dbtk.dialects.base
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.dialects.postgres
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.dialects.oracle
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.dialects.mysql
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.dialects.sqlserver
   :members:
   :undoc-members:
   :show-inheritance:

Cursors
-------

.. automodule:: dbtk.cursors
   :members:
   :undoc-members:
   :show-inheritance:

Record
------

.. automodule:: dbtk.record
   :members:
   :undoc-members:
   :show-inheritance:

Readers
-------

.. automodule:: dbtk.readers.utils
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.base
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.csv
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.excel
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.fixed_width
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.json
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.xml
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.readers.data_frame
   :members:
   :undoc-members:
   :show-inheritance:

Writers
-------

.. automodule:: dbtk.writers.base
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.writers.utils
   :members: select_columns
   :show-inheritance:

.. automodule:: dbtk.writers.csv
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.writers.database
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.writers.excel
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.writers.fixed_width
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.writers.json
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.writers.xml
   :members:
   :undoc-members:
   :show-inheritance:

Utilities
---------

.. automodule:: dbtk.utils
   :members: ErrorDetail, FixedColumn, ParamStyle, process_sql_parameters
   :undoc-members:
   :show-inheritance:

ETL
-----------

.. automodule:: dbtk.etl.table
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.base_surge
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.data_surge
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.bulk_surge
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.config_generators
   :members:
   :undoc-members:
   :show-inheritance:

ETL Managers
------------

.. automodule:: dbtk.etl.managers
   :members:
   :undoc-members:
   :show-inheritance:

ETL Transforms
--------------

Located across ``dbtk.etl.transforms.*``. ``dbtk.etl.transforms`` (the package)
re-exports a curated subset for convenience (``from dbtk.etl.transforms import
get_int, coalesce, ...``); the sections below document each submodule's full
surface, including members only reachable via a direct submodule import
(e.g. ``from dbtk.etl.transforms.phone import Phone, PhoneFormat``).

.. automodule:: dbtk.etl.transforms.core
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.transforms.phone
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.transforms.email
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.transforms.address
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.transforms.datetime
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: dbtk.etl.transforms.database
   :members:
   :undoc-members:
   :show-inheritance:

Logging Utilities
-----------------

Integration script logging with timestamped files and error tracking:

.. automodule:: dbtk.logging_utils
   :members:
   :undoc-members:
   :show-inheritance:

Command Line Interface
----------------------

DBTK provides command-line tools for managing encryption keys and configuration files.

.. code-block:: bash

   # Check dependencies, drivers, and configuration
   dbtk checkup

   # Interactive configuration setup wizard
   dbtk config-setup

   # Generate encryption key
   dbtk generate-key

   # Store encryption key in system keyring
   dbtk store-key [key] [--force]

   # Encrypt passwords in config file
   dbtk encrypt-config [config_file]

   # Encrypt a single password
   dbtk encrypt-password [password]

   # Migrate config to new encryption key
   dbtk migrate-config old_file new_file [--new-key KEY]

The CLI is implemented in ``dbtk.cli`` module:

.. automodule:: dbtk.cli
   :members:
   :undoc-members:
   :show-inheritance:

Formats
-------

.. automodule:: dbtk.formats.edi
   :members:
   :undoc-members:
   :show-inheritance:
