API Reference
=============

Database Connections
--------------------

.. automodule:: dbtk.database
   :members:
   :undoc-members:
   :show-inheritance:

Cursors
-------

.. automodule:: dbtk.cursors
   :members:
   :undoc-members:
   :show-inheritance:

Configuration
-------------

.. automodule:: dbtk.config
   :members:
   :undoc-members:
   :show-inheritance:

Command Line Interface
----------------------

DBTK provides command-line tools for managing encryption keys and configuration files.

.. code-block:: bash

   # Generate encryption key
   dbtk generate-key

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

Logging Utilities
-----------------

Integration script logging with timestamped files and error tracking:

.. automodule:: dbtk.logging_utils
   :members:
   :undoc-members:
   :show-inheritance:

ETL
-----------

The Table class provides schema-aware table operations:

.. autoclass:: dbtk.etl.Table
   :members:
   :undoc-members:
   :show-inheritance:

The DataSurge class handles bulk operations:

.. autoclass:: dbtk.etl.DataSurge
   :members:
   :undoc-members:
   :show-inheritance:

Column definition generator:

.. autofunction:: dbtk.etl.column_defs_from_db

Readers
-------

.. automodule:: dbtk.readers
   :members:
   :undoc-members:
   :show-inheritance:

Writers
-------

.. automodule:: dbtk.writers
   :members:
   :undoc-members:
   :show-inheritance: