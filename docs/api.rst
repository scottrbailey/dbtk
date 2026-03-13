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

ETL
-----------

.. automodule:: dbtk.etl
   :members:
   :undoc-members:
   :show-inheritance:

ETL Transforms
--------------

.. automodule:: dbtk.etl.transforms
   :members:
   :undoc-members:
   :show-inheritance:

Utilities
---------

.. automodule:: dbtk.utils
   :members: ErrorDetail, FixedColumn, ParamStyle
   :undoc-members:
   :show-inheritance:

Logging Utilities
-----------------

Integration script logging with timestamped files and error tracking:

.. automodule:: dbtk.logging_utils
   :members:
   :undoc-members:
   :exclude-members: TableLookup
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
