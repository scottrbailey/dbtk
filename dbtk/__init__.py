# dbtk/__init__.py
"""
DBTK - Data Benders ToolKit

A lightweight database integration / ETL toolkit that provides:
- Configuration: encrypted YAML, named connections, env vars, driver overrides, smart logging
- Connection: consistent connection and parameter handling, clean reference hierarchy
- Record: ergonomic row handling, memory-efficient at scale
- Table: field mapping, built-in and extensible transforms, robust transform shorthand, validation, CRUD + merge/upsert
- DataSurge/BulkSurge: efficient batched processing with logging and stats using either SQL or direct loads
- Readers/Writers: consistent API across every supported file format and compression type


Basic usage::

    import dbtk

    # From YAML config file
    with dbtk.connect('prod_warehouse') as db:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM users")

        # Export results
        dbtk.writers.to_excel(cursor, 'report.xlsx')

Direct connections:
    db = dbtk.database.postgres(user='user', password='pass', database='db')
    cursor = db.cursor()  # Returns Record objects
"""

__version__ = '0.8.6'
__author__ = 'Scott Bailey <scottrbailey@gmail.com>'

from .database import Database
from .config import connect, set_config_file
from .cursors import Cursor, PreparedStatement
from .logging_utils import setup_logging, cleanup_old_logs, errors_logged
from .record import fixed_record_factory, FixedWidthRecord
from .utils import FixedColumn
from . import readers
from . import writers
from . import etl

# Simple, clean exports
__all__ = [
    'connect',
    'Database',
    'Cursor',
    'PreparedStatement',
    'FixedColumn',
    'FixedWidthRecord',
    'fixed_record_factory',
    'etl',
    'readers',
    'writers',
    'setup_logging',
    'cleanup_old_logs',
    'errors_logged'
]