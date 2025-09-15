# dbtk/__init__.py
"""
dbtk - Database ToolKit - for ETL and data integration

A lightweight database toolkit that provides:
- Uniform interface across different databases (PostgreSQL, Oracle, MySQL, SQL Server, SQLite)
- Flexible cursor types returning different data structures
- YAML-based configuration with password encryption
- Writers for CSV, Excel, fixed-width, and database-to-database export
- Context managers for connections and transactions

Basic usage:
    import dbtk

    # From YAML config file
    with dbtk.connect('prod_warehouse') as db:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM users")

        # Export results
        dbtk.writers.to_csv(cursor, 'users.csv')
        dbtk.writers.to_excel(cursor, 'report.xlsx')

Direct connections:
    from dbtk.database import postgres, oracle

    db = postgres(user='user', password='pass', database='db')
    cursor = db.cursor('tuple')  # namedtuple results
"""

__version__ = '1.0.0'
__author__ = 'Scott Bailey <scottrbailey@gmail.com>'

from .database import Database
from .config import connect, set_config_file
from .cursors import Cursor, RecordCursor, TupleCursor, DictCursor
from . import readers
from . import writers

# Simple, clean exports
__all__ = [
    'connect',
    'config',
    'Database',
    'Cursor',
    'RecordCursor',
    'TupleCursor',
    'DictCursor',
    'etl',
    'readers',
    'writers'
]