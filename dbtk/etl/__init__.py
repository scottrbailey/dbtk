# dbtk/etl/__init__.py
"""
ETL (Extract, Transform, Load) operations and utilities.

This module provides tools for managing database tables and performing bulk operations:

- Table: Schema-aware table operations with SQL generation
- DataSurge: High-performance bulk INSERT, UPDATE, DELETE, and MERGE operations
- generate_table_config: Generate table configuration from database schema

Example
-------
::

    from dbtk.etl import Table, DataSurge

    # Define table structure
    table = Table('users', columns={
        'id': {'type': 'int', 'key': True},
        'name': {'type': 'str'},
        'email': {'type': 'str'}
    }, cursor=cursor)

    # Bulk operations
    surge = DataSurge(cursor, table, batch_size=500)
    surge.insert(records)
    surge.merge(records)
"""

from .table import Table
from .data_surge import DataSurge
from .bulk_surge import BulkSurge
from .config_generators import column_defs_from_db

__all__ = ['Table', 'DataSurge', 'BulkSurge', 'column_defs_from_db']
