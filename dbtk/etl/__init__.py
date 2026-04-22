# dbtk/etl/__init__.py
"""
ETL (Extract, Transform, Load) operations and utilities.

This module provides tools for managing database tables, resolving identities,
and performing bulk operations:

- :class:`Table`: Schema-aware table operations with automatic SQL generation,
  field mapping, transformations, and per-operation error tracking via ``last_error``.
- :class:`DataSurge`: Row-oriented bulk INSERT, UPDATE, DELETE, and MERGE using
  ``executemany``.  Works with all drivers; supports ``db_expr`` columns.
- :class:`BulkSurge`: High-throughput bulk loading via native database mechanisms
  (PostgreSQL ``COPY``, Oracle direct-path, SQL Server ``bcp``, etc.).
  Memory-efficient streaming; does not support ``db_expr`` columns.
- :class:`IdentityManager`: Resumable source-to-target identity resolution with
  per-entity status, error, and message tracking.  State can be saved/loaded as JSON.
- :class:`ValidationCollector`: Callable collector for fn-pipelines that enriches
  and accumulates unique codes from source data.
- :class:`TableLookup`: Cached SQL-backed lookup transform for use in fn-pipelines
  and as a resolver for :class:`IdentityManager`.
- :func:`column_defs_from_db`: Generate ``Table`` column definitions by introspecting
  a live database table.

Example
-------
::

    from dbtk.etl import Table, DataSurge, BulkSurge, IdentityManager

    # Define table structure
    table = Table('users', columns={
        'id': {'field': 'id', 'key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'email': {'field': 'email', 'fn': 'email'}
    }, cursor=cursor)

    # Row-oriented operations (INSERT + UPDATE/MERGE in same run, or db_expr columns)
    surge = DataSurge(table)
    surge.upsert(new_records)      # INSERT new, UPDATE existing
    surge.delete(stale_records)

    # High-throughput load (no db_expr columns, INSERT only)
    bulk = BulkSurge(table)
    bulk.insert(records)           # Streams via COPY / direct-path / bcp

    # Identity resolution
    stmt = cursor.prepare_file('sql/resolve_user.sql')
    im = IdentityManager('source_id', 'user_id', resolver=stmt)
    for row in reader:
        entity = im.resolve(row)
"""

from .table import Table
from .data_surge import DataSurge
from .bulk_surge import BulkSurge
from .managers import IdentityManager, ValidationCollector, EntityStatus
from .transforms.database import TableLookup, QueryLookup
from .config_generators import column_defs_from_db

__all__ = ['Table', 'DataSurge', 'BulkSurge', 'IdentityManager', 'ValidationCollector',
           'EntityStatus', 'TableLookup', 'QueryLookup', 'column_defs_from_db']
