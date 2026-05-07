# dbtk/dialects/__init__.py
from .base import DatabaseDialect
from .postgres import PostgresDialect
from .oracle import OracleDialect
from .mysql import MySQLDialect
from .sqlserver import SQLServerDialect

_DIALECT_MAP = {
    'postgres': PostgresDialect,
    'oracle': OracleDialect,
    'mysql': MySQLDialect,
    'sqlserver': SQLServerDialect,
    'sqlite': DatabaseDialect,  # base class IS the SQLite dialect
}


def get_dialect(database_type: str) -> DatabaseDialect:
    cls = _DIALECT_MAP.get(database_type, DatabaseDialect)
    return cls()
