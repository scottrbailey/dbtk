# dbtk/writers/utils.py

"""Internal utilities for writer implementations."""

import datetime as dt

from typing import Any, Iterator, List, Tuple

from ..database import ParamStyle
from ..utils import wrap_at_comma

MIDNIGHT = dt.time(0, 0, 0)

def create_insert_statement(table: str, columns: List[str], paramstyle: str = ParamStyle.NAMED) -> str:
    """
    Create an INSERT statement for the given columns and table.

    Args:
        columns: List of column names
        table: Table name
        paramstyle: Parameter style ('qmark', 'numeric', 'named', 'format', 'pyformat')

    Returns:
        INSERT statement string
    """

    if paramstyle == ParamStyle.QMARK:
        params = ', '.join(['?' for _ in columns])
    elif paramstyle == ParamStyle.FORMAT:
        params = ', '.join(['%s' for _ in columns])
    elif paramstyle == ParamStyle.NUMERIC:
        params = ', '.join([f':{i}' for i in range(1, len(columns) + 1)])
    elif paramstyle == 'named':
        params = ', '.join([f':{col}' for col in columns])
    elif paramstyle == 'pyformat':
        params = ', '.join([f'%({col})s' for col in columns])
    else:
        raise ValueError(f"Unsupported paramstyle: {paramstyle}")
    column_list = wrap_at_comma(', '.join(columns))
    params = wrap_at_comma(params)
    return f'INSERT INTO {table} ({column_list}) VALUES ({params})'