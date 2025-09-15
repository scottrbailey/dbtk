# dbtk/writers/utils.py

import datetime as dt

from typing import Any, Iterator, List, Tuple

from ..database import ParamStyle
from ..utils import wrap_at_comma

MIDNIGHT = dt.time(0, 0, 0)


def get_data_iterator(data) -> Tuple[Iterator, List[str]] :
    """
    Determines the data iterator and associated column labels based on the nature of the provided data.

    Args:
        data: Input data which can be in various forms like a database cursor or list of dicts, namedtuples or records.

    Returns:
        Tuple containing:
        - Iterator: An iterator over the provided data. Returns None if the data is invalid or unrecognized.
        - List[str]: A list of column names associated with the data. Returns None if columns cannot be determined.
    """
    if not data:
        return None, None
    elif hasattr(data, 'fetchall'):  # Cursor
        if hasattr(data, 'columns'):
            columns = data.columns()
        elif hasattr(data, 'description'):
            columns = [col[0] for col in data.description]
        else:
            columns = list()
        return data, columns  # Use cursor directly as iterator
    elif isinstance(data, (list, tuple)):
        # Determine columns and return iterator
        if hasattr(data[0], 'keys'):
            # dict and Record
            columns = list(data[0].keys())
        elif hasattr(data[0], '_fields'):
            columns = list(data[0]._fields)
        else:
            # simple list or tuple
            columns = [f'col_{x:03d}' for x in range(1, len(data[0]) + 1)]
        return iter(data), columns
    return None, None


def format_value(obj: Any) -> str:
    """
    Convert a database value to string representation.

    Args:
        obj: Value to format

    Returns:
        String representation
    """
    if obj is None:
        return ''
    elif isinstance(obj, dt.datetime):
        if obj.microsecond:
            if obj.tzinfo:
                return obj.strftime('%Y-%m-%d %H:%M:%S.%f %z')
            else:
                return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            if obj.tzinfo:
                return obj.strftime('%Y-%m-%d %H:%M:%S %z')
            if obj.time() == MIDNIGHT:
                return obj.strftime('%Y-%m-%d')
            else:
                return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, dt.date):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, dt.time):
        if obj.microsecond:
            if obj.tzinfo:
                return obj.strftime('%H:%M:%S.%f %z')
            else:
                return obj.strftime('%H:%M:%S.%f')
        else:
            if obj.tzinfo:
                return obj.strftime('%H:%M:%S %z')
            else:
                return obj.strftime('%H:%M:%S')
    elif isinstance(obj, (int, float)):
        return str(obj)
    elif isinstance(obj, str):
        return obj
    elif hasattr(obj, 'read'):
        # Handle LOB objects
        return str(obj.read())
    else:
        return str(obj)


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