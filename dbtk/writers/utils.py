# dbtk/writers/utils.py

"""Internal utilities for writer implementations."""

import datetime as dt
import operator

from typing import Any, Iterable, Iterator, List, Optional, Tuple

from ..database import ParamStyle
from ..record import Record
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


def _resolve_output_columns(source_columns: List[str], col_names: List[str], action: str) -> List[str]:
    missing = [c for c in col_names if c not in source_columns]
    if missing:
        raise ValueError(f"Column(s) not found in source data: {missing}")
    if action == 'include':
        return list(col_names)
    output_columns = [c for c in source_columns if c not in set(col_names)]
    if not output_columns:
        raise ValueError("action='exclude' would remove every column")
    return output_columns


def _make_getter(output_columns: List[str], source_columns: Optional[List[str]]):
    """
    Build a callable that pulls `output_columns` worth of values from a row, in order.

    When `source_columns` is None the row supports string-key access (dict/Record) and
    values are pulled by name directly. Otherwise the row is positional (namedtuple,
    list, tuple) so column names are translated to source indices up front.
    """
    items = output_columns if source_columns is None else [source_columns.index(c) for c in output_columns]
    if len(items) == 1:
        key = items[0]
        return lambda row: (row[key],)
    return operator.itemgetter(*items)


def _select_columns_gen(rows: Iterator[Any], col_names: List[str], action: str,
                         source_columns: Optional[List[str]]) -> Iterator[Record]:
    if source_columns is not None:
        src_cols = list(source_columns)
        by_name = False
        first = None
    else:
        try:
            first = next(rows)
        except StopIteration:
            return
        if hasattr(first, 'keys'):
            src_cols = list(first.keys())
            by_name = True
        elif hasattr(first, '_fields'):
            src_cols = list(first._fields)
            by_name = False
        else:
            raise TypeError(
                f"select_columns() can't determine column names from {type(first).__name__} rows; "
                "pass source_columns=[...] explicitly for plain list/tuple data."
            )

    output_columns = _resolve_output_columns(src_cols, col_names, action)
    output_record_cls = type('SelectedRecord', (Record,), {})
    output_record_cls.set_fields(output_columns)
    get = _make_getter(output_columns, None if by_name else src_cols)

    if first is not None:
        yield output_record_cls(*get(first))
    for row in rows:
        yield output_record_cls(*get(row))


def select_columns(rows: Iterable[Any], col_names: List[str], action: str = 'include',
                    source_columns: Optional[List[str]] = None) -> Iterator[Record]:
    """
    Project a stream of rows down to a subset of columns, yielding Record objects.

    Works lazily on any iterable of dict-like rows (dict, Record, namedtuple) without
    materializing the source. For plain list/tuple rows, which carry no column names
    of their own, pass `source_columns` explicitly.

    Args:
        rows: Source rows - dicts, Records, namedtuples, cursors, or (with
            source_columns given) plain lists/tuples.
        col_names: Column names to keep (action='include', also sets output order)
            or drop (action='exclude', source order is preserved).
        action: 'include' to treat col_names as an allow-list, 'exclude' to treat
            it as a block-list. Default 'include'.
        source_columns: Column names for positional (list/tuple) row data, in row
            order. Required for that case; ignored/unnecessary for self-describing
            rows, which infer their own columns from the first row.

    Yields:
        Record: one Record per input row, holding only the selected columns.

    Raises:
        ValueError: If action is invalid, col_names is empty, a requested column
            isn't present in the source, or action='exclude' would remove every
            column.
        TypeError: If source_columns isn't given and column names can't be
            determined from the row type (e.g. plain list/tuple rows).

    Examples:
        # Drop a couple of columns from cursor results before writing
        to_csv(select_columns(cursor, ['ssn', 'password'], action='exclude'), 'users.csv')

        # Reorder/select a subset for a report
        to_excel(select_columns(records, ['name', 'email', 'signup_date']), 'report.xlsx')

        # Plain list-of-lists data needs its source column names supplied
        select_columns(rows, ['id', 'name'], source_columns=['id', 'name', 'internal_flag'])
    """
    if action not in ('include', 'exclude'):
        raise ValueError(f"action must be 'include' or 'exclude', got {action!r}")
    if not col_names:
        raise ValueError("col_names must not be empty")

    return _select_columns_gen(iter(rows), list(col_names), action, source_columns)