# dbtk/writers/utils.py

"""Internal utilities for writer implementations."""

import datetime as dt

from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ..database import ParamStyle
from ..record import Record, _make_getter
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


def _peek_columns(rows: Iterator[Any]) -> Tuple[Optional[Any], Optional[List[str]], bool]:
    """
    Peek at the first row of a self-describing row stream to determine its columns.

    Returns (first_row, source_columns, by_name). by_name is True for dict/Record
    rows (string-key __getitem__ works directly) and False for namedtuples (which
    only support positional/attribute access, so names must be translated to
    indices before building a getter — see `_getter_keys`).

    Returns (None, None, False) for an empty stream. Raises TypeError if the row
    type is self-describing (e.g. plain tuples/lists) — use `tuples_to_records()`
    first to attach column names to positional data.
    """
    try:
        first = next(rows)
    except StopIteration:
        return None, None, False
    if hasattr(first, 'keys'):
        return first, list(first.keys()), True
    if hasattr(first, '_fields'):
        return first, list(first._fields), False
    raise TypeError(
        f"can't determine column names from {type(first).__name__} rows; "
        "use tuples_to_records() first to attach column names to positional data."
    )


def _getter_keys(names: List[str], by_name: bool, source_columns: List[str]) -> List[Any]:
    """Translate `names` into whatever `_make_getter` needs: the names themselves
    for by_name rows (dict/Record), or their positional index for namedtuples."""
    return names if by_name else [source_columns.index(c) for c in names]


def select_columns(rows: Iterable[Any], col_names: List[str]) -> Iterator[Record]:
    """
    Select and reorder columns from a stream of self-describing rows.

    `col_names` is an allow-list that also sets the output column order. Works
    lazily on any iterable of dict-like rows (dict, Record, namedtuple, cursor)
    without materializing the source. For plain list/tuple rows, which carry no
    column names of their own, use `tuples_to_records()` first.

    Args:
        rows: Source rows - dicts, Records, namedtuples, or cursors.
        col_names: Column names to keep, in the desired output order.

    Yields:
        Record: one Record per input row, holding only the selected columns.

    Raises:
        ValueError: If col_names is empty, or a requested column isn't present
            in the source.
        TypeError: If column names can't be determined from the row type.

    Examples:
        # Select and reorder a subset for a report
        to_excel(select_columns(records, ['name', 'email', 'signup_date']), 'report.xlsx')
    """
    if not col_names:
        raise ValueError("col_names must not be empty")
    return _select_columns_gen(iter(rows), list(col_names))


def _select_columns_gen(rows: Iterator[Any], col_names: List[str]) -> Iterator[Record]:
    first, src_cols, by_name = _peek_columns(rows)
    if first is None:
        return

    missing = [c for c in col_names if c not in src_cols]
    if missing:
        raise ValueError(f"Column(s) not found in source data: {missing}")

    output_cls = type('SelectedRecord', (Record,), {})
    output_cls.set_fields(col_names)
    get = _make_getter(_getter_keys(col_names, by_name, src_cols))

    yield output_cls(*get(first))
    for row in rows:
        yield output_cls(*get(row))


def exclude_columns(rows: Iterable[Any], col_names: Iterable[str], ignore_missing: bool = False) -> Iterator[Record]:
    """
    Drop columns from a stream of self-describing rows; source order is preserved.

    Works lazily on any iterable of dict-like rows (dict, Record, namedtuple, cursor)
    without materializing the source. For plain list/tuple rows, which carry no
    column names of their own, use `tuples_to_records()` first.

    Args:
        rows: Source rows - dicts, Records, namedtuples, or cursors.
        col_names: Column names to drop. Order doesn't matter; a list, tuple, or
            set all work.
        ignore_missing: If True, names in `col_names` that aren't present in the
            source are silently ignored instead of raising. Useful when the same
            drop-list needs to work against sources that may or may not carry a
            given column (e.g. a Banner extract with/without its table-name prefix).

    Yields:
        Record: one Record per input row, with the named columns dropped.

    Raises:
        ValueError: If col_names is empty, a named column isn't present in the
            source (unless `ignore_missing`), or dropping them would remove
            every column.
        TypeError: If column names can't be determined from the row type.

    Examples:
        # Drop sensitive columns from cursor results before writing
        to_csv(exclude_columns(cursor, ['ssn', 'password']), 'users.csv')

        # Drop a column that may or may not be present, depending on source
        to_csv(exclude_columns(cursor, ['spriden_ssn'], ignore_missing=True), 'users.csv')
    """
    if not col_names:
        raise ValueError("col_names must not be empty")
    return _exclude_columns_gen(iter(rows), col_names, ignore_missing)


def _exclude_columns_gen(rows: Iterator[Any], col_names: Iterable[str], ignore_missing: bool = False) -> Iterator[Record]:
    first, src_cols, by_name = _peek_columns(rows)
    if first is None:
        return

    exclude_set = set(col_names)
    missing = [c for c in exclude_set if c not in src_cols]
    if missing:
        if not ignore_missing:
            raise ValueError(f"Column(s) not found in source data: {sorted(missing)}")
        exclude_set -= set(missing)
    keep = [c for c in src_cols if c not in exclude_set]
    if not keep:
        raise ValueError("excluding these columns would remove every column")

    output_cls = type('SelectedRecord', (Record,), {})
    output_cls.set_fields(keep)
    get = _make_getter(_getter_keys(keep, by_name, src_cols))

    yield output_cls(*get(first))
    for row in rows:
        yield output_cls(*get(row))


def rename_columns(rows: Iterable[Any], mapping: Dict[str, str], ignore_missing: bool = False) -> Iterator[Record]:
    """
    Rename some columns in a stream of self-describing rows.

    Only the columns named in `mapping` are relabeled; everything else passes
    through unchanged, in its original position — this is a pure rename, not a
    select (use `select_columns()`/`exclude_columns()` to also narrow the
    columns). Works lazily on any iterable of dict-like rows (dict, Record,
    namedtuple, cursor) without materializing the source. For plain list/tuple
    rows, which carry no column names of their own, use `tuples_to_records()`
    first.

    Args:
        rows: Source rows - dicts, Records, namedtuples, or cursors.
        mapping: Source name -> new name. A falsy value ('' or None) is a no-op
            for that column (keeps its original name) rather than an error.
        ignore_missing: If True, keys in `mapping` that aren't present in the
            source are silently ignored instead of raising. Useful when one
            mapping needs to work whether or not a source carries a given
            column (e.g. a Banner extract with/without its table-name prefix).

    Yields:
        Record: one Record per input row, with the named columns relabeled.

    Raises:
        ValueError: If mapping is empty, or a named column isn't present in the
            source (unless `ignore_missing`).
        TypeError: If column names can't be determined from the row type.

    Examples:
        # Nicer headers for a report; every other column is untouched
        to_csv(rename_columns(cursor, {'signup_date': 'Joined On'}), 'customers.csv')

        # One mapping that works whether or not Banner prepended the table name
        # (a file without the prefix already has 'pidm', so that key is just skipped)
        mapping = {'spriden_pidm': 'pidm', 'spriden_id': 'id'}
        to_csv(rename_columns(cursor, mapping, ignore_missing=True), 'ids.csv')
    """
    if not mapping:
        raise ValueError("mapping must not be empty")
    return _rename_columns_gen(iter(rows), mapping, ignore_missing)


def _rename_columns_gen(rows: Iterator[Any], mapping: Dict[str, str], ignore_missing: bool = False) -> Iterator[Record]:
    first, src_cols, by_name = _peek_columns(rows)
    if first is None:
        return

    missing = [c for c in mapping if c not in src_cols]
    if missing and not ignore_missing:
        raise ValueError(f"Column(s) not found in source data: {missing}")

    output_names = [mapping.get(c) or c for c in src_cols]

    output_cls = type('RenamedRecord', (Record,), {})
    output_cls.set_fields(output_names)
    get = _make_getter(_getter_keys(src_cols, by_name, src_cols))

    yield output_cls(*get(first))
    for row in rows:
        yield output_cls(*get(row))