# dbtk/etl/transforms/database.py

"""
Database table lookup and validation using PreparedStatement.
"""

import logging
from typing import Any, Union, List, Optional, Callable

from ...utils import quote_identifier, validate_identifier
from ...cursors import PreparedStatement, DictCursor

logger = logging.getLogger(__name__)


class TableLookup:
    """
    Database table lookup with configurable caching for ETL transformations.

    Performs lookups against database tables or views using PreparedStatement for
    efficient repeated queries. Supports three caching strategies:

    - CACHE_NONE (0): No caching, always query database
    - CACHE_LAZY (1): Cache results as encountered (default)
    - CACHE_PRELOAD (2): Preload entire table into memory upfront

    Can operate in three modes:
    - Validator: No return_cols specified, returns bool indicating existence
    - Single lookup: One return column, returns scalar value
    - Multi lookup: Multiple return columns, returns row object (type depends on cursor)

    Examples
    --------
    Validator mode::

        temple_validator = TableLookup(cursor, 'temples', key_cols='temple_id')
        is_valid = temple_validator({'temple_id': 'eastern_air_temple'})  # True/False

    Single value lookup::

        state_lookup = TableLookup(cursor, 'states',
                                   key_cols='name',
                                   return_cols='abbreviation',
                                   cache=TableLookup.CACHE_PRELOAD)  # Small table, preload it
        abbrev = state_lookup({'name': 'California'})  # 'CA'

    Multi-value lookup::

        address_lookup = TableLookup(cursor, 'addresses',
                                     key_cols='address_id',
                                     return_cols=['street', 'city', 'state', 'zip'])
        address = address_lookup({'address_id': 123})  # Record/dict/namedtuple

    Multi-key lookup::

        person_lookup = TableLookup(cursor, 'people',
                                    key_cols=['first_name', 'last_name'],
                                    return_cols='person_id',
                                    cache=TableLookup.CACHE_NONE)  # Large table, don't cache
        person_id = person_lookup({'first_name': 'Aang', 'last_name': 'Avatar'})

    Use in Table config::

        table = Table('citizens', {
            'state_abbrev': {'field': 'state_name', 'fn': state_lookup}
        }, cursor=cursor)
    """

    # Cache strategy constants
    CACHE_NONE = 0  # No caching
    CACHE_LAZY = 1  # Cache as encountered (default)
    CACHE_PRELOAD = 2  # Preload entire table

    def __init__(self,
                 cursor,
                 table: str,
                 key_cols: Union[str, List[str]],
                 return_cols: Optional[Union[str, List[str]]] = None,
                 cache: int = 1):  # Default to lazy caching
        """
        Initialize TableLookup with table schema and caching strategy.

        Parameters
        ----------
        cursor : Cursor
            Database cursor for executing queries
        table : str
            Table or view name to query
        key_cols : str or list of str
            Column name(s) to use in WHERE clause. Cannot be empty.
        return_cols : str, list of str, or None
            Column(s) to return. If None, operates as validator (returns bool)
        cache : int, default 1 (CACHE_LAZY)
            Caching strategy:
            - 0 (CACHE_NONE): No caching, always query database
            - 1 (CACHE_LAZY): Cache results as encountered
            - 2 (CACHE_PRELOAD): Preload entire table into memory

        Raises
        ------
        ValueError
            If key_cols is empty or cache value is invalid

        Note
        ----
        Case sensitivity is determined by the database. If your database is
        case-sensitive (e.g., PostgreSQL), lookups must match the exact case
        in the database. If case-insensitive (e.g., SQLite by default), the
        database will handle case matching.
        """
        # Validate cache strategy
        if cache not in (self.CACHE_NONE, self.CACHE_LAZY, self.CACHE_PRELOAD):
            raise ValueError(
                f"Invalid cache value: {cache}. "
                f"Use TableLookup.CACHE_NONE (0), TableLookup.CACHE_LAZY (1), or TableLookup.CACHE_PRELOAD (2)"
            )

        # Handle DictCursor by getting a regular cursor
        if isinstance(cursor, DictCursor):
            self._cursor = cursor.connection.cursor()
        else:
            self._cursor = cursor

        # Validate and quote identifiers
        validate_identifier(table)
        self._table = quote_identifier(table)

        # Normalize key_cols to list and validate
        if isinstance(key_cols, str):
            key_cols = [key_cols]
        if not key_cols:
            raise ValueError("key_cols cannot be empty")
        for col in key_cols:
            validate_identifier(col)
        self._key_cols = [quote_identifier(col) for col in key_cols]
        self._key_col_names = key_cols  # Store unquoted for bind params

        # Normalize return_cols to list or None
        if return_cols is None:
            self._return_cols = None
            self._validator_mode = True
        else:
            if isinstance(return_cols, str):
                return_cols = [return_cols]
            for col in return_cols:
                validate_identifier(col)
            self._return_cols = [quote_identifier(col) for col in return_cols]
            self._validator_mode = False

        self._cache_strategy = cache
        self._cache = {}
        self._preloaded = False

        # Build SQL
        self._build_sql()

        # Apply caching strategy
        if cache == self.CACHE_NONE:
            # No caching at all
            pass
        elif cache == self.CACHE_LAZY:
            # Lazy cache - enabled but don't preload
            pass  # Cache dict already initialized
        elif cache == self.CACHE_PRELOAD:
            # Preload entire table - user's responsibility to know table size
            self._preload_all()
            self._preloaded = True

    def _build_sql(self):
        """Build the SELECT and PreparedStatement."""
        # Build SELECT clause
        if self._validator_mode:
            # For validation, just select one of the key columns
            select_clause = self._key_cols[0]
        else:
            select_clause = ', '.join(self._return_cols)

        # Build WHERE clause with named parameters
        where_conditions = []
        for col, param_name in zip(self._key_cols, self._key_col_names):
            where_conditions.append(f"{col} = :{param_name}")
        where_clause = ' AND '.join(where_conditions)

        # Complete SQL
        query = f"SELECT {select_clause} FROM {self._table} WHERE {where_clause}"

        # Create PreparedStatement
        self._stmt = PreparedStatement(self._cursor, query=query)

    def _preload_all(self):
        """Preload entire lookup table into cache."""
        # Build SELECT for all data
        if self._validator_mode:
            # Just need the keys
            select_cols = ', '.join(self._key_cols)
        else:
            # Need keys + return columns
            select_cols = ', '.join(self._key_cols + self._return_cols)

        query = f"SELECT {select_cols} FROM {self._table}"
        self._cursor.execute(query)

        num_keys = len(self._key_cols)

        for row in self._cursor.fetchall():
            # Extract key values (first N columns)
            key_values = row[:num_keys]

            # Skip rows with null/empty keys
            if any(k in (None, '') for k in key_values):
                continue

            # Build cache key (tuple, preserving original case)
            cache_key = tuple(key_values)

            # Store appropriate value
            if self._validator_mode:
                self._cache[cache_key] = True
            elif len(self._return_cols) == 1:
                # Single return column - store scalar
                self._cache[cache_key] = row[num_keys]
            else:
                # Multiple return columns - store the return portion of row
                # This will be a list slice - cursor type doesn't matter for preload
                self._cache[cache_key] = row[num_keys:]

    def _make_cache_key(self, bind_vars: dict) -> tuple:
        """Create cache key from bind variables."""
        return tuple(bind_vars[name] for name in self._key_col_names)

    def _lookup(self, bind_vars: dict) -> Any:
        """Perform database lookup."""
        self._stmt.execute(bind_vars)
        row = self._stmt.fetchone()

        if not row:
            return None

        if self._validator_mode:
            return True
        elif len(self._return_cols) == 1:
            # Single column - return scalar at position 0
            return row[0]
        else:
            # Multiple columns - return whole row
            return row

    def __call__(self, bind_vars: dict) -> Any:
        """
        Perform lookup with given key values.

        Parameters
        ----------
        bind_vars : dict
            Dictionary with key column names and their values

        Returns
        -------
        bool
            If in validator mode (no return_cols)
        scalar
            If single return column
        row object
            If multiple return columns (type depends on cursor)
        None
            If no match found
        """
        # Check for None/empty values in keys
        for key_name in self._key_col_names:
            if bind_vars.get(key_name) in (None, ''):
                return False if self._validator_mode else None

        # No caching - always query
        if self._cache_strategy == self.CACHE_NONE:
            result = self._lookup(bind_vars)
            if result is None:
                return False if self._validator_mode else None
            return result

        # With caching - check cache first
        cache_key = self._make_cache_key(bind_vars)

        if cache_key in self._cache:
            return self._cache[cache_key]

        # Not in cache - perform lookup
        result = self._lookup(bind_vars)

        # Cache result
        if result is not None:
            self._cache[cache_key] = result
        else:
            result = False if self._validator_mode else None
            if not self._preloaded:
                # Only cache misses if not preloaded (preloaded means we know all valid keys)
                self._cache[cache_key] = result

        return result


def Lookup(table: str,
           key_cols: Union[str, List[str]],
           return_cols: Union[str, List[str]],
           *,
           cache: int = TableLookup.CACHE_LAZY,
           missing: Any = None) -> Callable[[Any], Any]:
    """
    One-liner database lookup for Table column configs.
    """
    return _DeferredTransform.create_lookup(
        table=table,
        key_cols=key_cols,
        return_cols=return_cols,
        cache=cache,
        missing=missing
    )


def Validate(table: str,
             key_cols: Union[str, List[str]],
             *,
             cache: int = TableLookup.CACHE_LAZY,
             on_fail: str = 'warn') -> Callable[[Any], Any]:
    """
    One-liner validation — returns original value if key exists in table.
    """
    return _DeferredTransform.create_validator(
        table=table,
        key_cols=key_cols,
        cache=cache,
        on_fail=on_fail
    )


# ——————————————————— Internal Implementation ———————————————————

class _DeferredTransform:
    __slots__ = ('_args', '_kwargs', '_extra', '_bound_fn')

    def __init__(self, args, kwargs, extra=None):
        self._args = args
        self._kwargs = kwargs
        self._extra = extra or {}
        self._bound_fn = None

    @classmethod
    def create_lookup(cls, table, key_cols, return_cols, *, cache=TableLookup.CACHE_LAZY, missing=None):
        # Only pass valid TableLookup args
        return cls(
            args=(table, key_cols, return_cols),
            kwargs={'cache': cache},  # ← only this goes to TableLookup
            extra={'missing': missing}  # ← our wrapper-specific option
        )

    @classmethod
    def create_validator(cls, table, key_cols, *, cache=TableLookup.CACHE_LAZY, on_fail='warn'):
        return cls(
            args=(table, key_cols),
            kwargs={'cache': cache},
            extra={'on_fail': on_fail, 'validator': True}
        )

    def bind(self, cursor) -> Callable[[Any], Any]:
        if self._bound_fn is not None:
            return self._bound_fn

        is_validator = self._extra.get('validator', False)
        lookup = TableLookup(cursor, *self._args, **self._kwargs)  # ← clean, no junk

        key_cols = self._args[1]

        if is_validator:
            on_fail = self._extra.get('on_fail', 'warn')

            def validate(value):
                if value is None:
                    return value
                bind_vars = _make_bind_vars(key_cols, value)
                if not lookup(bind_vars):
                    msg = f"Validation failed: {bind_vars} not found in table {self._args[0]}"
                    if on_fail == 'raise':
                        raise ValueError(msg)
                    elif on_fail == 'warn':
                        logger.warning(msg)
                return value

            self._bound_fn = validate

        else:
            missing = self._extra.get('missing')

            def transform(value):
                if value is None:
                    return missing
                bind_vars = _make_bind_vars(key_cols, value)
                result = lookup(bind_vars)
                return result if result is not None else missing

            self._bound_fn = transform

        return self._bound_fn

    def __call__(self, value):
        if self._bound_fn is None:
            raise RuntimeError("Lookup/Validate used before Table cursor was bound")
        return self._bound_fn(value)


def _make_bind_vars(key_cols_spec: Union[str, List[str]], value: Any) -> dict:
    if isinstance(key_cols_spec, str):
        return {key_cols_spec: value}
    # key_cols_spec is list/tuple
    if isinstance(value, dict):
        return value
    if hasattr(value, '__iter__') and not isinstance(value, str):
        return dict(zip(key_cols_spec, value))
    # Fallback: single value, multiple keys → use first key
    return {key_cols_spec[0]: value}