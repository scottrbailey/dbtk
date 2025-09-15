# dbtk/cursors.py
"""
Cursor classes that wrap database cursors and provide different return types.
All cursors delegate to the underlying database cursor stored in _cursor.
"""

import re
from keyword import iskeyword
from typing import List, Any, Optional, Iterator
from collections import namedtuple, OrderedDict

from .record import Record

class ColumnCase:
    """Column case options."""
    UPPER = 'upper'
    LOWER = 'lower'
    TITLE = 'title'
    PRESERVE = 'preserve'
    DEFAULT = LOWER

    @classmethod
    def values(cls):
        return [getattr(cls, attr) for attr in dir(cls) if not attr.startswith('_')]

class Cursor:
    """
    Basic cursor that returns results as lists.
    Base class for other cursor types.
    """

    _local_attrs = [
        'connection', 'column_case', 'debug', 'logger', 'return_cursor',
        'placeholder', 'paramstyle', 'record_factory', '_cursor',
        '_validate_row_factory'
    ]

    def __init__(self, connection, column_case: str = ColumnCase.LOWER,
                 debug: bool = False, return_cursor: bool = False, logger = None, **kwargs):
        """
        Initialize cursor.

        Args:
            connection: Database connection object
            column_case: How to handle column name casing
            debug: Enable debug output
            return_cursor: Return a cursor on .execute(), useful for chaining, annoying otherwise
            logger: Optional logger function
            **kwargs: Additional arguments passed to underlying cursor
        """
        self.connection = connection
        self.debug = debug
        self.logger = logger
        self.column_case = column_case
        self.record_factory = None
        self._validate_row_factory = True
        self.return_cursor = return_cursor

        # Create underlying cursor
        try:
            if hasattr(self.connection, '_connection'):
                self._cursor = self.connection._connection.cursor(**kwargs)
            else:
                self._cursor = self.connection.cursor(**kwargs)
        except Exception as e:
            raise TypeError(f'First argument must be a database connection object: {e}')

        # Set parameter style info
        self.paramstyle = self.connection.interface.paramstyle
        if hasattr(self.connection, 'placeholder'):
            self.placeholder = self.connection.placeholder

        # Ensure arraysize exists (some adapters don't have it)
        if not hasattr(self._cursor, 'arraysize'):
            self.__dict__['arraysize'] = 1

    def __getattr__(self, key: str) -> Any:
        """Delegate attribute access to underlying cursor."""
        return getattr(self._cursor, key)

    def __setattr__(self, key: str, value: Any) -> None:
        """Set attributes on this cursor or delegate to underlying cursor."""
        if key in self._local_attrs:
            self.__dict__[key] = value
        else:
            setattr(self._cursor, key, value)

    def __dir__(self) -> List[str]:
        """Return available attributes."""
        return list(set(
            dir(self._cursor) +
            dir(self.__class__) +
            self._local_attrs
        ))

    def __iter__(self) -> Iterator:
        """Make cursor iterable."""
        if self._is_ready():
            return self

    def __next__(self) -> Any:
        """Iterator protocol."""
        row = self.fetchone()
        if row is not None:
            return row
        else:
            raise StopIteration

    def _create_row_factory(self) -> None:
        """Create the function to process each row. Override in subclasses."""

        def factory(*args):
            return list(args)

        self.record_factory = factory

    def _sanitize_column_name(self, col_name: str, idx: int) -> str:
        """Clean up column names to be valid Python identifiers."""
        if col_name is None or col_name == '':
            return f'column_{idx + 1}'

        # Replace non-word characters with underscore
        col_name = re.sub(r'\W+', '_', col_name)

        # Handle Python keywords
        if iskeyword(col_name):
            col_name = col_name.upper()

        return col_name

    def columns(self, case: Optional[str] = None) -> List[str]:
        """Return list of column names."""
        if not self.description:
            return []

        if case not in (ColumnCase.values()):
            case = self.column_case

        # Apply case transformation
        if case == ColumnCase.LOWER:
            cols = [c[0].lower() for c in self.description]
        elif case == ColumnCase.UPPER:
            cols = [c[0].upper() for c in self.description]
        elif case == ColumnCase.TITLE:
            cols = [c[0].title() for c in self.description]
        else:
            cols = [c[0] for c in self.description]

        # Sanitize column names
        return [self._sanitize_column_name(cols[i], i) for i in range(len(cols))]

    def _is_ready(self) -> bool:
        """Check if cursor is ready to fetch results."""
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')

        if self.record_factory is None:
            self._create_row_factory()

        return True

    def _debug_info(self, query: str, bind_vars: tuple = ()) -> None:
        """Print debug information."""
        print(f'Query:\n{query}')
        if bind_vars:
            print(f'Bind vars:\n{bind_vars}')

    def execute(self, query: str, bind_vars: tuple = ()) -> None:
        """Execute a database query."""
        self._validate_row_factory = True

        if self.debug:
            self._debug_info(query, bind_vars)

        # Store query info if adapter doesn't
        if not hasattr(self._cursor, 'statement'):
            self.__dict__['statement'] = query
        if not hasattr(self._cursor, 'bind_vars'):
            self.__dict__['bind_vars'] = bind_vars

        if self.logger:
            self.logger(str(self.connection), query, bind_vars)

        # some adapters return a cursor instead of the Database API specified None
        _ = self._cursor.execute(query, bind_vars)
        if self.return_cursor:
            return self
        else:
            return None


    def executemany(self, query: str, bind_vars: List[tuple]) -> None:
        """Execute a query against multiple parameter sets."""
        self._validate_row_factory = True

        if self.debug:
            self._debug_info(query, bind_vars)

        if not hasattr(self._cursor, 'statement'):
            self.__dict__['statement'] = f'--executemany\n{query}'
        if not hasattr(self._cursor, 'bind_vars'):
            self.__dict__['bind_vars'] = bind_vars[0] if bind_vars else ()

        if self.logger:
            self.logger(
                str(self.connection),
                query,
                bind_vars[0] if bind_vars else ()
            )

        return self._cursor.executemany(query, bind_vars)

    def selectinto(self, query: str, bind_vars: tuple = ()) -> Any:
        """Execute query that must return exactly one row."""
        self.execute(query, bind_vars)
        rows = self.fetchmany(2)

        if len(rows) == 0:
            raise self.connection.interface.DatabaseError('No Data Found.')
        elif len(rows) > 1:
            raise self.connection.interface.DatabaseError(
                'selectinto() must return one and only one row.'
            )
        else:
            return rows[0]

    def fetchone(self) -> Optional[Any]:
        """Fetch the next row."""
        if self._is_ready():
            row = self._cursor.fetchone()
            if row:
                return self.record_factory(*row)
        return None

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        """Fetch the next set of rows."""
        if size is None:
            size = self._cursor.arraysize

        if self._is_ready():
            return [
                self.record_factory(*row)
                for row in self._cursor.fetchmany(size)
            ]
        return []

    def fetchall(self) -> List[Any]:
        """Fetch all remaining rows."""
        if self._is_ready():
            return [
                self.record_factory(*row)
                for row in self._cursor.fetchall()
            ]
        return []

    def show_statement(self) -> str:
        """Return the last executed statement with parameters."""
        if hasattr(self, 'statement'):
            statement = self.statement
        elif hasattr(self._cursor, 'query'):
            statement = self._cursor.query
        else:
            statement = 'No statement available'

        if hasattr(self, 'bind_vars') and self.bind_vars:
            statement += f'\nParams:\n{self.bind_vars!r}'

        return statement


class RecordCursor(Cursor):
    """Cursor that returns Record objects with attribute and dict-like access."""

    def _is_ready(self) -> bool:
        """Check if ready and update record factory if columns changed."""
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')
        elif self.record_factory is None:
            self._create_row_factory()
        elif self._validate_row_factory:
            # Check if columns have changed since last query
            if hasattr(self.record_factory, '_fields'):
                if self.record_factory._fields != self.columns():
                    self._create_row_factory()
                else:
                    self._validate_row_factory = False
            else:
                self._create_row_factory()
        return True

    def _create_row_factory(self) -> None:
        """Create Record subclass with current columns."""
        self._validate_row_factory = False
        columns = self.columns()

        # Create dynamic Record subclass
        self.record_factory = type(
            'Record',
            (Record,),
            {'_fields': columns}
        )


class TupleCursor(Cursor):
    """Cursor that returns namedtuples."""

    def _is_ready(self) -> bool:
        """Check if ready and update record factory if columns changed."""
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')
        elif self.record_factory is None:
            self._create_row_factory()
        elif self._validate_row_factory:
            # Check if columns have changed
            if hasattr(self.record_factory, '_fields'):
                if self.record_factory._fields != tuple(self.columns()):
                    self._create_row_factory()
                else:
                    self._validate_row_factory = False
            else:
                self._create_row_factory()
        return True

    def _create_row_factory(self) -> None:
        """Create namedtuple factory with current columns."""
        self._validate_row_factory = False
        self.record_factory = namedtuple('TupleRecord', self.columns())


class DictCursor(Cursor):
    """Cursor that returns OrderedDict objects."""

    def _create_row_factory(self) -> None:
        """Create factory that returns OrderedDict."""
        columns = self.columns()

        def factory(*args):
            return OrderedDict(zip(columns, args))

        self.record_factory = factory