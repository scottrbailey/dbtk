# dbtk/cursors.py
"""
Cursor classes that wrap database cursors and provide different return types.
All cursors delegate to the underlying database cursor stored in _cursor.
"""

import re
import logging
from keyword import iskeyword
from typing import List, Any, Optional, Iterator
from collections import namedtuple, OrderedDict

from .record import Record
from .utils import ParamStyle, process_sql_parameters
from .defaults import settings

logger = logging.getLogger(__name__)
__all__ = ['Cursor', 'RecordCursor', 'TupleCursor', 'DictCursor',
           'ColumnCase', 'PreparedStatement']


class ColumnCase:
    """
    Column name case transformation options for result sets.

    Controls how column names from database queries are transformed:

    - UPPER: Convert to uppercase (USER_ID)
    - LOWER: Convert to lowercase (user_id) [default]
    - TITLE: Convert to title case (User_Id)
    - PRESERVE: Keep original case from database

    Example:
        >>> cursor = db.cursor(column_case=ColumnCase.UPPER)
        >>> cursor = db.cursor(column_case='preserve')
    """
    UPPER = 'upper'
    LOWER = 'lower'
    TITLE = 'title'
    PRESERVE = 'preserve'
    DEFAULT = LOWER

    @classmethod
    def values(cls):
        return [getattr(cls, attr) for attr in dir(cls) if not attr.startswith('_')]


class PreparedStatement:
    """
    A prepared SQL statement loaded from a file with cached parameter mapping.

    The statement is read from file once and SQL parameter conversion is performed
    once based on the cursor's paramstyle. The prepared statement can then be
    executed multiple times efficiently.  It retains a reference to the cursor,
    so it can be used in the same way as a regular cursor (fetchone(), fetchmany(), etc.).
    """

    def __init__(self, cursor, filename: str, encoding: str = 'utf-8'):
        """
        Create a prepared statement from a SQL file. It

        Args:
            cursor: The cursor that will execute this statement
            filename: Path to SQL file (relative to CWD)
            encoding: File encoding (default: utf-8)
        """
        self.cursor = cursor
        self.filename = filename

        # Read SQL from file
        with open(filename, encoding=encoding) as f:
            original_sql = f.read()

        # Transform SQL for cursor's paramstyle
        self.sql, self.param_names = process_sql_parameters(
            original_sql,
            cursor.paramstyle
        )

    def __iter__(self):
        """Make prepared statement iterable."""
        return self.cursor.__iter__()

    def __next__(self):
        """Iterator protocol."""
        return self.cursor.__next__()

    def execute(self, bind_vars: dict) -> Any:
        """
        Execute the prepared statement with the given parameters.

        Args:
            bind_vars: Dictionary of named parameters

        Returns:
            Cursor if return_cursor=True, else None
        """
        try:
            params = self._prepare_params(bind_vars)
            return self.cursor.execute(self.sql, params)
        except Exception as e:
            logger.error(
                f"Error executing prepared statement from {self.filename}\n"
                f"Transformed SQL: {self.sql}\n"
                f"Parameters: {bind_vars}"
            )
            raise

    def _prepare_params(self, bind_vars: dict) -> Any:
        """
        Convert dict parameters to format required by cursor's paramstyle.

        Args:
            bind_vars: Dictionary of named parameters

        Returns:
            Tuple for positional styles, dict for named styles
        """
        if self.cursor.paramstyle in ParamStyle.positional_styles():
            # Build tuple in param_names order
            return tuple(bind_vars.get(name) for name in self.param_names)
        else:
            # Return dict with only the params we need
            return {name: bind_vars[name] for name in self.param_names if name in bind_vars}

    def __getattr__(self, key: str):
        """Delegate attribute access to underlying cursor."""
        return getattr(self.cursor, key)


class Cursor:
    """
    Basic cursor that returns query results as lists.

    This is the base class for all DBTK cursor types. It wraps database-specific cursor
    objects and provides a consistent interface plus additional functionality like SQL
    file execution, parameter conversion, and prepared statements.

    The basic Cursor returns results as plain lists (index access only). Most users
    should use higher-level cursor types like RecordCursor, TupleCursor, or DictCursor
    which provide more convenient access patterns.

    Attributes
    ----------
    connection : Database
        The database connection this cursor belongs to
    paramstyle : str
        Parameter style of the underlying database ('qmark', 'named', etc.)
    placeholder : str
        Placeholder string for bind parameters (e.g., '?', ':1', etc.)
    description
        Column metadata from the last query (delegated to underlying cursor)

    Note
    ----
    Cursors delegate attribute access to the underlying database-specific cursor,
    so all native cursor functionality is available.

    Example
    -------
    ::

        # Usually created via Database.cursor()
        cursor = db.cursor('list')
        cursor.execute("SELECT id, name, email FROM users WHERE status = :status",
                      {'status': 'active'})

        for row in cursor:
            user_id, name, email = row  # Plain list - index access only
            print(f"{user_id}: {name} ({email})")

    See Also
    --------
    RecordCursor : Returns Record objects with multiple access patterns
    TupleCursor : Returns namedtuples
    DictCursor : Returns OrderedDict objects
    """

    _local_attrs = [
        'connection', 'column_case', 'debug', 'logger', 'return_cursor',
        'placeholder', 'paramstyle', 'record_factory', '_cursor',
        '_validate_row_factory'
    ]

    def __init__(self, connection, column_case: str = None,
                 debug: bool = False, return_cursor: bool = False, logger=None, **kwargs):
        """
        Initialize a cursor for database operations.

        Parameters
        ----------
        connection : Database
            Database connection object
        column_case : str, optional
            How to handle column name casing: 'lower', 'upper', 'title', or None (default)
        debug : bool, default False
            Enable debug output showing queries and bind variables
        return_cursor : bool, default False
            If True, execute() returns the cursor for method chaining
        logger : callable, optional
            Custom logger function for debug output
        **kwargs
            Additional arguments passed to the underlying database cursor

        Example
        -------
        ::

            # Typically created via Database.cursor()
            cursor = db.cursor()

            # With debug enabled
            cursor = db.cursor(debug=True)

            # With method chaining
            cursor = db.cursor(return_cursor=True)
            results = cursor.execute("SELECT * FROM users").fetchall()
        """
        self.connection = connection
        self.debug = debug
        self.logger = logger
        self.record_factory = None
        self._validate_row_factory = True
        self.return_cursor = return_cursor
        if column_case is None:
            column_case = settings.get('default_column_case', ColumnCase.DEFAULT)
        self.column_case = column_case

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

    def execute_file(self, filename: str, bind_vars: Optional[dict] = None, **kwargs) -> Any:
        """
        Execute SQL query from a file with named parameter substitution.

        This is a convenience method for one-off queries. For queries that will be
        executed multiple times, use prepare_file() instead for better performance.

        Args:
            filename: Path to SQL file (relative to CWD)
            bind_vars: Dictionary of named parameters
            **kwargs:
                encoding: File encoding (default: utf-8)

        Returns:
            Cursor if return_cursor=True, else None

        Example:
            cursor.execute_file('queries/get_user.sql', {'user_id': 123})
        """
        encoding = kwargs.get('encoding', 'utf-8')

        try:
            # Read SQL from file
            with open(filename, encoding=encoding) as f:
                sql = f.read()

            # Transform SQL for this cursor's paramstyle
            from .database import ParamStyle
            transformed_sql, param_names = process_sql_parameters(sql, self.paramstyle)

            # Prepare parameters
            if bind_vars:
                if self.paramstyle in ParamStyle.positional_styles():
                    params = tuple(bind_vars.get(name) for name in param_names)
                else:
                    params = {name: bind_vars[name] for name in param_names if name in bind_vars}
            else:
                params = () if self.paramstyle in ParamStyle.positional_styles() else {}

            return self.execute(transformed_sql, params)

        except Exception as e:
            logger.error(
                f"Error executing SQL file: {filename}\n"
                f"Parameters: {bind_vars}"
            )
            raise

    def prepare_file(self, filename: str, encoding: str = 'utf-8') -> PreparedStatement:
        """
        Prepare a SQL statement from a file for repeated execution.

        The SQL file is read once and parameter conversion is performed once.
        The returned PreparedStatement can be executed multiple times efficiently.

        Args:
            filename: Path to SQL file (relative to CWD)
            encoding: File encoding (default: utf-8)

        Returns:
            PreparedStatement object

        Example
        -------
        ::

            stmt = cursor.prepare_file('queries/insert_user.sql')
            for user in users:
                stmt.execute({'user_id': user.id, 'name': user.name})
        """
        return PreparedStatement(self, filename, encoding)

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