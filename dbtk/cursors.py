# dbtk/cursors.py
"""
Cursor classes that wrap database cursors and provide different return types.
All cursors delegate to the underlying database cursor stored in _cursor.
"""

import logging
from typing import List, Any, Optional, Iterator, Callable
from collections import namedtuple, OrderedDict

from .record import Record
from .utils import ParamStyle, process_sql_parameters, sanitize_identifier
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

    def __init__(self, cursor, query: Optional[str] = None, filename: Optional[str] = None, encoding: Optional[str] = 'utf-8-sig'):
        """
        Create a prepared statement from a SQL file. It

        Args:
            cursor: The cursor that will execute this statement
            query: SQL query string (optional)
            filename: Path to SQL file (relative to CWD)
            encoding: File encoding (default: utf-8-sig)
        """
        self.cursor = cursor

        if filename:
            self.filename = filename
            # Read SQL from file
            with open(filename, encoding=encoding) as f:
                original_sql = f.read()
        elif query is not None:
            self.filename = None
            original_sql = query
        else:
            raise ValueError('Must provide either query or filename')

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
            params = self.cursor._prepare_params(self.param_names, bind_vars)
            return self.cursor.execute(self.sql, params)
        except Exception as e:
            source = self.filename or '<query>'
            logger.error(
                f"Error executing prepared statement from {source}\n"
                f"Transformed SQL: {self.sql}\n"
                f"Parameters: {bind_vars}"
            )
            raise

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
    # Attributes that live on this class and are not delegated to the underlying cursor
    _local_attrs = [
        'connection', 'column_case', 'debug', 'return_cursor',
        'placeholder', 'paramstyle', 'record_factory', 'batch_size',
        '_cursor', '_row_factory_invalid', '_statement', '_bind_vars', '_bulk_method'
    ]
    # Attributes that are allowed to be passed in from the connection/configuration layer
    WRAPPER_SETTINGS = ('batch_size', 'column_case', 'debug', 'return_cursor', 'type')

    def __init__(self,
                 connection,
                 column_case: Optional[str] = None,
                 batch_size: Optional[int] = None,
                 debug: Optional[bool] = False,
                 return_cursor: Optional[bool] = False,
                 **kwargs):
        """
        Initialize a cursor for database operations.

        Parameters
        ----------
        connection : Database
            Database connection object
        column_case : str, optional
            How to handle column name casing: 'lower', 'upper', 'title', or None (default)
        batch_size: int, optional
            How many rows to process at a time when using executemany() or bulk operations in DataSurge
        debug : bool, default False
            Enable debug output showing queries and bind variables
        return_cursor : bool, default False
            If True, execute() returns the cursor for method chaining
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
        self.record_factory = None
        self._row_factory_invalid = True
        self.return_cursor = return_cursor
        if column_case is None:
            column_case = settings.get('default_column_case', ColumnCase.DEFAULT)
        self.column_case = column_case
        if batch_size is None:
            batch_size = settings.get('default_batch_size', 1000)
        self.batch_size = batch_size
        self._statement = None              # Stores statement locally if adapter doesn't
        self._bind_vars = None              # Stores bind vars locally if adapter doesn't
        self._bulk_method = None            # Allows us to override executemany if needed
        # remove any kwargs not intended for the underlying cursor
        filtered_kwargs = {key: val for key, val in kwargs.items() if key not in self.WRAPPER_SETTINGS}
        # Create underlying cursor
        try:
            if hasattr(self.connection, '_connection'):
                self._cursor = self.connection._connection.cursor(**filtered_kwargs)
            else:
                self._cursor = self.connection.cursor(**filtered_kwargs)
        except Exception as e:
            raise TypeError(f'First argument must be a database connection object: {e}')

        # Set parameter style info
        self.paramstyle = self.connection.interface.paramstyle
        if hasattr(self.connection, 'placeholder'):
            self.placeholder = self.connection.placeholder

        # Ensure arraysize exists (some adapters don't have it)
        if not hasattr(self._cursor, 'arraysize'):
            self.__dict__['arraysize'] = 1000

    def __getattr__(self, key: str) -> Any:
        """Delegate attribute access to underlying cursor."""
        if key == 'statement' and not hasattr(self._cursor, 'statement'):
            return self._statement
        if key == 'bind_vars' and not hasattr(self._cursor, 'bind_vars'):
            return self._bind_vars
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

    def _prepare_params(self, param_names: list, bind_vars: dict) -> Any:
        """
        Convert dict parameters to format required by cursor's paramstyle.

        Args:
            bind_vars: Dictionary of named parameters

        Returns:
            Tuple for positional styles, dict for named styles
        """
        missing = set(param_names) - set(bind_vars.keys())
        if missing:
            logger.info(f"Parameters not provided, defaulting to None: {', '.join(missing)}")
        if self.paramstyle in ParamStyle.positional_styles():
            # Build tuple in param_names order
            return tuple(bind_vars.get(name) for name in param_names)
        else:
            # Return dict with only the params we need
            return {name: bind_vars.get(name) for name in param_names}

    def _detect_bulk_method(self) -> Callable:
        """
        Detect and return the fastest bulk execution method for this cursor.

        Called once per cursor, on first executemany(). Stores in self._bulk_method.
        """
        adapter = self.connection.interface.__name__
        if adapter == 'psycopg2':
            try:
                from psycopg2.extras import execute_batch
                # Return a bound dispatcher: execute_batch(cur, sql, argslist, page_size)
                def psycopg_batch(cur, sql, argslist):
                    page_size=getattr(self, 'batch_size', 1000)
                    return execute_batch(cur, sql, argslist, page_size=page_size)

                logger.debug("Cursor upgraded: executemany → psycopg2.extras.execute_batch")
                return psycopg_batch
            except ImportError:
                logger.debug("psycopg2.extras not available — using native executemany")
        elif adapter == 'pyodbc':
            if hasattr(self._cursor, 'fast_executemany') and not getattr(self._cursor, 'fast_executemany', False):
                self._cursor.fast_executemany = True
                logger.debug("pyodbc: enabled fast_executemany for bulk operations")

        # Fallback for everything else (SQLite, MySQL, etc.)
        return lambda cur, sql, argslist: cur.executemany(sql, argslist)

    def _create_record_factory(self) -> None:
        """Create the function to process each row. Override in subclasses."""

        def factory(*args):
            return list(args)

        self.record_factory = factory

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
        return [sanitize_identifier(cols[i], i) for i in range(len(cols))]

    def _is_ready(self) -> bool:
        """Check if cursor is ready to fetch results."""
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')

        if self.record_factory is None:
            self._create_record_factory()

        return True

    def execute(self, query: str, bind_vars: tuple = ()) -> None:
        """Execute a database query."""
        self._row_factory_invalid = True

        if self.debug:
            logger.debug(f'Query:\n{query}')
            logger.debug(f'Bind vars:\n{bind_vars}')

        # Store statement and bind_vars locally if the adapter doesn't
        if not hasattr(self._cursor, 'statement'):
            self.__dict__['_statement'] = query
        if not hasattr(self._cursor, 'bind_vars'):
            self.__dict__['_bind_vars'] = bind_vars

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
                encoding: File encoding (default: utf-8-sig)

        Returns:
            Cursor if return_cursor=True, else None

        Example:
            cursor.execute_file('queries/get_user.sql', {'user_id': 123})
        """
        encoding = kwargs.get('encoding', 'utf-8-sig')

        try:
            # Read SQL from file
            with open(filename, encoding=encoding) as f:
                sql = f.read()

            # Transform SQL for this cursor's paramstyle
            from .database import ParamStyle
            transformed_sql, param_names = process_sql_parameters(sql, self.paramstyle)

            # Prepare parameters
            if bind_vars:
                params = self._prepare_params(param_names, bind_vars)
            else:
                params = None

            return self.execute(transformed_sql, params)

        except Exception as e:
            statement = locals().get('transformed_sql', 'N/A')
            logger.error(
                f"Error executing SQL file: {filename}\n"
                f"Transformed SQL: {statement}\n"
                f"Parameters: {bind_vars}"
            )
            raise

    def prepare_file(self, filename: str, encoding: str = 'utf-8-sig') -> PreparedStatement:
        """
        Prepare a SQL statement from a file for repeated execution.

        The SQL file is read once and parameter conversion is performed once.
        The returned PreparedStatement can be executed multiple times efficiently.

        Args:
            filename: Path to SQL file (relative to CWD)
            encoding: File encoding (default: utf-8-sig)

        Returns:
            PreparedStatement object

        Example
        -------
        ::

            stmt = cursor.prepare_file('queries/insert_user.sql')
            for user in users:
                stmt.execute({'user_id': user.id, 'name': user.name})
        """
        return PreparedStatement(self, filename=filename, encoding=encoding)

    def executemany(self, query: str, bind_vars: List[tuple]) -> None:
        """Execute a query against multiple parameter sets."""
        self._row_factory_invalid = True

        if self.debug:
            logger.debug(f'Executemany - Query:\n{query}')
            logger.debug(f'Bind vars (first row):\n{bind_vars[0]}')

        # Store statement and bind_vars (first row only) locally if the adapter doesn't
        if not hasattr(self._cursor, 'statement'):
            self.__dict__['_statement'] = query
        if not hasattr(self._cursor, 'bind_vars'):
            self.__dict__['_bind_vars'] = bind_vars[0]

        if self._bulk_method is None:
            # Detect and cache the fastest bulk execution method
            self._bulk_method = self._detect_bulk_method()

        _ = self._bulk_method(self._cursor, query, bind_vars)
        if self.return_cursor:
            return self
        else:
            return None

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


class RecordCursor(Cursor):
    """Cursor that returns Record objects with attribute and dict-like access."""

    def _is_ready(self) -> bool:
        """Check if ready and update record factory if columns changed."""
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')
        elif self.record_factory is None:
            self._create_record_factory()
        elif self._row_factory_invalid:
            # Check if columns have changed since last query
            if hasattr(self.record_factory, '_fields'):
                if self.record_factory._fields != self.columns():
                    self._create_record_factory()
                else:
                    self._row_factory_invalid = False
            else:
                self._create_record_factory()
        return True

    def _create_record_factory(self) -> None:
        """Create Record subclass with current columns."""
        self._row_factory_invalid = False
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
            self._create_record_factory()
        elif self._row_factory_invalid:
            # Check if columns have changed
            if hasattr(self.record_factory, '_fields'):
                if self.record_factory._fields != tuple(self.columns()):
                    self._create_record_factory()
                else:
                    self._row_factory_invalid = False
            else:
                self._create_record_factory()
        return True

    def _create_record_factory(self) -> None:
        """Create namedtuple factory with current columns."""
        self._row_factory_invalid = False
        self.record_factory = namedtuple('TupleRecord', self.columns())


class DictCursor(Cursor):
    """Cursor that returns OrderedDict objects."""

    def _create_record_factory(self) -> None:
        """Create factory that returns OrderedDict."""
        columns = self.columns()

        def factory(*args):
            return OrderedDict(zip(columns, args))

        self.record_factory = factory