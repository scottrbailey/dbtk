# dbtk/database.py
"""
Database connection wrapper that provides a uniform interface
to different database adapters.
"""

import importlib
import os
import logging
from typing import Dict, Any, Optional, Union, Type, List
from contextlib import contextmanager

from .cursors import Cursor, ParamStyle
from .defaults import settings

logger = logging.getLogger(__name__)

# users can define their own drivers in the config file
_user_drivers = {}

def _hide_password(kwargs):
    """Replace password with '********' to be printable"""
    parms = kwargs.copy()
    for key, val in parms.items():
        if key in ('password', 'PWD', 'passwd'):
            parms[key] = '********'
    return parms

DRIVERS = {
    # PostgreSQL Drivers
    'psycopg2': {
        'database_type': 'postgres',
        'priority': 11,
        'param_map': {'database': 'dbname'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password', 'sslmode', 'connect_timeout', 'application_name',
                           'client_encoding', 'options', 'sslcert', 'sslkey', 'sslrootcert'},
        'connection_method': 'connection_string',
        'default_port': 5432,
    },
    'psycopg': {  # psycopg3
        'database_type': 'postgres',
        'priority': 12,
        'param_map': {'database': 'dbname'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password', 'sslmode', 'connect_timeout', 'application_name',
                           'client_encoding', 'options', 'sslcert', 'sslkey', 'sslrootcert'},
        'connection_method': 'connection_string',
        'default_port': 5432,
    },
    'pgdb': {
        'database_type': 'postgres',
        'priority': 13,
        'param_map': {'database': 'database'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password'},
        'connection_method': 'kwargs',
        'default_port': 5432,
    },

    # Oracle Drivers
    'oracledb': {
        'database_type': 'oracle',
        'priority': 11,
        'param_map': {'database': 'service_name'},
        'required_params': [{'dsn', 'user'}, {'host', 'port', 'database', 'user'}],
        'optional_params': {'password', 'mode', 'events', 'purity', 'cclass', 'tag', 'matchanytag',
                           'config_dir', 'wallet_location', 'wallet_password'},
        'connection_method': 'dsn',
        'default_port': 1521
    },
    'cx_Oracle': {
        'database_type': 'oracle',
        'priority': 12,
        'param_map': {'database': 'service_name'},
        'required_params': [{'dsn'}, {'host', 'port', 'database', 'user'}],
        'optional_params': {'password', 'mode', 'events', 'purity', 'cclass', 'tag', 'matchanytag',
                           'encoding', 'nencoding', 'edition', 'appcontext'},
        'connection_method': 'dsn',
        'default_port': 1521
    },

    # MySQL Drivers
    'mysqlclient': {
        'database_type': 'mysql',
        'priority': 11,
        'param_map': {'database': 'db', 'password': 'passwd'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password', 'charset', 'use_unicode', 'sql_mode', 'read_default_file',
                           'conv', 'connect_timeout', 'compress', 'named_pipe', 'init_command',
                           'read_default_group', 'unix_socket', 'port'},
        'connection_method': 'kwargs',
        'default_port': 3306
    },
    'mysql.connector': {
        'database_type': 'mysql',
        'priority': 12,
        'param_map': {},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password', 'charset', 'collation', 'autocommit', 'time_zone',
                           'sql_mode', 'use_unicode', 'get_warnings', 'raise_on_warnings',
                           'connection_timeout', 'buffered', 'raw', 'consume_results'},
        'connection_method': 'kwargs',
        'default_port': 3306
    },
    'pymysql': {
        'database_type': 'mysql',
        'priority': 13,
        'param_map': {'database': 'db', 'password': 'passwd'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password', 'charset', 'sql_mode', 'read_default_file',
                           'conv', 'use_unicode', 'connect_timeout', 'read_timeout', 'write_timeout',
                           'bind_address', 'unix_socket', 'autocommit'},
        'connection_method': 'kwargs',
        'default_port': 3306
    },
    'MySQLdb': {
        'database_type': 'mysql',
        'priority': 14,
        'param_map': {'database': 'db', 'password': 'passwd'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password', 'charset', 'use_unicode', 'sql_mode', 'read_default_file',
                           'conv', 'connect_timeout', 'compress', 'named_pipe', 'init_command',
                           'read_default_group', 'unix_socket'},
        'connection_method': 'kwargs',
        'default_port': 3306
    },

    # SQL Server Drivers
    'pyodbc_sqlserver': {
        'database_type': 'sqlserver',
        'module': 'pyodbc',
        'priority': 11,
        'param_map': {'host': 'SERVER', 'database': 'DATABASE', 'user': 'UID', 'password': 'PWD'},
        'required_params': [{'host', 'database', 'user'}, {'dsn'}],
        'optional_params': {'password', 'port', 'driver', 'trusted_connection', 'encrypt', 'trustservercertificate'},
        'connection_method': 'odbc_string',
        'odbc_driver_name': 'ODBC Driver 17 for SQL Server',
        'default_port': 1433
    },
    'pymssql': {
        'database_type': 'sqlserver',
        'priority': 12,
        'param_map': {},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'password', 'port', 'timeout', 'login_timeout', 'charset', 'as_dict', 'appname'},
        'connection_method': 'kwargs',
        'default_port': 1433
    },

    # ODBC Drivers for other databases
    'pyodbc_postgres': {
        'database_type': 'postgres',
        'module': 'pyodbc',
        'priority': 14,
        'param_map': {'host': 'SERVER', 'database': 'DATABASE', 'user': 'UID', 'password': 'PWD'},
        'required_params': [{'host', 'database', 'user'}, {'dsn'}],
        'optional_params': {'password', 'port'},
        'connection_method': 'odbc_string',
        'odbc_driver_name': 'PostgreSQL Unicode',
        'default_port': 5432
    },
    'pyodbc_mysql': {
        'database_type': 'mysql',
        'module': 'pyodbc',
        'priority': 15,
        'param_map': {'host': 'SERVER', 'database': 'DATABASE', 'user': 'UID', 'password': 'PWD'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'password', 'port'},
        'connection_method': 'odbc_string',
        'odbc_driver_name': 'MySQL ODBC 8.0 Unicode Driver',
        'default_port': 3306
    },
    'pyodbc_oracle': {
        'database_type': 'oracle',
        'module': 'pyodbc',
        'priority': 13,
        'param_map': {'host': 'SERVER', 'database': 'DATABASE', 'user': 'UID', 'password': 'PWD'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'password', 'port'},
        'connection_method': 'odbc_string',
        'odbc_driver_name': 'Oracle in OraClient19Home1',
        'default_port': 1521
    },

    # SQLite Driver
    'sqlite3': {
        'database_type': 'sqlite',
        'priority': 1,
        'param_map': {},
        'required_params': [{'database'}],
        'optional_params': {'timeout', 'detect_types', 'isolation_level', 'check_same_thread',
                           'factory', 'cached_statements', 'uri'},
        'connection_method': 'kwargs'
    }
}


def register_user_drivers(drivers_config: dict) -> None:
    """Register drivers from config file."""
    global _user_drivers
    _user_drivers.update(drivers_config)


def _get_all_drivers() -> dict:
    """Get combined built-in and user drivers."""
    return {**DRIVERS, **_user_drivers}


def _get_drivers_for_database(db_type: str, valid_only: bool = True) -> List[str]:
    """
    Gets a list of drivers available for the specified database type.

    Iterates through DRIVERS to identify which ones match the specified database type.
    Drivers can be filtered to include only those that are currently importable and available for use.
    The result is a sorted list of driver names based on their priority.

    Parameters:
        db_type (str): The type of database for which to retrieve drivers.
        valid_only (bool): Specifies whether to include only valid and importable
            drivers (default is True).

    Returns:
        List[str]: A sorted list of driver names available for the given database type.
    """
    all_drivers = _get_all_drivers()
    available_drivers = []

    for driver_name, info in all_drivers.items():
        if info['database_type'] == db_type:
            if valid_only:
                # only add if the driver is available (importable)
                module_name = info.get('module', driver_name)
                spec = importlib.util.find_spec(module_name)
                if spec:
                    available_drivers.append(driver_name)
            else:
                available_drivers.append(driver_name)

    def sort_key(driver_name):
        priority = all_drivers[driver_name]['priority']
        # User drivers get slight priority boost for tie-breaking
        if driver_name in _user_drivers:
            priority -= 0.5
        return priority
    available_drivers.sort(key=sort_key)
    return available_drivers


def _get_db_type_for_driver(driver_name: str) -> str:
    """Get database type for a driver."""
    return _get_all_drivers().get(driver_name, dict()).get('database_type')


def _get_params_for_database(db_type: str, driver: str = None) -> set:
    """Get all valid parameters for a database type from DRIVERS metadata."""
    from .database import _get_all_drivers

    all_drivers = _get_all_drivers()
    valid_params = set()

    # Collect parameters from all drivers for this database type
    for driver_name, driver_info in all_drivers.items():
        if driver_info['database_type'] == db_type:
            if driver and driver_name != driver:
                continue
            # Add required params
            for param_set in driver_info['required_params']:
                valid_params.update(param_set)
            # Add optional params
            valid_params.update(driver_info.get('optional_params', set()))

    return valid_params


def get_supported_db_types() -> set:
    """Get all supported database types."""
    valid_db_types = set()
    all_drivers = _get_all_drivers()
    for driver_name, driver_info in all_drivers.items():
        valid_db_types.add(driver_info['database_type'])
    return valid_db_types


def _validate_connection_params(driver_name: str, config_only: bool = False, **params) -> dict:
    """
    Validate connection parameters against driver requirements.

    Args:
        driver_name: Name of the database driver
        config_only: If True, skip password validation for config storage
        **params: Connection parameters

    Returns:
        Dict of validated parameters with extras removed

    Raises:
        ValueError: If required parameters are missing
    """
    if driver_name not in DRIVERS:
        raise ValueError(f"Unknown driver: {driver_name}")

    driver_info = DRIVERS[driver_name]
    database_type = driver_info['database_type']

    # Initialize with config-only parameters if needed
    validated_params = {}
    if config_only and 'encrypted_password' in params:
        validated_params['encrypted_password'] = params['encrypted_password']

    # get default port if not specified
    if 'port' not in params:
        default_port = driver_info.get('default_port')
        if default_port:
            params['port'] = default_port

    # Check required parameters (any one set must be satisfied)
    required_satisfied = False
    for required_set in driver_info['required_params']:
        if config_only:
            # For config validation, ignore password requirement
            check_set = required_set - {'password'}
            if not check_set or check_set.issubset(params.keys()):
                required_satisfied = True
                break
        else:
            if required_set.issubset(params.keys()):
                required_satisfied = True
                break

    if not required_satisfied:
        msg = f"Missing required parameters. Need one of: {driver_info['required_params']}"
        logger.error(msg)
        logger.error(f"Current params: {params}")
        raise ValueError(msg)

    # Apply parameter mapping and filter valid params
    param_map = driver_info.get('param_map', {})

    all_valid_params = set()
    for req_set in driver_info['required_params']:
        all_valid_params.update(req_set)
    all_valid_params.update(driver_info.get('optional_params', set()))

    for key, value in params.items():
        if key in all_valid_params or (config_only and key == 'encrypted_password'):
            mapped_key = param_map.get(key, key)
            validated_params[mapped_key] = value

    return validated_params


def _get_connection_string(**kwargs) -> str:
    """ Get connection string from keyword arguments."""
    return " ".join([f"{key}={value}" for key, value in kwargs.items()])


def _get_odbc_string(**kwargs) -> str:
    """Build ODBC connection string"""
    port = kwargs.pop('port', None)
    if port and 'SERVER' in kwargs and '\\' not in kwargs['SERVER']:
        kwargs['SERVER'] += f',{port}'
    printable = ';'.join([f"{key.upper()}={value}" for key, value in _hide_password(kwargs).items()])
    logger.debug(f'ODBC connection string: {printable}')
    return ';'.join([f"{key.upper()}={value}" for key, value in kwargs.items()])


def _get_odbc_connection_string(**kwargs) -> str:
    """ Get connection string for ODBC from keyword arguments."""
    # logger.debug(f'Generating ODBC connection string from: {_hide_password(kwargs)}')
    if 'dsn' in kwargs and kwargs['dsn']:
        # DSN only send DSN and password if present
        conn_str = f"DSN={kwargs['dsn']}"
        printable_conn_str = conn_str
        if 'PWD' in kwargs:
            conn_str += f";PWD={kwargs['PWD']}"
            printable_conn_str += f";PWD=******"
    else:
        odbc_driver_name = kwargs.pop('odbc_driver_name', None)
        if 'port' in kwargs:
            kwargs['SERVER'] += f',{kwargs.pop("port")}'
        params = {key.upper(): value for key, value in kwargs.items()}
        conn_str = ";".join([f"{key}={value}" for key, value in params.items()])
        printable_conn_str = ";".join([f"{key}={value}" for key, value in _hide_password(params).items()])
        if odbc_driver_name:
            conn_str = f"DRIVER={{{odbc_driver_name}}};" + conn_str
            printable_conn_str = f"DRIVER={{{odbc_driver_name}}};" + printable_conn_str
    logger.debug(f"ODBC connection string: {printable_conn_str}")
    return conn_str


def _password_prompt(prompt: str = 'Enter password: ') -> str:
    """
    Prompts the user to enter a password securely without echoing it on the terminal.

    This function uses the `getpass` module to securely capture user input for a password.
    The prompt message can be customized by passing a specific string as the argument.

    Args:
        prompt (str): The message to display prompting the user for input. Defaults to 'Enter password: '.

    Returns:
        str: The password entered by the user.
    """
    import getpass
    return getpass.getpass(prompt)


class Database:
    """
    Database connection wrapper providing a uniform interface across database adapters.

    The Database class wraps database-specific connection objects and provides a consistent
    API regardless of which database driver is being used (psycopg2, oracledb, mysqlclient,
    etc.). It handles parameter style conversions, manages cursors, and delegates attribute
    access to the underlying connection for driver-specific functionality.

    Key features:

    * **Unified interface** - Same API for PostgreSQL, Oracle, MySQL, SQL Server, SQLite
    * **Cursor factory** - Create different cursor types (Record, tuple, dict, list)
    * **Transaction management** - Context managers for safe transactions
    * **Attribute delegation** - Access underlying driver features when needed
    * **Parameter style abstraction** - Automatic handling of different bind parameter formats

    Attributes
    ----------
    driver
        The database adapter module (e.g., psycopg2, oracledb)
    database_type : str
        Database type: 'postgres', 'oracle', 'mysql', 'sqlserver', or 'sqlite'
    database_name : str
        Name of the connected database
    placeholder : str
        Parameter placeholder for this database's parameter style

    Example
    -------
    ::

        import dbtk

        # Create connection
        db = dbtk.database.postgres(user='admin', password='secret', database='mydb')

        # Or from configuration
        db = dbtk.connect('production_db')

        # Use as context manager
        with db:
            cursor = db.cursor()
            cursor.execute("SELECT * FROM users WHERE status = :status", {'status': 'active'})
            users = cursor.fetchall()

        # Manual connection management
        db = dbtk.connect('production_db')
        cursor = db.cursor('dict')  # Dictionary cursor
        cursor.execute("SELECT * FROM orders")
        db.commit()
        db.close()

    See Also
    --------
    dbtk.connect : Connect to database from configuration
    Database.cursor : Create a cursor for executing queries
    Database.transaction : Context manager for transactions
    """

    # Attributes stored locally, others delegated to _connection
    _local_attrs = [
        '_connection', 'database_type', 'database_name', 'driver',
        'name', 'placeholder', '_cursor_settings'
    ]

    def __init__(self, connection, driver,
                 database_name: Optional[str] = None,
                 cursor_settings: Optional[dict] = None):
        """
        Initialize Database wrapper around an existing connection.

        This is typically called by connection factory functions rather than directly.
        Use ``dbtk.database.postgres()``, ``dbtk.connect()``, etc. instead.

        Parameters
        ----------
        connection
            Underlying database connection object from the adapter
        driver
            Database adapter module (psycopg2, oracledb, mysqlclient, etc.)
        database_name : str, optional
            Name of the database. If None, attempts to extract from connection.
        cursor_settings : dict, optional
            Values passed to the cursor constructor. e.g. {'type': 'dict', 'batch_size': 2000}

        Example
        -------
        ::

            import psycopg2
            from dbtk.database import Database

            # Direct instantiation (not typical)
            conn = psycopg2.connect(dbname='mydb', user='admin', password='secret')
            db = Database(conn, psycopg2, 'mydb')

            # Typical usage via factory functions
            db = dbtk.database.postgres(user='admin', password='secret', database='mydb')
        """
        self._connection = connection
        self.driver = driver

        # Normalize oracledb exception structure to match DB-API 2.0 spec
        # oracledb moved DatabaseError to exceptions submodule unlike other drivers
        if driver.__name__ == 'oracledb' and not hasattr(connection, 'DatabaseError'):
            from oracledb import exceptions
            connection.DatabaseError = exceptions.DatabaseError

        if database_name is None:
            database_name = (connection.get('database') or
                            connection.get('service_name') or
                            connection.get('dbname') or
                            connection.get('db'))

        self.database_name = database_name

        # Set parameter placeholder based on adapter style
        paramstyle = getattr(driver, 'paramstyle', ParamStyle.DEFAULT)
        self.placeholder = ParamStyle.get_placeholder(paramstyle)

        # Determine server type from driver name
        if driver.__name__ in DRIVERS:
            self.database_type = DRIVERS[driver.__name__]['database_type']
        else:
            self.database_type = 'unknown'

        logger.debug(f"Cursor Database.__init__ cursor_settings: {cursor_settings}")
        if cursor_settings:
            self._cursor_settings = {key: val for key, val in cursor_settings.items()
                                     if key in Cursor.WRAPPER_SETTINGS}
        else:
            self._cursor_settings = dict()

    def __getattr__(self, key: str) -> Any:
        """Delegate attribute access to underlying connection."""
        if key == '__name__':
            return self.name or self.database_name or 'unknown'
        else:
            return getattr(self._connection, key)

    def __setattr__(self, key: str, value: Any) -> None:
        """Set attributes locally or delegate to connection."""
        if key in self._local_attrs or key in ('name',):
            self.__dict__[key] = value
        else:
            setattr(self._connection, key, value)

    def __dir__(self) -> list:
        """Return available attributes."""
        return list(set(
            dir(self._connection) +
            dir(self.__class__) +
            self._local_attrs
        ))

    def __str__(self) -> str:
        """String representation of the database connection."""
        if self.database_name:
            return f'Database({self.database_name}:{self.database_type})'
        else:
            return f'Database({self.database_type})'

    def __repr__(self) -> str:
        if self.database_name:
            return f"Database('{self.database_name}', database_type='{self.database_type}')"
        else:
            return f"Database(database_type='{self.database_type}')"

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()

    def cursor(self, **kwargs) -> Cursor:
        """
        Create a cursor for executing database queries.

        Returns a cursor that yields Record objects, providing flexible access to query results
        through attribute, dictionary, and index notation.

        Parameters
        ----------
        **kwargs
            Optional cursor configuration:

            * ``batch_size`` (int) - Rows to process at once in bulk operations
            * ``column_case`` (str) - Column name casing: 'lower', 'upper', 'title', or 'preserve'
            * ``debug`` (bool) - Enable debug output showing queries and bind variables
            * ``return_cursor`` (bool) - If True, execute() returns cursor for method chaining

        Returns
        -------
        Cursor
            Cursor instance that returns Record objects

        Example
        -------
        ::

            # Create cursor - returns Records
            cursor = db.cursor()
            cursor.execute("SELECT id, name, email FROM users WHERE status = :status",
                          {'status': 'active'})

            # Record supports multiple access patterns
            for row in cursor:
                print(row['name'])    # Dictionary access
                print(row.name)       # Attribute access
                print(row[1])         # Index access
                print(row.get('phone', 'N/A'))  # Safe access with default

            # With configuration options
            cursor = db.cursor(debug=True, column_case='upper')

        See Also
        --------
        Record : Flexible row object with dict, attribute, and index access
        Database.transaction : Context manager for safe transactions
        """
        # Only pass through allowed kwargs
        filtered_kwargs = dict()
        for key in Cursor.WRAPPER_SETTINGS:
            if key in kwargs:
                filtered_kwargs[key] = kwargs.pop(key)
            elif key in self._cursor_settings:
                filtered_kwargs[key] = self._cursor_settings[key]
        return Cursor(self, **filtered_kwargs)

    @contextmanager
    def transaction(self):
        """
        Context manager for safe database transactions.

        Automatically commits the transaction on successful completion or rolls back
        if an exception occurs. This ensures transactions are properly cleaned up
        even if errors occur.

        Yields
        ------
        Database
            This database connection instance

        Raises
        ------
        Exception
            Re-raises any exception that occurs within the transaction block
            after rolling back the transaction

        Example
        -------
        ::

            with db.transaction():
                cursor = db.cursor()
                cursor.execute("INSERT INTO orders (customer, amount) VALUES (:c, :a)",
                             {'c': 'Aang', 'a': 100})
                cursor.execute("UPDATE inventory SET stock = stock - 1 WHERE item_id = :id",
                             {'id': 42})
                # Automatically commits on success

            # If exception occurs, transaction is automatically rolled back
            try:
                with db.transaction():
                    cursor = db.cursor()
                    cursor.execute("INSERT INTO invalid_table ...")  # Raises error
                    # Rollback happens automatically
            except Exception as e:
                logger.error(f"Transaction failed: {e}")

        See Also
        --------
        Database.commit : Manually commit a transaction
        Database.rollback : Manually roll back a transaction
        """
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise

    def param_help(self) -> None:
        """Print help on this driver's parameter style."""
        print(f"{self.driver.__name__}'s parameter style is \"{self.driver.paramstyle}\"")
        print(f'"SELECT * FROM people WHERE name = {self.placeholder} AND age > {self.placeholder}", ("Smith", 30)')

        if self.driver.paramstyle == ParamStyle.NAMED:
            print(r'"SELECT * FROM people WHERE name = :name AND age > :age", {"name": "Smith", "age": 30}')
        elif self.driver.paramstyle == ParamStyle.PYFORMAT:
            print(r'"SELECT * FROM people WHERE name = %(name)s AND age > %(age)s", {"name": "Smith", "age": 30}')

    @classmethod
    def create(cls, db_type: str, driver: Optional[str] = None,
               cursor_settings: Optional[dict] = None, **kwargs) -> 'Database':
        """
        Factory method to create database connections.

        Args:
            db_type: Database type ('postgres', 'oracle', 'mysql', etc.)
            cursor_settings: Defaults to use when creating cursors.
            **kwargs: Connection parameters

        Returns:
            Database instance
        """
        db_driver = None
        all_drivers = _get_all_drivers()
        if driver:
            if driver not in all_drivers:
                raise ValueError(f"Unknown driver: {driver}")
            if all_drivers[driver]['database_type'] != db_type:
                raise ValueError(f"Driver '{driver}' is not compatible with database type '{db_type}'")
            try:
                module_name = all_drivers[driver].get('module', driver)
                db_driver = importlib.import_module(module_name)
                driver_name = driver
            except ImportError:
                logger.warning(f"Driver '{driver}' not available, falling back to default")

        if db_driver is None:
            drivers_for_db = _get_drivers_for_database(db_type)
            for driver_name in drivers_for_db:
                try:
                    module_name = all_drivers[driver_name].get('module', driver_name)
                    db_driver = importlib.import_module(module_name)
                    break
                except ImportError:
                    pass

        if db_driver is None:
            raise ImportError(f"No database driver found for database type '{db_type}'")

        logger.debug(f'parms before _validate: {_hide_password(kwargs)}')
        params = _validate_connection_params(driver_name, **kwargs)
        logger.debug(f'parms after _validate: {_hide_password(params)}')
        if not params:
            raise ValueError("The connection parameters were not valid.")

        driver_conf = all_drivers[driver_name]
        if driver_conf['connection_method'] == 'kwargs':
            connection = db_driver.connect(**params)
        elif driver_conf['connection_method'] == 'connection_string':
            connection = db_driver.connect(_get_connection_string(**params))
        elif driver_conf['connection_method'] == 'dsn':
            if hasattr(db_driver, 'makedsn') and 'dsn' not in params:
                host = params.pop('host', 'localhost')
                port = params.pop('port', None)
                service_name = params.pop('service_name', None)
                params['dsn'] = db_driver.makedsn(host, port, service_name=service_name)
            connection = db_driver.connect(**params)
        elif driver_conf['connection_method'] == 'odbc_string':
            cx_string = _get_odbc_string(DRIVER=driver_conf.get('odbc_driver_name', None), **params)
            connection = db_driver.connect(cx_string)
        else:
            raise ValueError(f"Unknown connection method ({driver_conf['connection_method']}) for driver '{driver_name}'")

        if connection:
            db = cls(connection, db_driver, kwargs.get('database'), cursor_settings=cursor_settings)
            return db


def postgres(user: str, password: Optional[str] = None, database: str = 'postgres',
             host: str = 'localhost', port: int = 5432, driver: str = None, **kwargs) -> Database:
    """
    Create a PostgreSQL database connection.

    Automatically selects the best available PostgreSQL driver (psycopg2, psycopg3, or pgdb).
    You can specify a specific driver if needed.

    Args:
        user: Database username
        password: Database password (prompts if None)
        database: Database name (default: 'postgres')
        host: Server hostname or IP (default: 'localhost')
        port: Server port (default: 5432)
        driver: Specific driver to use ('psycopg2', 'psycopg', 'pgdb')
        **kwargs: Additional driver-specific connection parameters

    Returns:
        Database connection object with context manager support

    Example
    -------
    ::
        >>> from dbtk.database import postgres
        >>> with postgres(user='user', password='pass', database='mydb') as db:
        ...     cursor = db.cursor()
        ...     cursor.execute("SELECT * FROM users")

    See Also:
        Database.create() for more connection options
    """
    return Database.create('postgres', user=user, password=password, database=database,
                           host=host, port=port, driver=driver, **kwargs)


def oracle(user: str, password: Optional[str] = None, database: str = None,
           host: Optional[str] = None, port: int = 1521, driver: str = None,  **kwargs) -> Database:
    """
    Create an Oracle database connection.

    Supports both DSN and connection string formats. Automatically selects
    the best available Oracle driver (oracledb or cx_Oracle).

    Args:
        user: Database username
        password: Database password (prompts if None)
        database: Service name or SID
        host: Server hostname or IP (required if not using dsn)
        port: Server port (default: 1521)
        driver: Specific driver to use ('oracledb', 'cx_Oracle')
        **kwargs: Additional driver-specific parameters (dsn, mode, etc.)

    Returns:
        Database connection object with context manager support

    Example
    -------
    ::
        >>> from dbtk.database import oracle
        >>> # Using service name
        >>> db = oracle(user='scott', password='tiger',
        ...             host='oracle.example.com', database='ORCL')
        >>>
        >>> # Using DSN directly
        >>> db = oracle(user='scott', password='tiger',
        ...             dsn='oracle.example.com:1521/ORCL')

    See Also:
        Database.create() for more connection options
    """
    return Database.create('oracle', user=user, password=password, database=database,
                           host=host, port=port, driver=driver, **kwargs)


def mysql(user: str, password: Optional[str] = None, database: str = 'mysql',
          host: str = 'localhost', port: int = 3306, driver: str = None, **kwargs) -> Database:
    """
    Create a MySQL/MariaDB database connection.

    Automatically selects the best available MySQL driver (mysqlclient, mysql.connector,
    pymysql, or MySQLdb).

    Args:
        user: Database username
        password: Database password (prompts if None)
        database: Database name (default: 'mysql')
        host: Server hostname or IP (default: 'localhost')
        port: Server port (default: 3306)
        driver: Specific driver to use ('mysqlclient', 'mysql.connector', 'pymysql', 'MySQLdb')
        **kwargs: Additional driver-specific parameters (charset, ssl, etc.)

    Returns:
        Database connection object with context manager support

    Example
    -------
    ::
        >>> from dbtk.database import mysql
        >>> with mysql(user='root', password='pass', database='myapp') as db:
        ...     cursor = db.cursor()
        ...     cursor.execute("SELECT * FROM users")

    See Also:
        Database.create() for more connection options
    """
    return Database.create('mysql', user=user, password=password, database=database,
                           host=host, port=port, driver=driver, **kwargs)


def sqlserver(user: str, password: Optional[str] = None, database: str = None,
              host: str = 'localhost', port: int = 1433, **kwargs) -> Database:
    """
    Create a Microsoft SQL Server database connection.

    Automatically selects the best available SQL Server driver (pyodbc or pymssql).

    Args:
        user: Database username
        password: Database password (prompts if None)
        database: Database name
        host: Server hostname or IP (default: 'localhost')
        port: Server port (default: 1433)
        **kwargs: Additional driver-specific parameters (driver, encrypt, etc.)

    Returns:
        Database connection object with context manager support

    Example
    -------
    ::
        >>> from dbtk.database import sqlserver
        >>> db = sqlserver(user='sa', password='pass',
        ...                database='AdventureWorks', host='sqlserver.local')
        >>> cursor = db.cursor()

    Note:
        When using pyodbc, you may need to specify the ODBC driver:
        sqlserver(..., driver='ODBC Driver 17 for SQL Server')

    See Also:
        Database.create() for more connection options
    """
    return Database.create('sqlserver', user=user, password=password, database=database,
                           host=host, port=port, **kwargs)


def sqlite(database: str, **kwargs) -> Database:
    """
    Create a SQLite database connection.

    SQLite is a serverless, file-based database. Use ':memory:' for an in-memory database.

    Args:
        database: Path to database file or ':memory:' for in-memory database
        **kwargs: Additional sqlite3.connect() parameters (timeout, isolation_level, etc.)

    Returns:
        Database connection object with context manager support

    Example
    -------
    ::
        >>> from dbtk.database import sqlite
        >>> # File-based database
        >>> with sqlite('app.db') as db:
        ...     cursor = db.cursor()
        ...     cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        >>>
        >>> # In-memory database (useful for testing)
        >>> db = sqlite(':memory:')

    See Also:
        Database.create() for more connection options
        sqlite3 module documentation for additional parameters
    """
    import sqlite3

    connection = sqlite3.connect(database, **kwargs)
    return Database(connection, sqlite3, os.path.basename(database))
