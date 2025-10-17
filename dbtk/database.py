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

from .cursors import Cursor, RecordCursor, TupleCursor, DictCursor, ParamStyle
from .defaults import settings

logger = logging.getLogger(__name__)

# users can define their own drivers in the config file
_user_drivers = {}


class CursorType:
    RECORD = 'record'
    TUPLE = 'tuple'
    DICT = 'dict'
    LIST = 'list'

    @classmethod
    def values(cls):
        return [getattr(cls, attr) for attr in dir(cls) if not attr.startswith('_')]


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
        'priority': 11,
        'param_map': {'host': 'SERVER', 'database': 'DATABASE', 'user': 'UID', 'password': 'PWD'},
        'required_params': [{'host', 'database', 'user'}],
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
        'priority': 14,
        'param_map': {'host': 'SERVER', 'database': 'DATABASE', 'user': 'UID', 'password': 'PWD'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'password', 'port'},
        'connection_method': 'odbc_string',
        'odbc_driver_name': 'PostgreSQL UNICODE',
        'default_port': 5432
    },
    'pyodbc_mysql': {
        'database_type': 'mysql',
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


def get_all_drivers() -> dict:
    """Get combined built-in and user drivers."""
    return {**DRIVERS, **_user_drivers}


def get_drivers_for_database(db_type: str, valid_only: bool = True) -> List[str]:
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
    all_drivers = get_all_drivers()
    available_drivers = []

    for driver_name, info in all_drivers.items():
        if info['database_type'] == db_type:
            if valid_only:
                # only add if the driver is available (importable)
                spec = importlib.util.find_spec(driver_name)
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
    available_drivers.sort(key=lambda d: DRIVERS[d]['priority'])
    return available_drivers


def get_db_type_for_driver(driver_name: str) -> str:
    """Get database type for a driver."""
    return get_all_drivers().get(driver_name, dict()).get('database_type')


def get_params_for_database(db_type: str, driver: str = None) -> set:
    """Get all valid parameters for a database type from DRIVERS metadata."""
    from .database import get_all_drivers

    all_drivers = get_all_drivers()
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
    all_drivers = get_all_drivers()
    for driver_name, driver_info in all_drivers.items():
        valid_db_types.add(driver_info['database_type'])
    return valid_db_types


def validate_connection_params(driver_name: str, config_only: bool = False, **params) -> dict:
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
        print(params)
        raise ValueError(f"Missing required parameters. Need one of: {driver_info['required_params']}")

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


def get_connection_string(**kwargs) -> str:
    """ Get connection string from keyword arguments."""
    return " ".join([f"{key}={value}" for key, value in kwargs.items()])


def get_odbc_connection_string(**kwargs) -> str:
    """ Get connection string for ODBC from keyword arguments."""
    odbc_driver_name = kwargs.pop('odbc_driver_name', None)
    server = '%s,%s'.format(kwargs.pop('host', 'localhost'), kwargs.pop('port'))
    params = {key.upper(): value for key, value in kwargs}
    params['SERVER'] = server
    if odbc_driver_name:
        return f"DRIVER={{{odbc_driver_name}}};" + ";".join([f"{key}={value}" for key, value in params.items()])
    else:
        return ";".join([f"{key}={value}" for key, value in params.items()])


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
    Database connection wrapper that provides uniform interface
    across different database adapters.
    """

    # Attributes stored locally, others delegated to _connection
    _local_attrs = [
        '_connection', 'server_type', 'database_name', 'interface',
        'name', 'placeholder'
    ]

    # Cursor type mapping
    CURSOR_TYPES = {
        CursorType.RECORD: RecordCursor,
        CursorType.TUPLE: TupleCursor,
        CursorType.DICT: DictCursor,
        CursorType.LIST: Cursor
    }

    def __init__(self, connection, interface, database_name: Optional[str] = None):
        """
        Initialize Database wrapper.

        Args:
            connection: Underlying database connection object
            interface: Database adapter module (psycopg2, cx_Oracle, etc.)
            database_name: Name of the database
        """
        self._connection = connection
        self.interface = interface
        self.database_name = database_name
        # self.name = database_name  # Can be overridden

        # Set parameter placeholder based on adapter style
        paramstyle = getattr(interface, 'paramstyle', ParamStyle.DEFAULT)
        self.placeholder = ParamStyle.get_placeholder(paramstyle)

        # Determine server type from interface name
        if interface.__name__ in DRIVERS:
            self.server_type = DRIVERS[interface.__name__]['database_type']
        else:
            self.server_type = 'unknown'


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
            return f'Database({self.database_name}:{self.server_type})'
        else:
            return f'Database({self.server_type})'

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()

    def cursor(self, cursor_type: Union[str, Type] = None, **kwargs) -> Cursor:
        """
        Create a cursor of the specified type.

        Args:
            cursor_type: Type of cursor ('record', 'tuple', 'dict', 'list')
                        or cursor class
            **kwargs: Additional arguments passed to cursor

        Returns:
            Cursor instance

        Examples:
            cursor = db.cursor()  # RecordCursor by default
            cursor = db.cursor('tuple')  # TupleCursor
            cursor = db.cursor(RecordCursor)  # Explicit class
        """
        if cursor_type is None:
            cursor_type = settings.get('default_cursor_type', CursorType.RECORD)
        if isinstance(cursor_type, str):
            if cursor_type not in CursorType.values():
                raise ValueError(
                    f"Invalid cursor type '{cursor_type}'. "
                    f"Must be one of: {CursorType.values()}"
                )
            cursor_class = self.CURSOR_TYPES[cursor_type]
        elif callable(cursor_type) and hasattr(cursor_type, 'fetchone'):
            cursor_class = cursor_type
        else:
            raise ValueError(f"Invalid cursor type: {cursor_type}")

        return cursor_class(self, **kwargs)

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.

        Example:
            with db.transaction():
                cursor = db.cursor()
                cursor.execute("INSERT ...")
                cursor.execute("UPDATE ...")
                # Auto-commit on success, rollback on exception
        """
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise

    def param_help(self) -> None:
        """Print help on this interface's parameter style."""
        print(f"{self.interface.__name__}'s parameter style is \"{self.interface.paramstyle}\"")
        print(f'"SELECT * FROM people WHERE name = {self.placeholder} AND age > {self.placeholder}", ("Smith", 30)')

        if self.interface.paramstyle == ParamStyle.NAMED:
            print(r'"SELECT * FROM people WHERE name = :name AND age > :age", {"name": "Smith", "age": 30}')
        elif self.interface.paramstyle == ParamStyle.PYFORMAT:
            print(r'"SELECT * FROM people WHERE name = %(name)s AND age > %(age)s", {"name": "Smith", "age": 30}')

    @classmethod
    def create(cls, db_type: str, driver: str  = None, **kwargs) -> 'Database':
        """
        Factory method to create database connections.

        Args:
            db_type: Database type ('postgres', 'oracle', 'mysql', etc.)
            **kwargs: Connection parameters

        Returns:
            Database instance
        """
        db_driver = None
        if driver:
            if driver not in DRIVERS:
                raise ValueError(f"Unknown driver: {driver}")
            if DRIVERS[driver]['database_type'] != db_type:
                raise ValueError(f"Driver '{driver}' is not compatible with database type '{db_type}'")
            try:
                db_driver = importlib.import_module(driver)
                driver_name = driver
            except ImportError:
                logger.warning(f"Driver '{driver}' not available, falling back to default")

        if db_driver is None:
            for driver_name in get_drivers_for_database(db_type):
                try:
                    db_driver = importlib.import_module(driver_name)
                    break
                except ImportError:
                    pass

        if db_driver is None:
            raise ImportError(f"No database driver found for database type '{db_type}'")

        params = validate_connection_params(driver_name,  **kwargs)
        if not params:
            raise ValueError("The connection parameters were not valid.")

        driver_conf = DRIVERS[driver_name]
        if driver_conf['connection_method'] == 'kwargs':
            connection = db_driver.connect(**params)
        elif driver_conf['connection_method'] == 'connection_string':
            connection = db_driver.connect(get_connection_string(**params))
        elif driver_conf['connection_method'] == 'dsn':
            if hasattr(db_driver, 'makedsn') and 'dsn' not in params:
                host = params.pop('host', 'localhost')
                port = params.pop('port', 5432)
                service_name = params.pop('service_name', None)
                params['dsn'] = db_driver.makedsn(host, port, service_name=service_name)
            connection = db_driver.connect(**params)
        elif driver_conf['connection_method'] == 'odbc_string':
            connection = db_driver.connect(get_odbc_connection_string(**params))

        if connection:
            return cls(connection, db_driver, params.get('database'))


def postgres(user: str, password: Optional[str] = None, database: str = 'postgres',
             host: str = 'localhost', port: int = 5432, driver: str = None, **kwargs) -> Database:
    """Create PostgreSQL connection."""
    return Database.create('postgres', user=user, password=password, database=database,
                           host=host, port=port, driver=driver, **kwargs)


def oracle(user: str, password: Optional[str] = None, database: str = None,
           host: Optional[str] = None, port: int = 1521, driver: str = None,  **kwargs) -> Database:
    """Create Oracle connection."""
    return Database.create('oracle', user=user, password=password, database=database,
                           host=host, port=port, driver=driver, **kwargs)


def mysql(user: str, password: Optional[str] = None, database: str = 'mysql',
          host: str = 'localhost', port: int = 3306, driver: str = None, **kwargs) -> Database:
    """Create MySQL connection."""
    return Database.create('mysql', user=user, password=password, database=database,
                           host=host, port=port, driver=driver, **kwargs)


def sqlserver(user: str, password: Optional[str] = None, database: str = None,
              host: str = 'localhost', port: int = 1433, **kwargs) -> Database:
    """Create SQL Server connection."""
    return Database.create('sqlserver', user=user, password=password, database=database,
                           host=host, port=port, **kwargs)


def sqlite(database: str, **kwargs) -> Database:
    """Create SQLite connection."""
    import sqlite3

    connection = sqlite3.connect(database, **kwargs)
    return Database(connection, sqlite3, os.path.basename(database))
