# dbtk/utils.py
"""
Utility functions for dbtk.
"""

import logging
import re
import datetime as dt
from typing import Tuple, List, Any, Union, Dict, Iterable
from .defaults import settings
try:
    from typing import Mapping
except ImportError:
    from collections.abc import Mapping

MIDNIGHT = dt.time(0, 0, 0)
# cache format strings for performance
_format_cache = None

class ParamStyle:
    """
    SQL parameter placeholder styles for different database drivers.

    Different database drivers use different parameter placeholder formats.
    This class provides constants and utilities for working with these formats:

    - QMARK: Question mark placeholders (?, ?) - SQLite, ODBC
    - NUMERIC: Numeric placeholders (:1, :2) - Oracle
    - NAMED: Named placeholders (:name, :email) - Oracle, psycopg2
    - FORMAT: Printf-style (%s, %s) - MySQL (MySQLdb)
    - PYFORMAT: Python format (%(name)s) - psycopg2, pymysql

    Methods:
        values(): Get all available parameter styles
        positional_styles(): Get styles that require tuples
        named_styles(): Get styles that require dicts
        get_placeholder(paramstyle): Get placeholder string for a style

    Example
    -------
    ::
        >>> ParamStyle.get_placeholder('qmark')
        '?'
        >>> ParamStyle.get_placeholder('named')
        ':1'
    """
    QMARK = 'qmark'         # id = ?
    NUMERIC = 'numeric'     # id = :1
    NAMED = 'named'         # id = :id  also :1 for positional
    FORMAT = 'format'       # id = %s
    PYFORMAT = 'pyformat'   # id = %(id)s also %s for positional
    DEFAULT = NAMED

    @classmethod
    def values(cls):
        return [getattr(cls, attr) for attr in dir(cls) if not attr.startswith('_')]

    @classmethod
    def positional_styles(cls):
        """ Parameter styles where parameters must be in properly ordered tuple instead of dict"""
        return (cls.QMARK, cls.NUMERIC, cls.FORMAT)

    @classmethod
    def named_styles(cls):
        """ Parameter styles where parameters must be in dict instead of tuple"""
        return (cls.NAMED, cls.PYFORMAT)

    @classmethod
    def get_placeholder(cls, paramstyle: str) -> str:
        if paramstyle == cls.QMARK:
            return '?'
        elif paramstyle == cls.FORMAT:
            return '%s'
        elif paramstyle == cls.NUMERIC:
            return ':1'
        elif paramstyle == cls.NAMED:
            # adapters that use named parameters can also use :1 for positional parameters
            return ':1'
        elif paramstyle == cls.PYFORMAT:
            # adapters that use pyformat parameters can also use %s for positional parameters
            return '%s'
        return ''


class QueryLogger:
    """Simple query logger."""

    def __init__(self, level: int = logging.INFO):
        """
        Initialize query logger.

        Args:
            level: Logging level
        """
        self.logger = logging.getLogger('dbtk.queries')
        self.logger.setLevel(level)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def __call__(self, connection: str, query: str, params: Any = None) -> None:
        """
        Log a query.

        Args:
            connection: Connection identifier
            query: SQL query
            params: Query parameters
        """
        message = f"[{connection}] {query}"
        if params:
            message += f" | Params: {params}"

        self.logger.info(message)

def _build_format_strings():
    """Build format strings for datetime and date objects."""
    return {
        'date': settings.get('date_format', '%Y-%m-%d'),
        'datetime': settings.get('datetime_format', '%Y-%m-%d %H:%M:%S'),
        'datetime_tz': settings.get('datetime_format', '%Y-%m-%d %H:%M:%S') + \
                       settings.get('tz_suffix',  '%z'),
        'timestamp': settings.get('timestamp_format', '%Y-%m-%d %H:%M:%S.%f'),
        'timestamp_tz': settings.get('timestamp_format', '%Y-%m-%d %H:%M:%S.%f') + \
                        settings.get('tz_suffix',  '%z'),
        'time': settings.get('time_format', '%H:%M:%S'),
        'time_micro': settings.get('time_format', '%H:%M:%S') + '.%f',
        'time_tz': settings.get('time_format', '%H:%M:%S') + \
                   settings.get('tz_suffix',  '%z'),
        'null': settings.get('null_string', ''),
    }

def reset_format_cache():
    """Clear format cache to force rebuilding on next call."""
    global _format_cache
    _format_cache = None

def _get_format_strings():
    global _format_cache
    if _format_cache is None:
        _format_cache = _build_format_strings()
    return _format_cache

def to_string(obj: Any) -> str:
    """
    Convert a value to string representation.

    Args:
        obj: Value to convert

    Returns:
        String representation
    """
    fmts = _get_format_strings()
    if obj is None:
        return fmts['null']
    elif isinstance(obj, dt.datetime):
        if obj.microsecond:
            if obj.tzinfo:
                return obj.strftime(fmts['timestamp_tz'])
            else:
                return obj.strftime(fmts['timestamp'])
        else:
            if obj.tzinfo:
                return obj.strftime(fmts['datetime_tz'])
            if obj.time() == MIDNIGHT:
                return obj.strftime(fmts['date'])
            else:
                return obj.strftime(fmts['datetime'])
    elif isinstance(obj, dt.date):
        return obj.strftime(fmts['date'])
    elif isinstance(obj, dt.time):
        if obj.microsecond:
            if obj.tzinfo:
                return obj.strftime(fmts['time_tz'])
            else:
                return obj.strftime(fmts['time_micro'])
        else:
            if obj.tzinfo:
                return obj.strftime(fmts['time_tz'])
            else:
                return obj.strftime(fmts['time'])
    elif isinstance(obj, (int, float)):
        return str(obj)
    elif isinstance(obj, str):
        return obj
    elif hasattr(obj, 'read'):
        # Handle LOB objects
        return str(obj.read())
    else:
        return str(obj)

def wrap_at_comma(text: str) -> str:
    """Wrap text at commas, avoiding breaks inside parentheses."""
    # Split on parentheses to identify protected regions
    parts = re.split(r'(\([^)]*\))', text)

    wrapped_parts = []
    for i, part in enumerate(parts):
        if i % 2 == 0:  # Outside parentheses
            wrapped = re.sub(r'(.{70}[^,]*), ', r'\1,\n    ', part)
            wrapped_parts.append(wrapped)
        else:  # Inside parentheses - don't wrap
            wrapped_parts.append(part)

    return ''.join(wrapped_parts)


def process_sql_parameters(sql: str, paramstyle: str) -> Tuple[str, Tuple[str, ...]]:
    """
    Process SQL parameters according to the specified paramstyle.
    Always extracts parameter names; converts SQL format if needed.

    Parameters:
        sql: The SQL query string containing named parameters in the format ':name'.
        paramstyle: The desired parameter style for the resulting SQL string.

    Returns:
        A tuple containing the processed SQL query string and a tuple of all named parameters
        extracted in the order in which they appear in the original query.

    Raises:
        ValueError: If the provided paramstyle is not supported.
    """
    # Extract parameter names in order of appearance
    param_names = tuple(re.findall(r':(\w+)', sql))

    if paramstyle == ParamStyle.NAMED:
        # No conversion needed
        return sql, param_names
    elif paramstyle == ParamStyle.PYFORMAT:
        # Convert :param to %(param)s
        new_sql = re.sub(r':(\w+)', r'%(\1)s', sql)
        return new_sql, param_names
    elif paramstyle == ParamStyle.QMARK:
        # Convert :param to ?
        new_sql = re.sub(r':(\w+)', r'?', sql)
        return new_sql, param_names
    elif paramstyle == ParamStyle.FORMAT:
        # Convert :param to %s
        new_sql = re.sub(r':(\w+)', r'%s', sql)
        return new_sql, param_names
    elif paramstyle == ParamStyle.NUMERIC:
        counter = iter(range(1, len(param_names) + 1))
        new_sql = re.sub(r':(\w+)', lambda m: f':{next(counter)}', sql)
        return new_sql, param_names
    else:
        raise ValueError(f"Unsupported paramstyle: {paramstyle}")

def validate_identifier(identifier: str, max_length: int = 64) -> str:
    """
    Validate that an identifier is safe for use (even if it needs quoting).
    Returns the identifier if valid, raises ValueError if invalid.
    """
    if '.' in identifier:
        # Split and recursively validate each part
        parts = identifier.split('.')
        validated_parts = [validate_identifier(part, max_length) for part in parts]
        return '.'.join(validated_parts)

    # Single identifier validation
    if not identifier:
        raise ValueError("Invalid identifier: cannot be empty")
    if not identifier[0].isalpha():
        raise ValueError(f"Invalid identifier: must start with a letter: {identifier}")
    if len(identifier) > max_length:
        raise ValueError(f"Invalid identifier: exceeds max length of {max_length}")

    # Check for characters/sequences that could enable injection or break SQL parsing
    dangerous_patterns = ['\x00', '\n', '\r', '"', ';', '\x1a', '--', '/*', '*/']
    for pattern in dangerous_patterns:
        if pattern in identifier:
            raise ValueError(f"Invalid identifier: contains dangerous pattern '{pattern}': {identifier}")

    if identifier.startswith(' ') or identifier.endswith(' '):
        raise ValueError(f"Invalid identifier: has leading/trailing spaces: {identifier}")

    return identifier


def identifier_needs_quoting(identifier: str) -> bool:
    """Check if identifier needs quoting."""
    return not re.match(r'^([a-z][a-z0-9_]*|[A-Z][A-Z0-9_]*)$', identifier)


def quote_identifier(identifier: str) -> str:
    """Quote identifier, handling qualified names by splitting on dots."""
    if '.' in identifier:
        # Split and recursively quote each part
        parts = identifier.split('.')
        quoted_parts = [quote_identifier(part) for part in parts]
        return '.'.join(quoted_parts)

    # Single identifier quoting
    if identifier_needs_quoting(identifier):
        return f'"{identifier}"'
    else:
        return identifier

def sanitize_identifier(name: str, idx: int = 0) -> str:
    """Sanitize an identifier/column name."""
    if name is None or name == '':
        return f'col_{idx + 1}'

    # Replace non-alphanumeric chars with underscore, collapse multiple underscores
    sanitized = re.sub(r'[^a-z0-9_]+', '_', name.lower())

    # Ensure it starts with a letter
    if not sanitized[0].isalpha():
        sanitized = 'col_' + sanitized

    # Remove trailing underscore if present
    return sanitized.rstrip('_')


def batch_iterable(iterable: Iterable[Any], batch_size: int) -> Iterable[List[Any]]:
    """
    Batch an iterable into chunks of specified size.

    Args:
        iterable: The iterable to batch
        batch_size: Size of each batch

    Yields:
        Lists of items up to batch_size length
    """
    import itertools

    iterator = iter(iterable)
    while True:
        batch = list(itertools.islice(iterator, batch_size))
        if not batch:
            break
        yield batch


# For Python 3.6 compatibility, define type aliases
RecordLike = Union[Dict[str, Any], Mapping]