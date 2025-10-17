# dbtk/utils.py
"""
Utility functions for dbtk.
"""

import logging
import re

from typing import Tuple, List, Any, Union, Dict, Iterable
try:
    from typing import Mapping
except ImportError:
    from collections.abc import Mapping


class ParamStyle:
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
        new_sql = re.sub(r':(\w+)', r'?', sql)
        return new_sql, param_names
    elif paramstyle == ParamStyle.FORMAT:
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