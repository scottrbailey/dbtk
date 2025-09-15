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

from .database import ParamStyle

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