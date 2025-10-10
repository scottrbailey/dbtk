# dbtk/writers/base.py
"""
Base class for data writers with common file handling and data extraction patterns.
"""

import datetime as dt
import itertools
import logging
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

MIDNIGHT = dt.time(0, 0, 0)


class BaseWriter(ABC):
    """Base class for data writers with common file handling and iteration patterns."""

    def __init__(self,
                 data,
                 filename: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 preserve_data_types: bool = False,
                 **kwargs):
        """
        Initialize the base writer.

        Args:
            data: Cursor object or list of records
            filename: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (ignored for other types)
            encoding: File encoding
            preserve_data_types: If False, convert all values to strings
            **kwargs: Additional arguments for subclass writers
        """
        self.data = data
        self.filename = filename
        self.encoding = encoding
        self.preserve_data_types = preserve_data_types
        self.row_count = 0

        # Setup data iterator and columns
        self.data_iterator, self.columns = self._get_data_iterator(data, columns)
        if not self.data_iterator:
            raise ValueError("No data to export")

        # Limit stdout output to 20 rows
        if filename is None:
            self.data_iterator = itertools.islice(self.data_iterator, 20)

    def _get_file_handle(self, mode='w'):
        """
        Get file handle, returning stdout if filename is None.

        Returns:
            Tuple of (file_obj, should_close)
        """
        if self.filename is None:
            return sys.stdout, False
        else:
            return open(self.filename, mode, encoding=self.encoding, newline=''), True

    def _get_data_iterator(self, data, columns: Optional[List[str]] = None) -> Tuple[Iterator, List[str]]:
        """
        Get data iterator and column names.

        Args:
            data: Input data (cursor, list, etc.)
            columns: Optional column names for list-of-lists data

        Returns:
            Tuple of (iterator, column_names)
        """
        if not data:
            return None, None
        elif hasattr(data, 'fetchall'):  # Cursor
            if hasattr(data, 'columns'):
                data_columns = data.columns()
            elif hasattr(data, 'description'):
                data_columns = [col[0] for col in data.description]
            else:
                data_columns = []
            return data, data_columns
        elif isinstance(data, (list, tuple)):
            if not data:
                return None, None
            if hasattr(data[0], 'keys'):
                # dict and Record - use intrinsic keys
                data_columns = list(data[0].keys())
            elif hasattr(data[0], '_fields'):
                # namedtuple - use intrinsic field names
                data_columns = list(data[0]._fields)
            else:
                # list-of-lists - use provided columns or generate
                if columns:
                    if len(columns) != len(data[0]):
                        raise ValueError(f"Column count ({len(columns)}) must match data width ({len(data[0])})")
                    data_columns = columns
                else:
                    data_columns = [f'col_{x:03d}' for x in range(1, len(data[0]) + 1)]
            return iter(data), data_columns
        return None, None

    def to_string(self, obj: Any) -> str:
        """
        Convert a database value to string representation.

        Args:
            obj: Value to convert

        Returns:
            String representation
        """
        if obj is None:
            return ''
        elif isinstance(obj, dt.datetime):
            if obj.microsecond:
                if obj.tzinfo:
                    return obj.strftime('%Y-%m-%d %H:%M:%S.%f %z')
                else:
                    return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                if obj.tzinfo:
                    return obj.strftime('%Y-%m-%d %H:%M:%S %z')
                if obj.time() == MIDNIGHT:
                    return obj.strftime('%Y-%m-%d')
                else:
                    return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, dt.date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, dt.time):
            if obj.microsecond:
                if obj.tzinfo:
                    return obj.strftime('%H:%M:%S.%f %z')
                else:
                    return obj.strftime('%H:%M:%S.%f')
            else:
                if obj.tzinfo:
                    return obj.strftime('%H:%M:%S %z')
                else:
                    return obj.strftime('%H:%M:%S')
        elif isinstance(obj, (int, float)):
            return str(obj)
        elif isinstance(obj, str):
            return obj
        elif hasattr(obj, 'read'):
            # Handle LOB objects
            return str(obj.read())
        else:
            return str(obj)

    def _row_to_dict(self, record) -> dict:
        """
        Convert record to dictionary.

        Args:
            record: Record object, namedtuple, dict, list, etc.

        Returns:
            Dictionary representation
        """
        if hasattr(record, 'to_dict'):
            return record.to_dict()
        elif hasattr(record, '_asdict'):
            return record._asdict()
        elif hasattr(record, 'keys') and callable(record.keys):
            return {key: record[key] for key in record.keys()}
        elif isinstance(record, (list, tuple)):
            return {self.columns[i]: record[i] for i in range(min(len(self.columns), len(record)))}
        else:
            return {col: getattr(record, col, None) for col in self.columns}

    def _extract_row_values(self, record) -> List[Any]:
        """
        Extract values from record with optional text conversion.

        Args:
            record: Record object, namedtuple, dict, list, etc.

        Returns:
            List of values in column order
        """
        values = []
        for i, col in enumerate(self.columns):
            if hasattr(record, '__getitem__'):
                # dict-like (Record, dict) or list-like (list, tuple, namedtuple)
                value = record[i] if isinstance(record, (list, tuple)) else record[col]
            else:
                # Fallback for objects without __getitem__ that only support attribute access
                value = getattr(record, col, None)

            if not self.preserve_data_types:
                value = self.to_string(value)
            values.append(value)

        return values

    @abstractmethod
    def _write_data(self, file_obj) -> None:
        """
        Write the actual data. Subclasses implement format-specific logic.

        Args:
            file_obj: File object to write to
        """
        pass

    def write(self) -> int:
        """
        Main entry point for writing data.

        Returns:
            Number of rows written
        """
        file_obj, should_close = self._get_file_handle()
        try:
            self._write_data(file_obj)
            logger.info(f"Wrote {self.row_count} rows to {self.filename or 'stdout'}")
            return self.row_count
        except Exception as e:
            logger.error(f"Error writing data: {e}")
            raise
        finally:
            if should_close:
                file_obj.close()