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
    """
    Abstract base class for all data writers in DBTK.

    Provides common functionality for writing data to various formats (CSV, JSON, Excel, XML,
    etc.). Writers accept data from multiple sources - cursors, lists of Records, lists of
    dicts, or lists of lists - and handle the conversion to the target format automatically.

    All writers share common features like automatic column detection, stdout support for
    quick previews, configurable encodings, and optional type preservation. Writers are
    designed to work seamlessly with DBTK cursors and readers.

    Common Features
    ---------------
    * **Multiple data sources** - Cursors, Records, dicts, lists
    * **Automatic column detection** - From cursors, Record objects, or dict keys
    * **Stdout preview** - Write to console with automatic row limiting
    * **Configurable encoding** - UTF-8, Latin-1, etc.
    * **Type preservation** - Optionally keep native types vs converting to strings
    * **Consistent API** - Same interface regardless of output format

    Parameters
    ----------
    data
        Data to write. Accepts:

        * Cursor objects (from database queries)
        * List of Record objects (from readers)
        * List of dictionaries
        * List of lists (requires columns parameter)

    filename : str or Path, optional
        Output filename. If None, writes to stdout (limited to 20 rows for preview).
    columns : List[str], optional
        Column names for list-of-lists data. Ignored for other data types which
        have columns embedded.
    encoding : str, default 'utf-8'
        File encoding for text-based formats
    preserve_data_types : bool, default False
        If False, converts all values to strings. If True, preserves native Python
        types (useful for formats like JSON that support multiple types).
    **kwargs
        Additional format-specific arguments (passed to subclasses)

    Attributes
    ----------
    columns : List[str]
        Column names detected from data or provided explicitly
    _row_num : int
        Number of rows written (updated during write operation)

    Example
    -------
    ::

        # Subclasses implement specific formats
        from dbtk import writers

        # Write cursor results to CSV
        cursor.execute("SELECT * FROM users")
        writers.to_csv(cursor, 'users.csv')

        # Write list of records to JSON
        with readers.CSVReader(open('input.csv')) as reader:
            records = list(reader)
        writers.to_json(records, 'output.json')

        # Preview to stdout (shows first 20 rows)
        cursor.execute("SELECT * FROM large_table")
        writers.to_csv(cursor, None)  # Prints to console

        # Write with type preservation
        writers.to_json(cursor, 'data.json', preserve_data_types=True)

    See Also
    --------
    to_csv : Write CSV files
    to_json : Write JSON files
    to_excel : Write Excel files
    to_xml : Write XML files
    cursor_to_cursor : Database-to-database transfer

    Notes
    -----
    This is an abstract base class. Use one of the concrete implementations
    (CSVWriter, JSONWriter, etc.) or the convenience functions (to_csv, to_json, etc.).

    Subclasses must implement:

    * ``_write()`` - Perform the actual write operation

    When filename is None (stdout mode), output is automatically limited to 20 rows
    to prevent accidentally printing huge result sets to the console.
    """

    def __init__(self,
                 data,
                 filename: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 preserve_data_types: bool = False,
                 **kwargs):
        """
        Initialize the writer with data and options.

        Parameters
        ----------
        data
            Data source (cursor, list of records, etc.)
        filename : str or Path, optional
            Output file. None writes to stdout.
        columns : List[str], optional
            Column names for list-of-lists
        encoding : str, default 'utf-8'
            File encoding
        preserve_data_types : bool, default False
            Keep native types vs convert to strings
        **kwargs
            Format-specific arguments
        """
        self.data = data
        self.filename = filename
        self.encoding = encoding
        self.preserve_data_types = preserve_data_types
        self._row_num = 0

        # Setup data iterator and columns
        self.data_iterator, self.columns = self._get_data_iterator(data, columns)
        if not self.data_iterator:
            raise ValueError("No data to export")

        # Limit stdout output to 20 rows
        if filename is None:
            self.data_iterator = itertools.islice(self.data_iterator, 20)

    @property
    def row_count(self) -> int:
        """ Returns the number of rows written."""
        return self._row_num

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
            logger.info(f"Wrote {self._row_num} rows to {self.filename or 'stdout'}")
            return self._row_num
        except Exception as e:
            logger.error(f"Error writing data: {e}")
            raise
        finally:
            if should_close:
                file_obj.close()