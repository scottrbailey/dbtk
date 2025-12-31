# dbtk/writers/json.py
"""
JSON writer for database results.
"""
import json
import logging
from datetime import datetime, date, time
from typing import Union, List, Optional, Any
from pathlib import Path
from .base import BaseWriter, BatchWriter
from ..utils import to_string

logger = logging.getLogger(__name__)


class JSONWriter(BaseWriter):
    """JSON writer class that extends BaseWriter."""

    def __init__(self,
                 file: Optional[Union[str, Path]] = None,
                 data = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 indent: Optional[int] = 2,
                 **json_kwargs):
        """
        Initialize JSON writer.

        Args:
            file: Output filename. If None, writes to stdout
            data: Cursor object or list of records
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            indent: JSON indentation - defaults to 2 (pretty-print), 0 or None for compact
            **json_kwargs: Additional arguments passed to json.dump
        """
        # Preserve data types for JSON output
        super().__init__(data, file, columns, encoding, indent=indent, **json_kwargs)

    def to_string(self, obj: Any) -> Any:
        """Convert object to string. For JSON just convert dates and times. """
        if isinstance(obj, (datetime, date, time)):
            return to_string(obj)
        else:
            return obj

    def _write_data(self, file_obj) -> None:
        """Write JSON data to file object."""
        records = []
        for record in self.data_iterator:
            record_dict = self._row_to_dict(record)
            records.append(record_dict)
        self._row_num = len(records)
        json.dump(records, file_obj, **self._format_kwargs)


class NDJSONWriter(BatchWriter):
    """NDJSON (newline-delimited JSON) writer."""

    def __init__(self,
                 file: Optional[Union[str, Path]] = None,
                 data = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 **json_kwargs):
        """
        Initialize NDJSON writer.

        Args:
            file: Output filename. If None, writes to stdout
            data: Cursor object or list of records
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            **json_kwargs: Additional arguments passed to json.dumps
        """
        # NDJSON doesn't use indentation
        super().__init__(data, file, columns=columns, encoding=encoding, indent=None, **json_kwargs)

    def to_string(self, obj: Any) -> Any:
        """Convert object to string. For JSON just convert dates and times. """
        if isinstance(obj, (datetime, date, time)):
            return to_string(obj)
        else:
            return obj

    def _write_data(self, file_obj) -> None:
        """Write NDJSON data to file object."""
        for record in self.data_iterator:
            record_dict = self._row_to_dict(record)
            json_line = json.dumps(record_dict, **self._format_kwargs)
            file_obj.write(json_line + '\n')
            self._row_num += 1
        file_obj.flush()

def to_json(data,
            filename: Optional[Union[str, Path]] = None,
            encoding: str = 'utf-8',
            indent: Optional[int] = 2,
            **json_kwargs) -> None:
    """
    Export cursor or result set to JSON file as an array of dictionaries.

    Args:
        data: Cursor object or list of records
        filename: Output filename. If None, writes to stdout
        encoding: File encoding
        indent: JSON indentation - defaults to 2 (pretty-print), 0 or None for compact
        **json_kwargs: Additional arguments passed to json.dump

    Example:
        # Write to file as JSON array
        to_json(cursor, 'users.json')

        # Write to stdout
        to_json(cursor)

        # Compact format
        to_json(cursor, 'data.json', indent=None)
    """
    with JSONWriter(
        file=filename,
        data=data,
        encoding=encoding,
        indent=indent,
        **json_kwargs
    ) as writer:
        writer.write()


def to_ndjson(data,
              filename: Optional[Union[str, Path]] = None,
              encoding: str = 'utf-8',
              **json_kwargs) -> None:
    """
    Export cursor or result set to NDJSON (newline-delimited JSON) file.

    Args:
        data: Cursor object or list of records
        filename: Output filename. If None, writes to stdout
        encoding: File encoding
        **json_kwargs: Additional arguments passed to json.dumps

    Example:
        # Write to file as NDJSON
        to_ndjson(cursor, 'users.ndjson')

        # Write to stdout
        to_ndjson(cursor)
    """
    with NDJSONWriter(
        file=filename,
        data=data,
        encoding=encoding,
        **json_kwargs
    ) as writer:
        writer.write()