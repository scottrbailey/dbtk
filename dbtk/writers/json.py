# dbtk/writers/json.py
"""
JSON writer for database results.
"""
import json
import logging
from datetime import datetime, date, time
from typing import Union, List, Optional
from pathlib import Path

from .base import BaseWriter

logger = logging.getLogger(__name__)


class JSONWriter(BaseWriter):
    """JSON writer class that extends BaseWriter."""

    def __init__(self,
                 data,
                 filename: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 indent: Optional[int] = 2,
                 **json_kwargs):
        """
        Initialize JSON writer.

        Args:
            data: Cursor object or list of records
            filename: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            indent: JSON indentation - defaults to 2 (pretty-print), 0 or None for compact
            **json_kwargs: Additional arguments passed to json.dump
        """
        # Preserve data types for JSON output
        super().__init__(data, filename, columns, encoding, preserve_data_types=True)
        self.indent = indent
        self.json_kwargs = json_kwargs

    def _row_to_dict(self, record) -> dict:
        """Convert record to dict with dates converted to strings for JSON."""
        record_dict = super()._row_to_dict(record)
        for key, value in record_dict.items():
            if isinstance(value, (datetime, date, time)):
                record_dict[key] = self.to_string(value)
        return record_dict

    def _write_data(self, file_obj) -> None:
        """Write JSON data to file object."""
        records = [self._row_to_dict(record) for record in self.data_iterator]
        self.row_count = len(records)
        json.dump(records, file_obj, indent=self.indent, **self.json_kwargs)


class NDJSONWriter(JSONWriter):
    """NDJSON (newline-delimited JSON) writer that extends JSONWriter."""

    def __init__(self,
                 data,
                 filename: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 **json_kwargs):
        """
        Initialize NDJSON writer.

        Args:
            data: Cursor object or list of records
            filename: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            **json_kwargs: Additional arguments passed to json.dumps
        """
        # NDJSON doesn't use indentation
        super().__init__(data, filename, columns, encoding, indent=None, **json_kwargs)

    def _write_data(self, file_obj) -> None:
        """Write NDJSON data to file object."""
        for record in self.data_iterator:
            record_dict = self._row_to_dict(record)
            json_line = json.dumps(record_dict, **self.json_kwargs)
            file_obj.write(json_line + '\n')
            self.row_count += 1
            if self.row_count % 1000 == 0:
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
    writer = JSONWriter(
        data=data,
        filename=filename,
        encoding=encoding,
        indent=indent,
        **json_kwargs
    )
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
    writer = NDJSONWriter(
        data=data,
        filename=filename,
        encoding=encoding,
        **json_kwargs
    )
    writer.write()