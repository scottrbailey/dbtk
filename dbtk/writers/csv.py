# dbtk/writers/csv.py

import csv
import logging
from typing import Union, List, Optional, Any, TextIO
from pathlib import Path

from .base import BatchWriter, to_string
from ..defaults import settings

logger = logging.getLogger(__name__)


class CSVWriter(BatchWriter):
    """CSV writer class that extends BatchWriter."""

    def __init__(self,
                 file: Optional[Union[str, Path, TextIO]] = None,
                 data = None,
                 columns: Optional[List[str]] = None,
                 headers: Optional[List[str]] = None,
                 write_headers: bool = True,
                 null_string: str = None,
                 **csv_kwargs):
        """
        Initialize CSV writer.

        Args:
            file: Output filename. If None, writes to stdout
            data: Cursor object or list of records
            columns: Column names for list-of-lists data (optional for other types)
            headers: Header row text. If None, checks data.description for original column names,
                    then falls back to detected column names. Useful when field names have been
                    normalized but you want original database column names in the CSV header.
            encoding: File encoding
            write_headers: Whether to include column headers
            null_string: String representation for null values
            **csv_kwargs: Additional arguments passed to csv.writer
        """
        # Always convert to text for CSV output
        super().__init__(data, file, columns, write_headers=write_headers, **csv_kwargs)
        self.headers = headers
        self.null_string = null_string or settings.get('null_string_csv', '')

    def to_string(self, obj: Any) -> str:
        """Convert object to string for CSV output.
           Change settings['null_string_csv'] to change null value representation."""
        if obj is None:
            return self.null_string
        else:
            return to_string(obj)

    def _write_data(self, file_obj) -> None:
        """Write CSV data to file object."""
        writer = csv.writer(
            file_obj,
            **self._format_kwargs
        )

        # Write headers if requested
        if self.write_headers and not self._headers_written:
            # Determine header row: explicit headers → cursor.description → column names
            if self.headers:
                header_row = self.headers
            elif hasattr(self.data_iterator, 'description') and self.data_iterator.description:
                header_row = [col[0] for col in self.data_iterator.description]
            else:
                header_row = self.columns

            writer.writerow(header_row)
            self._headers_written = True

        # Write data rows
        for record in self.data_iterator:
            row = self._row_to_tuple(record)
            writer.writerow(row)
            self._row_num += 1

def to_csv(data,
           file: Optional[Union[str, Path]] = None,
           headers: Optional[List[str]] = None,
           write_headers: bool = True,
           null_string: str = None,
           **csv_kwargs) -> None:
    """
    Export cursor or result set to CSV file.

    Args:
        data: Cursor object or list of records
        file: Output filename. If None, writes to stdout
        headers: Header row text. If None, uses cursor.description or detected column names
        encoding: File encoding
        write_headers: Whether to include column headers
        null_string: String representation for null values
         **csv_kwargs: Additional arguments passed to csv.writer

    Example:
        # Write to file
        to_csv(cursor, 'users.csv')

        # Write to stdout
        to_csv(cursor)

        # Custom delimiter
        to_csv(cursor, 'data.tsv', delimiter='\t')

        # Override header names
        to_csv(cursor, 'users.csv', headers=['User ID', 'Full Name', 'Email'])
    """
    with CSVWriter(
        file=file,
        data=data,
        headers=headers,
        write_headers=write_headers,
        null_string=null_string,
        **csv_kwargs
    ) as writer:
        writer.write()