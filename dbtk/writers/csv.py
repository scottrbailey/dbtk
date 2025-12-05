# dbtk/writers/csv.py
"""
CSV writer for database results.
"""

import csv
import logging
from typing import Union, List, Optional, Any, TextIO
from pathlib import Path

from .base import BaseWriter, to_string
from ..defaults import settings

logger = logging.getLogger(__name__)


class CSVWriter(BaseWriter):
    """CSV writer class that extends BaseWriter."""

    def __init__(self,
                 data,
                 file: Optional[Union[str, Path, TextIO]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 include_headers: bool = True,
                 delimiter: str = ',',
                 quotechar: str = '"',
                 null_string: str = None,
                 **csv_kwargs):
        """
        Initialize CSV writer.

        Args:
            data: Cursor object or list of records
            file: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            include_headers: Whether to include column headers
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            null_string: String representation for null values
            **csv_kwargs: Additional arguments passed to csv.writer
        """
        # Always convert to text for CSV output
        super().__init__(data, file, columns, encoding, preserve_types=False)
        self.include_headers = include_headers
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.null_string = null_string or settings.get('null_string_csv', '')
        self.csv_kwargs = csv_kwargs

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
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            **self.csv_kwargs
        )

        # Write headers if requested
        if self.include_headers:
            writer.writerow(self.columns)

        # Write data rows
        for record in self.data_iterator:
            row = self._extract_row_values(record)
            writer.writerow(row)
            self._row_num += 1

def to_csv(data,
           file: Optional[Union[str, Path]] = None,
           encoding: str = 'utf-8-sig',
           include_headers: bool = True,
           delimiter: str = ',',
           quotechar: str = '"',
           null_string: str = None,
           **csv_kwargs) -> None:
    """
    Export cursor or result set to CSV file.

    Args:
        data: Cursor object or list of records
        file: Output filename. If None, writes to stdout
        encoding: File encoding
        include_headers: Whether to include column headers
        delimiter: CSV field delimiter
        quotechar: CSV quote character
        null_string: String representation for null values
         **csv_kwargs: Additional arguments passed to csv.writer

    Example:
        # Write to file
        to_csv(cursor, 'users.csv')

        # Write to stdout
        to_csv(cursor)

        # Custom delimiter
        to_csv(cursor, 'data.tsv', delimiter='\t')
    """
    writer = CSVWriter(
        data=data,
        file=file,
        encoding=encoding,
        include_headers=include_headers,
        delimiter=delimiter,
        quotechar=quotechar,
        null_string=null_string,
        **csv_kwargs
    )
    writer.write()