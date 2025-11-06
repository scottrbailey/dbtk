# dbtk/writers/csv.py
"""
CSV writer for database results.
"""

import csv
import logging
from typing import Union, List, Optional
from pathlib import Path

from .base import BaseWriter

logger = logging.getLogger(__name__)


class CSVWriter(BaseWriter):
    """CSV writer class that extends BaseWriter."""

    def __init__(self,
                 data,
                 filename: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 include_headers: bool = True,
                 delimiter: str = ',',
                 quotechar: str = '"',
                 **csv_kwargs):
        """
        Initialize CSV writer.

        Args:
            data: Cursor object or list of records
            filename: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            include_headers: Whether to include column headers
            delimiter: CSV field delimiter
            quotechar: CSV quote character
            **csv_kwargs: Additional arguments passed to csv.writer
        """
        # Always convert to text for CSV output
        super().__init__(data, filename, columns, encoding, preserve_data_types=False)
        self.include_headers = include_headers
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.csv_kwargs = csv_kwargs

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
            self.row_count += 1


def to_csv(data,
           filename: Optional[Union[str, Path]] = None,
           encoding: str = 'utf-8',
           include_headers: bool = True,
           delimiter: str = ',',
           quotechar: str = '"',
           **csv_kwargs) -> None:
    """
    Export cursor or result set to CSV file.

    Args:
        data: Cursor object or list of records
        filename: Output filename. If None, writes to stdout
        encoding: File encoding
        include_headers: Whether to include column headers
        delimiter: CSV field delimiter
        quotechar: CSV quote character
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
        filename=filename,
        encoding=encoding,
        include_headers=include_headers,
        delimiter=delimiter,
        quotechar=quotechar,
        **csv_kwargs
    )
    writer.write()