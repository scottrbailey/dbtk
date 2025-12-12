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
                 data,
                 file: Optional[Union[str, Path, TextIO]] = None,
                 columns: Optional[List[str]] = None,
                 write_headers: bool = True,
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
            null_string: String representation for null values
            **csv_kwargs: Additional arguments passed to csv.writer
        """
        # Always convert to text for CSV output
        super().__init__(data, file, columns, preserve_types=False, write_headers=write_headers, **csv_kwargs)
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
            writer.writerow(self.columns)
            self._headers_written = True

        # Write data rows
        for record in self.data_iterator:
            row = self._extract_row_values(record)
            writer.writerow(row)
            self._row_num += 1

def to_csv(data,
           file: Optional[Union[str, Path]] = None,
           include_headers: bool = True,
           null_string: str = None,
           **csv_kwargs) -> None:
    """
    Export cursor or result set to CSV file.

    Args:
        data: Cursor object or list of records
        file: Output filename. If None, writes to stdout
        encoding: File encoding
        include_headers: Whether to include column headers
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
        include_headers=include_headers,
        null_string=null_string,
        **csv_kwargs
    )
    writer.write()