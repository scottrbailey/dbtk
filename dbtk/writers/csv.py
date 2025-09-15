# dbtk/writers/csv.py
"""
CSV writer for database results.
"""

import csv
import itertools
import logging
import sys
from typing import Union, List, TextIO, Optional
from pathlib import Path

from .utils import get_data_iterator, format_value

logger = logging.getLogger(__name__)

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

    Examples:
        # Write to file
        to_csv(cursor, 'users.csv')

        # Write to stdout
        to_csv(cursor)

        # Custom delimiter
        to_csv(cursor, 'data.tsv', delimiter='\t')
    """
    data_iterator, columns = get_data_iterator(data)
    if filename is None:
        # limit to 20 rows when writing to stdout
        data_iterator = itertools.islice(data_iterator, 20)

    row_count = 0
    # Determine output destination
    if filename is None:
        file_obj = sys.stdout
        close_file = False
    else:
        file_obj = open(filename, 'w', newline='', encoding=encoding)
        close_file = True

    try:
        writer = csv.writer(
            file_obj,
            delimiter=delimiter,
            quotechar=quotechar,
            **csv_kwargs
        )

        # Write headers if requested
        if include_headers:
            writer.writerow([col.upper() for col in columns])

        # Write data rows
        for record in data_iterator:
            writer.writerow([
                format_value(record[i] if hasattr(record, '__getitem__') else record.__getattribute__(columns[i]))
                for i in range(len(columns))
            ])
            row_count += 1
        logger.info(f"Wrote {row_count} rows to {filename or 'stdout'}")
    finally:
        if close_file:
            file_obj.close()