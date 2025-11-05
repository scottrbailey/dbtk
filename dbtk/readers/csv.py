# dbtk/readers/csv.py

"""CSV file reader with flexible delimiter and quoting support."""

import csv
import io
from typing import TextIO, List, Any, Iterator, Optional
from .base import Reader, Clean, ReturnType


class CSVReader(Reader):
    """CSV file reader that returns Record objects or OrderedDict objects."""

    def __init__(self,
                 fp: TextIO,
                 dialect=csv.excel,
                 headers: Optional[List[str]] = None,
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 return_type: str = ReturnType.DEFAULT,
                 **kwargs):
        """Initialize CSV reader.

        Args:
            fp: File pointer to CSV file.
            dialect: CSV dialect (excel, excel_tab, unixuno_dialect, etc.) (default: csv.excel).
            headers: Optional list of header names to use instead of reading from the first row.
            add_rownum: If True, adds a rownum field to each record (default: True).
            clean_headers: Header cleaning level from Clean enum (default: Clean.DEFAULT).
            skip_records: Number of data records to skip after headers (default: 0).
            max_records: Maximum number of records to read, or None for all (default: None).
            return_type: Either 'record' for Record objects or 'dict' for OrderedDict.
            **kwargs: Additional arguments passed to csv.reader.
        """
        if kwargs.get('delimiter') == '\t' and dialect == csv.excel:
            dialect = csv.excel_tab
            kwargs.pop('delimiter')
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers,
                         skip_records=skip_records, max_records=max_records,
                         return_type=return_type)
        fp = io.TextIOWrapper(fp.buffer, encoding=fp.encoding or 'utf-8', newline='') if hasattr(fp, 'buffer') else fp
        self.fp = fp
        self._trackable = self.fp
        self._rdr = csv.reader(fp, dialect=dialect, **kwargs)
        self._headers_read = False
        self._raw_headers = headers  # Use provided headers if given

    def _read_headers(self) -> List[str]:
        """Read the header row from the CSV file or use provided headers.

        Returns:
            List of header values.

        Raises:
            StopIteration: If the file is empty and no headers are provided.
        """
        if self._raw_headers is not None:
            return self._raw_headers
        if not self._headers_read:
            try:
                self._raw_headers = next(self._rdr)
                self._headers_read = True
            except StopIteration:
                self._raw_headers = []
                self._headers_read = True
                raise ValueError("Empty CSV file")
        return self._raw_headers

    def _generate_rows(self) -> Iterator[List[Any]]:
        """Yield data rows from the CSV file, skipping _start_row number of rows.

        Yields:
            List of values for each data row.
        """
        # Yield remaining rows
        yield from self._rdr

    def _cleanup(self):
        """Close the file pointer."""
        if self.fp and hasattr(self.fp, 'close'):
            self.fp.close()