# dbtk/readers/csv.py

"""CSV file reader with flexible delimiter and quoting support."""

import csv
from typing import TextIO, List, Any, Iterator, Optional
from .base import Reader, Clean, logger


class CSVReader(Reader):
    """
    Read CSV (Comma-Separated Values) files with flexible formatting options.

    CSVReader provides a simple, consistent interface for reading CSV files with support
    for various delimiters, quoting styles, and header handling. It handles messy real-world
    CSV files by providing automatic header cleaning, custom dialects, and the ability to
    override headers entirely.

    The reader returns Record objects by default (supporting attribute, key, and index
    access) or plain dictionaries if preferred. It automatically handles tab-delimited
    files when delimiter='\\t' is specified.

    Parameters
    ----------
    fp : file-like object
        Open file pointer to CSV file (from open() or similar)
    dialect : csv.Dialect, default csv.excel
        CSV dialect defining formatting rules. Common options:

        * ``csv.excel`` - Standard CSV format (comma delimiter, quoted strings)
        * ``csv.excel_tab`` - Tab-delimited format
        * ``csv.unix_dialect`` - Unix-style CSV (LF line endings)

    headers : List[str], optional
        Custom header names to use instead of reading from first row. Useful when
        CSV has no header row or you want to rename columns.
    add_rownum : bool, default True
        Add '_row_num' field to each record with 1-based row number
    clean_headers : Clean, default Clean.DEFAULT
        Header cleaning level. See Clean enum for options.
    skip_records : int, default 0
        Number of data rows to skip after headers
    max_records : int, optional
        Maximum records to read, None for all
    **kwargs
        Additional arguments passed to csv.reader() like delimiter, quotechar, etc.

    Example
    -------
    ::

        from dbtk import readers

        # Basic CSV reading
        with readers.CSVReader(open('users.csv')) as reader:
            for user in reader:
                print(f"{user.name}: {user.email}")

        # Tab-delimited file
        with readers.CSVReader(open('data.tsv'), delimiter='\\t') as reader:
            for record in reader:
                process(record)

        # Custom delimiter and quoting
        with readers.CSVReader(open('data.txt'),
                              delimiter='|',
                              quotechar='"',
                              quoting=csv.QUOTE_MINIMAL) as reader:
            for record in reader:
                print(record)

        # Provide custom headers (file has no header row)
        headers = ['id', 'name', 'email', 'created']
        with readers.CSVReader(open('data.csv'), headers=headers) as reader:
            for record in reader:
                print(record.id, record.name)

        # Skip first 10 data rows, read only 100 rows
        with readers.CSVReader(open('large.csv'),
                              skip_rows=10,
                              n_rows=100) as reader:
            data = list(reader)

    See Also
    --------
    Reader : Base class with common reader features
    readers.get_reader : Automatic reader selection based on file extension
    Clean : Header cleaning options

    Notes
    -----
    * Automatically converts '\\t' delimiter to excel_tab dialect
    * Headers are read from first row unless custom headers provided
    * File pointer is automatically closed when used as context manager
    """

    def __init__(self,
                 fp: TextIO,
                 dialect=csv.excel,
                 headers: Optional[List[str]] = None,
                 add_row_num: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_rows: int = 0,
                 n_rows: Optional[int] = None,
                 null_values=None,
                 **kwargs):
        """
        Initialize CSV reader for a file.

        Parameters
        ----------
        fp : file-like object
            Open file pointer to CSV file
        dialect : csv.Dialect, default csv.excel
            CSV dialect (excel, excel_tab, unix_dialect, etc.)
        headers : List[str], optional
            Custom headers to use instead of reading from file
        add_row_num : bool, default True
            Add _row_num field to records
        clean_headers : Clean, default Clean.DEFAULT
            Header cleaning level
        skip_rows : int, default 0
            Data rows to skip after headers
        n_rows : int, optional
            Maximum rows to read
        null_values : str, list, tuple, or set, optional
            Values to convert to None (e.g., '\\N' for IMDB files)
        **kwargs
            Additional csv.reader() arguments (delimiter, quotechar, etc.)
        """
        if kwargs.get('delimiter') == '\t' and dialect == csv.excel:
            dialect = csv.excel_tab
            kwargs.pop('delimiter')
        super().__init__(add_row_num=add_row_num, clean_headers=clean_headers,
                         skip_rows=skip_rows, n_rows=n_rows,
                         headers=headers, null_values=null_values)
        self.fp = fp
        if hasattr(fp, 'encoding') and fp.encoding == 'utf-8':
            # Using the standard utf-8 encoding can cause issues with BOM headers in column names
            logger.warning("utf-8 encoding detected. Consider using 'utf-8-sig' encoding instead.")

        # Set trackable for progress tracking
        if hasattr(fp, '_uncompressed_size'):
            # Compressed file - use buffer's tell() but preserve _uncompressed_size
            self._trackable = fp.buffer
            self._trackable._uncompressed_size = fp._uncompressed_size
        elif hasattr(fp, 'buffer'):
            # Text mode file - use buffer for better performance
            self._trackable = fp.buffer
        else:
            # Binary mode or other file type
            self._trackable = fp

        self._rdr = csv.reader(fp, dialect=dialect, **kwargs)
        self._headers_read = False

    def _read_headers(self) -> List[str]:
        """Read the header row from the CSV file or use provided headers.

        Returns:
            List of header values.

        Raises:
            StopIteration: If the file is empty and no headers are provided.
        """
        if self._raw_headers is not None:
            self._headers_read = True
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