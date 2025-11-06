# dbtk/readers/csv.py

"""CSV file reader with flexible delimiter and quoting support."""

import csv
import io
from typing import TextIO, List, Any, Iterator, Optional
from .base import Reader, Clean, ReturnType


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
        Add 'rownum' field to each record with 1-based row number
    clean_headers : Clean, default Clean.DEFAULT
        Header cleaning level. See Clean enum for options.
    skip_records : int, default 0
        Number of data rows to skip after headers
    max_records : int, optional
        Maximum records to read, None for all
    return_type : str, default 'record'
        'record' for Record objects, 'dict' for OrderedDict
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

        # Return dictionaries instead of Records
        with readers.CSVReader(open('users.csv'), return_type='dict') as reader:
            for user_dict in reader:
                print(user_dict['name'])

        # Skip first 10 data rows, read only 100 records
        with readers.CSVReader(open('large.csv'),
                              skip_records=10,
                              max_records=100) as reader:
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
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 return_type: str = ReturnType.DEFAULT,
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
        add_rownum : bool, default True
            Add rownum field to records
        clean_headers : Clean, default Clean.DEFAULT
            Header cleaning level
        skip_records : int, default 0
            Data rows to skip after headers
        max_records : int, optional
            Maximum records to read
        return_type : str, default 'record'
            'record' or 'dict'
        **kwargs
            Additional csv.reader() arguments (delimiter, quotechar, etc.)
        """
        if kwargs.get('delimiter') == '\t' and dialect == csv.excel:
            dialect = csv.excel_tab
            kwargs.pop('delimiter')
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers,
                         skip_records=skip_records, max_records=max_records,
                         return_type=return_type)
        fp = io.TextIOWrapper(fp.buffer, encoding=fp.encoding or 'utf-8', newline='') if hasattr(fp, 'buffer') else fp
        self.fp = fp
        self._trackable = fp.buffer if hasattr(fp, 'buffer') else fp
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