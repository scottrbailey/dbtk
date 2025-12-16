# dbtk/writers/base.py
"""
Base classes for data writers with common file handling and data extraction patterns.
"""

import itertools
import logging
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator, List, Optional, TextIO, Union

from ..utils import RecordLike, to_string

logger = logging.getLogger(__name__)


class BaseWriter(ABC):
    """
    Abstract base class for all data writers in DBTK.

    Provides common functionality for writing data to various formats (CSV, JSON, Excel, XML,
    etc.). Writers accept data from multiple sources - cursors, lists of Records, lists of
    dicts, or lists of lists - and handle the conversion to the target format automatically.

    All writers share common features like automatic column detection, stdout support for
    quick previews, configurable encodings, and optional type preservation. Writers are
    designed to work seamlessly with DBTK cursors and readers.

    Common Features
    ---------------
    * **Multiple data sources** - Cursors, Records, dicts, lists
    * **Automatic column detection** - From cursors, Record objects, or dict keys
    * **Stdout preview** - Write to console with automatic row limiting
    * **Configurable encoding** - UTF-8, Latin-1, etc.
    * **Type preservation** - Optionally keep native types vs converting to strings
    * **Consistent API** - Same interface regardless of output format

    Parameters
    ----------
    data : Iterable[RecordLike]
        Data to write. Accepts:

        * Cursor objects (from database queries)
        * List of Record objects (from readers)
        * List of dictionaries
        * List of lists/tuples (requires columns parameter)

    file : str, Path, TextIO, or BinaryIO, optional
        Output filename or file handle. If None, writes to stdout (limited to 20 rows for preview).
    columns : List[str], optional
        Column names for list-of-lists data. Ignored for other data types which
        have columns embedded.
    encoding : str, default 'utf-8-sig'
        File encoding for text-based formats
    write_headers : bool, default True
        If True, include header row in formats that support it.
    **fmt_kwargs
        Additional format-specific arguments (passed to subclasses)

    Attributes
    ----------
    columns : List[str]
        Column names detected from data or provided explicitly
    data_iterator : Iterator
        Iterator over data records
    row_count : int
        Number of rows written (updated during write operation)

    Examples
    --------
    Subclasses implement specific formats::

        from dbtk import writers

        # Write cursor results to CSV
        cursor.execute("SELECT * FROM users")
        writers.to_csv(cursor, 'users.csv')

        # Write list of records to JSON
        with readers.CSVReader(open('input.csv')) as reader:
            records = list(reader)
        writers.to_json(records, 'output.json')

        # Preview to stdout (shows first 20 rows)
        cursor.execute("SELECT * FROM large_table")
        writers.to_csv(cursor, None)  # Prints to console

    See Also
    --------
    to_csv : Write CSV files
    to_json : Write JSON files
    to_excel : Write Excel files
    to_xml : Write XML files
    cursor_to_cursor : Database-to-database transfer

    Notes
    -----
    This is an abstract base class. Use one of the concrete implementations
    (CSVWriter, JSONWriter, etc.) or the convenience functions (to_csv, to_json, etc.).

    Subclasses must implement:

    * ``_write_data()`` - Perform the actual write operation

    When filename is None (stdout mode), output is automatically limited to 20 rows
    to prevent accidentally printing huge result sets to the console.
    """

    # Class attribute indicating whether this writer can accept an open file handle
    # and write to it directly. Set to False for writers that manage their own files
    # (e.g., ExcelWriter, DatabaseWriter).
    accepts_file_handle = True

    # Class attribute that controls if native types are preserved or converted to strings
    # The to_string method can be overridden in each subclass if specific types need converted
    # while others are preserved.
    preserve_types = False

    def __init__(
            self,
            data: Iterable[RecordLike],
            file: Optional[Union[str, Path, TextIO, BinaryIO]] = None,
            columns: Optional[List[str]] = None,
            encoding: str = "utf-8-sig",
            write_headers: bool = True,
            **fmt_kwargs,
    ):
        """
        Initialize the writer with data and options.

        Parameters
        ----------
        data : Iterable[RecordLike]
            Data source (cursor, list of records, etc.)
        file : str, Path, TextIO, or BinaryIO, optional
            Output file. None writes to stdout.
        columns : List[str], optional
            Column names for list-of-lists
        encoding : str, default 'utf-8-sig'
            File encoding
        write_headers : bool, default True
            Include header row in output
        **fmt_kwargs
            Format-specific arguments
        """
        self.file = file
        self.encoding = encoding
        self.write_headers = write_headers
        self._headers_written = False
        self._format_kwargs = fmt_kwargs
        self._row_num = 0

        # Setup data iterator and columns
        self.data_iterator, self.columns = self._get_data_iterator(data, columns)
        if not self.data_iterator:
            raise ValueError("No data to export")

        # Limit stdout output to 20 rows
        if file is None:
            self.data_iterator = itertools.islice(self.data_iterator, 20)

        # File handling
        self._file_obj: Optional[Union[TextIO, BinaryIO]] = None
        self._should_close_file = False
        self.output_path: Optional[Path] = None

        if self.accepts_file_handle:
            self._file_obj, self._should_close_file = self._open_file_handle()
        else:
            # Writers that manage their own files (ExcelWriter, DatabaseWriter)
            if file is None:
                raise ValueError(f"{self.__class__.__name__} requires an output file path")
            self.output_path = Path(file)

    def _open_file_handle(self, mode: str = "w") -> tuple[Union[TextIO, BinaryIO], bool]:
        """
        Open the output file/stream and return (handle, should_close).

        Parameters
        ----------
        mode : str, default 'w'
            File open mode ('w' for text, 'wb' for binary)

        Returns
        -------
        tuple[Union[TextIO, BinaryIO], bool]
            (file_handle, should_close_flag)
        """
        if self.file is None:
            # Write to stdout
            return (sys.stdout.buffer if "b" in mode else sys.stdout), False

        if hasattr(self.file, "write"):
            # Already an open file handle
            return self.file, False

        # Open file from path
        kwargs = {}
        if "b" not in mode:
            kwargs["newline"] = ""
            kwargs["encoding"] = self.encoding
        return open(self.file, mode, **kwargs), True

    @property
    def row_count(self) -> int:
        """Number of rows written so far."""
        return self._row_num

    def write(self) -> int:
        """
        Write all data in one operation.

        Returns
        -------
        int
            Number of rows written.

        Raises
        ------
        ValueError
            If no data is available to write.
        """
        if not self.data_iterator:
            raise ValueError("No data to write")

        file_obj = self._file_obj if self.accepts_file_handle else self.output_path
        try:
            self._write_data(file_obj)
            logger.info(f"Wrote {self._row_num} rows to {self.file or 'stdout'}")
            return self._row_num
        finally:
            self.close()

    def close(self) -> None:
        """
        Close the output file if it was opened by this writer.

        Safe to call multiple times (idempotent). Automatically called
        when using the writer as a context manager.
        """
        if self._should_close_file and self._file_obj:
            self._file_obj.close()
            self._file_obj = None
            self._should_close_file = False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close file."""
        self.close()
        return False

    @abstractmethod
    def _write_data(self, file_obj: Any) -> None:
        """
        Perform the actual write operation.

        Subclasses implement format-specific logic here.

        Parameters
        ----------
        file_obj : Any
            Either an open file handle (when ``accepts_file_handle=True``)
            or a Path (when ``accepts_file_handle=False``).
        """
        pass

    def _get_data_iterator(
            self, data: Iterable[RecordLike], columns: Optional[List[str]] = None
    ) -> tuple[Optional[Iterator], Optional[List[str]]]:
        """
        Get data iterator and column names.

        Parameters
        ----------
        data : Iterable[RecordLike]
            Input data (cursor, list, etc.)
        columns : List[str], optional
            Optional column names for list-of-lists data

        Returns
        -------
        tuple[Optional[Iterator], Optional[List[str]]]
            (iterator, column_names)
        """
        if not data:
            return None, None

        # Database cursor
        if hasattr(data, "fetchall"):
            if hasattr(data, "columns"):
                data_columns = data.columns()
            elif hasattr(data, "description"):
                data_columns = [col[0] for col in data.description]
            else:
                data_columns = []
            return data, data_columns

        # List or tuple
        if isinstance(data, (list, tuple)):
            if not data:
                return None, None

            first_item = data[0]

            # Dict or Record with keys
            if hasattr(first_item, "keys"):
                data_columns = list(first_item.keys())
            # Named tuple
            elif hasattr(first_item, "_fields"):
                data_columns = list(first_item._fields)
            # List of lists
            else:
                if columns:
                    if len(columns) != len(first_item):
                        raise ValueError(
                            f"Column count ({len(columns)}) must match data width ({len(first_item)})"
                        )
                    data_columns = columns
                else:
                    data_columns = [f"col_{x:03d}" for x in range(1, len(first_item) + 1)]

            return iter(data), data_columns

        return None, None

    def to_string(self, obj: Any) -> str:
        """
        Convert a database value to string representation.

        Parameters
        ----------
        obj : Any
            Value to convert

        Returns
        -------
        str
            String representation
        """
        if obj is None:
            return ""
        return to_string(obj)

    def _row_to_dict(self, record: RecordLike) -> dict:
        """
        Convert record to dictionary.

        Parameters
        ----------
        record : RecordLike
            Record object, namedtuple, dict, list, etc.

        Returns
        -------
        dict
            Dictionary representation
        """
        if isinstance(record, dict):
            record_dict = record
        elif hasattr(record, "to_dict"):
            record_dict = record.to_dict()
        elif hasattr(record, "_asdict"):
            record_dict = record._asdict()
        elif hasattr(record, "keys") and callable(record.keys):
            record_dict = {key: record[key] for key in record.keys()}
        elif isinstance(record, (list, tuple)):
            record_dict = {self.columns[i]: record[i] for i in range(min(len(self.columns), len(record)))}
        else:
            record_dict = {col: getattr(record, col, None) for col in self.columns}

        # Apply to_string conversion if preserve_types is False
        if not self.preserve_types:
            record_dict = {k: self.to_string(v) for k, v in record_dict.items()}

        return record_dict

    def _row_to_tuple(self, record: RecordLike) -> List[Any]:
        """
        Extract values from record with optional text conversion.

        Parameters
        ----------
        record : RecordLike
            Record object, namedtuple, dict, list, etc.

        Returns
        -------
        List[Any]
            List of values in column order
        """
        values = []
        for i, col in enumerate(self.columns):
            if hasattr(record, "__getitem__"):
                # dict-like (Record, dict) or list-like (list, tuple, namedtuple)
                value = record[i] if isinstance(record, (list, tuple)) else record[col]
            else:
                # Fallback for objects without __getitem__
                value = getattr(record, col, None)

            if not self.preserve_types:
                value = self.to_string(value)
            values.append(value)

        return tuple(values)


class BatchWriter(BaseWriter):
    """
    Base class for writers that support incremental, batch-based output.

    Unlike traditional writers that require all data up-front, BatchWriter
    subclasses are designed for streaming and bulk ETL workloads where data
    arrives in chunks (e.g., from BulkSurge, large queries, or infinite streams).

    Key Features
    ------------
    * **Lazy initialization** - Columns and iterator are resolved on first write
    * **Reusable file handle** - Write multiple batches without reopening
    * **Header control** - First batch includes headers, subsequent batches omit
    * **Zero-copy compatible** - Works with Record objects and generators
    * **Dual-mode operation** - Use as traditional writer or streaming writer

    Usage Patterns
    --------------
    **Pattern 1: Traditional (single-shot)**
        >>> writer = CSVWriter(data=all_records, file='output.csv')
        >>> writer.write()

    **Pattern 2: Pure streaming**
        >>> with CSVWriter(data=None, file='output.csv') as writer:
        ...     for batch in surge.batched(records):
        ...         writer.write_batch(batch)

    **Pattern 3: Hybrid**
        >>> writer = CSVWriter(data=first_batch, file='output.csv')
        >>> writer.write()  # Process initial batch
        >>> writer.write_batch(second_batch)  # Continue streaming
        >>> writer.write_batch(third_batch)

    Parameters
    ----------
    data : Iterable[RecordLike], optional
        Initial data. If None, setup is deferred until first write_batch().
        This enables streaming use cases where data arrives in batches.
    file : str, Path, TextIO, or BinaryIO, optional
        Output destination. For streaming, pass an open file handle.
    columns : List[str], optional
        Explicit column names. If not provided, inferred from first batch.
    encoding : str, default 'utf-8'
        File encoding for text-based formats
    write_headers : bool, default True
        Whether to write column headers on the first batch.
    **fmt_kwargs
        Format-specific options passed to _write_data().

    Notes
    -----
    Subclasses must implement ``_write_data()`` but inherit ``write_batch()`` for free.

    Used by:
        - BulkSurge.dump() and .load(fallback_path=...)
        - Any high-performance streaming export pipeline

    See Also
    --------
    CSVWriter : Batchable CSV writer
    NDJSONWriter : Batchable newline-delimited JSON writer
    XMLStreamer : Batchable streaming XML writer
    """

    accepts_file_handle = True
    preserve_types = False

    def __init__(
            self,
            data: Optional[Iterable[RecordLike]] = None,
            file: Optional[Union[str, Path, TextIO, BinaryIO]] = None,
            columns: Optional[List[str]] = None,
            encoding: Optional[str] = 'utf-8-sig',
            write_headers: bool = True,
            **fmt_kwargs,
    ):
        """
        Initialize a batch-capable writer with optional deferred setup.

        Parameters
        ----------
        data : Iterable[RecordLike], optional
            Initial data. If None, setup is deferred until first write_batch().
        file : str, Path, TextIO, or BinaryIO, optional
            Output destination.
        columns : List[str], optional
            Explicit column names. If not provided, inferred from data.
        encoding : str, default 'utf-8'
            File encoding
        write_headers : bool, default True
            Include header row in output
        **fmt_kwargs
            Format-specific options
        """
        self.file = file
        self.write_headers = write_headers
        self.encoding = encoding
        self._format_kwargs = fmt_kwargs
        self._row_num = 0
        self._headers_written = False
        self._initialized = False

        self.columns = columns
        self.data_iterator: Optional[Iterator] = None

        # Set up file handle for streaming
        self._file_obj: Optional[Union[TextIO, BinaryIO]] = None
        self._should_close_file = False

        if self.__class__.accepts_file_handle:
            self._file_obj, self._should_close_file = self._open_file_handle()

        # If data provided, set up iterator immediately for traditional write() usage
        if data is not None:
            self._lazy_init(data)


    def _lazy_init(self, data: Iterable[RecordLike]) -> None:
        """
        Resolve columns and iterator on first batch.

        Parameters
        ----------
        data : Iterable[RecordLike]
            First batch of data to determine columns from

        Raises
        ------
        ValueError
            If columns cannot be determined from data
        """
        if self._initialized:
            return

        self.data_iterator, detected_columns = self._get_data_iterator(data, self.columns)
        if self.columns is None:
            self.columns = detected_columns

        if not self.columns:
            raise ValueError("Could not determine columns from data")

        self._initialized = True

    def write(self) -> int:
        """
        Write initial data provided at initialization.

        Returns
        -------
        int
            Number of rows written

        Raises
        ------
        RuntimeError
            If no initial data was provided to __init__. Use write_batch() for streaming.
        """
        if self.data_iterator is None:
            raise RuntimeError(
                "No initial data to write. Either provide data at initialization "
                "or use write_batch() for streaming mode."
            )

        try:
            self._write_data(self._file_obj)
            logger.info(f"Wrote {self._row_num} rows to {self.file or 'stdout'}")
            return self._row_num
        finally:
            # Clear iterator after writing to allow subsequent write_batch() calls
            self.data_iterator = None

    def write_batch(self, data: Iterable[RecordLike]) -> None:
        """
        Write a batch of records to the output stream.

        This is the core method that makes BatchWriter suitable for BulkSurge
        and other high-volume streaming scenarios.

        Parameters
        ----------
        data : Iterable[RecordLike]
            A batch of Record objects (or compatible row objects).

        Raises
        ------
        RuntimeError
            If initial data was provided but write() hasn't been called yet.
        """
        # Check if initial data hasn't been written yet
        if self.data_iterator is not None:
            raise RuntimeError(
                "Initial data was provided to __init__ but has not been written. "
                "Call write() first to process initial data, or initialize with data=None "
                "for pure streaming mode."
            )

        # Initialize on first batch if needed
        if not self._initialized:
            self._lazy_init(data)

        # Set up iterator for this batch
        self.data_iterator = iter(data)

        # Write the batch
        self._write_data(self._file_obj)

        # Flush after each batch
        if hasattr(self._file_obj, "flush"):
            self._file_obj.flush()

        # Clear data_iterator so
        self.data_iterator = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close file if we opened it."""
        if self._should_close_file and self._file_obj:
            self._file_obj.close()
            self._file_obj = None
        return False
