# dbtk/readers/base.py

"""
Base classes and utilities for file readers.

Defines the abstract Reader interface and Clean enumeration for
header normalization across all reader implementations.
"""

import itertools
import logging
import re
import time

from abc import ABC, abstractmethod
from typing import Any, Iterator, List, Optional, Union
from collections import OrderedDict
from os import path
from ..record import Record
from ..defaults import settings

logger = logging.getLogger(__name__)

class Clean:
    """Header cleaning levels for column names."""
    NOOP = 0  # Leave unchanged
    LOWER = 1  # Lower case header
    LOWER_NOSPACE = 2  # Lower case and replace spaces with _
    LOWER_ALPHANUM = 3  # Lower case, remove all non-alphanum characters
    STANDARDIZE = 4  # Lower case, remove non-alphanum, strip "code" endings
    DEFAULT = LOWER_NOSPACE

    @classmethod
    def from_string(cls, value):
        """Convert string to Clean constant, or pass through if already int."""
        if isinstance(value, int):
            return value

        string_map = {
            'noop': cls.NOOP,
            'lower': cls.LOWER,
            'lower_nospace': cls.LOWER_NOSPACE,
            'lower_alphanum': cls.LOWER_ALPHANUM,
            'standardize': cls.STANDARDIZE
        }
        return string_map.get(value.lower(), cls.NOOP)

    @staticmethod
    def normalize(val: Any, clean_level: Union[int, 'Clean', None] = None) -> str:
        """
        Normalize column header names.

        Args:
            val: Column name (any type, will be converted to string)
            clean_level: Cleaning level, either an integer (0-4), a Clean class constant,
                        or None to use Clean.DEFAULT

        Returns:
            Normalized column name string

        Examples:
            Clean.normalize("#Term Code", Clean.NOOP)           # -> "#Term Code"
            Clean.normalize("#Term Code", Clean.LOWER)          # -> "#term code"
            Clean.normalize("#Term Code", Clean.LOWER_NOSPACE)  # -> "#term_code"
            Clean.normalize("#Term Code", Clean.LOWER_ALPHANUM) # -> "termcode"
            Clean.normalize("#Term Code", Clean.STANDARDIZE)    # -> "term"
            Clean.normalize("#Term Code")                       # -> "#term_code" (default to Clean.LOWER_NOSPACE)
        """
        # Use Clean.DEFAULT if clean_level is None
        if clean_level is None:
            clean_level = Clean.LOWER_NOSPACE
        # Convert clean_level to integer if it's a Clean constant
        clean_level = clean_level if isinstance(clean_level, int) else clean_level.value
        # Validate clean_level
        if clean_level not in {Clean.NOOP, Clean.LOWER, Clean.LOWER_NOSPACE, Clean.LOWER_ALPHANUM, Clean.STANDARDIZE}:
            raise ValueError(f"Invalid clean_level: {clean_level}. Must be 0-4 or a Clean constant.")

        if val in (None, '') or clean_level == Clean.NOOP:
            return str(val) if val is not None else ''
        val = str(val).lower().strip()
        if clean_level == Clean.LOWER:
            return val
        elif clean_level == Clean.LOWER_NOSPACE:
            return val.replace(' ', '_')
        elif clean_level == Clean.LOWER_ALPHANUM:
            return re.sub(r'[^a-z0-9]', '', val)
        elif clean_level == Clean.STANDARDIZE:
            return re.sub(r'[^a-z0-9]+|code$', '', val)
        else:
            return val


class ReturnType:
    """Return type options for readers."""
    RECORD = 'record'
    DICT = 'dict'
    DEFAULT = RECORD

class _Progress:
    __slots__ = ('tell', 'byte_total', 'row_total')

    def __init__(self, source_obj=None, row_total: Optional[int] = None):
        if source_obj is not None and hasattr(source_obj, 'tell'):
            self.tell = source_obj.tell
            self.byte_total = getattr(source_obj, '_uncompressed_size', None)
        else:
            self.tell = lambda: 0
            self.byte_total = None

        self.row_total = row_total

    def current(self) -> int:
        try:
            return self.tell()
        except Exception:
            return 0

    def update(self, row_num: int) -> str:
        if self.byte_total is not None:
            # Byte-based progress
            pos = self.current()
            if self.byte_total == 0:
                return ""
            pct = pos / self.byte_total
            current_val = pos // 1024
            total_val = self.byte_total // 1024
            unit = "K"
        elif self.row_total is not None:
            # Row-based progress
            if self.row_total == 0:
                return ""
            pct = row_num / self.row_total
            current_val = row_num
            total_val = self.row_total
            unit = ""
        else:
            return ""

        filled = max(0, min(20, round(20 * pct)))
        bar = "█" * filled + "░" * (20 - filled)
        return f"{bar} {current_val:,}{unit}/{total_val:,}{unit}"


class Reader(ABC):
    """
    Abstract base class for all file readers in DBTK.

    Provides unified interface and common functionality for reading various file formats
    (CSV, Excel, JSON, XML, fixed-width). All readers support the same features regardless
    of file format: header cleaning, record skipping, row number tracking, and flexible
    return types.

    Readers are designed to work as context managers and iterators, making them ideal
    for memory-efficient processing of large files. They automatically handle resource
    cleanup and support both Record objects (with multiple access patterns) and plain
    dictionaries as return types.

    Common Features
    ---------------
    * **Automatic header cleaning** - Standardize messy column names
    * **Row number tracking** - Automatic _row_num field for debugging
    * **Record skipping** - Skip header rows or bad data
    * **Record limiting** - Process only first N records
    * **Flexible return types** - Record objects or dictionaries
    * **Context manager** - Automatic resource cleanup
    * **Iterator protocol** - Memory-efficient streaming
    * **Null value conversion** - Convert specified values to None

    Parameters
    ----------
    add_row_num : bool, default True
        Add a '_row_num' field to each record containing the 1-based row number
    clean_headers : Clean or str, optional
        Header cleaning level. Options: Clean.LOWER_NOSPACE (default), Clean.STANDARDIZE,
        Clean.NONE. Can also pass string like 'lower_nospace'.
    skip_rows : int, default 0
        Number of data rows to skip after headers (useful for skipping footer rows
        or known bad data at start of file)
    n_rows : int, optional
        Maximum number of rows to read. None (default) reads all rows.
    return_type : str, default 'record'
        Return type for records: 'record' for Record objects, 'dict' for OrderedDict
    null_values : str, list, tuple, or set, optional
        Values to convert to None. Can be a single string or a collection of strings.
        Common examples: '\\N' (IMDB files), 'NULL', 'NA', '' (empty string)

    Example
    -------
    ::

        # Subclasses implement specific file formats
        from dbtk import readers

        # CSV with default settings
        with readers.CSVReader(open('data.csv')) as reader:
            for record in reader:
                print(record.name, record.email)

        # Skip first 5 rows, read only 100, return dicts
        with readers.CSVReader(open('data.csv'),
                              skip_rows=5,
                              n_rows=100,
                              return_type='dict') as reader:
            for row in reader:
                print(row['name'])

        # Standardize messy headers
        with readers.CSVReader(open('messy.csv'),
                              clean_headers=readers.Clean.STANDARDIZE) as reader:
            # Headers like "ID #", "Student Name" become "id", "studentname"
            for record in reader:
                print(record.id, record.studentname)

    See Also
    --------
    CSVReader : Read CSV files
    JSONReader : Read JSON files
    XLSXReader : Read Excel .xlsx files
    XMLReader : Read XML files
    FixedReader : Read fixed-width text files
    Clean : Header cleaning options
    Record : Flexible row objects with multiple access patterns

    Notes
    -----
    This is an abstract base class. Use one of the concrete implementations
    (CSVReader, JSONReader, etc.) for actual file reading.

    Subclasses must implement:

    * ``_read_headers()`` - Return list of raw column names from file
    * ``_generate_rows()`` - Yield raw data rows as lists

    Optionally override:

    * ``_cleanup()`` - Release resources (file handles, etc.)
    """

    # Class constants for "big" thresholds for adding progress tracking
    BIG_ROW_THRESHOLD = 10_000  # Show progress for >10k rows
    BIG_BYTE_THRESHOLD = 5 * 1024 * 1024  # Show progress for >5MB files

    def __init__(self,
                 add_row_num: bool = True,
                 clean_headers: Clean = None,
                 skip_rows: int = 0,
                 n_rows: Optional[int] = None,
                 headers: Optional[List[str]] = None,
                 return_type: str = ReturnType.DEFAULT,
                 null_values: Union[str, List[str], tuple, set, None] = None
                 ):
        """
        Initialize the reader with common options.

        Parameters
        ----------
        add_row_num : bool, default True
            Add a '_row_num' field to each record containing the 1-based row number
        clean_headers : Clean or str, optional
            Header cleaning level from Clean enum or string. If None, uses
            default_header_clean from settings (default: Clean.LOWER_NOSPACE)
        skip_rows : int, default 0
            Number of data rows to skip after headers
        n_rows : int, optional
            Maximum number of rows to read, or None for all rows
        headers: Optional list of header names to use instead of reading from row 0
        return_type : str, default 'record'
            Either 'record' for Record objects or 'dict' for OrderedDict
        null_values : str, list, tuple, or set, optional
            Values to convert to None. Can be a single string or collection of strings.
            Common examples: '\\N' (IMDB), 'NULL', 'NA', '' (empty string)

        Example
        -------
        ::

            # In subclass implementation
            class MyReader(Reader):
                def __init__(self, file_path, **kwargs):
                    super().__init__(**kwargs)
                    self.file = open(file_path)

                def _read_headers(self):
                    return ['id', 'name', 'email']

                def _generate_rows(self):
                    for line in self.file:
                        yield line.strip().split(',')
        """
        self.add_row_num = add_row_num
        if clean_headers is None:
            clean_headers = settings.get('default_header_clean', Clean.LOWER_NOSPACE)
        self.clean_headers = Clean.from_string(clean_headers)
        self._row_num = 0
        self.skip_rows = skip_rows
        self.n_rows = n_rows
        self.return_type = return_type
        self._record_class = None

        # Normalize null_values to a set for O(1) lookup
        if null_values is None:
            self._null_values = set()
        elif isinstance(null_values, str):
            self._null_values = {null_values}
        elif isinstance(null_values, (list, tuple, set)):
            self._null_values = set(null_values)
        else:
            raise TypeError(f"null_values must be str, list, tuple, or set, got {type(null_values)}")

        self._raw_headers: Optional[List[str]] = headers
        self._headers: List[str] = []
        self._headers_initialized: bool = False
        self._data_iter: Optional[Iterator[List[Any]]] = None
        self._total_records = 0         # used by progress tracker when we know the number of rows ahead of time (Excel, DataFrames)
        self._trackable = None          # used by progress tracker
        self._prog: _Progress = None    # progress tracker _Progress object
        self._big: bool = False         # True if source > 5MB (adds progress bar)
        self._source: str = None        # keep track of source filename for subclasses that use a file pointer directly (Excel)
        self._start_time: float = 0     # will get updated when the first record is read (time.monotonic())

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self._cleanup()

    def __iter__(self) -> Iterator[Union[Record, OrderedDict]]:
        """Make reader iterable."""
        return self

    def __next__(self):
        if not self._headers_initialized:
            self._setup_record_class()
        if not self._start_time:
            self._start_time = time.monotonic()
        if self._prog is None:
            if self._trackable and hasattr(self._trackable, 'tell'):
                self._prog = _Progress(self._trackable)
                self._big = self._prog.byte_total is not None and self._prog.byte_total > self.BIG_BYTE_THRESHOLD
            elif hasattr(self, '_total_rows') and self._total_rows is not None:
                self._prog = _Progress(row_total=self._total_rows)
                self._big = self._total_rows > self.BIG_ROW_THRESHOLD

        try:
            row_data = self._read_next_row()
        except StopIteration:
            took = time.monotonic() - self._start_time
            rate = self._row_num / took if took else 0
            if self._big:
                print(f"\r{self.__class__.__name__[:-6]} → {self._prog.update(self._row_num)} ✅")
            print(f"Done in {took:.2f}s ({int(rate):,} rec/s)")
            logger.info(f"Read {self._row_num:,} rows in {took:.2f}s ({int(rate):,} rec/s)")
            raise  # ← let for-loop end

        self._row_num += 1

        if self._big and (self._row_num == 500 or self._row_num % 50_000 == 0):
            print(f"\r{self.__class__.__name__[:-6]} → {self._prog.update(self._row_num)} "
                  f"({self._row_num:,})", end="", flush=True)

        return self._create_record(row_data)

    def __repr__(self):
        source = self._get_source(base_name=True)
        if source:
            source = f"'{source}'"
        return f"{self.__class__.__name__}({source})"

    @property
    def source(self) -> str:
        """
        Get the filename of the source file.

        For the XLSXReader and XLSReader, the source must be set manually because the Workbook objects do not keep
        a reference to the original file.
        """
        if self._source is None:
            self._source = self._get_source()
        return self._source

    @source.setter
    def source(self, value: str):
        self._source = value

    def _get_source(self, base_name: Optional[bool] = False) -> str:
        """ Get the source filename for the Reader.

        Args:
            base_name: If True, return the base filename (no path)

        Returns: filename
        """

        if hasattr(self, 'fp') and hasattr(self.fp, 'name'):
            source = self.fp.name
        elif hasattr(self, 'source'):
            source = self.source
        else:
            source = ''
        if base_name:
            source = path.basename(source)
        return source

    @property
    def row_count(self) -> int:
        """
        Returns the number of rows.

        This property provides access to the total number of rows, which
        is stored in the private attribute `_row_num`.

        Returns:
            int: The total number of rows.
        """
        return self._row_num

    @abstractmethod
    def _read_headers(self) -> List[str]:
        """
        Read and return raw headers from the file.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def _generate_rows(self) -> Iterator[List[Any]]:
        """Generate all raw data rows as lists, without applying skip or limit."""
        pass

    def _read_next_row(self) -> List[Any]:
        if self._data_iter is None:
            gen = self._generate_rows()
            start = self.skip_rows
            stop = start + self.n_rows if self.n_rows is not None else None
            self._data_iter = itertools.islice(gen, start, stop)

        return next(self._data_iter)

    def _cleanup(self):
        """
        Cleanup resources. Override in subclasses if needed.
        Default implementation does nothing.
        """
        pass

    def _setup_record_class(self):
        """Initialize headers and create Record subclass if needed."""
        if self._headers_initialized:
            return

        # Read raw headers from file
        raw_headers = self._read_headers()

        # Clean headers
        self._headers = [Clean.normalize(h, self.clean_headers) for h in raw_headers]

        # Add self.add_row_num if requested and not already present
        if self.add_row_num:
            if '_row_num' in self._headers:
                raise ValueError("Header '_row_num' already exists. Remove it or set add_row_num=False.")
            self._headers.append('_row_num')

        # Create Record subclass only if return_type is 'record'
        if self.return_type == ReturnType.RECORD:
            self._record_class = type('FileRecord', (Record,), {})
            self._record_class.set_columns(self._headers)

        self._headers_initialized = True

    def _convert_nulls(self, row_data: List[Any]) -> List[Any]:
        """
        Convert null values to None in row data.

        Args:
            row_data: List of values for this row

        Returns:
            List with null values converted to None
        """
        if not self._null_values:
            return row_data

        return [None if val in self._null_values else val for val in row_data]

    def _create_record(self, row_data: List[Any]) -> Union[Record, OrderedDict]:
        """
        Create a Record or dict from row data.

        Args:
            row_data: List of values for this row

        Returns:
            Record instance or OrderedDict with values populated
        """
        if not self._headers_initialized:
            self._setup_record_class()

        # Make a copy to avoid modifying the original
        row_data = list(row_data)

        # Convert null values to None
        row_data = self._convert_nulls(row_data)

        # Pad with None if row is shorter than expected (excluding _row_num)
        expected_data_cols = len(self._headers) - (1 if self.add_row_num and '_row_num' in self._headers else 0)
        while len(row_data) < expected_data_cols:
            row_data.append(None)

        # Add _row_num if it's in headers (always goes at the end)
        if self.add_row_num and '_row_num' in self._headers:
            row_data.append(self.skip_rows + self._row_num)

        # Truncate if row is longer than headers
        if len(row_data) > len(self._headers):
            row_data = row_data[:len(self._headers)]

        # Return appropriate type
        if self.return_type == ReturnType.RECORD:
            return self._record_class(*row_data)
        else:  # ReturnType.DICT
            return OrderedDict(zip(self._headers, row_data))

    @property
    def headers(self) -> List[str]:
        """
        Get the column headers.

        Returns:
            List of cleaned header names
        """
        if not self._headers_initialized:
            self._setup_record_class()
        return self._headers.copy()

    @property
    def fieldnames(self) -> List[str]:
        """
        Alias for headers to maintain compatibility with csv.DictReader.

        Returns:
            List of cleaned header names
        """
        return self.headers