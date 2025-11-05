# dbtk/readers/base.py

"""
Base classes and utilities for file readers.

Defines the abstract Reader interface and Clean enumeration for
header normalization across all reader implementations.
"""

import itertools
import re
import time

from abc import ABC, abstractmethod
from typing import Any, Iterator, List, Optional, Union
from collections import OrderedDict
from ..record import Record
from ..defaults import settings


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
    __slots__ = ('tell', 'total')

    def __init__(self, obj):
        if hasattr(obj, 'buffer'):
            buf = obj.buffer
            pos = buf.tell()
            buf.seek(0, 2)
            self.total = buf.tell()
            buf.seek(pos)
            self.tell = buf.tell
        elif hasattr(obj, 'tell'):
            pos = obj.tell()
            obj.seek(0, 2)
            self.total = obj.tell()
            obj.seek(pos)
            self.tell = obj.tell
        else:
            # Excel: fake byte size from row count
            rows = getattr(obj, 'max_row', 1_000_000)
            self.total = rows * 1024
            self.tell = lambda: getattr(obj, '_current_row', 1) * 1024

    def update(self):
        if not self.total:
            return ""
        pos = self.tell()
        pct = pos / self.total
        filled = round(20 * pct)  # ← ROUND, NOT INT
        bar = "█" * filled + "░" * (20 - filled)
        return f"{bar} {pos // 1024:,}K/{self.total // 1024:,}K"


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
    * **Row number tracking** - Automatic rownum field for debugging
    * **Record skipping** - Skip header rows or bad data
    * **Record limiting** - Process only first N records
    * **Flexible return types** - Record objects or dictionaries
    * **Context manager** - Automatic resource cleanup
    * **Iterator protocol** - Memory-efficient streaming

    Parameters
    ----------
    add_rownum : bool, default True
        Add a 'rownum' field to each record containing the 1-based row number
    clean_headers : Clean or str, optional
        Header cleaning level. Options: Clean.LOWER_NOSPACE (default), Clean.STANDARDIZE,
        Clean.NONE. Can also pass string like 'lower_nospace'.
    skip_records : int, default 0
        Number of data records to skip after headers (useful for skipping footer rows
        or known bad data at start of file)
    max_records : int, optional
        Maximum number of records to read. None (default) reads all records.
    return_type : str, default 'record'
        Return type for records: 'record' for Record objects, 'dict' for OrderedDict

    Example
    -------
    ::

        # Subclasses implement specific file formats
        from dbtk import readers

        # CSV with default settings
        with readers.CSVReader(open('data.csv')) as reader:
            for record in reader:
                print(record.name, record.email)

        # Skip first 5 records, read only 100, return dicts
        with readers.CSVReader(open('data.csv'),
                              skip_records=5,
                              max_records=100,
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

    def __init__(self,
                 add_rownum: bool = True,
                 clean_headers: Clean = None,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 return_type: str = ReturnType.DEFAULT
                 ):
        """
        Initialize the reader with common options.

        Parameters
        ----------
        add_rownum : bool, default True
            Add a 'rownum' field to each record containing the 1-based row number
        clean_headers : Clean or str, optional
            Header cleaning level from Clean enum or string. If None, uses
            default_header_clean from settings (default: Clean.LOWER_NOSPACE)
        skip_records : int, default 0
            Number of data records to skip after headers
        max_records : int, optional
            Maximum number of records to read, or None for all records
        return_type : str, default 'record'
            Either 'record' for Record objects or 'dict' for OrderedDict

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
        self.add_rownum = add_rownum
        if clean_headers is None:
            clean_headers = settings.get('default_header_clean', Clean.LOWER_NOSPACE)
        self.clean_headers = Clean.from_string(clean_headers)
        self.record_num = 0
        self.skip_records = skip_records
        self.max_records = max_records
        self.return_type = return_type
        self._record_class = None
        self._headers: List[str] = []
        self._headers_initialized = False
        self._data_iter: Optional[Iterator[List[Any]]] = None
        self._trackable = None
        self._start_time = None

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
            self._prog = _Progress(self._trackable)
            self._big = self._prog.total > 5_242_880
            self._start = time.monotonic()

        try:
            row_data = self._read_next_row()
        except StopIteration:
            took = time.monotonic() - self._start
            rate = self.record_num / took if took else 0
            print(f"\r{self.__class__.__name__[:-6]} → {self._prog.update()} ✅")
            print(f"Done in {took:.2f}s ({int(rate):,} rec/s)")
            raise  # ← let for-loop end

        self.record_num += 1

        if self._big and self.record_num % 50_000 == 0:
            print(f"\r{self.__class__.__name__[:-6]} → {self._prog.update()} "
                  f"({self.record_num:,})", end="", flush=True)

        return self._create_record(row_data)

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
            start = self.skip_records
            stop = start + self.max_records if self.max_records is not None else None
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

        # Add rownum if requested and not already present
        if self.add_rownum and 'rownum' not in self._headers:
            self._headers.append('rownum')

        # Create Record subclass only if return_type is 'record'
        if self.return_type == ReturnType.RECORD:
            self._record_class = type('FileRecord', (Record,), {})
            self._record_class.set_columns(self._headers)

        self._headers_initialized = True

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

        # Pad with None if row is shorter than expected (excluding rownum)
        expected_data_cols = len(self._headers) - (1 if self.add_rownum and 'rownum' in self._headers else 0)
        while len(row_data) < expected_data_cols:
            row_data.append(None)

        # Add rownum if it's in headers (always goes at the end)
        if self.add_rownum and 'rownum' in self._headers:
            row_data.append(self.skip_records + self.record_num)

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