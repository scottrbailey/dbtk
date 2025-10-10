# dbtk/readers/base.py

import itertools
import re

from abc import ABC, abstractmethod
from typing import Any, Iterator, List, Optional, Union
from collections import OrderedDict
from ..record import Record


class Clean:
    """Header cleaning levels for column names."""
    NOOP = 0  # Leave unchanged
    LOWER = 1  # Lower case header
    LOWER_NOSPACE = 2  # Lower case and replace spaces with _
    LOWER_ALPHANUM = 3  # Lower case, remove all non-alphanum characters
    STANDARDIZE = 4  # Lower case, remove non-alphanum, strip "code" endings
    DEFAULT = 2  # Default to LOWER_NOSPACE

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
            clean_level = Clean.DEFAULT
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


class Reader(ABC):
    """
    Abstract base class for all file readers.

    Provides common functionality for CSV, Excel, and fixed-width file readers.
    Can return either Record objects or dict objects based on return_type parameter.
    """

    def __init__(self,
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 return_type: str = ReturnType.DEFAULT
                 ):
        """
        Initialize the reader.

        Args:
            add_rownum: Add a 'rownum' field to each record
            clean_headers: Header cleaning level from Clean
            skip_records: Number of data records to skip after headers
            max_records: Maximum number of records to read, or None for all
            return_type: Either 'record' for Record objects or 'dict' for OrderedDict
        """
        self.add_rownum = add_rownum
        self.clean_headers = clean_headers
        self.record_num = 0
        self.skip_records = skip_records
        self.max_records = max_records
        self.return_type = return_type
        self._record_class = None
        self._headers: List[str] = []
        self._headers_initialized = False
        self._data_iter: Optional[Iterator[List[Any]]] = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self._cleanup()

    def __iter__(self) -> Iterator[Union[Record, OrderedDict]]:
        """Make reader iterable."""
        return self

    def __next__(self) -> Union[Record, OrderedDict]:
        if not self._headers_initialized:
            self._setup_record_class()
        row_data = self._read_next_row()
        self.record_num += 1  # Starts at 1 for first yielded record
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