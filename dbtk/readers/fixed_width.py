# dbtk/readers/fixed_width.py

"""Fixed-width text file reader with column position specifications."""

import logging
import os
from typing import TextIO, List, Dict, Optional, Iterator, Type

from .base import Reader
from ..record import Record, FixedWidthRecord
from ..utils import FixedColumn

logger = logging.getLogger(__name__)


class FixedReader(Reader):
    """ Reader for fixed width files """

    def __init__(self,
                 fp: TextIO,
                 columns: List[FixedColumn],
                 auto_trim: bool = True,
                 add_row_num: bool = False,
                 skip_rows: int = 0,
                 n_rows: Optional[int] = None,
                 null_values=None):
        """
        Initializes the instance with the provided file pointer, column definitions, and
        processing options.

        Attributes:
            fp (TextIO): The file pointer from which data is read.
            columns (List[FixedColumn]): A list of FixedColumn objects defining the
                structure of columns in the data.
            auto_trim (bool): Determines whether to automatically trim whitespace
                from field values. Default is True.
            add_row_num (bool): Determines whether to add a row number attribute
            skip_rows (int): The number of rows to skip before reading data.
            n_rows (Optional[int]): The maximum number of rows to read.
            null_values: Values to convert to None (e.g., '\\N', 'NULL', 'NA').
        """
        super().__init__(add_row_num=add_row_num,
                         skip_rows=skip_rows, n_rows=n_rows,
                         null_values=null_values)
        self.fp = fp

        # Set trackable for progress tracking
        if hasattr(fp, '_uncompressed_size'):
            # Compressed file - use buffer's tell() but preserve _uncompressed_size
            self._trackable = fp.buffer
            self._trackable._uncompressed_size = fp._uncompressed_size
        elif hasattr(fp, 'buffer'):
            # Text mode file - use buffer for better performance
            self._trackable = fp.buffer
            try:
                self._trackable._uncompressed_size = os.fstat(self._trackable.fileno()).st_size
            except (AttributeError, OSError):
                pass
        else:
            # Binary mode or other file type
            self._trackable = fp
            try:
                self._trackable._uncompressed_size = os.fstat(self._trackable.fileno()).st_size
            except (AttributeError, OSError):
                pass

        self.columns = columns
        self.auto_trim = auto_trim

    def _read_headers(self) -> List[str]:
        """Return column names from FixedColumn definitions."""
        return [col.name for col in self.columns]

    def _generate_rows(self) -> Iterator[FixedWidthRecord]:
        while True:
            line = self.fp.readline()
            if not line:
                break
            yield self._record_class.from_line(line.rstrip('\n'), auto_trim=self.auto_trim)

    def _create_record(self, record: FixedWidthRecord) -> FixedWidthRecord:
        if self._null_values:
            for i in range(len(self._record_class._columns)):
                if record[i] in self._null_values:
                    record[i] = None
        if self.add_row_num:
            record['_row_num'] = self.skip_rows + self._row_num
        return record

    def _cleanup(self):
        """Close the file pointer."""
        if self.fp and hasattr(self.fp, 'close'):
            self.fp.close()

    def visualize(self, sample_lines: int = 2) -> str:
        """
        Visualize column boundaries over sample data from the file.

        Seeks to the beginning of the file, reads up to ``sample_lines`` records,
        then restores the file pointer. Output shows the rulers and column boundary
        markers once, then for each record both the raw source line and the
        interpreted line reconstructed via ``record.to_line()``.

        Args:
            sample_lines: Number of records to include in the preview.

        Returns:
            String representation of column layout with sample data.
        """
        temp_cls = type('_VizRecord', (FixedWidthRecord,), {})
        temp_cls.set_fields(self.columns)

        pos = self.fp.tell()
        self.fp.seek(0)
        pairs = []  # list of (raw_line, record)
        try:
            for line in self.fp:
                raw = line.rstrip('\n')
                if not raw:
                    continue
                pairs.append((raw, temp_cls.from_line(raw)))
                if len(pairs) >= sample_lines:
                    break
        finally:
            self.fp.seek(pos)

        if not pairs:
            return ''

        # Rulers + boundary from the first record (same for all rows of this type)
        header = '\n'.join(pairs[0][1].visualize().split('\n')[:3])
        parts = [header]
        for i, (raw, record) in enumerate(pairs):
            if i > 0:
                parts.append('')
            parts.append(f'{raw}  ← source')
            parts.append(f'{record.to_line()}  ← interpreted')
        return '\n'.join(parts)

    def _setup_record_class(self):
        """Initialize headers and create Record subclass with original field names."""
        if self._headers_initialized:
            return

        # Read raw headers from file (original field names)
        raw_headers = self._read_headers()

        # Store original headers (no normalization - Record.set_fields() handles it)
        self._headers = raw_headers[:]

        # Add _row_num if requested and not already present
        if self.add_row_num:
            if '_row_num' in self._headers:
                raise ValueError("Header '_row_num' already exists. Remove it or set add_row_num=False.")
            self._headers.append('_row_num')

        # Create Record subclass: set_fields(columns) captures widths/alignment/padding,
        # then re-call Record.set_fields with full _headers so _row_num is registered.
        self._record_class = type('FileFWRecord', (FixedWidthRecord,), {})
        self._record_class.set_fields(self.columns)
        Record.set_fields.__func__(self._record_class, self._headers)

        self._headers_initialized = True


class EDIReader(FixedReader):
    """
        Reader for fixed-width files containing multiple record types (EDI-like formats).

        Parses files where each line's layout is determined by a type identifier prefix
        (e.g., NACHA ACH files with '1', '5', '6', '7', '8', '9' record types). Each record
        type uses its own set of FixedColumn definitions, allowing different column positions
        and formats per type.

        Record type codes must all be the same length (automatically detected from keys).
        The reader dispatches parsing based on the prefix of each line and returns typed
        Record instances (one dynamic subclass per record type).

        Supports common legacy formats such as NACHA ACH, COBOL copybooks, and other
        multi-layout fixed-width EDI-style files. The column specifications for several
        common EDI-like files, including ACH, are defined in `dbtk.formats.edi`

        Parameters
        ----------
        fp : TextIO
            Open file pointer in text mode
        columns : Dict[str, List[FixedColumn]]
            Mapping of record type codes (keys) to their column definitions.
            All keys must be strings of identical length.
        type_name_map : Dict[str, str], optional
            Optional friendly names for record types (e.g., {'1': 'File Header'})
            used in logging or output fields.
        strict : bool, default False
            If True raise error if record type code not mapped in columns, else skipped and logged
        auto_trim : bool, default True
            Trim whitespace from field values
        **kwargs
            Additional arguments passed to FixedReader base class

        Raises
        ------
        ValueError
            If record type keys have inconsistent lengths or columns dict is invalid

        Example
        -------
        >>> columns = {
        ...     '1': [FixedColumn('record_type', 1, 1), FixedColumn('priority_code', 2, 3), ...],
        ...     '5': [FixedColumn('record_type', 1, 1), FixedColumn('service_class_code', 2, 4), ...],
        ...     # ... other types ...
        ... }
        >>> reader = EDIReader(open('ach_file.ach'), columns=columns)
        >>> for record in reader:
        ...     print(record.company_name)  # fields available depend on record type
        """

    def __init__(
            self,
            fp: TextIO,
            columns: Dict[str, List[FixedColumn]],
            type_name_map: Optional[Dict[str, str]] = None,
            strict: Optional[bool] = False,
            # ... pass through all FixedReader params ...
            **kwargs
    ):
        super().__init__(fp, columns=None, **kwargs)  # no single columns

        self.columns = columns
        self.type_name_map = type_name_map or {}
        self.strict = strict

        # Auto-detect _record_type_len from keys
        if columns:
            lengths = {len(k) for k in columns}
            if len(lengths) != 1:
                raise ValueError("All record type keys must have the same length")
            self._record_type_len = next(iter(lengths))
            if self._record_type_len == 0:
                raise ValueError("Record type keys cannot be empty")
        else:
            raise ValueError("columns dict is required for TypedFixedReader")

        self._type_factories: Dict[str, type[Record]] = {}

    def _read_headers(self) -> List[str]:
        """EDIReader has no fixed headers; each record type has its own field set."""
        return []

    def _setup_record_class(self):
        """Skip base class record class creation; EDIReader uses per-type factories."""
        self._headers_initialized = True

    def _create_record(self, row_data):
        """Pass through the Record object yielded by _generate_rows."""
        return row_data

    def _get_columns(self, type_code: str) -> List[FixedColumn]:
        return self.columns.get(type_code)

    def _get_factory(self, type_code: str) -> Type[Record]:
        if type_code not in self._type_factories:
            cols = self._get_columns(type_code)
            if cols is None:
                raise ValueError(f"No column definition for record type '{type_code}'")

            RecordClass = type(f'EDI_{type_code}_Record', (FixedWidthRecord,), {})
            RecordClass.set_fields(cols)
            self._type_factories[type_code] = RecordClass
        return self._type_factories[type_code]

    def visualize(self) -> str:
        """
        Visualize column boundaries for each record type found in the file.

        Scans the entire file, emitting one block per record type the first time
        that type is encountered. Each block shows the rulers, column boundary
        markers, the raw source line, and the interpreted line. Blocks are
        separated by blank lines. The file pointer is saved and restored.

        Returns:
            String with one visualization block per record type.
        """
        pos = self.fp.tell()
        self.fp.seek(0)
        seen = {}  # type_code -> (raw_line, record, cols)
        try:
            for line in self.fp:
                raw = line.rstrip('\n')
                if len(raw) < self._record_type_len:
                    continue
                type_code = raw[:self._record_type_len]
                if type_code in seen:
                    continue
                cols = self._get_columns(type_code)
                if cols is None:
                    continue
                factory = self._get_factory(type_code)
                seen[type_code] = (raw, factory.from_line(raw), cols)
        finally:
            self.fp.seek(pos)

        if not seen:
            return ''

        blocks = []
        for type_code, (raw, record, cols) in seen.items():
            label = self.type_name_map.get(type_code, f"type '{type_code}'")
            type_comment = cols[0].comment
            heading = f"Record {label}" + (f"  # {type_comment}" if type_comment else '') + ':'
            rulers_boundary = '\n'.join(record.visualize().split('\n')[:3])
            block = (
                f"{heading}\n"
                f"{rulers_boundary}\n"
                f"{raw}  ← source\n"
                f"{record.to_line()}  ← interpreted"
            )
            blocks.append(block)
        return '\n\n'.join(blocks)

    def _generate_rows(self) -> Iterator[Record]:
        for line in self.fp:
            line = line.rstrip('\n')
            if len(line) < self._record_type_len:
                logger.debug("Line too short — skipping")
                continue

            type_code = line[:self._record_type_len]
            cols = self._get_columns(type_code)
            if cols is None:
                if self.strict:
                    raise ValueError(f"Unknown record type '{type_code}' at line {self._row_num}")
                else:
                    logger.debug(f"Skipping unknown record type '{type_code}'")
                    continue

            record = self._get_factory(type_code).from_line(line, auto_trim=self.auto_trim)
            yield record