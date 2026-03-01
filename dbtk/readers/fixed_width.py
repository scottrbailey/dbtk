# dbtk/readers/fixed_width.py

"""Fixed-width text file reader with column position specifications."""

import logging
from typing import TextIO, List, Dict, Any, Optional, Iterator

from .base import Reader, Record
from ..etl.transforms.datetime import parse_date, parse_datetime, parse_timestamp

logger = logging.getLogger(__name__)

class FixedColumn(object):
    """ Column definition for fixed width files """

    def __init__(self, name:str, start_pos:int, end_pos:int,
                 column_type:str='text',
                 comment: Optional[str] = None,
                 alignment: Optional[str] = None,
                 pad_char: Optional[str] = None):
        """
        :param str name:  database column name
        :param int start_pos: start position of field, first position is 1 not 0
        :param int end_pos: end position of field
        :param str column_type: text, int, float, date
        :param str comment: discription for column usage/options
        :param str alignment: override alignment (left, right, center)
        :param str pad_char: override pad character

        FixedColumn('birthdate', 25, 35, 'date')
        """
        self.name = name
        self.start_pos = start_pos
        self.end_pos = end_pos if end_pos else start_pos
        self.column_type = column_type
        self.start_idx = start_pos - 1
        self.comment = comment
        self.alignment = alignment
        self.pad_char = pad_char

    @property
    def width(self) -> int:
        return self.end_pos - self.start_pos + 1

    def __repr__(self):
        parts = [f"'{self.name}'", str(self.start_pos), str(self.end_pos), f"'{self.column_type}'"]
        if self.comment:
            parts.append(f"comment='{self.comment}'")
        if self.alignment:
            parts.append(f"alignment='{self.alignment}'")
        if self.pad_char:
            parts.append(f"pad_char='{self.pad_char}'")
        return f"FixedColumn({', '.join(parts)})"


class FixedReader(Reader):
    """ Reader for fixed width files """

    def __init__(self,
                 fp: TextIO,
                 columns: List[FixedColumn],
                 auto_trim: bool = True,
                 add_row_num: bool = True,
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
        else:
            # Binary mode or other file type
            self._trackable = fp

        self.columns = columns
        self.auto_trim = auto_trim

    def _read_headers(self) -> List[str]:
        """Return column names from FixedColumn definitions."""
        return [col.name for col in self.columns]

    def _generate_rows(self) -> Iterator[List[Any]]:
        while True:
            line = self.fp.readline()
            if not line:
                break
            row_data = []
            for col in self.columns:
                val = line[col.start_idx:col.end_pos]
                try:
                    if col.column_type == 'text' and self.auto_trim:
                        val = str(val).strip()
                    elif col.column_type == 'date':
                        val = parse_date(val)
                    elif col.column_type == 'datetime':
                        val = parse_datetime(val)
                    elif col.column_type == 'timestamp':
                        val = parse_timestamp(val)
                    elif col.column_type == 'int':
                        val = int(val.strip()) if val.strip() else None
                    elif col.column_type == 'float':
                        val = float(val.strip()) if val.strip() else None
                    else:
                        val = str(val)
                except (ValueError, TypeError):
                    val = str(val).strip() if self.auto_trim else str(val)
                row_data.append(val)
            yield row_data

    def _cleanup(self):
        """Close the file pointer."""
        if self.fp and hasattr(self.fp, 'close'):
            self.fp.close()

    @classmethod
    def visualize_columns(cls,
                          fp: TextIO,
                          columns: List[FixedColumn] = None,
                          sample_lines: int = 4) -> str:
        """
        Visualizes column boundaries and sample data from a fixed-width file.

        Args:
            fp: file pointer or file-like object in text mode
            columns: list of columns
            sample_lines: number of lines to show in preview

        Returns:
            String representation of column layout
        """
        fp.seek(0)
        lines = [next(fp).rstrip('\n') for _ in range(sample_lines)]
        max_len = max([len(line) for line in lines])
        ruler_10s = ''.join(str(i // 10 % 10) if i % 10 == 0 else ' ' for i in range(1, max_len))
        ruler_1s = ''.join(str(i % 10) for i in range(1, max_len))
        boundary_line = [' '] * max_len
        for col in columns:
            if col.start_pos <= max_len:
                boundary_line[col.start_pos - 1] = '|'
        return f'{ruler_10s}\n{ruler_1s}\n{"".join(boundary_line)}\n' + '\n'.join(lines)


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
        common EDI-like files, including ACH, are defined in `dbtk.readers.edi_formats.py`

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
        auto_trim : bool, default True
            Trim whitespace from field values
        skip_rows : int, default 0
            Number of initial data rows to skip after headers (if any)
        null_values : Collection[Any], optional
            Values to interpret as None
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
        ...     print(record._record_type, record.company_name, record.amount)
        """

    def __init__(
            self,
            fp: TextIO,
            columns: Dict[str, List[FixedColumn]],
            type_name_map: Optional[Dict[str, str]] = None,
            # ... pass through all FixedReader params ...
            **kwargs
    ):
        super().__init__(fp, columns=None, **kwargs)  # no single columns

        self.columns = columns
        self.type_name_map = type_name_map or {}

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

    def _get_columns(self, type_code: str) -> List[FixedColumn]:
        return self.columns.get(type_code)

    def _get_factory(self, type_code: str) -> type[Record]:
        if type_code not in self._type_factories:
            cols = self._get_columns(type_code)
            if cols is None:
                raise ValueError(f"No column definition for record type '{type_code}'")
            fields = [c.name for c in cols]
            RecordClass = type(f'EDI_{type_code}_Record', (Record,), {})
            RecordClass.set_fields(fields)
            self._type_factories[type_code] = RecordClass
        return self._type_factories[type_code]

    def _generate_rows(self) -> Iterator[Record]:
        for line in self._rdr:
            if len(line) < self._record_type_len:
                logger.debug("Line too short — skipping")
                continue

            type_code = line[:self._record_type_len]
            cols = self._get_columns(type_code)
            if cols is None:
                logger.debug(f"Skipping unknown record type '{type_code}'")
                continue

            # Parse fields
            row_values = []
            for col in cols:
                val = line[col.start_idx:col.end_idx]
                if self.auto_trim:
                    val = val.strip()
                row_values.append(val)

            if self.add_record_type:
                row_values.append(type_code)

            record = self._get_factory(type_code)(*row_values)
            yield record