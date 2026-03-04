# dbtk/writers/fixed_width.py
"""
Fixed-width and EDI text writers with batch streaming support.

Both writers are driven by FixedColumn schema objects — the same objects used
by FixedReader and EDIReader on the read side.  When input records are already
FixedWidthRecord instances (e.g. round-tripped through a reader), they go
straight to to_line().  Any other record type is cast into the appropriate
FixedWidthRecord subclass first.
"""

import logging
from typing import Dict, Iterable, List, Optional, Union, BinaryIO, TextIO
from pathlib import Path

from .base import BatchWriter, RecordLike
from ..record import FixedWidthRecord
from ..utils import FixedColumn

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Module-level helpers shared by both writers
# ------------------------------------------------------------------ #

def _cast_to_fixed_record(record: RecordLike, record_class: type) -> FixedWidthRecord:
    """Cast any record-like to a FixedWidthRecord subclass by positional values."""
    if isinstance(record, (list, tuple)):
        return record_class(*record)
    if hasattr(record, 'values'):   # dict or Record
        return record_class(*record.values())
    return record_class(*record)    # fallback: try iterating


def _extract_type_code(record: RecordLike, length: int) -> str:
    """Extract the type-code prefix from the first field of any record-like."""
    if isinstance(record, (list, tuple)):
        val = record[0]
    elif hasattr(record, 'values'):
        val = next(iter(record.values()))
    else:
        val = next(iter(record))
    return str(val)[:length]


# ------------------------------------------------------------------ #
# FixedWidthWriter
# ------------------------------------------------------------------ #

class FixedWidthWriter(BatchWriter):
    """
    Fixed-width text file writer with batch streaming capabilities.

    Field widths, alignment, and padding are driven by a ``List[FixedColumn]``
    schema — the same schema used by ``FixedReader``.  When input records are
    already ``FixedWidthRecord`` instances they are written directly via
    ``to_line()``.  Any other record type (dict, list, namedtuple, generic
    Record) is cast into the appropriate ``FixedWidthRecord`` subclass first.

    Parameters
    ----------
    data : Iterable[RecordLike], optional
        Initial data to write.  ``None`` for streaming mode.
    file : str, Path, TextIO, or BinaryIO, optional
        Output file or handle.  ``None`` writes to stdout.
    columns : List[FixedColumn]
        Column definitions — width, alignment, padding, and type per field.
    encoding : str, default ``'utf-8'``
        File encoding.
    truncate_overflow : bool, default ``True``
        Truncate values that exceed their column width.  ``False`` raises
        ``ValueError`` instead.

    Examples
    --------
    **Round-trip from FixedReader**::

        with open('input.txt') as fp:
            records = list(FixedReader(fp, MY_COLS))
        to_fixed_width(records, MY_COLS, 'output.txt')

    **Write from dicts**::

        rows = [{'code': 'A', 'amount': 42}, ...]
        FixedWidthWriter(rows, 'output.txt', MY_COLS).write()

    **Streaming / batch mode**::

        with FixedWidthWriter(file='output.txt', columns=MY_COLS) as w:
            for batch in source:
                w.write_batch(batch)
    """

    accepts_file_handle = True
    preserve_types = True   # to_line() handles all type conversion

    def __init__(self,
                 data: Optional[Iterable[RecordLike]] = None,
                 file: Optional[Union[str, Path, TextIO, BinaryIO]] = None,
                 columns: List[FixedColumn] = None,
                 encoding: str = 'utf-8',
                 truncate_overflow: bool = True):
        if not columns:
            raise ValueError("columns (List[FixedColumn]) is required for FixedWidthWriter")
        self.fixed_columns = list(columns)
        self.truncate_overflow = truncate_overflow
        self._fw_record_class = None    # lazy-created FixedWidthRecord subclass

        super().__init__(
            data=data,
            file=file,
            columns=[c.name for c in columns],  # string names for BatchWriter
            encoding=encoding,
            write_headers=False,
        )

    def _get_record_class(self) -> type:
        """Lazy-create a FixedWidthRecord subclass from fixed_columns."""
        if self._fw_record_class is None:
            cls = type('FWRecord', (FixedWidthRecord,), {})
            cls.set_fields(self.fixed_columns)
            self._fw_record_class = cls
        return self._fw_record_class

    def _write_data(self, file_obj) -> None:
        for record in self.data_iterator:
            if isinstance(record, FixedWidthRecord) and record.__class__._columns:
                fw = record
            else:
                fw = _cast_to_fixed_record(record, self._get_record_class())
            file_obj.write(fw.to_line(self.truncate_overflow) + '\n')
            self._row_num += 1


# ------------------------------------------------------------------ #
# EDIWriter
# ------------------------------------------------------------------ #

class EDIWriter(BatchWriter):
    """
    Writer for fixed-width files containing multiple record types (EDI-like formats).

    Symmetric counterpart to ``EDIReader``.  Takes the same
    ``Dict[str, List[FixedColumn]]`` schema and dispatches writes by record
    type code (always the first field of each record).

    When input records are already ``FixedWidthRecord`` instances (e.g. from
    ``EDIReader``), the type code is read from ``record[0]`` and checked
    against the schema, then ``to_line()`` is called directly.  Any other
    record type is cast into the appropriate ``FixedWidthRecord`` subclass.

    Parameters
    ----------
    data : Iterable[RecordLike], optional
        Initial data to write.  ``None`` for streaming mode.
    file : str, Path, TextIO, or BinaryIO, optional
        Output file or handle.  ``None`` writes to stdout.
    columns : Dict[str, List[FixedColumn]]
        Mapping of type codes to column definitions — same format as
        ``EDIReader``.  All keys must be strings of identical length.
    encoding : str, default ``'utf-8'``
        File encoding.
    truncate_overflow : bool, default ``False``
        Truncate values that exceed their column width.  ``False`` (default)
        raises ``ValueError`` — EDI files are typically length-strict.

    Examples
    --------
    **Read-modify-write loop**::

        from dbtk.readers.fixed_width import EDIReader
        from dbtk.writers.fixed_width import EDIWriter
        from dbtk.readers.edi_formats import ACH_COLUMNS

        with open('in.ach') as fp, EDIWriter('out.ach', ACH_COLUMNS) as w:
            w.write_batch(EDIReader(fp, ACH_COLUMNS))

    **Single-shot**::

        records = list(EDIReader(open('in.ach'), ACH_COLUMNS))
        to_edi(records, ACH_COLUMNS, 'out.ach')
    """

    accepts_file_handle = True
    preserve_types = True

    def __init__(self,
                 data: Optional[Iterable[RecordLike]] = None,
                 file: Optional[Union[str, Path, TextIO, BinaryIO]] = None,
                 columns: Dict[str, List[FixedColumn]] = None,
                 encoding: str = 'utf-8',
                 truncate_overflow: bool = False):
        if not columns:
            raise ValueError(
                "columns (Dict[str, List[FixedColumn]]) is required for EDIWriter"
            )
        lengths = {len(k) for k in columns}
        if len(lengths) != 1:
            raise ValueError("All record type keys must have the same length")

        self.edi_columns = dict(columns)
        self._record_type_len = next(iter(lengths))
        self.truncate_overflow = truncate_overflow
        self._type_factories: Dict[str, type] = {}

        super().__init__(
            data=data,
            file=file,
            columns=None,   # no single column list; detected per record by BatchWriter
            encoding=encoding,
            write_headers=False,
        )

    def _get_factory(self, type_code: str) -> type:
        """Lazy-create a FixedWidthRecord subclass for the given type code."""
        if type_code not in self._type_factories:
            cols = self.edi_columns.get(type_code)
            if cols is None:
                raise ValueError(f"No column definition for record type '{type_code}'")
            cls = type(f'EDI_{type_code}_Record', (FixedWidthRecord,), {})
            cls.set_fields(cols)
            self._type_factories[type_code] = cls
        return self._type_factories[type_code]

    def _write_data(self, file_obj) -> None:
        for record in self.data_iterator:
            if isinstance(record, FixedWidthRecord) and record.__class__._columns:
                type_code = str(record[0])[:self._record_type_len]
                if type_code not in self.edi_columns:
                    raise ValueError(f"Record type '{type_code}' not in schema")
                fw = record
            else:
                type_code = _extract_type_code(record, self._record_type_len)
                factory = self._get_factory(type_code)
                fw = _cast_to_fixed_record(record, factory)
            file_obj.write(fw.to_line(self.truncate_overflow) + '\n')
            self._row_num += 1


# ------------------------------------------------------------------ #
# Convenience functions
# ------------------------------------------------------------------ #

def to_fixed_width(data,
                   columns: List[FixedColumn],
                   file: Optional[Union[str, Path]] = None,
                   encoding: str = 'utf-8',
                   truncate_overflow: bool = True) -> None:
    """
    Export records to a fixed-width text file.

    Args:
        data: Iterable of records (FixedWidthRecord, dict, list, etc.)
        columns: FixedColumn definitions for width, alignment, and padding.
        file: Output file path.  If None, writes to stdout.
        encoding: File encoding.
        truncate_overflow: Truncate values that exceed column width.
    """
    FixedWidthWriter(
        data=data,
        file=file,
        columns=columns,
        encoding=encoding,
        truncate_overflow=truncate_overflow,
    ).write()


def to_edi(data,
           columns: Dict[str, List[FixedColumn]],
           file: Optional[Union[str, Path]] = None,
           encoding: str = 'utf-8',
           truncate_overflow: bool = False) -> None:
    """
    Export EDI records to a fixed-width text file.

    Args:
        data: Iterable of FixedWidthRecord (or compatible) instances.
        columns: Dict mapping type codes to FixedColumn definitions.
        file: Output file path.  If None, writes to stdout.
        encoding: File encoding.
        truncate_overflow: Truncate values that exceed column width.
    """
    EDIWriter(
        data=data,
        file=file,
        columns=columns,
        encoding=encoding,
        truncate_overflow=truncate_overflow,
    ).write()
