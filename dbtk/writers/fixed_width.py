# dbtk/writers/fixed_width.py
"""
Fixed width text writer with batch streaming support.
"""

import logging
from typing import Union, Optional, Sequence, List, Iterable, TextIO, BinaryIO
from pathlib import Path

from .base import BatchWriter, RecordLike

logger = logging.getLogger(__name__)


class FixedWidthWriter(BatchWriter):
    """
    Fixed-width text file writer with batch streaming capabilities.

    Writes data to fixed-width text files where each field occupies a specific number
    of characters. Supports batch streaming for large datasets and configurable
    formatting options including alignment, truncation, and padding.

    Parameters
    ----------
    data : Iterable[RecordLike], optional
        Initial data to write. Can be None for streaming mode.
    column_widths : Sequence[int]
        List of column widths in characters. Must match number of columns in data.
    file : str, Path, TextIO, or BinaryIO, optional
        Output file or file handle. If None, writes to stdout.
    columns : List[str], optional
        Column names for list-of-lists data
    encoding : str, default 'utf-8'
        File encoding for text output
    right_align_numbers : bool, default True
        If True, right-align numeric values within their columns
    truncate_overflow : bool, default True
        If True, truncate values that exceed column width. If False, raise ValueError.
    fill_char : str, default ' '
        Character used for padding to reach column width

    Raises
    ------
    ValueError
        If column_widths length doesn't match number of columns, or if value
        exceeds column width when truncate_overflow=False

    Examples
    --------
    **Traditional single-write mode**::

        widths = [10, 25, 15, 8]
        writer = FixedWidthWriter(data, widths, 'report.txt')
        writer.write()

    **Batch streaming mode**::

        with FixedWidthWriter(None, [10, 25, 15], 'output.txt') as writer:
            for batch in batched_data:
                writer.write_batch(batch)

    **Custom formatting**::

        writer = FixedWidthWriter(
            data,
            [15, 30, 10],
            'custom.txt',
            right_align_numbers=False,
            fill_char='.',
            truncate_overflow=True
        )
        writer.write()

    Notes
    -----
    * Values are always converted to strings (preserve_types=False)
    * Numeric type detection happens on original values before string conversion
    * Warning logged if any values are truncated
    * Each line is exactly sum(column_widths) characters plus newline
    """

    accepts_file_handle = True
    preserve_types = False  # Always convert to strings for fixed-width

    def __init__(self,
                 data: Optional[Iterable[RecordLike]],
                 column_widths: Sequence[int],
                 file: Optional[Union[str, Path, TextIO, BinaryIO]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 right_align_numbers: bool = True,
                 truncate_overflow: bool = True,
                 fill_char: str = ' '):
        """Initialize fixed width writer with batch streaming support."""
        self.column_widths = list(column_widths)
        self.right_align_numbers = right_align_numbers
        self.truncate_overflow = truncate_overflow
        self.fill_char = fill_char
        self.length_warning = False

        # Initialize BatchWriter (handles data=None case)
        super().__init__(
            data=data,
            file=file,
            columns=columns,
            encoding=encoding,
            write_headers=False  # Fixed-width doesn't have headers
        )

        # Validate column widths after columns are determined
        # (deferred if data=None, validated on first write_batch)
        if self.columns and len(self.column_widths) != len(self.columns):
            raise ValueError(
                f"Number of column widths ({len(self.column_widths)}) must match "
                f"number of columns ({len(self.columns)})"
            )

    def write_batch(self, data: Iterable[RecordLike]) -> None:
        """
        Write a batch of records to fixed-width format.

        Parameters
        ----------
        data : Iterable[RecordLike]
            Batch of records to write

        Raises
        ------
        ValueError
            If value exceeds column width and truncate_overflow=False
        """
        if not self.file_handle:
            raise RuntimeError("File handle not initialized. Use as context manager.")

        for record in data:
            # Detect columns on first record if not already set
            if self.columns is None:
                self.columns = self._detect_columns(record)
                # Validate widths now that we have columns
                if len(self.column_widths) != len(self.columns):
                    raise ValueError(
                        f"Number of column widths ({len(self.column_widths)}) must match "
                        f"number of columns ({len(self.columns)})"
                    )

            # Extract values as strings
            values = self._row_to_tuple(record)

            line = ''
            for i, (value, width) in enumerate(zip(values, self.column_widths)):
                # Get original value for numeric check
                if hasattr(record, '__getitem__'):
                    original_value = record[i] if isinstance(record, (list, tuple)) else record[self.columns[i]]
                else:
                    original_value = getattr(record, self.columns[i], None)

                if len(value) > width and not self.truncate_overflow:
                    raise ValueError(
                        f"Value '{value}' (length {len(value)}) exceeds column width {width}. "
                        f"Set truncate_overflow=True or increase column width."
                    )

                # Determine if value should be right-aligned (numbers)
                should_right_align = (
                    self.right_align_numbers and
                    isinstance(original_value, (int, float)) and
                    original_value is not None
                )

                # Format for fixed width
                formatted_value = self._format_fixed_width(
                    value,
                    width,
                    should_right_align,
                    self.truncate_overflow,
                    self.fill_char
                )

                if not self.length_warning and len(value) > width:
                    self.length_warning = True

                line += formatted_value

            self.file_handle.write(line + '\n')
            self._row_num += 1

        # Warn if values exceeded column widths
        if self.length_warning and self.truncate_overflow:
            logger.warning("Some values were truncated to fit column widths.")

    def _write_data(self, file_obj) -> None:
        """Write fixed-width data using write_batch (legacy support)."""
        self.file_handle = file_obj
        if self.data_iterator:
            self.write_batch(self.data_iterator)

    def _format_fixed_width(self, value: str, width: int, right_align: bool,
                            truncate: bool, fill_char: str) -> str:
        """
        Format a value to fit in a fixed width column.

        Args:
            value: String value to format
            width: Target width
            right_align: Whether to right-align the value
            truncate: Whether to truncate if too long
            fill_char: Character for padding

        Returns:
            Formatted string of exactly 'width' characters
        """
        if len(value) > width:
            if truncate:
                return value[:width]
            else:
                # Don't truncate, but this will mess up the fixed width format
                return value
        elif len(value) < width:
            # Pad to width
            padding_needed = width - len(value)
            if right_align:
                return fill_char * padding_needed + value
            else:
                return value + fill_char * padding_needed
        else:
            # Exact width
            return value


def to_fixed_width(data,
                   column_widths: Sequence[int],
                   file: Optional[Union[str, Path]] = None,
                   encoding: str = 'utf-8',
                   right_align_numbers: bool = True,
                   truncate_overflow: bool = True,
                   fill_char: str = ' ') -> None:
    """
    Export cursor or result set to fixed width text file.

    Args:
        data: Cursor object or list of records
        column_widths: List of column widths in characters
        file: Output filename. If None, writes to stdout
        encoding: File encoding
        right_align_numbers: Whether to right-align numeric values
        truncate_overflow: Whether to truncate values that exceed column width
        fill_char: Character to use for padding

    Example:
        # Define column widths
        widths = [10, 25, 15, 8]
        to_fixed_width(cursor, widths, 'report.txt')

        # Write to stdout with custom formatting
        to_fixed_width(cursor, [15, 30], right_align_numbers=False)
    """
    writer = FixedWidthWriter(
        data=data,
        column_widths=column_widths,
        file=file,
        encoding=encoding,
        right_align_numbers=right_align_numbers,
        truncate_overflow=truncate_overflow,
        fill_char=fill_char
    )
    writer.write()