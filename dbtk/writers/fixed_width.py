# dbtk/writers/fixed_width.py
"""
Fixed width text writer.
"""

import logging
from typing import Union, Optional, Sequence, List
from pathlib import Path

from .base import BaseWriter

logger = logging.getLogger(__name__)


class FixedWidthWriter(BaseWriter):
    """Fixed width writer class that extends BaseWriter."""

    def __init__(self,
                 data,
                 column_widths: Sequence[int],
                 file: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 right_align_numbers: bool = True,
                 truncate_overflow: bool = True,
                 fill_char: str = ' '):
        """
        Initialize fixed width writer.

        Args:
            data: Cursor object or list of records
            column_widths: List of column widths in characters
            file: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            right_align_numbers: Whether to right-align numeric values
            truncate_overflow: Whether to truncate values that exceed column width
            fill_char: Character to use for padding
        """
        # Always convert to text for fixed-width output
        super().__init__(data, file, columns, encoding, preserve_types=False)

        self.column_widths = list(column_widths)
        self.right_align_numbers = right_align_numbers
        self.truncate_overflow = truncate_overflow
        self.fill_char = fill_char
        self.length_warning = False

        # Validate column widths match column count
        if len(self.column_widths) != len(self.columns):
            raise ValueError(
                f"Number of column widths ({len(self.column_widths)}) must match "
                f"number of columns ({len(self.columns)})"
            )

    def _write_data(self, file_obj) -> None:
        """Write fixed-width data to file object."""
        # Write data rows
        for record in self.data_iterator:
            # Use BaseWriter to extract values as strings
            values = self._extract_row_values(record)

            line = ''
            for i, (value, width) in enumerate(zip(values, self.column_widths)):
                # Get original value for numeric check
                if hasattr(record, '__getitem__'):
                    original_value = record[i] if isinstance(record, (list, tuple)) else record[self.columns[i]]
                else:
                    original_value = getattr(record, self.columns[i], None)

                if len(value) > width and not self.truncate_overflow:
                    raise ValueError(f"Value '{value}' (length {len(value)}) exceeds column width {width}. "
                                     f"Set truncate_overflow=True or increase column width.")

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

            file_obj.write(line + '\n')
            self._row_num += 1

        # Warn if values exceeded column widths
        if self.length_warning and self.truncate_overflow:
            logger.warning("Some values were truncated to fit column widths.")

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
                   filename: Optional[Union[str, Path]] = None,
                   encoding: str = 'utf-8',
                   right_align_numbers: bool = True,
                   truncate_overflow: bool = True,
                   fill_char: str = ' ') -> None:
    """
    Export cursor or result set to fixed width text file.

    Args:
        data: Cursor object or list of records
        column_widths: List of column widths in characters
        filename: Output filename. If None, writes to stdout
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
        file=filename,
        encoding=encoding,
        right_align_numbers=right_align_numbers,
        truncate_overflow=truncate_overflow,
        fill_char=fill_char
    )
    writer.write()