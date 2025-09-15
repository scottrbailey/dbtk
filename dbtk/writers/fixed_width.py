# dbtk/writers/fixed_width.py
"""
Fixed width text writer.
"""

import itertools
import logging
import sys

from typing import Union, Optional, Sequence
from pathlib import Path

from .utils import get_data_iterator, format_value

logger = logging.getLogger(__name__)

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

    Examples:
        # Define column widths
        widths = [10, 25, 15, 8]
        to_fixed_width(cursor, widths, 'report.txt')

        # Write to stdout with custom formatting
        to_fixed_width(cursor, [15, 30], right_align_numbers=False)
    """
    data_iterator, columns = get_data_iterator(data)
    if filename is None:
        data_iterator = itertools.islice(data_iterator, 20)

    if len(column_widths) != len(columns):
        raise ValueError(
            f"Number of column widths ({len(column_widths)}) must match "
            f"number of columns ({len(columns)})"
        )

    # Track if any values were truncated
    length_warning = False

    # Determine output destination
    if filename is None:
        file_obj = sys.stdout
        close_file = False
    else:
        file_obj = open(filename, 'w', encoding=encoding)
        close_file = True

    try:
        # Write data rows
        for record in data_iterator:
            line = ''
            for i, width in enumerate(column_widths):
                # Get value from record
                if hasattr(record, '__getitem__'):
                    # List-like or dict-like access
                    try:
                        value = record[i] if isinstance(record, (list, tuple)) else record[columns[i]]
                    except (IndexError, KeyError):
                        value = None
                else:
                    # Attribute access
                    value = getattr(record, columns[i], None)

                # Format value as string
                str_value = format_value(value)

                # Determine if value should be right-aligned (numbers)
                should_right_align = (
                        right_align_numbers and
                        isinstance(value, (int, float)) and
                        value is not None
                )

                # Format for fixed width
                formatted_value = _format_fixed_width(
                    str_value,
                    width,
                    should_right_align,
                    truncate_overflow,
                    fill_char
                )

                if not length_warning and len(str_value) > width:
                    length_warning = True

                line += formatted_value

            file_obj.write(line + '\n')

        # Warn if values exceeded column widths
        if length_warning and truncate_overflow:
            logger.warning("Some values were truncated to fit column widths.")
        elif length_warning:
            # file is likely invalid
            logger.error("Some values were longer than column widths.  Columns may be misaligned!")
    finally:
        if close_file:
            file_obj.close()


def _format_fixed_width(value: str,
                        width: int,
                        right_align: bool,
                        truncate: bool,
                        fill_char: str) -> str:
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