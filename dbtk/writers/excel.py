# dbtk/writers/excel.py
"""
Excel writer for database results using openpyxl.
"""
import logging

from typing import Union
from pathlib import Path
from datetime import datetime, date, time

from .utils import get_data_iterator

try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, NamedStyle
    from openpyxl.utils.exceptions import InvalidFileException
except ImportError:
    raise ImportError("openpyxl is required for Excel support. Install with: pip install openpyxl")

logger = logging.getLogger(__name__)


def to_excel(data,
             filename: Union[str, Path],
             sheet: str = 'Data',
             include_headers: bool = True,
             overwrite_sheet: bool = True) -> None:
    """
    Export cursor or result set to Excel file.

    Args:
        data: Cursor object or list of records
        filename: Output Excel filename (.xlsx)
        sheet: Sheet name to write to
        include_headers: Whether to include column headers
        overwrite_sheet: Whether to overwrite existing sheet

    Examples:
        # Create new file
        to_excel(cursor, 'report.xlsx')

        # Add to existing workbook
        to_excel(cursor, 'existing.xlsx', sheet='NewData', overwrite_sheet=False)

        # Multiple sheets
        to_excel(users_cursor, 'report.xlsx', sheet='Users')
        to_excel(orders_cursor, 'report.xlsx', sheet='Orders')
    """
    MIDNIGHT = time(0, 0)
    data_iterator, columns = get_data_iterator(data)

    # Try to load existing workbook, create new if it doesn't exist
    try:
        if Path(filename).exists():
            workbook = load_workbook(filename)
        else:
            workbook = Workbook()
            # Remove default sheet if we're creating our own
            if 'Sheet' in workbook.sheetnames and sheet != 'Sheet':
                workbook.remove(workbook['Sheet'])
    except (FileNotFoundError, InvalidFileException):
        workbook = Workbook()

    # Create datetime styles
    date_style = NamedStyle(name='date_style', number_format='YYYY-MM-DD')
    datetime_style = NamedStyle(name='datetime_style', number_format='YYYY-MM-DD HH:MM:SS')

    # Register styles with workbook if not already present
    if 'date_style' not in workbook.named_styles:
        workbook.add_named_style(date_style)
    if 'datetime_style' not in workbook.named_styles:
        workbook.add_named_style(datetime_style)

    # Handle sheet creation/selection
    if sheet in workbook.sheetnames:
        if overwrite_sheet:
            workbook.remove(workbook[sheet])
            worksheet = workbook.create_sheet(sheet)
        else:
            worksheet = workbook[sheet]
            # Find next available row
            start_row = worksheet.max_row + 1
    else:
        worksheet = workbook.create_sheet(sheet)
        start_row = 1

    if not overwrite_sheet and sheet in workbook.sheetnames:
        # Appending to existing sheet
        row_offset = start_row - 1
    else:
        row_offset = 0

    # Initialize column width tracking
    column_widths = [len(col) for col in columns]  # Start with header lengths
    width_sample_size = 15 # number of rows to sample for column widths
    rows_processed = 0

    # Create header style
    header_font = Font(bold=True)

    # Write headers
    if include_headers:
        for col_idx, column_name in enumerate(columns, 1):
            cell = worksheet.cell(row=1 + row_offset, column=col_idx, value=column_name.upper())
            if header_font:
                cell.font = header_font
        data_start_row = 2 + row_offset
    else:
        data_start_row = 1 + row_offset

    # Write data and track column widths for sample
    for row_idx, record in enumerate(data_iterator, data_start_row):
        for col_idx in range(len(columns)):
            # Get value from record
            if hasattr(record, '__getitem__'):
                # List-like or dict-like access
                try:
                    value = record[col_idx] if isinstance(record, (list, tuple)) else record[columns[col_idx]]
                except (IndexError, KeyError):
                    value = None
            else:
                # Attribute access (namedtuple, etc.)
                value = getattr(record, columns[col_idx], None)

            # Handle special data types and formatting
            cell = worksheet.cell(row=row_idx, column=col_idx + 1)

            if isinstance(value, datetime) and value.time() != MIDNIGHT:
                cell.value = value
                cell.style = 'datetime_style'
                # Update width tracking for datetime format
                if rows_processed < width_sample_size:
                    column_widths[col_idx] = max(column_widths[col_idx], 19)  # "YYYY-MM-DD HH:MM:SS"
            elif isinstance(value, (date, datetime)):
                cell.value = value
                cell.style = 'date_style'
                # Update width tracking for date format
                if rows_processed < width_sample_size:
                    column_widths[col_idx] = max(column_widths[col_idx], 10)  # "YYYY-MM-DD"
            elif value is None:
                cell.value = ''
                if rows_processed < width_sample_size:
                    # Don't update width for None values
                    pass
            else:
                cell.value = value
                # Update column width tracking (only for sample)
                if rows_processed < width_sample_size:
                    value_length = len(str(value))
                    column_widths[col_idx] = max(column_widths[col_idx], value_length)

        rows_processed += 1

    # Apply column widths
    for col_idx, width in enumerate(column_widths, 1):
        adjusted_width = min(max(width + 2, 10), 50)
        worksheet.column_dimensions[worksheet.cell(row=1, column=col_idx).column_letter].width = adjusted_width

    # Save workbook
    workbook.save(filename)
    logger.info(f"Wrote {rows_processed} records to {filename} sheet:({sheet})")