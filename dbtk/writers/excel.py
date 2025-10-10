# dbtk/writers/excel.py
"""
Excel writer for database results using openpyxl.
"""
import logging
from typing import Union, List, Optional
from pathlib import Path
from datetime import datetime, date, time

from .base import BaseWriter
from zipfile import BadZipFile

try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, NamedStyle
    from openpyxl.utils.exceptions import InvalidFileException
    from openpyxl.utils import get_column_letter
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


logger = logging.getLogger(__name__)

MIDNIGHT = time(0, 0)


class ExcelWriter(BaseWriter):
    """Excel writer class that extends BaseWriter."""

    def __init__(self,
                 data,
                 filename: Union[str, Path],
                 columns: Optional[List[str]] = None,
                 sheet: str = 'Data',
                 include_headers: bool = True,
                 overwrite_sheet: bool = True):
        """
        Initialize Excel writer.

        Args:
            data: Cursor object or list of records
            filename: Output Excel filename (.xlsx)
            columns: Column names for list-of-lists data (optional for other types)
            sheet: Sheet name to write to
            include_headers: Whether to include column headers
            overwrite_sheet: Whether to overwrite existing sheet
        """
        # Preserve data types for Excel output
        super().__init__(data, filename, columns, encoding='utf-8', preserve_data_types=True)
        self.sheet = sheet
        self.include_headers = include_headers
        self.overwrite_sheet = overwrite_sheet

    def _write_data(self, file_obj) -> None:
        """Write Excel data to workbook."""
        # Try to load existing workbook, create new if it doesn't exist
        try:
            if Path(self.filename).exists():
                workbook = load_workbook(self.filename)
            else:
                workbook = Workbook()
                # Remove default sheet if we're creating our own
                if 'Sheet' in workbook.sheetnames and self.sheet != 'Sheet':
                    workbook.remove(workbook['Sheet'])
        except (InvalidFileException, BadZipFile, ValueError) as e:
            raise ValueError(
                f"File '{self.filename}' exists but is not a valid Excel workbook that openpyxl can read. "
                f"Original error: {e}") from e

        # Create datetime styles
        date_style = NamedStyle(name='date_style', number_format='YYYY-MM-DD', font=Font(color='FF0000'))
        datetime_style = NamedStyle(name='datetime_style', number_format='YYYY-MM-DD HH:MM:SS', font=Font(color='0000FF'))

        # Register styles with workbook if not already present
        if 'date_style' not in workbook.named_styles:
            workbook.add_named_style(date_style)
        if 'datetime_style' not in workbook.named_styles:
            workbook.add_named_style(datetime_style)

        # Handle sheet creation/selection
        if self.sheet in workbook.sheetnames:
            if self.overwrite_sheet:
                workbook.remove(workbook[self.sheet])
                worksheet = workbook.create_sheet(self.sheet)
            else:
                worksheet = workbook[self.sheet]
                # Find next available row
                start_row = worksheet.max_row + 1
        else:
            worksheet = workbook.create_sheet(self.sheet)
            start_row = 1

        if not self.overwrite_sheet and self.sheet in workbook.sheetnames:
            # Appending to existing sheet
            row_offset = start_row - 1
        else:
            row_offset = 0

        # Initialize column width tracking
        column_widths = [len(col) for col in self.columns]  # Start with header lengths
        width_sample_size = 15  # number of rows to sample for column widths

        # Create header style
        header_font = Font(bold=True)

        # Write headers
        if self.include_headers:
            for col_idx, column_name in enumerate(self.columns, 1):
                cell = worksheet.cell(row=1 + row_offset, column=col_idx, value=column_name)
                if header_font:
                    cell.font = header_font
            data_start_row = 2 + row_offset
        else:
            data_start_row = 1 + row_offset

        # Write data and track column widths for sample
        for row_idx, record in enumerate(self.data_iterator, data_start_row):
            # Use BaseWriter to extract values with preserved data types
            values = self._extract_row_values(record)

            for col_idx, value in enumerate(values, 1):
                # Handle special data types and formatting
                cell = worksheet.cell(row=row_idx, column=col_idx)

                if isinstance(value, datetime) and value.time() != MIDNIGHT:
                    cell.value = value
                    cell.style = 'datetime_style'
                    # Update width tracking for datetime format
                    if self.row_count < width_sample_size:
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], 19)  # "YYYY-MM-DD HH:MM:SS"
                elif isinstance(value, (date, datetime)):
                    cell.value = value
                    cell.style = 'date_style'
                    # Update width tracking for date format
                    if self.row_count < width_sample_size:
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], 10)  # "YYYY-MM-DD"
                elif value is None:
                    cell.value = ''
                    # Don't update width for None values
                else:
                    cell.value = value
                    # Update column width tracking (only for sample)
                    if self.row_count < width_sample_size:
                        value_length = len(str(value))
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], value_length)

            self.row_count += 1

        # Apply column widths using get_column_letter
        for col_idx, width in enumerate(column_widths, 1):
            adjusted_width = min(max(width + 2, 6), 60)
            column_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[column_letter].width = adjusted_width

        # Save workbook
        workbook.save(self.filename)

    def write(self) -> int:
        """Override to bypass file handle creation and call _write_data directly."""
        try:
            self._write_data(None)  # Pass None since we don't need file_obj
            logger.info(f"Wrote {self.row_count} rows to {self.filename}")
            return self.row_count
        except Exception as e:
            logger.error(f"Error writing Excel data: {e}")
            raise


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
    writer = ExcelWriter(
        data=data,
        filename=filename,
        sheet=sheet,
        include_headers=include_headers,
        overwrite_sheet=overwrite_sheet
    )
    writer.write()

def check_dependencies():
    """Check for optional dependencies and issue warnings if missing."""
    if not HAS_OPENPYXL:
        logger.error('Openpyxl is not available. Excel files not supported.')

check_dependencies()