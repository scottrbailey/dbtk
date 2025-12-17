# dbtk/writers/excel.py
"""
Excel writer for database results using openpyxl.
"""
import logging
from typing import Any, Union, List, Optional, Iterable
from pathlib import Path
from datetime import datetime, date, time
from zipfile import BadZipFile
from dataclasses import dataclass, field
from typing import Any, Optional, Dict
from openpyxl.worksheet.worksheet import Worksheet

from .base import BatchWriter, RecordLike

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


class ExcelWriter(BatchWriter):
    """
    Stateful Excel writer using openpyxl.

    Keeps the workbook open across multiple write_batch() calls and saves only on context exit.
    Designed for both single-sheet legacy use and multi-sheet reports.

    Usage examples:

    # Legacy single-sheet
    with ExcelWriter('report.xlsx') as writer:
        writer.write_batch(cursor)  # goes to sheet 'Data'

    # Multi-sheet report
    with ExcelWriter('report.xlsx', sheet_name='Summary') as writer:
        writer.write_batch(summary_data, sheet_name='Summary')
        writer.write_batch(users_data, sheet_name='Users')
        writer.write_batch(orders_data, sheet_name='Orders')

    # Streaming / batch mode
    with ExcelWriter('large.xlsx') as writer:
        for batch in large_generator:
            writer.write_batch(batch, sheet_name='Data')  # appends to 'Data'
    """

    accepts_file_handle = False
    preserve_types = True

    def __init__(
        self,
        file: Union[str, Path],
        sheet_name: Optional[str] = None,
        write_headers: bool = True,
    ):
        """
        Initialize the Excel writer.

        Parameters
        ----------
        file : str or Path
            Output Excel filename (.xlsx)
        sheet_name : str, optional
            Default/active sheet name to use for write_batch() calls without explicit sheet_name
        write_headers : bool, default True
            Whether to write column headers (only when sheet is empty)
        """
        super().__init__(data=None, file=file, write_headers=write_headers)

        self.output_path = Path(file)
        self.active_sheet: Optional[str] = sheet_name
        self.workbook: Optional[Workbook] = None

        self._load_or_create_workbook()

    def _load_or_create_workbook(self) -> None:
        """Load existing workbook or create a new one."""
        try:
            if self.output_path.exists():
                self.workbook = load_workbook(self.output_path)
                logger.info(f"Loaded existing workbook: {self.output_path}")
            else:
                self.workbook = Workbook()
                if 'Sheet' in self.workbook.sheetnames:
                    self.workbook.remove(self.workbook['Sheet'])
        except (InvalidFileException, BadZipFile, ValueError) as e:
            raise ValueError(
                f"File '{self.output_path}' exists but is not a valid Excel workbook. "
                f"Original error: {e}"
            ) from e

        self._register_styles()

    def _register_styles(self) -> None:
        """Register common styles (date, datetime, hyperlink)."""
        if self.workbook is None:
            return

        styles = [
            NamedStyle(name='date_style', number_format='YYYY-MM-DD'),
            NamedStyle(name='datetime_style', number_format='YYYY-MM-DD HH:MM:SS'),
            NamedStyle(
                name='hyperlink_style',
                font=Font(color="0000FF", underline="single")
            ),
        ]

        for style in styles:
            if style.name not in self.workbook.named_styles:
                self.workbook.add_named_style(style)


    def _get_or_create_worksheet(self, sheet_name: str) -> 'Worksheet':
        """Get existing worksheet or create new one."""
        from openpyxl.worksheet.worksheet import Worksheet

        if sheet_name in self.workbook.sheetnames:
            return self.workbook[sheet_name]
        else:
            return self.workbook.create_sheet(sheet_name)

    def _get_named_style(self, name: str) -> NamedStyle:
        for style in self.workbook.named_styles:
            if style.name == name:
                return style
        raise KeyError(f"Named style '{name}' not found")

    def _write_to_worksheet(
        self,
        data: Iterable[RecordLike],
        worksheet: 'Worksheet',
        columns: Optional[List[str]] = None,
        write_headers: bool = True,

    ) -> int:
        """
        Internal method: write data to an already-selected worksheet.

        Returns number of rows written.
        """
        from openpyxl.worksheet.worksheet import Worksheet

        # Lazy init columns
        self.data_iterator, detected_columns = self._get_data_iterator(data, columns)
        if self.columns is None:
            self.columns = detected_columns

        if not self.columns:
            raise ValueError("Could not determine columns from data")

        row_count = 0
        column_widths = [len(col) for col in self.columns]
        width_sample_size = 15
        header_font = Font(bold=True)

        # Decide if we should write headers (only if sheet is empty and requested)
        # Write headers if the first cell in row 1 is empty
        should_write_headers = write_headers and worksheet.cell(1, 1).value is None
        data_start_row = 2 if should_write_headers else worksheet.max_row + 1

        if should_write_headers:
            for col_idx, column_name in enumerate(self.columns, 1):
                cell = worksheet.cell(row=1, column=col_idx, value=column_name)
                cell.font = header_font

        # Write data rows
        for row_idx, record in enumerate(self.data_iterator, data_start_row):
            values = self._row_to_tuple(record)

            for col_idx, value in enumerate(values, 1):
                cell = worksheet.cell(row=row_idx, column=col_idx)

                if isinstance(value, datetime) and value.time() != MIDNIGHT:
                    cell.value = value
                    cell.style = 'datetime_style'
                    if row_count < width_sample_size:
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], 19)
                elif isinstance(value, (date, datetime)):
                    cell.value = value
                    cell.style = 'date_style'
                    if row_count < width_sample_size:
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], 10)
                elif value is None:
                    cell.value = ''
                else:
                    cell.value = value
                    if row_count < width_sample_size:
                        value_length = len(str(value))
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], value_length)

            row_count += 1

        # Apply column widths
        for col_idx, width in enumerate(column_widths, 1):
            adjusted_width = min(max(width + 2, 6), 60)
            column_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[column_letter].width = adjusted_width

        return row_count

    def write_batch(
        self,
        data: Iterable[RecordLike],
        sheet_name: Optional[str] = None,
    ) -> None:
        """
        Write a batch of data to a sheet.

        Parameters
        ----------
        data : Iterable[RecordLike]
            The data batch
        sheet_name : str, optional
            Target sheet. If None, uses active_sheet or defaults to 'Data'
        """
        if self.workbook is None:
            raise RuntimeError("Workbook not initialized")

        target_sheet = sheet_name or self.active_sheet or 'Data'
        if sheet_name:
            self.active_sheet = sheet_name

        worksheet = self._get_or_create_worksheet(target_sheet)

        row_count = self._write_to_worksheet(
            data=data,
            worksheet=worksheet,
            write_headers=self.write_headers,
            target_sheet_name=target_sheet,
        )

        self._row_num += row_count
        logger.info(f"Wrote {row_count} rows to sheet '{target_sheet}' (total: {self._row_num})")

    def _write_data(self, file_obj: Any) -> None:
        """
        BatchWriter contract implementation for legacy compatibility.

        Writes current data_iterator to the active sheet or 'Data'.
        """
        if self.data_iterator is None:
            raise RuntimeError("No data provided for legacy write mode")

        self.write_batch(self.data_iterator)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Save workbook on context exit."""
        if self.workbook is not None:
            try:
                self.workbook.save(self.output_path)
                logger.info(f"Saved workbook: {self.output_path}")
            except Exception as e:
                logger.error(f"Failed to save workbook: {e}")
                raise
        super().__exit__(exc_type, exc_val, exc_tb)


def to_excel(
    data,
    filename: Union[str, Path],
    sheet: str = 'Data',
    write_headers: bool = True,
) -> None:
    """
    Legacy convenience function — writes a single sheet.

    For multi-sheet or advanced reports, use ExcelWriter as a context manager with write_batch().
    """
    with ExcelWriter(file=filename) as writer:
        writer.write_batch(data=data, sheet_name=sheet)


class LinkSource:
    """
    Definition of a linkable entity for rich hyperlinking.

    Created and registered once, then used across multiple sheets.
    """
    def __init__(self,
                 name: str,
                 source_sheet: str,
                 key_column: str,
                 url_template: str = None,
                 text_template: str = None,
                 missing_text: str = None):
        """
        Defines a link that will generate links as LinkedExcelWriter creates spreadsheets.
        Links can be either internal #Data!A1 or external 'https://my.company/com...'
        Links can be used on any sheet, including the source_sheet. However, the source sheet must
        be written first.

        All linked records will store the internal link (where it was written to). If url_template is
        provided, it will also store an external link. By default, the external link will be returned
        if it exists.  If you have an external link, but want the internal link, use [link-name]:internal

        Attributes:
            name (str): The name of the link.
            source_sheet (str): The worksheet name that will server as the source for the links.
                         The internal links will point to this sheet.
            key_column (str): The key column defining unique records.
                        If linking between sheets, this key must be present in data for both.
            url_template (str, optional): The URL template for generating
                        URLs. 'https://my.company/employee/{employee_id}/details'
            text_template (str, optional): The text template for formatting
                        output text. '{full_name} ({department})'
            missing_text (str, optional): The fallback text for missing entries.
        """
        self.name = name
        self.source_sheet = source_sheet
        self.key_column = key_column
        self.url_template = url_template
        self.text_template = text_template
        self.missing_text = missing_text
        self._records = {}

    def cache_record(self, key_value: Any, row_dict: Dict[str, Any], ref: str) -> None:
        key_str = str(key_value)

        if self.text_template:
            try:
                display_text = self.text_template.format_map(row_dict)
            except KeyError as e:
                logger.warning(f"Missing key {e} in text_template for {self.name}")
                display_text = f"{key_value} ({self.missing_text})"
        else:
            # No template → use the raw key value (clean, expected)
            display_text = str(key_value)

        record = {
            "ref": ref,
            "display_text": display_text,
        }

        if self.url_template:
            try:
                record["url"] = self.url_template.format_map(row_dict)
            except KeyError as e:
                logger.warning(f"Missing key {e} in url_template for {self.name}")

        self._records[key_str] = record

    def get_link(
        self,
        key_value: Any,
        mode: str = "external",
    ) -> Optional[dict]:
        """
        Resolve link for a key.

        mode: "external" (default) or "internal"
        Returns dict with "target" and "display_text" or None if missing.
        """
        record = self._records.get(str(key_value))
        if not record:
            return None

        if mode == "external":
            target = record.get("url") or record.get("ref")
        else:
            target = record.get("ref")

        if not target:
            return None

        return {
            "target": target,
            "display_text": record["display_text"],
        }


class LinkedExcelWriter(ExcelWriter):
    """
    Advanced Excel writer with rich internal + external hyperlinking.

    Usage:
        with LinkedExcelWriter('report.xlsx') as writer:
            student_source = LinkSource(
                name="student",
                sheet="Students",
                key_column="student_id",
                url_template="https://crm.example.com/contact/{crm_contact_id}",
                text_template="{full_name} ({student_id})"
            )
            writer.register_link_source(student_source)

            writer.write_batch(students_data, sheet_name="Students")

            writer.write_batch(
                enrollments_data,
                sheet_name="Enrollments",
                links={
                    "full_name": "student",                    # external (CRM)
                    "student_id": "student:internal",          # internal navigation
                }
            )
    """

    def __init__(
        self,
        file: Union[str, Path],
        sheet_name: Optional[str] = None,
        write_headers: bool = True,
    ):
        super().__init__(file=file, sheet_name=sheet_name, write_headers=write_headers)
        self.link_sources: Dict[str, LinkSource] = {}

    def register_link_source(self, source: LinkSource) -> None:
        """Register a link source for use across sheets."""
        if source.name in self.link_sources:
            logger.warning(f"LinkSource '{source.name}' already registered — overwriting")
        self.link_sources[source.name] = source

    def write_batch(
        self,
        data: Iterable[RecordLike],
        sheet_name: Optional[str] = None,
        links: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Write a batch with optional hyperlinking.

        links: dict column_name → "source_name" or "source_name:internal"
        """
        target_sheet = sheet_name or self.active_sheet or 'Data'
        if sheet_name:
            self.active_sheet = target_sheet

        worksheet = self._get_or_create_worksheet(target_sheet)

        # Parse links dict into resolved mapping: column → (source, mode)
        link_mapping = {}
        if links:
            for col, spec in links.items():
                if ':' in spec:
                    source_name, mode_str = spec.split(':', 1)
                    mode = mode_str.lower()
                    if mode not in {'internal', 'external'}:
                        raise ValueError(f"Invalid mode '{mode_str}' in link spec '{spec}'")
                else:
                    source_name = spec
                    mode = "external"

                if source_name not in self.link_sources:
                    raise ValueError(f"Unknown LinkSource '{source_name}'")

                link_mapping[col] = (self.link_sources[source_name], mode)

        # Determine if this sheet is a source sheet for any registered LinkSource
        source_for_this_sheet = [
            src for src in self.link_sources.values() if src.source_sheet == target_sheet
        ]

        row_count = self._write_to_worksheet(
            data=data,
            worksheet=worksheet,
            write_headers=self.write_headers and (worksheet.cell(1, 1).value is None),
            link_mapping=link_mapping,
            source_for_this_sheet=source_for_this_sheet,
            target_sheet=target_sheet
        )

        self._row_num += row_count
        logger.info(f"Wrote {row_count} rows to sheet '{target_sheet}' with linking")

    def _write_to_worksheet(
        self,
        data: Iterable[RecordLike],
        worksheet: Worksheet,
        columns: Optional[List[str]] = None,
        write_headers: bool = True,
        link_mapping: Optional[Dict[str, tuple]] = None,
        source_for_this_sheet: Optional[list] = None,
        target_sheet: Optional[Worksheet] = None
    ) -> int:
        link_mapping = link_mapping or {}
        source_for_this_sheet = source_for_this_sheet or []

        # Lazy init columns
        self.data_iterator, detected_columns = self._get_data_iterator(data, columns)
        if self.columns is None:
            self.columns = detected_columns

        if not self.columns:
            raise ValueError("Could not determine columns from data")

        row_count = 0
        column_widths = [len(col) for col in self.columns]
        width_sample_size = 15
        header_font = Font(bold=True)

        should_write_headers = write_headers and (worksheet.cell(1, 1).value is None)
        data_start_row = 2 if should_write_headers else worksheet.max_row + 1

        if should_write_headers:
            for col_idx, column_name in enumerate(self.columns, 1):
                cell = worksheet.cell(row=1, column=col_idx, value=column_name)
                cell.font = header_font

        col_index_map = {name: idx + 1 for idx, name in enumerate(self.columns)}

        for row_idx, record in enumerate(self.data_iterator, data_start_row):
            row_dict = dict(zip(self.columns, self._row_to_tuple(record)))
            values = self._row_to_tuple(record)

            for col_idx, value in enumerate(values, 1):
                col_name = self.columns[col_idx - 1]
                cell = worksheet.cell(row=row_idx, column=col_idx)

                # Check if this column has a link spec
                link_spec = link_mapping.get(col_name)
                if link_spec:
                    source, mode = link_spec
                    key_value = row_dict.get(source.key_column, value)
                    link_info = source.get_link(key_value, mode=mode) if key_value is not None else None

                    if link_info:
                        cell.hyperlink = link_info["target"]
                        cell.value = link_info["display_text"]
                        cell.style = 'hyperlink_style'
                    elif source.missing_text is not None:
                        cell.value = source.missing_text
                    else:
                        # No link found — use raw value from detail row
                        cell.value = value

                # Normal value handling (dates, etc.)
                if isinstance(value, datetime) and value.time() != MIDNIGHT:
                    cell.value = value
                    cell.style = 'datetime_style'
                    if row_count < width_sample_size:
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], 19)
                elif isinstance(value, (date, datetime)):
                    cell.value = value
                    cell.style = 'date_style'
                    if row_idx < width_sample_size:
                        # sample column size to adjust later
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], 10)
                elif value is None:
                    cell.value = ''
                else:
                    cell.value = value
                    if row_count < width_sample_size:
                        value_length = len(str(value))
                        column_widths[col_idx - 1] = max(column_widths[col_idx - 1], value_length)

            # If this sheet is a source, cache the row for future linking
            if source_for_this_sheet:
                key_col_letter = get_column_letter(col_index_map[source_for_this_sheet[0].key_column])
                ref = f"#{target_sheet}!{key_col_letter}{row_idx}"
                key_value = row_dict.get(source_for_this_sheet[0].key_column)
                if key_value is not None:
                    source_for_this_sheet[0].cache_record(key_value, row_dict, ref)

            row_count += 1

        # Apply column widths
        for col_idx, width in enumerate(column_widths, 1):
            adjusted_width = min(max(width + 2, 6), 60)
            column_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[column_letter].width = adjusted_width

        return row_count


def check_dependencies():
    if not HAS_OPENPYXL:
        logger.error('Openpyxl is not available. Excel files not supported.')

check_dependencies()