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
    Stateful Excel writer with multi-sheet support and batch streaming capabilities.

    ExcelWriter extends BatchWriter to provide high-performance Excel file generation using
    openpyxl. The workbook remains open across multiple write_batch() calls, enabling
    efficient multi-sheet reports and large dataset streaming without memory overhead.

    Key Features
    ------------
    * **Stateful workbook** - Opens once, writes multiple times, saves on exit
    * **Multi-sheet support** - Write to different sheets in a single workbook
    * **Batch streaming** - Append data incrementally without loading entire dataset
    * **Smart headers** - Automatically writes headers only when sheet is empty
    * **Type preservation** - Maintains dates, datetimes, and numeric types
    * **Auto-formatting** - Applies date/datetime styles and adjusts column widths
    * **Append mode** - Load existing workbooks and add/update sheets

    The writer can load existing .xlsx files and append new sheets or add rows to
    existing sheets. Headers are only written if the target sheet is empty (first
    cell of row 1 is None).

    Parameters
    ----------
    file : str or Path
        Output Excel filename (.xlsx). If file exists, it will be loaded and
        modified. If it doesn't exist, a new workbook will be created.
    sheet_name : str, optional
        Default sheet name for write_batch() calls that don't specify a sheet.
        If not provided, defaults to 'Data'.
    write_headers : bool, default True
        Whether to write column headers. Headers are only written when the
        target sheet is empty (determined by checking cell A1).

    Attributes
    ----------
    workbook : openpyxl.Workbook
        The active workbook instance, kept open during the context manager lifecycle.
    active_sheet : str
        The current default sheet name for write operations.
    columns : List[str]
        Column names detected from the first batch of data.
    row_count : int
        Total number of data rows written across all batches and sheets.

    Examples
    --------
    **Single-sheet export (legacy compatibility)**::

        with ExcelWriter('report.xlsx') as writer:
            writer.write_batch(cursor)  # Creates 'Data' sheet

    **Multi-sheet report**::

        with ExcelWriter('quarterly_report.xlsx') as writer:
            writer.write_batch(summary_data, sheet_name='Summary')
            writer.write_batch(revenue_data, sheet_name='Revenue')
            writer.write_batch(expenses_data, sheet_name='Expenses')

    **Streaming large datasets in batches**::

        with ExcelWriter('large_export.xlsx') as writer:
            for batch in surge.batched(reader):
                writer.write_batch(batch, sheet_name='Data')

    **Appending to existing workbook**::

        # First run creates file
        with ExcelWriter('monthly.xlsx') as writer:
            writer.write_batch(january_data, sheet_name='January')

        # Later run appends new sheet
        with ExcelWriter('monthly.xlsx') as writer:
            writer.write_batch(february_data, sheet_name='February')

    **Using with convenience function**::

        to_excel(cursor, 'simple_export.xlsx', sheet='Results')

    Notes
    -----
    * ExcelWriter preserves native types (dates, numbers) unlike CSV writers
    * Column widths are auto-adjusted based on first 15 rows of data
    * Maximum column width is capped at 60 characters for readability
    * Named styles (date_style, datetime_style, hyperlink_style) are registered on init
    * The workbook is saved only on context manager exit (__exit__)
    * For very large datasets (>100K rows), consider using CSV or database bulk loads

    See Also
    --------
    LinkedExcelWriter : Advanced writer with internal and external hyperlinking
    BatchWriter : Base class providing streaming/batch capabilities
    to_excel : Convenience function for simple single-sheet exports
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
        Write a batch of records to a worksheet.

        This is the primary method for writing data to Excel files. It can be called
        multiple times to append data to the same sheet or write to different sheets
        within the same workbook.

        Parameters
        ----------
        data : Iterable[RecordLike]
            Data to write. Accepts cursors, lists of Record objects, lists of dicts,
            or lists of tuples. Columns are auto-detected from the first record.
        sheet_name : str, optional
            Target worksheet name. If None, uses the active_sheet set during __init__,
            or defaults to 'Data'. Once set, becomes the new active_sheet for
            subsequent calls.

        Returns
        -------
        None

        Raises
        ------
        RuntimeError
            If workbook was not initialized properly.
        ValueError
            If columns cannot be determined from data.

        Notes
        -----
        * Headers are written only if the sheet is empty (cell A1 is None)
        * If the sheet already exists, data is appended starting after the last row
        * Column detection happens on first batch - subsequent batches must match columns
        * Native types (dates, numbers) are preserved with appropriate formatting
        * Row count is tracked in self._row_num across all batches

        Examples
        --------
        **Writing multiple batches to same sheet**::

            with ExcelWriter('output.xlsx') as writer:
                for batch in batched_reader:
                    writer.write_batch(batch, sheet_name='Data')

        **Writing different data to different sheets**::

            with ExcelWriter('report.xlsx') as writer:
                writer.write_batch(summary, sheet_name='Summary')
                writer.write_batch(details, sheet_name='Details')
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
    Defines a linkable data source for creating internal and external hyperlinks in Excel.

    LinkSource enables rich hyperlinking between worksheets and to external URLs. Each
    LinkSource is registered once with LinkedExcelWriter and can be used across multiple
    sheets to create consistent, navigable Excel reports.

    As the source sheet is written, LinkSource caches each record's cell reference and
    generates formatted link text and URLs using Python's str.format_map(). Later sheets
    can reference these cached records to create hyperlinks.

    Link Types
    ----------
    * **Internal links** - Excel cell references like ``#Students!A5`` for navigation
    * **External links** - HTTP/HTTPS URLs like ``https://crm.example.com/contact/123``
    * **Hybrid mode** - Stores both internal and external, defaults to external if available

    Parameters
    ----------
    name : str
        Unique identifier for this link source. Used when registering links in
        write_batch() (e.g., ``links={"student_name": "student"}``).
    source_sheet : str
        The worksheet name that serves as the authoritative source for this entity.
        Internal links will point to rows in this sheet. This sheet must be written
        before any sheets that reference it.
    key_column : str
        The column name containing unique identifiers for records (e.g., "student_id").
        This column must exist in both the source sheet data and any sheets that
        create links to it.
    url_template : str, optional
        Python format string for generating external URLs. Uses str.format_map() with
        the full record dict as context. Example: ``"https://app.com/users/{user_id}"``
    text_template : str, optional
        Python format string for generating link display text. Uses str.format_map()
        with the full record dict. Example: ``"{last_name}, {first_name} ({dept})"``
        If not provided, displays the key_column value.
    missing_text : str, optional
        Fallback text to display when a link target cannot be resolved. If None,
        displays the raw value from the detail row.

    Attributes
    ----------
    _records : dict
        Internal cache mapping key values to link metadata (ref, display_text, url).
        Populated automatically as the source sheet is written.

    Examples
    --------
    **Internal links only (sheet navigation)**::

        student_link = LinkSource(
            name="student",
            source_sheet="Students",
            key_column="student_id"
        )

    **External links with custom text**::

        employee_link = LinkSource(
            name="employee",
            source_sheet="Employees",
            key_column="employee_id",
            url_template="https://hr.company.com/profile/{employee_id}",
            text_template="{last_name}, {first_name} ({department})"
        )

    **Hybrid with missing value handling**::

        customer_link = LinkSource(
            name="customer",
            source_sheet="Customers",
            key_column="customer_id",
            url_template="https://crm.company.com/customers/{crm_id}",
            text_template="{company_name} - {contact_name}",
            missing_text="[Unknown Customer]"
        )

    Notes
    -----
    * The source sheet MUST be written before sheets that reference it
    * All template fields must exist in the record data or KeyError will be logged
    * Key values are converted to strings for cache lookups
    * Templates use Python's str.format_map() - use double braces {{}} to escape

    See Also
    --------
    LinkedExcelWriter : Writer that uses LinkSource for hyperlinking
    LinkedExcelWriter.register_link_source : Method to register a LinkSource
    """
    def __init__(self,
                 name: str,
                 source_sheet: str,
                 key_column: str,
                 url_template: str = None,
                 text_template: str = None,
                 missing_text: str = None):
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
    Advanced Excel writer with internal and external hyperlink management.

    LinkedExcelWriter extends ExcelWriter to enable rich, bidirectional hyperlinking
    within Excel workbooks and to external systems. It automatically caches source
    records as they're written and creates formatted hyperlinks in detail sheets that
    reference those sources.

    This is particularly powerful for creating navigable multi-sheet reports with
    master-detail relationships, drill-through capabilities, and integration with
    external CRM, ticketing, or web applications.

    Key Features
    ------------
    * **Internal navigation** - Links between worksheets (e.g., ``#Students!B5``)
    * **External integration** - Deep links to web applications
    * **Hybrid linking** - Store both internal and external, choose which to display
    * **Template-based formatting** - Use Python format strings for link text and URLs
    * **Automatic caching** - Source records cached as written, no manual tracking
    * **Mode control** - Force internal or external links per column via ``source:internal``

    Workflow
    --------
    1. Create LinkSource definitions for each linkable entity
    2. Register them with LinkedExcelWriter
    3. Write source sheets first (e.g., Students, Products)
    4. Write detail sheets with link specifications (e.g., Enrollments, Orders)
    5. Links are resolved from cache and applied automatically

    Parameters
    ----------
    file : str or Path
        Output Excel filename (.xlsx)
    sheet_name : str, optional
        Default sheet name for write_batch() calls
    write_headers : bool, default True
        Whether to write column headers

    Attributes
    ----------
    link_sources : Dict[str, LinkSource]
        Registered LinkSource instances, keyed by name

    Examples
    --------
    **Basic internal linking between sheets**::

        with LinkedExcelWriter('school_report.xlsx') as writer:
            # Define linkable entity
            student_link = LinkSource(
                name="student",
                source_sheet="Students",
                key_column="student_id"
            )
            writer.register_link_source(student_link)

            # Write source sheet
            writer.write_batch(students_data, sheet_name="Students")

            # Write detail sheet with internal links
            writer.write_batch(
                enrollments_data,
                sheet_name="Enrollments",
                links={"student_name": "student:internal"}
            )

    **External links to CRM system**::

        with LinkedExcelWriter('sales_report.xlsx') as writer:
            customer_link = LinkSource(
                name="customer",
                source_sheet="Customers",
                key_column="customer_id",
                url_template="https://crm.company.com/customers/{crm_id}",
                text_template="{company_name} ({customer_id})"
            )
            writer.register_link_source(customer_link)

            writer.write_batch(customers_data, sheet_name="Customers")
            writer.write_batch(
                orders_data,
                sheet_name="Orders",
                links={"customer": "customer"}  # Uses external URL
            )

    **Hybrid mode with internal and external links**::

        with LinkedExcelWriter('support_tickets.xlsx') as writer:
            ticket_link = LinkSource(
                name="ticket",
                source_sheet="Tickets",
                key_column="ticket_id",
                url_template="https://support.company.com/ticket/{ticket_id}",
                text_template="#{ticket_id} - {subject}"
            )
            writer.register_link_source(ticket_link)

            writer.write_batch(tickets_data, sheet_name="Tickets")
            writer.write_batch(
                comments_data,
                sheet_name="Comments",
                links={
                    "ticket_link": "ticket",           # External to support system
                    "ticket_ref": "ticket:internal"    # Internal sheet navigation
                }
            )

    **Multiple link sources in one sheet**::

        with LinkedExcelWriter('class_roster.xlsx') as writer:
            student_link = LinkSource(
                name="student",
                source_sheet="Students",
                key_column="student_id",
                text_template="{last_name}, {first_name}"
            )
            course_link = LinkSource(
                name="course",
                source_sheet="Courses",
                key_column="course_id",
                text_template="{course_code} - {title}"
            )

            writer.register_link_source(student_link)
            writer.register_link_source(course_link)

            writer.write_batch(students_data, sheet_name="Students")
            writer.write_batch(courses_data, sheet_name="Courses")
            writer.write_batch(
                enrollments_data,
                sheet_name="Enrollments",
                links={
                    "student_name": "student:internal",
                    "course_name": "course:internal"
                }
            )

    Notes
    -----
    * Source sheets MUST be written before detail sheets that reference them
    * The key_column must exist in both source and detail datasets
    * Missing links display missing_text if set, otherwise show raw value
    * Link mode syntax: ``"source_name"`` (external) or ``"source_name:internal"``
    * Templates use str.format_map() - all fields must exist in record data
    * Hyperlink styling (blue, underlined) is applied automatically

    See Also
    --------
    LinkSource : Link definition class
    ExcelWriter : Base writer without linking capabilities
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