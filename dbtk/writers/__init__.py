# dbtk/writers/__init__.py
"""
Data export writers for multiple formats.

Provides convenience functions and writer classes for exporting database results
to various file formats. All writers support:

- Iterables of records (Record, dict, namedtuple, list)
- Database cursors
- Automatic header generation
- Context manager protocol

Supported formats:
- CSV: Comma-separated values
- Excel: XLSX workbooks with multiple sheets
- JSON: Standard and newline-delimited JSON
- XML: Structured XML documents
- Fixed-width: Fixed-column-width text files
- Database: Direct database-to-database copying

Example
-------
::
    import dbtk.writers as writers

    # Quick export functions
    writers.to_csv(cursor, 'output.csv')
    writers.to_excel(cursor, 'report.xlsx', sheet='Results')
    writers.to_json(cursor, 'data.json')

    # Database-to-database
    writers.cursor_to_cursor(source_cursor, dest_cursor, 'target_table')
"""

from .csv import to_csv, CSVWriter
from .excel import to_excel, ExcelWriter, LinkedExcelWriter, LinkSource
from .fixed_width import to_fixed_width, FixedWidthWriter
from .json import to_json, to_ndjson, JSONWriter, NDJSONWriter
from .xml import to_xml, XMLWriter, XMLStreamer
from .database import cursor_to_cursor, DatabaseWriter

__all__ = ['to_csv', 'CSVWriter', 'to_excel', 'ExcelWriter', 'LinkedExcelWriter', 'LinkSource',
           'to_fixed_width', 'FixedWidthWriter',
           'to_json', 'to_ndjson', 'JSONWriter', 'NDJSONWriter',
           'to_xml', 'XMLWriter', 'XMLStreamer', 'DatabaseWriter', 'cursor_to_cursor']