# dbtk/readers/__init__.py

"""
File readers for data integration.

Supports CSV, Excel (XLS/XLSX), JSON, XML and fixed-width text files with
consistent interface and automatic format detection.

"""

from .base import Reader
from .utils import get_reader, open_file
from .csv import CSVReader
from .data_frame import DataFrameReader
from .json import JSONReader, NDJSONReader
from .excel import XLSReader, XLSXReader, open_workbook, get_sheet_by_index, check_dependencies
from .fixed_width import FixedReader, FixedColumn, EDIReader
from .xml import XMLReader, XMLColumn


# Re-export everything
__all__ = [
    'Reader', 'get_reader', 'open_file',
    'CSVReader', 'EDIReader', 'FixedColumn', 'FixedReader', 'JSONReader', 'NDJSONReader', 'XLSReader', 'XLSXReader',
    'XMLColumn', 'XMLReader', 'DataFrameReader', 'open_workbook', 'get_sheet_by_index'
]
