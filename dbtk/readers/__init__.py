# dbtk/readers/__init__.py
"""
File readers for data integration.

Supports CSV, Excel (XLS/XLSX), JSON, XML and fixed-width text files with
consistent interface and automatic format detection.
"""

import re

from .base import Reader, Clean
from .utils import get_reader
from .csv import CSVReader
from .json import JSONReader, NDJSONReader
from .excel import XLReader, XLSXReader, open_workbook, get_sheet_by_index, check_dependencies
from .fixed_width import FixedReader, FixedColumn
from .xml import XMLReader, XMLColumn

from typing import List, Any, Optional

# Re-export everything
__all__ = [
    'Reader', 'Clean', 'get_reader',
    'CSVReader', 'FixedColumn',  'FixedReader', 'JSONReader', 'NDJSONReader', 'XLReader', 'XLSXReader',
    'XMLColumn', 'XMLReader', 'open_workbook', 'get_sheet_by_index'
]
