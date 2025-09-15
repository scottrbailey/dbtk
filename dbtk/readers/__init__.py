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
from .json import JSONReader
from .excel import XLReader, XLSXReader, open_workbook, get_sheet_by_index, check_dependencies
from .fixed_width import FixedReader, FixedColumn
from .xml import XMLReader, XMLColumn

from typing import List, Any, Optional

# Re-export everything
__all__ = [
    'Reader', 'Clean', 'get_reader',
    'CSVReader', 'FixedColumn',  'FixedReader', 'JSONReader', 'XLReader', 'XLSXReader', 'XMLColumn', 'XMLReader',
    'open_workbook', 'get_sheet_by_index'
]


class Clean:
    """Header cleaning levels for column names."""
    NOOP = 0  # Leave unchanged
    LOWER = 1  # Lower case header
    LOWER_NOSPACE = 2  # Lower case and replace spaces with _
    LOWER_ALPHANUM = 3  # Lower case, remove all non-alphanum characters
    STANDARDIZE = 4  # Lower case, remove non-alphanum, strip "code" endings
    DEFAULT = 2  # Default to LOWER_NOSPACE


def clean_header(val: Any, level: Clean = Clean.DEFAULT) -> str:
    """
    Clean up column header names.

    Args:
        val: Column name (any type, will be converted to string)
        level: Cleaning level from Clean

    Returns:
        Cleaned column name string

    Examples:
        clean_header("#Term Code")                        # -> "#term_code"
        clean_header("#Term Code", Clean.LOWER)           # -> "#term code"
        clean_header("#Term Code", Clean.LOWER_NOSPACE)   # -> "#term_code"
        clean_header("#Term Code", Clean.LOWER_ALPHANUM)  # -> "termcode"
        clean_header("#Term Code", Clean.STANDARDIZE)     # -> "term"
    """
    if val in (None, '') or level == Clean.NOOP:
        return str(val) if val is not None else ''

    val = str(val).lower().strip()

    if level == Clean.LOWER:
        # "#Term Code" -> "#term code"
        return val
    elif level == Clean.LOWER_NOSPACE:
        # "#Term Code" -> "#term_code"
        return val.replace(' ', '_')
    elif level == Clean.LOWER_ALPHANUM:
        # "#Term Code" -> "termcode"
        return re.sub(r'[^a-z0-9]', '', val)
    elif level == Clean.STANDARDIZE:
        # "#Term Code" -> "term"
        return re.sub(r'[^a-z0-9]+|code$', '', val)
    else:
        return val


def get_reader(filename: str,
               sheet_index: int = 0,
               fixed_config: Optional[List[FixedColumn]] = None,
               encoding: str = 'utf-8',
               **kwargs):
    """
    Initialize a reader based on file extension.

    Args:
        filename: Path to data file
        sheet_index: Sheet index for Excel files (0-based)
        fixed_config: Column configuration for fixed-width files
        encoding: File encoding for text files
        **kwargs: Additional arguments passed to specific readers

    Returns:
        CSVReader, XLReader, XLSXReader, or FixedReader instance

    Examples:
        # CSV file
        with get_reader('data.csv') as reader:
            for record in reader:
                print(record)

        # Excel file
        with get_reader('data.xlsx', sheet_index=1) as reader:
            for record in reader:
                print(record)

        # Fixed width file
        config = [
            FixedColumn('name', 1, 20),
            FixedColumn('age', 21, 23, 'int')
        ]
        with get_reader('data.txt', fixed_config=config) as reader:
            for record in reader:
                print(record)
    """
    ext = filename.lower().split('.')[-1]

    if ext == 'csv':
        fp = open(filename, encoding=encoding)
        return CSVReader(fp, **kwargs)

    elif ext in ('xls', 'xlsx'):
        check_dependencies()
        wb = open_workbook(filename)
        ws = get_sheet_by_index(wb, sheet_index)

        if ws.__class__.__name__ == 'Worksheet':
            # openpyxl
            return XLSXReader(ws, **kwargs)
        else:
            # xlrd
            return XLReader(ws, **kwargs)

    else:
        # Assume fixed-width file
        if not fixed_config:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                "For fixed-width files, provide fixed_config parameter."
            )

        fp = open(filename, encoding=encoding)
        return FixedReader(fp, fixed_config, **kwargs)
