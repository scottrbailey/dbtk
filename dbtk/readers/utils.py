# dbtk/readers/utils.py

"""Utility functions for automatic file format detection and reader selection."""

from typing import List, Optional


def get_reader(filename: str,
               sheet_index: int = 0,
               fixed_config: Optional[List['FixedColumn']] = None,
               encoding: str = 'utf-8',
               clean_headers: Optional['Clean'] = None,
               **kwargs) -> 'Reader':
    """
    Initialize a reader based on file extension.

    Args:
        filename: Path to data file
        sheet_index: Sheet index for Excel files (0-based)
        fixed_config: Column configuration for fixed-width files
        encoding: File encoding for text files
        clean_headers: Header cleaning level (defaults vary by file type)
        **kwargs: Additional arguments passed to specific readers

    Returns:
        CSVReader, FixedReader, JSONReader, NDJSONReader, XLSXReader, or XMLReader instance

    Examples::

        # CSV file with custom header cleaning
        with get_reader('data.csv', clean_headers=Clean.STANDARDIZE) as reader:
            for record in reader:
                print(record.name)  # Attribute access
                print(record['age'])  # Dict-style access

        # Excel file (uses Clean.DEFAULT)
        with get_reader('data.xlsx', sheet_index=1) as reader:
            for record in reader:
                print(record)

        # Fixed width file (uses Clean.NOOP by default)
        config = [
            FixedColumn('name', 1, 20),
            FixedColumn('age', 21, 23, 'int')
        ]
        with get_reader('data.txt', fixed_config=config) as reader:
            for record in reader:
                print(record.name)
    """
    ext = filename.lower().split('.')[-1]

    if ext == 'csv':
        from .csv import CSVReader
        fp = open(filename, encoding=encoding)
        return CSVReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext in ('xls', 'xlsx'):
        from .excel import open_workbook, get_sheet_by_index, XLReader, XLSXReader
        wb = open_workbook(filename)
        ws = get_sheet_by_index(wb, sheet_index)

        if ws.__class__.__name__ == 'Worksheet':
            # openpyxl
            return XLSXReader(ws, clean_headers=clean_headers, **kwargs)
        else:
            # xlrd
            return XLReader(ws, clean_headers=clean_headers, **kwargs)
    elif ext == 'json':
        from .json import JSONReader
        fp = open(filename, encoding=encoding)
        return JSONReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext == 'ndjson':
        from .json import NDJSONReader
        fp = open(filename, encoding=encoding)
        return NDJSONReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext == 'xml':
        from .xml import XMLReader
        fp = open(filename, encoding=encoding)
        return XMLReader(fp, clean_headers=clean_headers, **kwargs)
    else:
        # Assume fixed-width file
        if not fixed_config:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                "For fixed-width files, provide fixed_config parameter."
            )
        from .fixed_width import FixedReader
        fp = open(filename, encoding=encoding)
        return FixedReader(fp, fixed_config, clean_headers=clean_headers, **kwargs)