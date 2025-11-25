# dbtk/readers/utils.py

"""Utility functions for automatic file format detection and reader selection."""

from typing import List, Optional


def get_reader(filename: str,
               encoding: Optional[str] = None,
               clean_headers: Optional['Clean'] = None,
               **kwargs) -> 'Reader':
    """
    Initialize a reader based on file extension.

    Args:
        filename: Path to data file
        encoding: File encoding for text files
        clean_headers: Header cleaning level (defaults vary by file type)
        **kwargs: Additional arguments passed to specific readers (sheet_name, sheet_index, fixed_config, etc.)

    Returns:
        CSVReader, FixedReader, JSONReader, NDJSONReader, XLSXReader, or XMLReader instance

    Example
    -----------------
    ::

        # CSV file with custom header cleaning
        with get_reader('data.csv', clean_headers=Clean.STANDARDIZE) as reader:
            for record in reader:
                print(record.name)  # Attribute access
                print(record['age'])  # Dict-style access

        # Use sheet_name or sheet_index to choose a worksheet
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
    effective_encoding = ('utf-8-sig' if encoding is None or str(encoding).lower() in ('utf-8', 'utf8') else encoding)

    if ext in ('csv', 'tsv'):
        from .csv import CSVReader
        fp = open(filename, encoding=effective_encoding)
        return CSVReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext in ('xls', 'xlsx'):
        from .excel import open_workbook, get_sheet_by_index, get_sheet_by_name, XLSReader, XLSXReader
        wb = open_workbook(filename)
        if 'sheet_name' in kwargs:
            sheet_name = kwargs.pop('sheet_name', None)
            ws = get_sheet_by_name(wb, sheet_name)
        elif 'sheet_index' in kwargs:
            sheet_index = kwargs.pop('sheet_index', None)
            ws = get_sheet_by_index(wb, sheet_index)
        else:
            ws = get_sheet_by_index(wb, 0)

        if ws.__class__.__name__ == 'Worksheet':
            # openpyxl
            reader = XLSXReader(ws, clean_headers=clean_headers, **kwargs)
        else:
            # xlrd
            reader = XLSReader(ws, clean_headers=clean_headers, **kwargs)
        reader.source = filename
        return reader
    elif ext == 'json':
        from .json import JSONReader
        fp = open(filename, encoding=effective_encoding)
        return JSONReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext == 'ndjson':
        from .json import NDJSONReader
        fp = open(filename, encoding=effective_encoding)
        return NDJSONReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext == 'xml':
        from .xml import XMLReader
        fp = open(filename, encoding=effective_encoding)
        return XMLReader(fp, clean_headers=clean_headers, **kwargs)
    else:
        # Assume fixed-width file
        fixed_config = kwargs.pop('fixed_config', None)
        if fixed_config is None:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                "For fixed-width files, provide fixed_config parameter."
            )
        from .fixed_width import FixedReader
        fp = open(filename, encoding=effective_encoding)
        return FixedReader(fp, columns=fixed_config, clean_headers=clean_headers, **kwargs)
