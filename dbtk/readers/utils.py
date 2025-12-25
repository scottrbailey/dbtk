# dbtk/readers/utils.py

"""Utility functions for automatic file format detection and reader selection."""

import os
from typing import List, Optional, TextIO, Union
from pathlib import Path
from ..defaults import settings


def open_file(filename: Union[str, Path],
              mode: str = 'rt',
              encoding: Optional[str] = None,
              zip_member: Optional[str] = None) -> TextIO:
    """
    Open a file with automatic decompression based on extension.

    Supports: .gz (gzip), .bz2 (bzip2), .xz (lzma), .zip (zipfile)

    Args:
        filename: Path to file (e.g., 'data.csv.gz', 'archive.zip', or Path object)
        mode: File mode (default 'rt' for text reading)
        encoding: Text encoding (default None = use Python default)
        zip_member: For ZIP files, specific member to extract. If None, uses smart selection:
            - If only one file in ZIP → use it
            - If archive name matches a member (sans extensions) → use it
            - Otherwise → raise error

    Returns:
        File-like object ready for reading

    Raises:
        ImportError: If required compression library not installed
        ValueError: If ZIP file requires explicit zip_member parameter

    Example:
        # Compressed CSV
        fp = open_file('data.csv.gz', encoding='utf-8-sig')
        reader = CSVReader(fp)

        # ZIP with single file
        fp = open_file('data.zip')  # Auto-extracts single file

        # ZIP with specific member
        fp = open_file('archive.zip', zip_member='data.csv')

        # Using pathlib.Path
        from pathlib import Path
        fp = open_file(Path('data.csv.gz'))
    """
    # Convert Path to string for compatibility with extension checks
    filename = str(filename) if isinstance(filename, Path) else filename

    effective_encoding = encoding or 'utf-8-sig'
    buffer_size = settings.get('compressed_file_buffer_size', 1024 * 1024)

    # Detect compression from extension
    if filename.endswith('.gz'):
        try:
            import gzip
            import io
            import struct
        except ImportError:
            raise ImportError(
                "gzip support requires the 'gzip' module (usually included in Python standard library). "
                "If using a minimal Python installation, install it via your package manager."
            )
        # Open in binary mode, then wrap with BufferedReader + TextIOWrapper for buffer control
        if 't' in mode:
            binary_fp = gzip.open(filename, 'rb')
            buffered = io.BufferedReader(binary_fp, buffer_size)
            text_fp = io.TextIOWrapper(buffered, encoding=effective_encoding, newline='')

            # Extract uncompressed size from GZIP footer (last 4 bytes, mod 2^32)
            try:
                with open(filename, 'rb') as f:
                    f.seek(-4, 2)  # Seek to last 4 bytes
                    uncompressed_size = struct.unpack('<I', f.read(4))[0]
                    text_fp._uncompressed_size = uncompressed_size
            except (OSError, struct.error):
                # If we can't read the size, set to None (progress will be disabled)
                text_fp._uncompressed_size = None

            return text_fp
        else:
            return gzip.open(filename, mode)

    elif filename.endswith('.bz2'):
        try:
            import bz2
            import io
        except ImportError:
            raise ImportError(
                "bz2 support requires the 'bz2' module (usually included in Python standard library). "
                "If using a minimal Python installation, install it via your package manager."
            )
        # Open in binary mode, then wrap with TextIOWrapper for buffering control
        if 't' in mode:
            binary_fp = bz2.open(filename, 'rb')
            buffered = io.BufferedReader(binary_fp, buffer_size)
            text_fp = io.TextIOWrapper(buffered, encoding=effective_encoding, newline='')

            # BZ2 format does not store uncompressed size - disable progress
            text_fp._uncompressed_size = None

            return text_fp
        else:
            return bz2.open(filename, mode)

    elif filename.endswith('.xz'):
        try:
            import lzma
            import io
        except ImportError:
            raise ImportError(
                "xz/lzma support requires the 'lzma' module (usually included in Python standard library). "
                "If using a minimal Python installation, install it via your package manager."
            )
        # Open in binary mode, then wrap with TextIOWrapper for buffering control
        if 't' in mode:
            binary_fp = lzma.open(filename, 'rb')
            buffered = io.BufferedReader(binary_fp, buffer_size)
            text_fp = io.TextIOWrapper(buffered, encoding=effective_encoding, newline='')

            # XZ/LZMA can optionally store uncompressed size, but parsing is complex
            # For simplicity, disable progress for XZ files
            text_fp._uncompressed_size = None

            return text_fp
        else:
            return lzma.open(filename, mode)

    elif filename.endswith('.zip'):
        try:
            import zipfile
            import io
        except ImportError:
            raise ImportError(
                "ZIP support requires the 'zipfile' module (usually included in Python standard library)."
            )

        zf = zipfile.ZipFile(filename, 'r')
        members = zf.namelist()

        # Determine which member to extract
        if zip_member:
            # Explicit member specified
            if zip_member not in members:
                raise ValueError(
                    f"ZIP member '{zip_member}' not found in archive. "
                    f"Available members: {', '.join(members)}"
                )
            selected = zip_member
        elif len(members) == 1:
            # Only one file - use it
            selected = members[0]
        else:
            # Multiple files - try to match archive name
            archive_base = os.path.basename(filename)
            # Strip .zip and any other extensions (e.g., 'data.csv.zip' → 'data')
            archive_name = archive_base.split('.')[0]

            # Look for exact matches or matches with extensions
            candidates = [m for m in members if os.path.basename(m).startswith(archive_name)]

            if len(candidates) == 1:
                selected = candidates[0]
            elif len(candidates) > 1:
                # Try exact match with one extension
                exact = [m for m in candidates if os.path.basename(m).split('.')[0] == archive_name]
                if len(exact) == 1:
                    selected = exact[0]
                else:
                    raise ValueError(
                        f"ZIP archive '{filename}' contains multiple files matching '{archive_name}'. "
                        f"Specify zip_member parameter explicitly: {', '.join(candidates)}"
                    )
            else:
                raise ValueError(
                    f"ZIP archive '{filename}' contains {len(members)} files, "
                    f"but none match the archive name '{archive_name}'. "
                    f"Specify zip_member parameter explicitly: {', '.join(members)}"
                )

        # Extract and wrap in TextIOWrapper for text mode
        binary_fp = zf.open(selected, 'r')
        if 't' in mode:
            text_fp = io.TextIOWrapper(binary_fp, encoding=effective_encoding, newline='')

            # ZIP stores uncompressed size in the central directory
            info = zf.getinfo(selected)
            text_fp._uncompressed_size = info.file_size

            return text_fp
        else:
            return binary_fp

    else:
        # Regular uncompressed file
        return open(filename, mode, encoding=effective_encoding)


def get_reader(filename: Union[str, Path],
               encoding: Optional[str] = None,
               clean_headers: Optional['Clean'] = None,
               **kwargs) -> 'Reader':
    """
    Initialize a reader based on file extension.

    Automatically handles compressed files (.gz, .bz2, .xz, .zip) transparently.

    Args:
        filename: Path to data file (e.g., 'data.csv', 'data.csv.gz', 'archive.zip', or Path object)
        encoding: File encoding for text files
        clean_headers: Header cleaning level (defaults vary by file type)
        **kwargs: Additional arguments passed to specific readers:
            - sheet_name, sheet_index: For Excel files
            - fixed_config: For fixed-width files
            - zip_member: For ZIP archives with multiple files
            - delimiter, dialect: For CSV files
            - flatten: For JSON files

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

        # Compressed CSV (automatically decompressed)
        with get_reader('data.csv.gz') as reader:
            for record in reader:
                print(record.name)

        # ZIP archive (auto-selects member if unambiguous)
        with get_reader('data.zip') as reader:  # Contains single file
            for record in reader:
                print(record)

        # ZIP archive with explicit member selection
        with get_reader('archive.zip', zip_member='data.csv') as reader:
            for record in reader:
                print(record)

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

        # Using pathlib.Path
        from pathlib import Path
        with get_reader(Path('data.csv.gz')) as reader:
            for record in reader:
                print(record)
    """
    # Convert Path to string for compatibility with extension detection
    filename = str(filename) if isinstance(filename, Path) else filename

    # Extract file format, handling compression extensions
    parts = filename.lower().split('.')
    compression_exts = {'gz', 'bz2', 'xz', 'zip'}
    known_formats = {'csv', 'tsv', 'json', 'ndjson', 'xml', 'xls', 'xlsx', 'txt'}

    # Remove compression extensions from the end
    format_parts = [p for p in parts if p not in compression_exts]

    # If all parts were compression extensions, we need to peek at ZIP contents
    if not format_parts or len(format_parts) == 0:
        # For ZIP files, we might need to look at member names
        if parts[-1] == 'zip':
            zip_member = kwargs.get('zip_member')
            if zip_member:
                # Use member's extension
                member_parts = zip_member.lower().split('.')
                ext = member_parts[-1] if member_parts else ''
            else:
                raise ValueError(
                    f"Cannot determine file format from '{filename}'. "
                    "For ZIP archives, specify zip_member parameter or use a filename like 'data.csv.zip'."
                )
        else:
            ext = parts[-1]  # Fallback to last extension
    else:
        ext = format_parts[-1]

    # If format is unknown and file is a ZIP, peek at member to get format
    if ext not in known_formats and filename.lower().endswith('.zip'):
        import zipfile
        try:
            zf = zipfile.ZipFile(filename, 'r')
            members = zf.namelist()

            # Determine which member will be selected (use same logic as open_file)
            zip_member_param = kwargs.get('zip_member')
            if zip_member_param:
                selected = zip_member_param
            elif len(members) == 1:
                selected = members[0]
            else:
                # Try to match archive name
                archive_base = os.path.basename(filename)
                archive_name = archive_base.split('.')[0]
                candidates = [m for m in members if os.path.basename(m).startswith(archive_name)]

                if len(candidates) == 1:
                    selected = candidates[0]
                elif len(candidates) > 1:
                    exact = [m for m in candidates if os.path.basename(m).split('.')[0] == archive_name]
                    if len(exact) == 1:
                        selected = exact[0]
                    else:
                        selected = None
                else:
                    selected = None

            zf.close()

            # Extract format from selected member
            if selected:
                member_parts = selected.lower().split('.')
                # Get the format (skip compression extensions)
                member_format_parts = [p for p in member_parts if p not in compression_exts]
                if member_format_parts:
                    ext = member_format_parts[-1]
        except (zipfile.BadZipFile, FileNotFoundError, OSError):
            pass  # Keep original ext if ZIP inspection fails

    effective_encoding = ('utf-8-sig' if encoding is None or str(encoding).lower() in ('utf-8', 'utf8') else encoding)

    # Extract zip_member parameter if provided
    zip_member = kwargs.pop('zip_member', None)

    if ext in ('csv', 'tsv'):
        from .csv import CSVReader
        fp = open_file(filename, encoding=effective_encoding, zip_member=zip_member)
        return CSVReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext in ('xls', 'xlsx'):
        # Excel files are already compressed (ZIP-based), handle normally
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
        fp = open_file(filename, encoding=effective_encoding, zip_member=zip_member)
        return JSONReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext == 'ndjson':
        from .json import NDJSONReader
        fp = open_file(filename, encoding=effective_encoding, zip_member=zip_member)
        return NDJSONReader(fp, clean_headers=clean_headers, **kwargs)
    elif ext == 'xml':
        from .xml import XMLReader
        fp = open_file(filename, encoding=effective_encoding, zip_member=zip_member)
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
        fp = open_file(filename, encoding=effective_encoding, zip_member=zip_member)
        return FixedReader(fp, columns=fixed_config, clean_headers=clean_headers, **kwargs)
