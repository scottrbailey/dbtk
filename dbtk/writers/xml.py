# dbtk/writers/xml.py
"""
XML writer for database results using lxml.
"""

import io
import logging
import re
import sys
from typing import Any, BinaryIO, List, Optional, TextIO, Union
from pathlib import Path

try:
    from lxml import etree

    HAS_LXML = True
except ImportError:
    HAS_LXML = False

from .base import BaseWriter, BatchWriter
from ..utils import to_string

logger = logging.getLogger(__name__)


def _sanitize_element_name(name: str) -> str:
    """
    Sanitize column name to be valid XML element name.

    XML element names must start with a letter or underscore, and can only
    contain letters, digits, hyphens, underscores, and periods.

    Parameters
    ----------
    name : str
        Original column name.

    Returns
    -------
    str
        Valid XML element name
    """
    sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '_', str(name))

    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'col_' + sanitized

    return sanitized or 'unnamed'


def _prepare_record_for_xml(record_dict: dict) -> dict:
    """
    Prepare a record dictionary for XML output.

    Sanitizes keys to be valid XML element names and converts all values
    to strings suitable for XML text content.

    Parameters
    ----------
    record_dict : dict
        Dictionary representation of a record

    Returns
    -------
    dict
        Dictionary with sanitized keys and stringified values
    """
    result = {}
    for key, value in record_dict.items():
        sanitized_key = _sanitize_element_name(key)
        result[sanitized_key] = to_string(value)
    return result


class XMLWriter(BaseWriter):
    """
    XML writer that builds complete XML tree in memory.

    Best for small to medium datasets. For large datasets that don't fit
    in memory, use XMLStreamer instead.

    Parameters
    ----------
    data : Iterable[RecordLike]
        Data to write
    file : str, Path, TextIO, or BinaryIO, optional
        Output filename or file handle. If None, writes to stdout.
    columns : List[str], optional
        Column names for list-of-lists data
    encoding : str, default 'utf-8'
        XML encoding declaration
    root_element : str, default 'data'
        Name of the root XML element
    record_element : str, default 'record'
        Name of each record element
    pretty : bool, default True
        Whether to format with indentation

    Examples
    --------
    >>> to_xml(cursor, 'users.xml')
    >>> to_xml(records, 'output.xml', root_element='users', record_element='user')
    """

    preserve_types = False #

    def __init__(
            self,
            data,
            file: Optional[Union[str, Path, TextIO, BinaryIO]] = None,
            columns: Optional[List[str]] = None,
            encoding: str = 'utf-8',
            root_element: str = 'data',
            record_element: str = 'record',
            pretty: bool = True,
    ):
        """Initialize XML writer."""
        self.root_element = root_element
        self.record_element = record_element
        self.pretty = pretty

        # Preserve types initially (we'll convert in _prepare_record_for_xml)
        super().__init__(data, file, columns, encoding)
        self._xml_columns = {col: _sanitize_element_name(col) for col in self.columns}

    def _write_data(self, file_obj: Union[TextIO, BinaryIO]) -> None:
        """Write XML data by building complete tree in memory."""
        root = etree.Element(self.root_element)

        for record in self.data_iterator:
            record_elem = etree.SubElement(root, self.record_element)

            # Convert record to dict and prepare for XML
            record_dict = self._row_to_dict(record)

            # Add elements for each field
            for key, value in record_dict.items():
                xml_key = self._xml_columns[key]
                elem = etree.SubElement(record_elem, xml_key)
                elem.text = value

            self._row_num += 1

        # Generate XML string
        xml_str = etree.tostring(root, encoding='unicode', pretty_print=self.pretty)

        # Write output
        file_obj.write(f'<?xml version="1.0" encoding="{self.encoding}"?>\n')
        file_obj.write(xml_str)

class XMLStreamer(BatchWriter):
    """
    Streaming XML writer that writes records incrementally.

    Memory-efficient for large datasets. Writes XML elements as they arrive
    without building the entire tree in memory.

    Parameters
    ----------
    data : Iterable[RecordLike], optional
        Initial data. For streaming mode, use data=None.
    file : str, Path, or BinaryIO, optional
        Output filename or binary file handle. Must be binary mode for streaming.
    columns : List[str], optional
        Column names for list-of-lists data
    encoding : str, default 'utf-8'
        XML encoding declaration
    root_element : str, default 'data'
        Name of the root XML element
    record_element : str, default 'record'
        Name of each record element

    Examples
    --------
    Streaming mode::

        with open('output.xml', 'wb') as f:
            with XMLStreamer(data=None, file=f, root_element='data') as writer:
                for batch in surge.batched(records):
                    writer.write_batch(batch)

    Single-shot mode::

        XMLStreamer(data=records, file='output.xml').write()

    Notes
    -----
    - Requires lxml library
    - File must be opened in binary mode ('wb') for streaming
    - No pretty-printing (streaming writes compact XML)
    - More memory-efficient than XMLWriter for large datasets
    """

    def __init__(
            self,
            data=None,
            file: Optional[Union[str, Path, BinaryIO]] = None,
            columns: Optional[List[str]] = None,
            encoding: str = 'utf-8',
            root_element: str = 'data',
            record_element: str = 'record',
    ):
        """Initialize streaming XML writer."""
        # Set these BEFORE super().__init__() in case _lazy_init is called
        self.root_element = root_element
        self.record_element = record_element

        # XML streaming contexts (set up in _lazy_init)
        self._xmlfile_ctx = None
        self._xf = None
        self._root_ctx = None
        self._xml_columns = {}

        super().__init__(
            data=data,
            file=file,
            columns=columns,
            encoding=encoding,
            preserve_types=True,  # We'll convert in _prepare_record_for_xml
        )

    def _open_file_handle(self, mode: str = 'wb') -> tuple[BinaryIO, bool]:
        """
        Override to use binary mode and validate binary streams.

        Parameters
        ----------
        mode : str, default 'wb'
            File open mode (must be binary)

        Returns
        -------
        tuple[BinaryIO, bool]
            (file_handle, should_close_flag)

        Raises
        ------
        ValueError
            If a text stream is provided instead of binary
        """
        if self.file is None:
            return sys.stdout.buffer, False

        if hasattr(self.file, 'write'):
            # Validate it's a binary stream
            if isinstance(self.file, io.TextIOWrapper):
                raise ValueError(
                    "XMLStreamer requires a binary file handle, got TextIOWrapper. "
                    "Open file in 'wb' mode or use file.buffer"
                )
            if hasattr(self.file, 'mode') and 'b' not in self.file.mode:
                raise ValueError(
                    f"XMLStreamer requires binary mode, file opened in '{self.file.mode}' mode. "
                    "Use 'wb' mode instead."
                )
        return super()._open_file_handle('wb')

    def _lazy_init(self, data) -> None:
        """
        Set up columns and XML streaming contexts on first use.

        Parameters
        ----------
        data : Iterable[RecordLike]
            First batch of data
        """
        if self._initialized:
            return

        # Parent handles columns and data_iterator
        super()._lazy_init(data)
        self._xml_columns = {col: _sanitize_element_name(col) for col in self.columns}

        # Set up XML streaming contexts
        self._xmlfile_ctx = etree.xmlfile(self._file_obj)
        self._xf = self._xmlfile_ctx.__enter__()
        self._root_ctx = self._xf.element(self.root_element)
        self._root_ctx.__enter__()

        # Newline after opening root tag
        self._xf.write('\n')

    def _write_data(self, file_obj: BinaryIO) -> None:
        """
        Write XML records to stream.

        Parameters
        ----------
        file_obj : BinaryIO
            Binary file handle (managed by parent class)
        """
        if not self._initialized:
            self._lazy_init(self.data_iterator)

        for record in self.data_iterator:
            record_dict = self._row_to_dict(record)

            with self._xf.element(self.record_element):
                for key, value in record_dict.items():
                    xml_key = self._xml_columns[key]
                    with self._xf.element(xml_key):
                        if value != '':  # Only write non-empty values
                            self._xf.write(value)

            # Newline after each record
            self._xf.write('\n')
            self._row_num += 1

        # Flush after writing
        self._xf.flush()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close XML contexts, then close file."""
        # Close XML contexts
        if self._root_ctx:
            self._root_ctx.__exit__(exc_type, exc_val, exc_tb)
            self._root_ctx = None

        if self._xmlfile_ctx:
            self._xmlfile_ctx.__exit__(exc_type, exc_val, exc_tb)
            self._xmlfile_ctx = None

        # Let parent close the file
        return super().__exit__(exc_type, exc_val, exc_tb)


def to_xml(
        data,
        filename: Optional[Union[str, Path]] = None,
        encoding: str = 'utf-8',
        root_element: str = 'data',
        record_element: str = 'record',
        stream: bool = False,
        pretty: bool = None,
) -> None:
    """
    Export cursor or result set to XML file.

    Parameters
    ----------
    data : Iterable[RecordLike]
        Cursor object or list of records
    filename : str or Path, optional
        Output filename. If None, writes to stdout (limited to 20 rows)
    encoding : str, default 'utf-8'
        XML encoding declaration
    root_element : str, default 'data'
        Name of the root XML element
    record_element : str, default 'record'
        Name of each record element
    stream : bool, default False
        Whether to use streaming mode (reduces memory usage for large datasets)
    pretty : bool, optional
        Whether to format with indentation. Defaults to True for tree mode,
        False for streaming mode.

    Examples
    --------
    Write to file::

        to_xml(cursor, 'users.xml')

    Write to stdout (limited to 20 rows)::

        to_xml(cursor)

    Custom element names with streaming::

        to_xml(cursor, 'active_users.xml',
               root_element='users',
               record_element='user',
               stream=True)

    Notes
    -----
    - Tree mode (stream=False): Builds complete XML tree in memory, supports pretty printing
    - Streaming mode (stream=True): Memory-efficient, writes incrementally, no pretty printing
    - For large datasets (>100K rows), use stream=True
    """
    if pretty is None:
        pretty = not stream

    if stream:
        writer = XMLStreamer(
            data=data,
            file=filename,
            encoding=encoding,
            root_element=root_element,
            record_element=record_element,
        )
    else:
        writer = XMLWriter(
            data=data,
            file=filename,
            encoding=encoding,
            root_element=root_element,
            record_element=record_element,
            pretty=pretty,
        )

    writer.write()


def check_dependencies():
    """Check for optional dependencies and issue warnings if missing."""
    if not HAS_LXML:
        logger.error('lxml is required for XML support. Install with "pip install lxml".')


check_dependencies()
