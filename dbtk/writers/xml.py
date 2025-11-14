# dbtk/writers/xml.py
"""
XML writer for database results using lxml.
"""

import logging
import sys
from typing import Union, Optional, List
from pathlib import Path

try:
    from lxml import etree
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

from .base import BaseWriter

logger = logging.getLogger(__name__)


class XMLWriter(BaseWriter):
    """XML writer class that extends BaseWriter."""

    def __init__(self,
                 data,
                 filename: Optional[Union[str, Path]] = None,
                 columns: Optional[List[str]] = None,
                 encoding: str = 'utf-8',
                 root_element: str = 'data',
                 record_element: str = 'record',
                 pretty: bool = True):
        """
        Initialize XML writer.

        Args:
            data: Cursor object or list of records
            filename: Output filename. If None, writes to stdout
            columns: Column names for list-of-lists data (optional for other types)
            encoding: File encoding
            root_element: Name of the root XML element
            record_element: Name of each record element
            pretty: Whether to format with indentation
        """
        # Preserve data types for XML output
        super().__init__(data, filename, columns, encoding, preserve_types=True)
        self.root_element = root_element
        self.record_element = record_element
        self.pretty = pretty

    def _row_to_dict(self, record) -> dict:
        """Convert record to dict with keys sanitized for XML element names."""
        record_dict = super()._row_to_dict(record)

        # Sanitize keys to be valid XML element names
        sanitized_dict = {}
        for key, value in record_dict.items():
            sanitized_key = self._sanitize_element_name(key)
            sanitized_dict[sanitized_key] = value

        return sanitized_dict

    def _sanitize_element_name(self, name: str) -> str:
        """
        Sanitize column name to be valid XML element name.

        Args:
            name: Column name from database

        Returns:
            Valid XML element name
        """
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', str(name))

        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized

        return sanitized or 'unnamed'

    def _format_xml_value(self, value) -> str:
        """
        Format a value for XML content, handling None and database types.

        Args:
            value: Database value

        Returns:
            String representation suitable for XML
        """
        if value is None:
            return ''
        else:
            return self.to_string(value)

    def _write_data(self, file_obj) -> None:
        """Write XML data to file object using tree building."""
        root = etree.Element(self.root_element)

        for record in self.data_iterator:
            record_elem = etree.SubElement(root, self.record_element)

            # Convert record to dict with sanitized keys
            record_dict = self._row_to_dict(record)

            # Add elements for each field
            for key, value in record_dict.items():
                elem = etree.SubElement(record_elem, key)
                elem.text = self._format_xml_value(value)

            self._row_num += 1

        # Generate XML string using lxml's built-in pretty printing
        xml_str = etree.tostring(root, encoding='unicode', pretty_print=self.pretty)

        # Write output
        file_obj.write(f'<?xml version="1.0" encoding="{self.encoding}"?>\n')
        file_obj.write(xml_str)


class XMLStreamer(XMLWriter):
    """Streaming XML writer that extends XMLWriter."""

    def _get_file_handle(self, mode='wb'):
        """Override to open file in binary mode for streaming XML."""
        if self.filename is None:
            return sys.stdout.buffer, False  # Use binary stdout
        else:
            return open(self.filename, mode), True

    def _write_data(self, file_obj) -> None:
        """Write XML data to file object using streaming approach."""
        with etree.xmlfile(file_obj, encoding=self.encoding) as xf:
            with xf.element(self.root_element):
                for record in self.data_iterator:
                    record_dict = self._row_to_dict(record)
                    with xf.element(self.record_element):
                        for key, value in record_dict.items():
                            with xf.element(key):
                                if value is not None:
                                    xf.write(self._format_xml_value(value))

                    self._row_num += 1
                    if self._row_num % 1000 == 0:
                        xf.flush()


def to_xml(data,
           filename: Optional[Union[str, Path]] = None,
           encoding: str = 'utf-8',
           root_element: str = 'data',
           record_element: str = 'record',
           stream: bool = False,
           pretty: bool = None) -> None:
    """
    Export cursor or result set to XML file.

    Args:
        data: Cursor object or list of records
        filename: Output filename. If None, writes to stdout (limited to 20 rows)
        encoding: File encoding
        root_element: Name of the root XML element
        record_element: Name of each record element
        stream: Whether to write incrementally (reduces memory usage for large datasets)
        pretty: Whether to format with indentation

    Example:
        # Write to file
        to_xml(cursor, 'users.xml')

        # Write to stdout (limited to 20 rows)
        to_xml(cursor)

        # Custom element names with streaming
        to_xml(cursor, 'active_users.xml', root_element='users', record_element='user', stream=True)
    """
    if pretty is None:
        pretty = not stream

    if stream:
        writer = XMLStreamer(
            data=data,
            filename=filename,
            encoding=encoding,
            root_element=root_element,
            record_element=record_element,
            pretty=pretty
        )
    else:
        writer = XMLWriter(
            data=data,
            filename=filename,
            encoding=encoding,
            root_element=root_element,
            record_element=record_element,
            pretty=pretty
        )

    writer.write()


def check_dependencies():
    if not HAS_LXML:
        logger.error('lxml is required for XML support. Install with "pip install lxml".')

check_dependencies()