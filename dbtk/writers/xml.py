# dbtk/writers/xml.py
"""
XML writer for database results using lxml.
"""

import itertools
import logging
from typing import Union, Optional
from pathlib import Path

try:
    from lxml import etree
except ImportError:
    raise ImportError("lxml is required for XML support. Install with: pip install lxml")

from .utils import get_data_iterator, format_value

logger = logging.getLogger(__name__)


def to_xml(data,
           filename: Optional[Union[str, Path]] = None,
           encoding: str = 'utf-8',
           root_element: str = 'data',
           record_element: str = 'record',
           pretty: bool = True,
           stream: bool = False) -> None:
    """
    Export cursor or result set to XML file.

    Args:
        data: Cursor object or list of records
        filename: Output filename. If None, writes to stdout (limited to 20 rows)
        encoding: File encoding
        root_element: Name of the root XML element
        record_element: Name of each record element
        pretty: Whether to format with indentation
        stream: Whether to write incrementally (reduces memory usage for large datasets)

    Examples:
        # Write to file
        to_xml(cursor, 'users.xml')

        # Write to stdout (limited to 20 rows)
        to_xml(cursor)

        # Custom element names with streaming
        to_xml(cursor, 'active_users.xml', root_element='users', record_element='user', stream=True)
    """
    data_iterator, columns = get_data_iterator(data)

    if not data_iterator or not columns:
        logger.warning("No data to export")
        return

    # Apply stdout limit
    if filename is None:
        data_iterator = itertools.islice(data_iterator, 20)

    row_count = 0

    # Determine output destination
    if filename is None:
        file_obj = sys.stdout
        close_file = False
    else:
        file_obj = open(filename, 'w', encoding=encoding)
        close_file = True

    try:
        if stream:
            # Streaming mode - write incrementally
            with etree.xmlfile(file_obj, encoding=encoding) as xf:
                with xf.element(root_element):
                    for record in data_iterator:
                        record_dict = _convert_record(record, columns)
                        record_elem = xf.element(record_element)
                        for key, value in record_dict.items():
                            elem = record_elem.element(_sanitize_element_name(key))
                            elem.text = _format_xml_value(value)
                        xf.write(record_elem, pretty_print=pretty)
                        row_count += 1
                        if row_count % 1000 == 0:
                            xf.flush()
        else:
            # Standard mode - build full XML tree in memory
            root = etree.Element(root_element)

            for record in data_iterator:
                record_elem = etree.SubElement(root, record_element)

                # Convert record to dict
                record_dict = _convert_record(record, columns)

                # Add elements for each field
                for key, value in record_dict.items():
                    elem = etree.SubElement(record_elem, _sanitize_element_name(key))
                    elem.text = _format_xml_value(value)

                row_count += 1

            # Generate XML string using lxml's built-in pretty printing
            xml_str = etree.tostring(root, encoding='unicode', pretty_print=pretty)

            # Write output
            file_obj.write(f'<?xml version="1.0" encoding="{encoding}"?>\n')
            file_obj.write(xml_str)

        logger.info(f"Wrote {row_count} rows to {filename or 'stdout'}")
    finally:
        if close_file:
            file_obj.close()


def _convert_record(record, columns):
    """Convert record to dict using same logic as JSON writer."""
    if hasattr(record, 'to_dict'):
        return record.to_dict()
    elif hasattr(record, '_asdict'):
        return record._asdict()
    elif hasattr(record, 'keys') and callable(record.keys):
        return {key: record[key] for key in record.keys()}
    elif isinstance(record, (list, tuple)):
        return {columns[i]: record[i] for i in range(min(len(columns), len(record)))}
    else:
        return {col: getattr(record, col, None) for col in columns}


def _sanitize_element_name(name: str) -> str:
    """
    Sanitize column name to be valid XML element name.

    Args:
        name: Column name from database

    Returns:
        Valid XML element name
    """
    # Replace invalid characters with underscore
    import re
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', str(name))

    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'col_' + sanitized

    return sanitized or 'unnamed'


def _format_xml_value(value) -> str:
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
        return format_value(value)