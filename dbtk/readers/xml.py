# dbtk/readers/xml.py

from typing import List, Any, Dict, Optional, TextIO, Union
from .base import Reader, Clean

try:
    from lxml import etree
except ImportError:
    raise ImportError("XML support requires lxml. Install with: pip install lxml")


class XMLColumn:
    """Column definition for XML extraction."""

    def __init__(self, name: str, xpath: Optional[str] = None,
                 data_type: str = 'text'):
        """
        Args:
            name: Column name for the Record
            xpath: XPath expression (if None, uses simple element matching)
            data_type: Data type hint (not enforced, just documentation)
        """
        self.name = name
        self.xpath = xpath
        self.data_type = data_type

    def __repr__(self):
        return f"XMLColumn('{self.name}', xpath='{self.xpath}', data_type='{self.data_type}')"


class XMLReader(Reader):
    """XML file reader that returns Record objects."""

    def __init__(self,
                 fp: TextIO,
                 record_xpath: str = "//row",
                 columns: Optional[List[XMLColumn]] = None,
                 sample_size: int = 10,
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.DEFAULT):
        """
        Initialize XML reader.

        Args:
            fp: File pointer to XML file
            record_xpath: XPath expression to find record elements
            columns: List of XMLColumn definitions for custom extraction
            sample_size: Number of records to sample for column discovery
            add_rownum: Add rownum to each record
            clean_headers: Header cleaning level
        """
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers)
        self.fp = fp
        self.record_xpath = record_xpath
        self.custom_columns = columns or []
        self.sample_size = sample_size

        self._tree = None
        self._record_nodes = None
        self._current_record_index = 0
        self._column_cache = None
        self._all_columns = []  # Combined auto-discovered + custom columns

        # Parse XML and discover structure
        self._parse_xml()

    def _parse_xml(self):
        """Parse the XML file and prepare for reading."""
        try:
            self._tree = etree.parse(self.fp)
        except etree.XMLSyntaxError as e:
            raise ValueError(f"Invalid XML: {e}")

        # Find all record nodes
        try:
            self._record_nodes = self._tree.xpath(self.record_xpath)
        except etree.XPathEvalError as e:
            raise ValueError(f"Invalid XPath expression '{self.record_xpath}': {e}")

        if not self._record_nodes:
            raise ValueError(f"No records found with XPath: {self.record_xpath}")

    def _introspect_columns(self) -> List[str]:
        """Analyze first few records to discover all possible columns."""
        if self._column_cache is not None:
            return self._column_cache

        # Start with custom columns
        self._all_columns = list(self.custom_columns)
        custom_names = {col.name for col in self.custom_columns}

        # Auto-discover columns from sample records
        discovered_elements = set()
        sample_records = self._record_nodes[:self.sample_size]

        for record_node in sample_records:
            for child in record_node:
                if child.tag:
                    col_name = self._flatten_element_name(child.tag)
                    if col_name not in custom_names:  # Don't duplicate custom columns
                        discovered_elements.add(col_name)

        # Add discovered columns as XMLColumn objects
        for element_name in sorted(discovered_elements):
            cleaned_name = self.clean_header(element_name)
            self._all_columns.append(XMLColumn(cleaned_name))

        # Extract just the names for the column cache
        self._column_cache = [col.name for col in self._all_columns]

        # Ensure we start reading from the beginning
        self._current_record_index = 0
        self.line_num = 0

        return self._column_cache

    def _flatten_element_name(self, tag: str) -> str:
        """Convert XML element name to valid column name."""
        # Handle namespaces: {namespace}localname -> localname
        if '}' in tag:
            tag = tag.split('}')[1]

        # Replace invalid characters for Python identifiers
        tag = tag.replace('-', '_').replace('.', '_').replace(':', '_')

        return tag

    def _extract_column_value(self, record_node, xml_column: XMLColumn) -> Optional[str]:
        """Extract value for a column from a record node."""
        # Check if it has custom XPath
        if xml_column.xpath:
            try:
                result = record_node.xpath(xml_column.xpath)
                if result:
                    # Handle different XPath result types
                    if isinstance(result[0], str):
                        # Text result
                        value = result[0]
                    elif hasattr(result[0], 'text'):
                        # Element result
                        value = result[0].text
                    else:
                        # Other result (attribute, etc.)
                        value = str(result[0])

                    return value.strip() if value else None
                else:
                    return None
            except etree.XPathEvalError:
                return None

        # Look for simple child element by column name
        # Convert column name back to possible XML tag names
        possible_tags = [xml_column.name, xml_column.name.replace('_', '-'), xml_column.name.replace('_', '.')]

        for tag in possible_tags:
            # Try with and without namespace
            child = record_node.find(tag)
            if child is None:
                # Try with any namespace
                child = record_node.find(f".//{tag}")

            if child is not None:
                text = child.text
                return text.strip() if text else None

        return None

    def _read_headers(self) -> List[str]:
        """Read and return column names from XML structure."""
        return self._introspect_columns()

    def _read_next_row(self) -> List[Any]:
        """Read the next data row from XML."""
        # Ensure columns are discovered before reading data
        if not self._all_columns:
            self._introspect_columns()

        if self._current_record_index >= len(self._record_nodes):
            raise StopIteration

        record_node = self._record_nodes[self._current_record_index]
        self._current_record_index += 1
        self.line_num = self._current_record_index

        row_data = []
        for xml_column in self._all_columns:
            value = self._extract_column_value(record_node, xml_column)
            row_data.append(value)

        return row_data

    def __next__(self) -> 'Record':
        """Return the next record."""
        row_data = self._read_next_row()
        return self._create_record(row_data)

    def _cleanup(self):
        """Close the file pointer."""
        if hasattr(self, 'fp') and self.fp:
            self.fp.close()

    @property
    def record_count(self) -> int:
        """Return total number of records found."""
        return len(self._record_nodes) if self._record_nodes else 0

    @property
    def columns(self) -> List[XMLColumn]:
        """Return all column definitions (custom + auto-discovered)."""
        if not self._all_columns:
            self._introspect_columns()  # Trigger discovery
        return self._all_columns.copy()

    def reset(self):
        """Reset reader to beginning."""
        self._current_record_index = 0
        self.line_num = 0


# Convenience function to match other readers
def open_xml(filename: str, **kwargs) -> XMLReader:
    """
    Open XML file for reading.

    Args:
        filename: Path to XML file
        **kwargs: Arguments passed to XMLReader

    Returns:
        XMLReader instance

    Example:
        with open_xml('data.xml', record_xpath='//user') as reader:
            for record in reader:
                print(record.name)
    """
    fp = open(filename, 'rb')  # lxml prefers binary mode
    return XMLReader(fp, **kwargs)