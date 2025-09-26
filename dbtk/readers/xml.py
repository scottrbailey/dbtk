# dbtk/readers/xml.py

from typing import List, Any, Dict, Optional, TextIO, Union, Iterator
from .base import Reader, Clean, ReturnType

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
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 return_type: str = ReturnType.DEFAULT):
        """
        Initialize XML reader.

        Args:
            fp: File pointer to XML file
            record_xpath: XPath expression to find record elements
            columns: List of XMLColumn definitions for custom extraction
            sample_size: Number of records to sample for column discovery
            add_rownum: Add rownum to each record
            clean_headers: Header cleaning level (default: Clean.DEFAULT)
            skip_records: Number of data records to skip after headers
            max_records: Maximum number of records to read, or None for all
            return_type: Either 'record' for Record objects or 'dict' for OrderedDict
        """
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers,
                         skip_records=skip_records, max_records=max_records,
                         return_type=return_type)
        self.fp = fp
        self.record_xpath = record_xpath
        self.custom_columns = columns or []
        self.sample_size = sample_size

        self._tree = None
        self._record_nodes = None
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
        discovered_elements = []
        sample_records = self._record_nodes[:self.sample_size]

        for record_node in sample_records:
            for child in record_node:
                if child.tag:
                    col_name = self._flatten_element_name(child.tag)
                    if col_name not in custom_names and col_name not in discovered_elements:  # Don't duplicate custom columns
                        discovered_elements.append(col_name)

        # Add discovered columns as XMLColumn objects with cleaned names
        for element_name in discovered_elements:
            cleaned_name = self.clean_header(element_name)
            self._all_columns.append(XMLColumn(cleaned_name))

        # Extract just the names for the column cache
        self._column_cache = [col.name for col in self._all_columns]

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

    def _generate_rows(self) -> Iterator[List[Any]]:
        """Generate data rows from XML record nodes."""
        # Ensure columns are discovered before reading data
        if not self._all_columns:
            self._introspect_columns()

        for record_node in self._record_nodes:
            row_data = []
            for xml_column in self._all_columns:
                value = self._extract_column_value(record_node, xml_column)
                row_data.append(value)
            yield row_data

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