# dbtk/readers/json.py

"""JSON and NDJSON (newline-delimited JSON) file readers."""

import json
from typing import List, Any, Dict, Optional, TextIO, Iterator
from .base import Reader, Clean


class JSONReader(Reader):
    """
    Read JSON files containing arrays of objects.

    JSONReader parses JSON files that contain an array of objects (standard JSON format
    for tabular data). It can optionally flatten nested objects using dot notation,
    making it easy to work with hierarchical JSON data in a flat, record-based format.

    The reader automatically discovers the schema by scanning all objects in the array,
    ensuring all possible keys are available even if some objects don't have all fields.

    Parameters
    ----------
    fp : file-like object
        Open file pointer to JSON file
    flatten : bool, default True
        Flatten nested objects with dot notation. For example, ``{"user": {"name": "Bob"}}``
        becomes ``{"user.name": "Bob"}``. Arrays are preserved as-is.
    add_rownum : bool, default True
        Add _row_num field to each record
    clean_headers : Clean, default Clean.DEFAULT
        Header cleaning level
    skip_records : int, default 0
        Number of records to skip
    max_records : int, optional
        Maximum records to read
    **kwargs
        Reserved for future use

    Example
    -------
    ::

        from dbtk import readers

        # Simple JSON array
        # [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        with readers.JSONReader(open('users.json')) as reader:
            for user in reader:
                print(user.id, user.name)

        # Nested JSON with flattening
        # [{"id": 1, "user": {"name": "Alice", "email": "a@example.com"}}]
        with readers.JSONReader(open('nested.json'), flatten=True) as reader:
            for record in reader:
                print(record.id, record['user.name'], record['user.email'])

        # Disable flattening to keep nested structure
        with readers.JSONReader(open('nested.json'), flatten=False) as reader:
            for record in reader:
                print(record.user)  # {'name': 'Alice', 'email': 'a@example.com'}

    See Also
    --------
    NDJSONReader : Read newline-delimited JSON files
    Reader : Base reader class
    writers.to_json : Write JSON files

    Notes
    -----
    * JSON file must contain an array at the root level
    * All objects in array are scanned to discover complete schema
    * Empty arrays raise ValueError
    * Nested objects are flattened with dot notation by default
    * Arrays within objects are never flattened
    """

    def __init__(self,
                 fp: TextIO,
                 flatten: bool = True,
                 add_row_num: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_rows: int = 0,
                 n_rows: Optional[int] = None,
                 null_values=None,
                 **kwargs):
        super().__init__(add_row_num=add_row_num, clean_headers=clean_headers,
                         skip_rows=skip_rows, n_rows=n_rows,
                         null_values=null_values)
        self.fp = fp

        # Set trackable for progress tracking
        if hasattr(fp, '_uncompressed_size'):
            # Compressed file - use buffer's tell() but preserve _uncompressed_size
            self._trackable = fp.buffer
            self._trackable._uncompressed_size = fp._uncompressed_size
        elif hasattr(fp, 'buffer'):
            # Text mode file - use buffer for better performance
            self._trackable = fp.buffer
        else:
            # Binary mode or other file type
            self._trackable = fp

        self.flatten = flatten
        self._data = None
        self._column_cache = None
        self._keys = []  # Either flattened keys or original keys depending on flatten setting
        self._parse_json()

    def _parse_json(self):
        """Parse the JSON file and validate it's an array."""
        try:
            self._data = json.load(self.fp)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")

        if not isinstance(self._data, list):
            raise ValueError(f"JSON file must contain an array, got {type(self._data).__name__}")

        if not self._data:
            raise ValueError("JSON array is empty")

    def _flatten_object(self, obj: Dict, prefix: str = '') -> Dict[str, Any]:
        """
        Recursively flatten a JSON object with dot notation.
        Arrays are preserved as lists.
        """
        result = {}

        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                nested = self._flatten_object(value, full_key)
                result.update(nested)
            else:
                result[full_key] = value

        return result

    def _discover_schema(self) -> List[str]:
        """Analyze all objects to discover the complete set of possible keys."""
        if self._column_cache is not None:
            return self._column_cache

        all_keys = set()

        for obj in self._data:
            if isinstance(obj, dict):
                if self.flatten:
                    flattened = self._flatten_object(obj)
                    all_keys.update(flattened.keys())
                else:
                    all_keys.update(obj.keys())

        # Clean and sort the keys
        if not all_keys:
            raise ValueError("No keys discovered in NDJSON file")
        self._keys = sorted(all_keys)
        self._column_cache = [Clean.normalize(key, self.clean_headers) for key in self._keys]

        return self._column_cache

    def _extract_values(self, obj: Dict) -> List[Any]:
        """Extract values from a JSON object in the order of discovered columns."""
        if self.flatten:
            flattened = self._flatten_object(obj) if isinstance(obj, dict) else {}
            return [flattened.get(key) for key in self._keys]
        else:
            if not isinstance(obj, dict):
                return [None] * len(self._keys)
            return [obj.get(key) for key in self._keys]

    def _read_headers(self) -> List[str]:
        return self._discover_schema()

    def _generate_rows(self) -> Iterator[List[Any]]:
        if not self._keys:
            self._discover_schema()
        for obj in self._data:
            yield self._extract_values(obj)

    def _cleanup(self):
        if hasattr(self, 'fp') and self.fp:
            self.fp.close()

    @property
    def record_count(self) -> int:
        return len(self._data) if self._data else 0


class NDJSONReader(Reader):
    """Newline-delimited JSON file reader that returns Record objects or dicts."""

    def __init__(self,
                 fp: TextIO,
                 add_row_num: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_rows: int = 0,
                 n_rows: Optional[int] = None,
                 null_values=None):
        """
        Initialize NDJSON reader.

        Args:
            fp: File pointer to NDJSON file (one JSON object per line)
            add_row_num: Add _row_num to each record
            clean_headers: Header cleaning level
            skip_rows: Number of rows to skip from the beginning
            n_rows: Maximum number of rows to read (None = unlimited)
            null_values: Values to convert to None (e.g., '\\N', 'NULL', 'NA')
        """
        super().__init__(add_row_num=add_row_num, clean_headers=clean_headers,
                         skip_rows=skip_rows, n_rows=n_rows,
                         null_values=null_values)
        self.fp = fp
        self._trackable = self.fp
        self._column_cache = None
        self._original_keys = []  # Track original keys for value extraction
        self._schema_sample_size = 100

    def _discover_schema(self) -> List[str]:
        """
        Discover schema by sampling the first N records.
        Returns to original file position after sampling.
        """
        if self._column_cache is not None:
            return self._column_cache

        # Save current position
        current_pos = self.fp.tell()

        # Reset to beginning for schema discovery
        self.fp.seek(0)

        all_keys = []
        sample_count = 0

        try:
            for line in self.fp:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        # Preserve order of first appearance
                        for key in obj.keys():
                            if key not in all_keys:
                                all_keys.append(key)
                        sample_count += 1

                        if sample_count >= self._schema_sample_size:
                            break
                except json.JSONDecodeError:
                    continue  # Skip malformed lines during schema discovery

        except Exception:
            pass  # If anything goes wrong, use what we have

        # Restore original position
        self.fp.seek(current_pos)

        # Store original keys and create cleaned headers
        if not all_keys:
            raise ValueError("No keys discovered in NDJSON file")
        self._original_keys = all_keys
        self._column_cache = [Clean.normalize(key, self.clean_headers) for key in all_keys]

        return self._column_cache

    def _extract_values(self, obj: Dict) -> List[Any]:
        """Extract values from a JSON object in the order of discovered columns."""
        if not isinstance(obj, dict):
            return [None] * len(self._original_keys)

        return [obj.get(key) for key in self._original_keys]

    def _read_headers(self) -> List[str]:
        """Read and return column names from NDJSON structure."""
        return self._discover_schema()

    def _generate_rows(self) -> Iterator[List[Any]]:
        """Generate data rows from NDJSON file."""
        self.fp.seek(0)  # Reset for data reading

        # Ensure schema is discovered
        if not self._original_keys:
            self._discover_schema()
            self.fp.seek(0)  # Reset again after schema discovery

        for line in self.fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                yield self._extract_values(obj)
            except json.JSONDecodeError:
                continue  # Skip malformed lines

    def _cleanup(self):
        """Close the file pointer."""
        if hasattr(self, 'fp') and self.fp:
            self.fp.close()