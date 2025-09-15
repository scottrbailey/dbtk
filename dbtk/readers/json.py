# dbtk/readers/json.py

import json
from typing import List, Any, Dict, Optional, TextIO, Iterator
from .base import Reader, Clean


class JSONReader(Reader):
    """JSON array file reader that returns Record objects."""

    def __init__(self,
                 fp: TextIO,
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 **kwargs):
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers,
                         skip_records=skip_records, max_records=max_records)
        self.fp = fp
        self._data = None
        self._column_cache = None
        self._flattened_keys = []
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

        Args:
            obj: Dictionary to flatten
            prefix: Current key prefix for nested objects

        Returns:
            Flattened dictionary
        """
        result = {}

        for key, value in obj.items():
            # Create the full key name
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively flatten nested objects
                nested = self._flatten_object(value, full_key)
                result.update(nested)
            else:
                # Keep primitive values and arrays as-is
                result[full_key] = value

        return result

    def _discover_schema(self) -> List[str]:
        """Analyze all objects to discover the complete set of possible keys."""
        if self._column_cache is not None:
            return self._column_cache

        all_keys = set()

        # Flatten all objects and collect all possible keys
        for obj in self._data:
            if isinstance(obj, dict):
                flattened = self._flatten_object(obj)
                all_keys.update(flattened.keys())

        # Clean and sort the keys
        self._flattened_keys = sorted(all_keys)
        self._column_cache = [self.clean_header(key) for key in self._flattened_keys]

        return self._column_cache

    def _extract_values(self, obj: Dict) -> List[Any]:
        """
        Extract values from a JSON object in the order of discovered columns.

        Args:
            obj: JSON object to extract from

        Returns:
            List of values corresponding to column order
        """
        flattened = self._flatten_object(obj) if isinstance(obj, dict) else {}

        values = []
        for key in self._flattened_keys:
            values.append(flattened.get(key))

        return values

    def _read_headers(self) -> List[str]:
        return self._discover_schema()

    def _generate_rows(self) -> Iterator[List[Any]]:
        if not self._flattened_keys:
            self._discover_schema()
        for obj in self._data:
            yield self._extract_values(obj)

    def _cleanup(self):
        if hasattr(self, 'fp') and self.fp:
            self.fp.close()

    @property
    def record_count(self) -> int:
        return len(self._data) if self._data else 0

    def reset(self):
        self.record_num = 0
        self._data_iter = None


class NDJSONReader(Reader):
    """Newline-delimited JSON file reader that returns Record objects."""

    def __init__(self,
                 fp: TextIO,
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.DEFAULT,
                 skip_records: int = 0,
                 max_records: Optional[int] = None):
        """
        Initialize NDJSON reader.

        Args:
            fp: File pointer to NDJSON file (one JSON object per line)
            add_rownum: Add rownum to each record
            clean_headers: Header cleaning level
            skip_records: Number of records to skip from the beginning
            max_records: Maximum number of records to read (None = unlimited)
        """
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers,
                         skip_records=skip_records, max_records=max_records)
        self.fp = fp
        self._column_cache = None
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

        all_keys = set()
        sample_count = 0

        try:
            for line in self.fp:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        all_keys.update(obj.keys())
                        sample_count += 1

                        if sample_count >= self._schema_sample_size:
                            break
                except json.JSONDecodeError:
                    continue  # Skip malformed lines during schema discovery

        except Exception:
            pass  # If anything goes wrong, use what we have

        # Restore original position
        self.fp.seek(current_pos)

        # Clean and sort the keys
        sorted_keys = sorted(all_keys)
        self._column_cache = [self.clean_header(key) for key in sorted_keys]

        return self._column_cache

    def _read_headers(self) -> List[str]:
        """Read and return column names from NDJSON structure."""
        return self._discover_schema()

    def _generate_rows(self) -> Iterator[List[Any]]:
        self.fp.seek(0)  # Reset for data reading
        while True:
            line = self.fp.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if not self._column_cache:
                    self._discover_schema()  # Ensure schema before yielding
                yield self._extract_values(obj)
            except json.JSONDecodeError:
                continue


    def _discover_original_keys(self) -> List[str]:
        """
        Get the original keys used for schema discovery.
        This is a bit of a hack - we need to track original keys separately
        from cleaned headers for proper value extraction.
        """
        # For now, re-sample to get original keys
        # TODO: Optimize by caching original keys during first discovery
        current_pos = self.fp.tell()
        self.fp.seek(0)

        all_keys = set()
        sample_count = 0

        try:
            for line in self.fp:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        all_keys.update(obj.keys())
                        sample_count += 1

                        if sample_count >= self._schema_sample_size:
                            break
                except json.JSONDecodeError:
                    continue

        except Exception:
            pass

        self.fp.seek(current_pos)
        return sorted(all_keys)

    def __next__(self) -> 'Record':
        """Return the next record."""
        row_data = self._read_next_row()
        return self._create_record(row_data)

    def _cleanup(self):
        """Close the file pointer."""
        if hasattr(self, 'fp') and self.fp:
            self.fp.close()

    def reset(self):
        """Reset reader to beginning (respecting skip_records)."""
        self.fp.seek(0)
        self._records_read = 0
        self._data_iter = None
