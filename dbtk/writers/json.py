# dbtk/writers/json.py
"""
JSON writer for database results.
"""
import itertools
import json
import logging
import sys
from typing import Union, List, TextIO, Optional, Any
from pathlib import Path

from .utils import get_data_iterator, format_value

logger = logging.getLogger(__name__)


def to_json(data,
            filename: Optional[Union[str, Path]] = None,
            encoding: str = 'utf-8',
            indent: Optional[int] = 2,
            stream: bool = False,
            **json_kwargs) -> None:
    """
    Export cursor or result set to JSON file.  The 'array' format converts the entire result set into an array of dictionaries.
    For large result sets, use either the 'streaming_array' or 'ndjson' formats to write the result incrementally.
    The streaming_array format produces a valid JSON array, while ndjson produces a newline-delimited JSON stream.

    Args:
        data: Cursor object or list of records
        filename: Output filename. If None, writes to stdout
        encoding: File encoding
        indent: JSON indentation - defaults to 2 (pretty-print), 0 or None for compact
        **json_kwargs: Additional arguments passed to json.dump/dumps

    Examples:
        # Write to file as JSON array
        to_json(cursor, 'users.json')

        # Write to stdout
        to_json(cursor)

        # stream format - valid JSON array
        to_json(cursor, 'data.json', format='streaming_array')

    """
    data_iterator, columns = get_data_iterator(data)
    if not data_iterator or not columns:
        logger.warning("No data to export")
        return

    # Determine output destination
    if filename is None:
        file_obj = sys.stdout
        close_file = False
        # limit to 20 rows when writing to stdout
        data_iterator = itertools.islice(data_iterator, 20)
    else:
        file_obj = open(filename, 'w', encoding=encoding)
        close_file = True

    row_count = 0
    try:
        if not stream:
            records = [_convert_record(record, columns) for record in data_iterator]
            row_count = len(records)
            json.dump(records, file_obj, indent=indent, default=format_value, **json_kwargs)
        else:
            file_obj.write('[\n')
            for record in data_iterator:
                json_line = json.dumps(_convert_record(record, columns), default=format_value, indent=indent, **json_kwargs)
                if row_count:
                    file_obj.write(',\n' + json_line)
                else:
                    file_obj.write(json_line)
                row_count += 1
                if row_count % 1000 == 0:
                    file_obj.flush()
            file_obj.write('\n]')
        logger.info(f"Wrote {row_count} rows to {filename or 'stdout'}")
    finally:
        if close_file:
            file_obj.close()


def to_ndjson(data,
              filename: Optional[Union[str, Path]] = None,
              encoding: str = 'utf-8',
              **json_kwargs) -> None:

    data_iterator, columns = get_data_iterator(data)
    if filename is None:
        file_obj = sys.stdout
        close_file = False
        # limit to 20 rows when writing to stdout
        data_iterator = itertools.islice(data_iterator, 20)
    else:
        file_obj = open(filename, 'w', encoding=encoding)
        close_file = True

    row_count = 0
    try:
        for record in data_iterator:
            json_line = json.dumps(_convert_record(record, columns), default=format_value, **json_kwargs)
            file_obj.write(json_line + '\n')
            row_count += 1
            if row_count % 1000 == 0:
                file_obj.flush()
        logger.info(f"Wrote {row_count} rows to {filename or 'stdout'}")
    finally:
        if close_file:
            file_obj.close()


def _convert_record(record, columns: List = None) -> dict:
    """ Convert record, namedtuple or list (with columns) to dict"""
    if hasattr(record, 'to_dict'):
        return record.to_dict()
    elif hasattr(record, '_asdict'):
        return record._asdict()
    elif hasattr(record, 'keys') and callable(record.keys):
        # dict-like object
        return {key: record[key] for key in record.keys()}
    elif isinstance(record, (list, tuple)):
        # List cursor - convert to dict using columns
        return {columns[i]: record[i] for i in range(min(len(columns), len(record)))}
    else:
        return record
