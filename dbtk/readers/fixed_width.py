# dbtk/readers/fixed_width.py

import re
from typing import TextIO, List, Any, Optional, Iterator
from collections import Counter, defaultdict
from .base import Reader, Clean, ReturnType
from ..etl.transforms.datetime import parse_date, parse_datetime, parse_timestamp


class FixedColumn(object):
    """ Column definition for fixed width files """

    def __init__(self, name, start_pos, end_pos, column_type='text'):
        """
        :param str name:  database column name
        :param int start_pos: start position of field, first position is 1 not 0
        :param int end_pos: end position of field
        :param str column_type: text, int, float, date

        FixedColumn('birthdate', 25, 35, 'date')
        """
        self.name = name
        self.start_pos = start_pos
        self.end_pos = end_pos if end_pos else start_pos
        self.column_type = column_type
        self.start_idx = start_pos - 1

    def __repr__(self):
        return f"FixedColumn('{self.name}', {self.start_pos}, {self.end_pos}, '{self.column_type}')"


class FixedReader(Reader):
    """ Reader for fixed width files """

    def __init__(self,
                 fp: TextIO,
                 columns: List[FixedColumn],
                 auto_trim: bool = True,
                 add_rownum: bool = True,
                 clean_headers: Clean = Clean.NOOP,
                 skip_records: int = 0,
                 max_records: Optional[int] = None,
                 return_type: str = ReturnType.DEFAULT):
        """
        Initializes the instance with the provided file pointer, column definitions, and
        processing options.

        Attributes:
            fp (TextIO): The file pointer from which data is read.
            columns (List[FixedColumn]): A list of FixedColumn objects defining the
                structure of columns in the data.
            auto_trim (bool): Determines whether to automatically trim whitespace
                from field values. Default is True.
            add_rownum (bool): Determines whether to add a row number attribute
            clean_headers (Clean): Determines the header cleaning level. Default is NOOP.
            skip_records (int): The number of records to skip before reading data.
            max_records (Optional[int]): The maximum number of records to read.
            return_type: Either 'record' for Record objects or 'dict' for OrderedDict.
        """
        super().__init__(add_rownum=add_rownum, clean_headers=clean_headers,
                         skip_records=skip_records, max_records=max_records,
                         return_type=return_type)
        self.fp = fp
        self.columns = columns
        self.auto_trim = auto_trim

    def _read_headers(self) -> List[str]:
        """Return column names from FixedColumn definitions."""
        return [col.name for col in self.columns]

    def _generate_rows(self) -> Iterator[List[Any]]:
        while True:
            line = self.fp.readline()
            if not line:
                break
            row_data = []
            for col in self.columns:
                val = line[col.start_idx:col.end_pos]
                try:
                    if col.column_type == 'text' and self.auto_trim:
                        val = str(val).strip()
                    elif col.column_type == 'date':
                        val = parse_date(val)
                    elif col.column_type == 'datetime':
                        val = parse_datetime(val)
                    elif col.column_type == 'timestamp':
                        val = parse_timestamp(val)
                    elif col.column_type == 'int':
                        val = int(val.strip()) if val.strip() else None
                    elif col.column_type == 'float':
                        val = float(val.strip()) if val.strip() else None
                    else:
                        val = str(val)
                except (ValueError, TypeError):
                    val = str(val).strip() if self.auto_trim else str(val)
                row_data.append(val)
            yield row_data

    def _cleanup(self):
        """Close the file pointer."""
        if self.fp and hasattr(self.fp, 'close'):
            self.fp.close()

    @classmethod
    def infer_columns(cls, fp: TextIO, min_occurrences: int = 5) -> List[FixedColumn]:
        """
        Infers column boundaries and types from a fixed-width file using regex.

        Args:
            fp: A file-like object opened in text mode.
            min_occurrences: Minimum number of times a (start, end) pair must appear to be considered a column.

        Returns:
            A list of FixedColumn objects.
        """

        def guess_type(value: str) -> str:
            if re.fullmatch(r"-?\d+", value):
                return "int"
            elif re.fullmatch(r"-?\d+\.\d+", value):
                return "float"
            elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) or re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
                return "date"
            else:
                return "text"

        boundary_counts = Counter()
        type_counts = defaultdict(Counter)

        for line in fp:
            for match in re.finditer(r'\S+', line.rstrip('\n')):
                start, end = match.start(), match.end()
                value = match.group()
                boundary_counts[(start, end)] += 1
                type_counts[(start, end)][guess_type(value)] += 1

        columns = []
        i = 0
        for (start, end), count in boundary_counts.items():
            if count >= min_occurrences:
                i += 1
                most_common_type, _ = type_counts[(start, end)].most_common(1)[0]
                add_column = True
                for col in columns:
                    if col.start_pos == start + 1:
                        add_column = False
                        if col.end_pos > end + 1:
                            col.end_pos = end + 1
                if add_column:
                    columns.append(FixedColumn(f'column_{i:02d}',
                                               start_pos=start + 1,
                                               end_pos=end + 1,
                                               column_type=most_common_type))

        return sorted(columns, key=lambda col: col.start_pos)

    @classmethod
    def visualize_columns(cls,
                          fp: TextIO,
                          columns: List[FixedColumn] = None,
                          sample_lines: int = 4) -> str:
        """
        Visualizes column boundaries and sample data from a fixed-width file.

        Args:
            fp: file pointer or file-like object in text mode
            columns: list of columns
            sample_lines: number of lines to show in preview

        Returns:
            String representation of column layout
        """
        fp.seek(0)
        lines = [next(fp).rstrip('\n') for _ in range(sample_lines)]
        max_len = max([len(line) for line in lines])
        ruler_10s = ''.join(str(i // 10 % 10) if i % 10 == 0 else ' ' for i in range(1, max_len))
        ruler_1s = ''.join(str(i % 10) for i in range(1, max_len))
        boundary_line = [' '] * max_len
        for col in columns:
            if col.start_pos <= max_len:
                boundary_line[col.start_pos - 1] = '|'
        return f'{ruler_10s}\n{ruler_1s}\n{"".join(boundary_line)}\n' + '\n'.join(lines)