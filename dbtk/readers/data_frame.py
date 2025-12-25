# dbtk/readers/data_frame.py
import itertools
from typing import Iterable, Optional
from .base import Reader, Record, Clean, ReturnType, logger


class DataFrameReader(Reader):
    """
    Read directly from pandas or polars DataFrames — zero intermediate files.

    This reader accepts a pre-loaded DataFrame (from pandas or polars) and streams
    rows as if reading from a file. It supports all standard Reader features
    (add_rownum, return_type, skip_records, max_records) while providing accurate
    progress tracking based on known row count.

    No pandas or polars are imported in this module — the user has already imported
    one and passed a DataFrame.

    Parameters
    ----------
    df : DataFrame
        pandas.DataFrame or polars.DataFrame containing the data
    add_rownum : bool, default True
        Add '_row_num' field with 1-based row number
    clean_headers: Clean or str, optional
        Header cleaning level. Options: Clean.NOOP (default).
    return_type : str, default 'record'
        'record' for Record objects, 'dict' for OrderedDict
    skip_records : int, default 0
        Number of rows to skip from the beginning
    max_records : int, optional
        Maximum number of records to yield

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.read_parquet("data.parquet")
    >>> with DataFrameReader(df) as reader:
    >>>     for row in reader:
    >>>         print(row.id)

    >>> import polars as pl
    >>> df = pl.read_parquet("data.parquet")
    >>> with DataFrameReader(df, add_rownum=False) as reader:
    >>>     BulkSurge(table).load(reader)
    """

    def __init__(
        self,
        df,
        add_rownum: bool = True,
        clean_headers: Clean = Clean.NOOP,
        return_type: str = 'record',
        skip_records: int = 0,
        max_records: Optional[int] = None,
        null_values=None
    ):
        super().__init__(
            add_rownum=add_rownum,
            clean_headers=clean_headers,
            skip_records=skip_records,
            max_records=max_records,
            return_type=return_type,
            headers=None,  # we'll set this ourselves
            null_values=null_values
        )

        # 2. Now do our DataFrame-specific work
        df_type = type(df).__module__

        if df_type.startswith('pandas'):
            self.columns = list(df.columns)
            iterator = df.itertuples(index=False, name=None)
        elif df_type.startswith('polars'):
            self.columns = df.columns
            iterator = df.rows()
        else:
            raise TypeError(f"Unsupported DataFrame type: {type(df)}")

        # Apply skip/max if needed (parent already stored the values)
        if self.skip_records:
            iterator = itertools.islice(iterator, self.skip_records, None)
        if self.max_records is not None:
            iterator = itertools.islice(iterator, self.max_records)

        self._iterator = iterator

        # Progress tracking
        self._trackable = None

        self._total_rows = len(df)
        if self.skip_records:
            self._total_rows -= self.skip_records
        if self.max_records is not None:
            self._total_rows = min(self._total_rows, self.max_records)
        self._total_rows = max(self._total_rows, 0)

    @property
    def total(self) -> Optional[int]:
        """Total rows for progress bar — known for DataFrames."""
        return self._total_rows if self._total_rows > 0 else None

    def _setup_record_class(self):
        """Override: use pre-detected columns instead of reading from file."""
        if self._headers_initialized:
            return

        # Use columns we already detected from DataFrame
        self._headers = self.columns[:]

        if self.add_rownum:
            if '_row_num' in self._headers:
                raise ValueError("Header '_row_num' already exists.")
            self._headers.append('_row_num')

        if self.return_type == ReturnType.RECORD:
            self._record_class = type('DataFrameRecord', (Record,), {})
            self._record_class.set_columns(self._headers)

        self._headers_initialized = True

    def _generate_rows(self) -> Iterable[list]:
        yield from self._iterator

    def _read_headers(self):
        return self.columns