# dbtk/readers/data_frame.py
import itertools
from typing import Iterable, Optional
from .base import Reader, Record, Clean, logger


class DataFrameReader(Reader):
    """
    Read directly from pandas or polars DataFrames — zero intermediate files.

    This reader accepts a pre-loaded DataFrame (from pandas or polars) and streams
    rows as Record objects. It supports all standard Reader features
    (add_rownum, skip_records, max_records) while providing accurate
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
        add_row_num: bool = True,
        clean_headers: Clean = Clean.NOOP,
        skip_rows: int = 0,
        n_rows: Optional[int] = None,
        null_values=None
    ):
        super().__init__(
            add_row_num=add_row_num,
            clean_headers=clean_headers,
            skip_rows=skip_rows,
            n_rows=n_rows,
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

        # Apply skip/n_rows if needed (parent already stored the values)
        if self.skip_rows:
            iterator = itertools.islice(iterator, self.skip_rows, None)
        if self.n_rows is not None:
            iterator = itertools.islice(iterator, self.n_rows)

        self._iterator = iterator

        # Progress tracking
        self._trackable = None

        self._total_rows = len(df)
        if self.skip_rows:
            self._total_rows -= self.skip_rows
        if self.n_rows is not None:
            self._total_rows = min(self._total_rows, self.n_rows)
        self._total_rows = max(self._total_rows, 0)

    @property
    def total(self) -> Optional[int]:
        """Total rows for progress bar — known for DataFrames."""
        return self._total_rows if self._total_rows > 0 else None

    def _setup_record_class(self):
        """Override: use pre-detected columns from DataFrame (original names)."""
        if self._headers_initialized:
            return

        # Use original column names from DataFrame
        self._headers = self.columns[:]

        if self.add_row_num:
            if '_row_num' in self._headers:
                raise ValueError("Header '_row_num' already exists.")
            self._headers.append('_row_num')

        # Create Record subclass with original field names
        # set_fields() will automatically normalize for attribute access
        self._record_class = type('DataFrameRecord', (Record,), {})
        self._record_class.set_fields(self._headers)

        self._headers_initialized = True

    def _generate_rows(self) -> Iterable[list]:
        yield from self._iterator

    def _read_headers(self):
        return self.columns