"""
Bulk loads title.basics.tsv.gz from the IMDB dataset into a Postgres database using
Postgres COPY FROM CSV
The title.basics.tsv dataset has over 12M records and is over 205MB compressed.

The IMDB dataset can be found at https://developer.imdb.com/non-commercial-datasets/

CREATE TABLE titles (
  tconst          varchar(10) PRIMARY KEY,
  title_type      varchar(30) NOT NULL,
  primary_title   text NOT NULL,
  original_title  text,
  is_adult        bool,
  start_year      int,
  end_year        int,
  runtime_minutes int,
  genres          varchar(30)[]
);
CREATE TABLE genres (
   genre   varchar(30) PRIMARY KEY,
   title   varchar(30)
);
"""

import dbtk
import polars as pl
from pathlib import Path
from dbtk.etl import Table, BulkSurge, ValidationCollector
from dbtk.etl.transforms import TableLookup
from dbtk.readers import DataFrameReader


def wrap_array(val) -> str:
    """Wrap comma separated string or list with '{}' for postgres"""
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        return '{' + ','.join(val) + '}'
    elif isinstance(val, str):
        if val[0] == '{':
            return val
        elif val[0] == '[':
            return '{' + val[1:-1] + '}'
        else:
            return '{' + val + '}'


if __name__ == '__main__':
    dbtk.setup_logging()
    db = dbtk.connect('imdb')
    cur = db.cursor()
    cur.execute('TRUNCATE table public.titles')

    # load existing genre validation
    genre_lookup = TableLookup(cur,
                               table='genres',
                               key_cols='genre',
                               return_cols=['genre', 'title'],
                               cache=TableLookup.CACHE_PRELOAD)
    # keep track of new genre
    genre_collector = ValidationCollector(lookup=genre_lookup)
    title_cols = {
        'tconst': {'field': 'tconst', 'primary_key': True},
        'title_type': {'field': 'titleType', 'nullable': False},
        'primary_title': {'field': 'primaryTitle', 'nullable': False},
        'start_year': {'field': 'startYear'},
        'end_year': {'field': 'endYear'},
        'is_adult': {'field': 'isAdult', 'fn': 'bool'},
        'runtime_minutes': {'field': 'runtimeMinutes', 'fn': 'int'},
        'genres': {'field': 'genres', 'fn': ['split:,', genre_collector, wrap_array]}
    }
    titles = Table('titles', title_cols, cursor=cur)
    # Use polars ridiculously fast... but lazy csv reader
    titles_path = Path.home() / 'Downloads' / 'title.basics.tsv.gz'
    df = pl.scan_csv(titles_path,
                     separator="\t",
                     null_values=r'\N',
                     quote_char=None,     # This file has partially quoted fields that cause errors otherwise
                     ignore_errors=True,
                     # n_rows=100_000,    # uncomment to limit number of rows loaded
                     ).collect()
    with DataFrameReader(df, add_row_num=False) as reader:
        title_surge = BulkSurge(titles)
        title_surge.load(reader)
        # Swap these two lines to load using inserts instead of COPY FROM
        # title_surge = dbtk.etl.DataSurge(titles, use_transaction=True)
        # title_surge.insert(reader)

    genre_insert = 'INSERT INTO genres (genre, title) VALUES (%s, %s) ON CONFLICT DO NOTHING'
    new_genre = [(val, val.replace('_', ' ').title()) for val in genre_collector.get_new_codes()]

    if new_genre:
        cur.executemany(genre_insert, new_genre)
        db.commit()
