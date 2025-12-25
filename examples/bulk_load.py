import dbtk
import polars as pl
from dbtk.etl.manager import ValidationCollector
from dbtk.etl.transforms import TableLookup
from dbtk.readers import DataFrameReader

"""
Loads the first 100,000 rows of title.basics.tsv.gz from the IMDB dataset into a Postgres database.
The title.basics.tsv dataset has over 11M records and is over 205MB compressed.

The IMDB dataset can be found at https://developer.imdb.com/non-commercial-datasets/

CREATE TABLE titles_subset (
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

def wrap_array(val) -> str:
    """Wrap comma separated string or list with '{}' for postgres"""
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        return '{' + ','.join(val) + '}'
    elif isinstance(val, str):
        if val[0] == '{':
            return val
        else:
            return '{' + val + '}'


if __name__ == '__main__':
    db = dbtk.connect('imdb')
    cur = db.cursor()
    dbtk.setup_logging()
    cur.execute('TRUNCATE table public.titles_subset')
    # load existing genre validation

    genre_lookup = TableLookup(cur,
                              table='genres',
                              key_cols='genre',
                              return_cols=['genre', 'title'],
                              cache=TableLookup.CACHE_PRELOAD)

    # keep track of new genre
    genre_collector = ValidationCollector(lookup=genre_lookup, desc_field='title', return_desc=False)
    # Use polars ridiculously fast... but lazy csv reader
    df = pl.scan_csv(r'c:\Temp\title.basics.tsv.gz', separator="\t", null_values=r'\N', n_rows=100_000).collect()
    reader = DataFrameReader(df, add_row_num=False)
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
    titles = dbtk.etl.Table('titles_subset', title_cols, cursor=cur)
    with dbtk.etl.BulkSurge(titles) as title_loader:
        title_loader.load(reader)

    genre_insert = 'INSERT INTO genres (genre, title) VALUES (%s, %s)'
    new_genre = [(val, val.replace('_', ' ').title()) for val in genre_collector.get_new_codes()]
    if new_genre:
        cur.executemany(genre_insert, new_genre)
        db.commit()
