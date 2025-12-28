import dbtk
import logging
import polars as pl
from pathlib import Path
from dbtk.etl import DataSurge, Table, ValidationCollector
from dbtk.etl.transforms import TableLookup

"""
Loads a subset (Drama, Crime movies from title.basics.tsv.gz from the IMDB dataset into a Postgres database.
This will give you a "complete" subset of the data, in that all of the people in the names_subset will be 
actors, directors, producers, etc in the movies in titles_subset.

The title.basics.tsv.gz data file has over 11M records and is over 200MB compressed.
The title.principals.tsv.gz data file has over 96M records and is over 700MB compressed.
The title.ratings.tsv.gz data file has 1.6M records and is 8MB compressed.
The name.basics.tsv.gz data file has over 14M records and is over 280MB compressed.

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
  genres          varchar(30)[],
  avg_rating      float,
  num_votes       int
);
CREATE TABLE title_principals_subset (
  tconst         varchar(10) NOT NULL,
  ordering       int         NOT NULL,
  nconst         varchar(10) NOT NULL,
  category       varchar(10),
  job            text,
  characters     text[],
  PRIMARY KEY (tconst, ordering)
);
CREATE TABLE etl.names_subset (
    nconst varchar(10) PRIMARY KEY,
    primary_name varchar(100) NOT NULL,
    birth_year int,
    death_year int,
    primary_profession varchar(100)[],
    known_for_titles varchar(10)[]
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
        elif val[0] == '[':
            return '{' + val[1:-1] + '}'
        else:
            return '{' + val + '}'


if __name__ == '__main__':
    logger = logging(__name__)
    dbtk.setup_logging()
    db = dbtk.connect('imdb')
    cur = db.cursor()
    cur.execute('TRUNCATE TABLE public.titles_subset')
    cur.execute('TRUNCATE TABLE public.title_principals_subset')
    cur.execute('TRUNCATE TABLE public.names_subset')

    titles_path = Path.home() / 'Downloads' / 'title.basics.tsv.gz'
    ratings_path = Path.home() / 'Downloads' / 'title.ratings.tsv.gz'
    principals_path = Path.home() / 'Downloads' / 'title.principals.tsv.gz'
    names_path = Path.home() / 'Downloads' / 'name.basics.tsv.gz'

    # load existing genre validation
    genre_lookup = TableLookup(cur,
                              table='genres',
                              key_cols='genre',
                              return_cols=['genre', 'title'],
                              cache=TableLookup.CACHE_PRELOAD)
    genre_collector = ValidationCollector(lookup=genre_lookup)
    title_collector = ValidationCollector()
    names_collector = ValidationCollector()

    #----------------------------------------------------------------------------------------------
    # Import title.basics.tsv.gz into titles_subset - filtered to ~16K movies
    #----------------------------------------------------------------------------------------------
    title_cols = {
        'tconst': {'field': 'tconst', 'primary_key': True, 'fn': title_collector},
        'title_type': {'field': 'titleType', 'nullable': False},
        'primary_title': {'field': 'primaryTitle', 'nullable': False},
        'start_year': {'field': 'startYear'},
        'end_year': {'field': 'endYear'},
        'is_adult': {'field': 'isAdult', 'fn': 'bool'},
        'runtime_minutes': {'field': 'runtimeMinutes', 'fn': 'int'},
        'genres': {'field': 'genres', 'fn': ['split:,', genre_collector, wrap_array]}
    }
    titles = Table('titles_subset', columns=title_cols, cursor=cur)
    df = pl.scan_csv(
        titles_path,
        separator="\t",
        null_values=r"\N",
        quote_char=None,     # required for the partially quoted values in the dataset
        ignore_errors=True,  # Skip bad rows / insert nulls on parse errors
    ).filter(
        (pl.col("titleType") == "movie")
        & pl.col("genres").str.contains("Crime")
        & pl.col("genres").str.contains("Drama")
        & pl.col("startYear").cast(pl.Int16, strict=False).is_between(2020, 2022)
    ).collect()
    with dbtk.readers.DataFrameReader(df) as reader:
        surge = DataSurge(titles, use_transaction=True)
        surge.insert(reader)
    # materialize a set that combines existing and added (the table was truncated earlier so there were no existing records)
    all_titles = title_collector.get_all()

    # ----------------------------------------------------------------------------------------------
    # Import title.ratings.tsv.gz into titles_subset (updates)
    # ----------------------------------------------------------------------------------------------
    title_ratings = Table('titles_subset',
                          columns={'tconst': {'field': 'tconst', 'key': True},
                                   'avg_rating': {'field': 'averageRating'},
                                   'num_votes': {'field': 'numVotes'}},
                          cursor=cur)
    df = pl.scan_csv(
        ratings_path,
        separator="\t",
        null_values=r"\N",
        quote_char=None,     # required for the partially quoted values in the dataset
        ignore_errors=True,  # Skip bad rows / insert nulls on parse errors
    ).filter(pl.col('tconst').is_in(all_titles)).collect()
    with dbtk.readers.DataFrameReader(df) as reader:
        surge = DataSurge(title_ratings)
        surge.update(reader)

    # ----------------------------------------------------------------------------------------------
    # Import title.principals.tsv.gz into titles_principals_subset -
    # ----------------------------------------------------------------------------------------------
    principals = Table('title_principals_subset',
                        columns={
                            'tconst': {'field': 'tconst', 'key': True},
                            'ordering': {'field': 'ordering', 'key': True},
                            'nconst': {'field': 'nconst', 'fn': names_collector},
                            'category': {'field': 'category'},
                            'job': {'field': 'job'},
                            'characters': {'field': 'characters', 'fn': wrap_array}
                        },
                       cursor=cur)
    df = pl.scan_csv(
            principals_path,
            separator="\t",
            null_values=r"\N",
            quote_char=None,  # required for the partially quoted values in the dataset
            ignore_errors=True,  # Skip bad rows / insert nulls on parse errors
    ).filter((pl.col("tconst").is_in(all_titles))).collect()
    with dbtk.readers.DataFrameReader(df) as reader:
        surge = DataSurge(principals, use_transaction=True)
        surge.insert(reader)
    all_names = names_collector.get_all()

    # ----------------------------------------------------------------------------------------------
    # Import name.basics.tsv.gz into names_subset
    # ----------------------------------------------------------------------------------------------
    names_cols = {
        'nconst': {'field': 'nconst', 'primary_key': True},
        'primary_name': {'field': 'primaryName', 'nullable': False, 'fn': 'maxlen:100'},
        'birth_year': {'field': 'birthYear', 'fn': 'int'},
        'death_year': {'field': 'deathYear', 'fn': 'int'},
        'primary_profession': {'field': 'primaryProfession', 'fn': wrap_array},
        'known_for_titles': {'field': 'knownForTitles', 'fn': wrap_array},
    }
    names = Table('names_subset', columns=names_cols, cursor=cur)
    df = pl.scan_csv(
            names_path,
            separator="\t",
            null_values=r"\N",
            quote_char=None,  # required for the partially quoted values in the dataset
            ignore_errors=True,  # Skip bad rows / insert nulls on parse errors
    ).filter((pl.col("nconst").is_in(all_names))).collect()
    with dbtk.readers.DataFrameReader(df) as reader:
        surge = DataSurge(names, use_transaction=True)
        surge.insert(reader)

    # ----------------------------------------------------------------------------------------------
    # Import new genres we collected.
    # ----------------------------------------------------------------------------------------------
    new_genre = [(val, val.replace('_', ' ').title()) for val in genre_collector.get_new_codes()]
    if new_genre:
        genre_insert = 'INSERT INTO genres (genre, title) VALUES (%s, %s)'
        cur.executemany(genre_insert, new_genre)
    db.commit()