import dbtk
import logging
import polars as pl
import time
from pathlib import Path
from dbtk.etl import BulkSurge, DataSurge, Table, ValidationCollector
from dbtk.etl.transforms import TableLookup

"""
IMDB Subset Loader: High-Performance ETL for Relational Movie Data

This example demonstrates building a complete, referentially-intact subset from massive
IMDB datasets (121M+ rows total) using dbtk's ETL capabilities. It showcases filtering
11+ million title records down to ~16K movies, then intelligently extracting only the
related cast, crew, and ratings data to build a cohesive relational database.

The Challenge
-------------
IMDB's non-commercial datasets are enormous:
* **title.basics.tsv.gz**: 11M+ titles, 200MB+ compressed
* **title.principals.tsv.gz**: 96M+ cast/crew records, 700MB+ compressed
* **name.basics.tsv.gz**: 14M+ people, 280MB+ compressed
* **title.ratings.tsv.gz**: 1.6M ratings, 8MB compressed

You don't want all of it - you want a focused, complete subset where all foreign keys
resolve correctly. This example shows how to extract 2020-2022 Crime/Drama movies plus
*only* the people and ratings associated with those specific films.

Features Demonstrated
---------------------
* **Large dataset filtering**: Use Polars lazy evaluation to filter 11M rows efficiently
* **Referential integrity**: ValidationCollector tracks foreign keys across ETL stages
* **Multi-stage pipeline**: Load titles → update ratings → load principals → load names
* **Smart subsetting**: Extract ~16K movies and ~40K related people from 121M+ total rows
* **BulkSurge performance**: Fast bulk inserts with automatic batching
* **DataSurge updates**: Merge ratings data into existing title records
* **Data transformation**: Array formatting, type coercion, column mapping
* **Validation tracking**: Automatically collect new genre codes for reference table

The Pipeline
------------
1. **Load Movies** (titles_subset):
   - Filter title.basics for 2020-2022 Crime+Drama movies
   - Collect tconst values using ValidationCollector for next stage
   - Result: ~16K filtered movies from 11M+ titles

2. **Update Ratings** (titles_subset):
   - Filter title.ratings to only collected movie IDs
   - Use DataSurge to update avg_rating and num_votes
   - Result: Ratings merged into existing movie records

3. **Load Cast & Crew** (title_principals_subset):
   - Filter title.principals to only collected movie IDs
   - Collect nconst values (person IDs) using ValidationCollector
   - Result: Complete cast/crew records for selected movies

4. **Load People** (names_subset):
   - Filter name.basics to only collected person IDs
   - Result: Only people who worked on selected movies (~40K names from 14M+)

5. **Update Reference Data** (genres):
   - Insert any new genre codes discovered during processing
   - Demonstrates automatic reference table maintenance

Key Techniques
--------------
**ValidationCollector Pattern**: Track primary/foreign keys during ETL stages:
```python
title_collector = ValidationCollector()  # Track movie IDs
names_collector = ValidationCollector()  # Track person IDs

# Use as transform function to collect while processing
'tconst': {'field': 'tconst', 'primary_key': True, 'fn': title_collector}
'nconst': {'field': 'nconst', 'fn': names_collector}

# Later stages use collected IDs for filtering
all_titles = title_collector.get_all()  # All movie IDs seen
df.filter(pl.col('tconst').is_in(all_titles))
```

**Polars + dbtk Integration**: Efficient lazy loading and filtering:
```python
df = pl.scan_csv(huge_file, ...).filter(conditions).collect()
with dbtk.readers.DataFrameReader(df) as reader:
    surge = BulkSurge(table)
    surge.load(reader)
```

Output
------
Four populated tables with referential integrity maintained:
- **titles_subset**: ~16K Crime/Drama movies from 2020-2022 with ratings
- **title_principals_subset**: Complete cast and crew for those movies
- **names_subset**: ~40K people who worked on those movies
- **genres**: Reference table with all genre codes

Performance
-----------
Processes 121M+ rows in approximately **20 seconds** (typical hardware), demonstrating
the raw power of dbtk's bulk loading combined with Polars' lazy evaluation. That's
over 6 million rows per second of throughput!

Prerequisites
-------------
* Download IMDB datasets from https://developer.imdb.com/non-commercial-datasets/
* Place .tsv.gz files in ~/Downloads/
* Create database tables (DDL below)
* Install dependencies: `pip install polars dbtk`

Database Schema
---------------
```sql
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
  category       text,
  job            text,
  characters     text[],
  PRIMARY KEY (tconst, ordering)
);

CREATE TABLE names_subset (
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
```

See Also
--------
- linked_spreadsheet.py: Uses this data to generate Excel reports with hyperlinks
- movie_list.sql: Query for Drama movies
- movie_principals.sql: Query for cast/crew with role filtering
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
    logger = logging.getLogger(__name__)
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
    # start timer
    st = time.monotonic()
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
        surge = BulkSurge(titles)
        surge.load(reader)
    # materialize a set that combines existing and added
    # the table was truncated earlier so there were no existing records
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
        surge = BulkSurge(principals)
        surge.load(reader)
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
        surge = BulkSurge(names)
        surge.load(reader)
    # ----------------------------------------------------------------------------------------------
    # Import new genres we collected.
    # ----------------------------------------------------------------------------------------------
    new_genre = [(val, val.replace('_', ' ').title()) for val in genre_collector.get_new_codes()]
    if new_genre:
        genre_insert = 'INSERT INTO genres (genre, title) VALUES (%s, %s)'
        cur.executemany(genre_insert, new_genre)
    db.commit()
    et = time.monotonic()
    print(f'Read through and filtered over 121 million rows, loaded into database in {et-st:.01f} seconds.')