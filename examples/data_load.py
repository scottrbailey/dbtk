import dbtk
import polars as pl
from dbtk.etl.manager import ValidationCollector
from dbtk.etl.transforms import TableLookup
from dbtk.readers import DataFrameReader

"""
Loads the first 100,000 rows of name.basics.tsv.gz from the IMDB dataset into a Postgres database.
The name.basics.tsv dataset has over 14M records and is over 280MB compressed.

The IMDB dataset can be found at https://developer.imdb.com/non-commercial-datasets/

CREATE TABLE names_subset (
    nconst varchar(10) PRIMARY KEY,
    primary_name varchar(100) NOT NULL,
    birth_year int,
    death_year int,
    primary_profession varchar(30)[],
    known_for_titles varchar(10)[]
);
CREATE TABLE professions (
   profession  VARCHAR(30) PRIMARY KEY,
   title       VARCHAR(30)
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
    cur.execute('TRUNCATE table public.names_subset')
    # load existing professions validation
    prof_lookup = TableLookup(cur,
                              table='professions',
                              key_cols='profession',
                              return_cols=['profession', 'title'],
                              cache=TableLookup.CACHE_PRELOAD)
    # keep track of new professions
    prof_collector = ValidationCollector(lookup=prof_lookup, desc_field='title', return_desc=False)
    # Use polars ridiculously fast... but lazy csv reader
    df = pl.scan_csv(r'c:\Temp\name.basics.tsv.gz', separator="\t", n_rows=100_000).collect()
    reader = DataFrameReader(df, add_row_num=False)
    names_cols = {
        'nconst': {'field': 'nconst', 'primary_key': True},
        'primary_name': {'field': 'primaryName', 'nullable': False, 'fn': 'maxlen:100'},
        'birth_year': {'field': 'birthYear', 'fn': 'int'},
        'death_year': {'field': 'deathYear', 'fn': 'int'},
        'primary_profession': {'field': 'primaryProfession', 'fn': ['split:,', prof_collector, wrap_array]},
        'known_for_titles': {'field': 'knownForTitles', 'fn': wrap_array},
    }
    names = dbtk.etl.Table('names_subset', names_cols, cursor=cur)
    with dbtk.etl.DataSurge(names) as names_loader:
        names_loader.insert(reader)
    prof_insert = 'INSERT INTO professions (profession, title) VALUES (%s, %s)'
    new_professions = [(val, val.replace('_', ' ').title()) for val in prof_collector.get_new_codes()]
    if new_professions:
        cur.executemany(prof_insert, new_professions)
        db.commit()
