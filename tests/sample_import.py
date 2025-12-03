import dbtk

"""
Sample inbound integration.

--Table create script (postgres):
CREATE TABLE etl.names_subset (
    nconst varchar(10) PRIMARY KEY,
    primary_name varchar(100) NOT NULL,
    birth_year int,
    death_year int,
    primary_profession varchar(100)[],
    known_for_titles varchar(100)[]
);
"""

def wrap_array(val):
    return f"{{{val}}}" if val else None

if __name__ == '__main__':
    # Logging & connection configuration in ~/.config/dbtk.yml
    dbtk.setup_logging()
    # Connecting to Postgres database... also running on my laptop!
    db = dbtk.connect('imdb')
    # Psycopg2's extras/execute_batch will be called instead of executemany
    cur = db.cursor()
    # Cleanup
    cur.execute('TRUNCATE TABLE etl.names_subset')
    # First 1,000,000 rows of IMDB's name dataset
    fn = r'c:\Temp\name.subset.tsv.gz'
    # Compressed archive automatically detected and streamed
    reader = dbtk.readers.get_reader(fn, delimiter='\t')
    # Maps reader column names to table columns and applies transforms
    names_cols = {
        'nconst': {'field': 'nconst', 'primary_key': True},
        'primary_name': {'field': 'primaryname', 'nullable': False, 'fn': 'maxlen:100'},
        'birth_year': {'field': 'birthyear', 'fn': 'int'},
        'death_year': {'field': 'deathyear', 'fn': 'int'},
        'primary_profession': {'field': 'primaryprofession', 'fn': wrap_array},
        'known_for_titles': {'field': 'knownfortitles', 'fn': wrap_array},
    }
    # Table does the heavy lifting, generating SQL, mapping bind parameters, applying transforms, etc.
    table = dbtk.etl.Table('etl.names_subset', names_cols, cursor=cur)
    # DataSurge handles batching and committing
    names_loader = dbtk.etl.DataSurge(table)
    # Iterate reader and load data
    names_loader.insert(reader)
    db.close()
