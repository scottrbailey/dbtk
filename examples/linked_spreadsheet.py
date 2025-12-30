import dbtk
from pathlib import Path

"""

"""
if __name__ == '__main__':
    dbtk.setup_logging()
    db = dbtk.connect('imdb')
    cur = db.cursor()
    output_path = Path.cwd()  /  'output'
    output_path.mkdir(parents=True, exist_ok=True)
    query_path = Path.cwd()
    with dbtk.writers.LinkedExcelWriter(str(output_path / 'linked_spreadsheet.xlsx')) as writer:
        # title_links will be used both to create external links on the "Movies" tab
        # and create internal links from "Cast" and "Crew" tabs to that movie on the "Movies" tab
        title_links = dbtk.writers.LinkSource('titles',
                                              source_sheet='Movies',
                                              key_column='tconst',
                                              url_template='https://www.imdb.com/title/{tconst}',
                                              text_template='{primary_title} ({start_year})')
        writer.register_link_source(title_links)

        # name_links will be used to create (only) external links from the "Cast" and "Crew" tabs to that person
        # on the IMDB site. Because it is external_only, `source_sheet` and `key_column` are not used and the location
        # where the source record was written will not be tracked.
        name_links = dbtk.writers.LinkSource('names',
                                              url_template='https://www.imdb.com/name/{nconst}',
                                              text_template='{name}',
                                              external_only=True)
        writer.register_link_source(name_links)

        # Get list of movies from titles_subset (loaded in examples/bulk_load_imdb_subset.py)
        cur.execute_file(query_path / 'movie_list.sql', {'genre': 'Drama'})
        writer.write_batch(cur, 'Movies',
                           links={'primary_title': 'titles'})

        # Create a PreparedStatement from a query file
        principal_stmt = cur.prepare_file(query_path / 'movie_principals.sql')
        principal_stmt.execute({'genre': 'Drama', 'incl_roles': ['actor', 'actress'],
                                'excl_roles': None})
        writer.write_batch(cur, 'Cast',
                           links={'name': 'names',
                                  'movie_1': 'titles:internal',
                                  'movie_2': 'titles:internal',
                                  'movie_3': 'titles:internal',
                                  'movie_4': 'titles:internal'})
        principal_stmt.execute({'genre': 'Drama', 'incl_roles': None,
                                'excl_roles': ['actor', 'actress', 'director', 'producer']})
        writer.write_batch(cur, 'Crew',
                           links={'name': 'names',
                                  'movie_1': 'titles:internal',
                                  'movie_2': 'titles:internal',
                                  'movie_3': 'titles:internal',
                                  'movie_4': 'titles:internal'})
