import dbtk
from pathlib import Path

if __name__ == '__main__':
    dbtk.setup_logging()
    db = dbtk.connect('imdb')
    cur = db.cursor()
    output_path = Path.cwd()  /  'output'
    output_path.mkdir(parents=True, exist_ok=True)
    query_path = Path.cwd()
    writer = dbtk.writers.LinkedExcelWriter(str(output_path / 'linked_spreadsheet.xlsx'))
    #writer = dbtk.writers.ExcelWriter(fp)
    movie_links = dbtk.writers.LinkSource('titles',
                                          source_sheet='Movies',
                                          key_column='tconst',
                                          url_template='https://www.imdb.com/title/{tconst}',
                                          text_template='{primary_title} ({start_year})')
    actor_links = dbtk.writers.LinkSource('names',
                                          source_sheet='Cast',
                                          key_column='nconst',
                                          url_template='https://www.imdb.com/name/{nconst}',
                                          text_template='{name}')
    writer.register_link_source(movie_links)
    #writer.register_link_source(actor_links)

    # Get list of movies from titles_subset (loaded in examples/bulk_load_imdb_subset.py)
    params = {'genre': 'Drama'}
    cur.execute_file(query_path / 'movie_list.sql', params)
    writer.write_batch(cur, 'Movies'
                       , links={'primary_title': 'titles'}
    )

    cur.execute_file(query_path / 'movie_cast.sql', params)
    writer.write_batch(cur, 'Cast',
                       links={'movie_1_id': 'titles:internal'}
                       )

    writer.close()


