"""
LinkedExcelWriter Example: Multi-Sheet Excel Report with Hyperlinks

This example demonstrates creating a navigable multi-sheet Excel workbook with both
internal (cross-sheet) and external (web) hyperlinks using LinkedExcelWriter.

Features Demonstrated
---------------------
* **Multiple sheets**: Movies, Cast, and Crew tabs
* **External hyperlinks**: Clickable links to IMDB movie and person pages
* **Internal hyperlinks**: Cross-sheet references from Cast/Crew back to Movies tab
* **external_only LinkSource**: Reusable LinkSource across Cast and Crew sheets
* **Link modes**: Default external links and explicit :internal mode
* **PreparedStatement**: Reusable parameterized queries
* **Batch writing**: Efficient multi-sheet report generation

LinkSource Types Used
---------------------
1. **title_links** (standard LinkSource):
   - Caches movie records from Movies sheet
   - Creates external links to IMDB on Movies sheet
   - Creates internal links from Cast/Crew sheets back to Movies sheet
   - Requires source_sheet and key_column for cross-sheet linking

2. **name_links** (external_only LinkSource):
   - Creates external links to IMDB person pages
   - Reused on both Cast and Crew sheets (no caching needed)
   - No source_sheet or key_column required
   - Memory efficient for large datasets

Link Specifications
-------------------
- `'primary_title': 'titles'` - External link to IMDB movie page
- `'name': 'names'` - External link to IMDB person page (reusable)
- `'movie_1': 'titles:internal'` - Internal link to Movies sheet row

Output
------
Generates examples/output/linked_spreadsheet.xlsx with:
- Movies sheet: Movie listings with external IMDB links
- Cast sheet: Actors/actresses with name links to IMDB + movie links to Movies sheet
- Crew sheet: Other crew with name links to IMDB + movie links to Movies sheet

Prerequisites
-------------
- IMDB sample data loaded via examples/bulk_load_imdb_subset_pg.py or data_load_imdb_subset.py
- Database connection configured in dbtk config

See Also
--------
- movie_list.sql: Query for Drama movies
- movie_principals.sql: Query for cast/crew with role filtering
"""

import dbtk
from pathlib import Path


if __name__ == '__main__':
    dbtk.setup_logging()
    db = dbtk.connect('imdb')
    cur = db.cursor()
    output_path = Path.cwd()  /  'output'
    output_path.mkdir(parents=True, exist_ok=True)
    query_path = Path.cwd()
    with dbtk.writers.LinkedExcelWriter(str(output_path / 'IMDB_Linked.xlsx')) as writer:
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

        # Get list of movies from titles_subset (loaded in examples/bulk_load_imdb_subset_pg.py)
        cur.execute_file(query_path / 'movie_list.sql', {'genre': 'Drama'})
        writer.write_batch(cur, 'Movies',
                           links={'Primary Title': 'titles'})

        # Create a PreparedStatement from a query file
        principal_stmt = cur.prepare_file(query_path / 'movie_principals.sql')
        principal_stmt.execute({'genre': 'Drama', 'incl_roles': ['actor', 'actress'],
                                'excl_roles': None})
        writer.write_batch(cur, 'Cast',
                           links={'Name': 'names',
                                  'Movie 1': 'titles:internal',
                                  'Movie 2': 'titles:internal',
                                  'Movie 3': 'titles:internal',
                                  'Movie 4': 'titles:internal'})
        principal_stmt.execute({'genre': 'Drama', 'incl_roles': None,
                                'excl_roles': ['actor', 'actress', 'director', 'producer']})
        writer.write_batch(cur, 'Crew',
                           links={'Name': 'names',
                                  'Movie 1': 'titles:internal',
                                  'Movie 2': 'titles:internal',
                                  'Movie 3': 'titles:internal',
                                  'Movie 4': 'titles:internal'})
