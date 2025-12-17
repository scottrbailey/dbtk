import dbtk
from pathlib import Path
TEST_DB_PATH = Path(__file__).parent / 'test_states.db'
QUERY_PATH = Path(__file__).parent / 'fixtures' / 'sql' / 'states_by_region.sql'

dbtk.setup_logging()

db = dbtk.database.sqlite(TEST_DB_PATH)
cur = db.cursor('record')

cur.execute('SELECT * FROM region_codes ORDER BY region')
regions = cur.fetchall()

prepared_query = cur.prepare_file(QUERY_PATH)
for region in regions:
    prepared_query.execute(region)
    # Generates an Excel file with tabs for each region
    dbtk.writers.to_excel(prepared_query, 'StatesByRegion.xlsx', sheet=region['region'])
