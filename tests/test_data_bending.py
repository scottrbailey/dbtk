# tests/test_data_bending.py
"""
Integration tests for the complete data lifecycle.

Tests the entire dbtk stack: reading CSV files, transforming data, bulk loading,
querying, validation, lookups, and SQL file execution. Like a true data bender,
we demonstrate complete control over the flow of data.
"""

import pytest
from pathlib import Path

from dbtk.etl import Table, DataSurge
from dbtk.etl.transforms.database import TableLookup, Lookup, Validate

# SQL directory for test SQL files
SQL_DIR = Path(__file__).parent / 'sql'


class TestDataLoading:
    """Test the complete data loading lifecycle."""

    def test_states_loaded(self, states_db):
        """Verify all 50 states were loaded correctly."""
        cursor = states_db.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM states")
        assert cursor.fetchone()['cnt'] == 50

    def test_state_data_integrity(self, states_db):
        """Verify data was loaded with correct types and values."""
        cursor = states_db.cursor()
        cursor.execute("SELECT * FROM states WHERE code = 'CA'")
        ca = cursor.fetchone()

        assert ca['state'] == 'California'
        assert ca['capital'] == 'Sacramento'
        assert ca['population'] == 39538223
        assert ca['area_sq_mi'] == 155779
        assert ca['admitted'] == '1850-09-09'
        assert ca['sales_tax_rate'] == 0.0725
        assert ca['region'] == 'West'

    def test_null_handling(self, states_db):
        """Verify NULL values in admitted date for original 13 colonies."""
        cursor = states_db.cursor()
        cursor.execute("SELECT COUNT(*) as cnt FROM states WHERE admitted IS NULL")
        original_13 = cursor.fetchone()['cnt']

        # Original 13 colonies have NULL admitted dates
        assert original_13 == 13

        # Verify specific original colony
        cursor.execute("SELECT * FROM states WHERE code = 'VA'")
        va = cursor.fetchone()
        assert va['admitted'] is None


class TestTableLookup:
    """Test TableLookup class with real database."""

    def test_validator_mode_preloads_small_table(self, states_db):
        """Test that validator mode with preload works on small tables."""
        cursor = states_db.cursor()
        validator = TableLookup(
            cursor, 'valid_regions',
            key_cols='region_name',
            cache=TableLookup.CACHE_PRELOAD
        )

        # Should be preloaded (only 5 regions)
        assert validator._preloaded is True
        assert len(validator._cache) == 5

    def test_validator_mode_validates_correctly(self, states_db):
        """Test that validator mode correctly validates values."""
        cursor = states_db.cursor()
        validator = TableLookup(cursor, 'valid_regions', key_cols='region_name')

        # Valid regions return True (case handling depends on database)
        assert validator({'region_name': 'Northeast'}) is True
        assert validator({'region_name': 'West'}) is True
        assert validator({'region_name': 'Southeast'}) is True

        # Invalid regions return False
        assert validator({'region_name': 'Antarctica'}) is False
        assert validator({'region_name': 'Middle Earth'}) is False

    def test_validator_mode_handles_null_empty(self, states_db):
        """Test that validator mode returns False for None and empty strings."""
        cursor = states_db.cursor()
        validator = TableLookup(cursor, 'valid_regions', key_cols='region_name')

        assert validator({'region_name': None}) is False
        assert validator({'region_name': ''}) is False

    def test_lookup_preloads_small_table(self, states_db):
        """Test that lookup with preload works on small tables."""
        cursor = states_db.cursor()
        lookup = TableLookup(
            cursor, 'region_codes',
            key_cols='region',
            return_cols='code',
            cache=TableLookup.CACHE_PRELOAD
        )

        # Should be preloaded (only 5 regions)
        assert lookup._preloaded is True
        assert len(lookup._cache) == 5

    def test_lookup_translates_correctly(self, states_db):
        """Test that lookup correctly translates values."""
        cursor = states_db.cursor()
        lookup = TableLookup(
            cursor, 'region_codes',
            key_cols='region',
            return_cols='code'
        )

        assert lookup({'region': 'Northeast'}) == 'NE'
        assert lookup({'region': 'Southeast'}) == 'SE'
        assert lookup({'region': 'Midwest'}) == 'MW'
        assert lookup({'region': 'Southwest'}) == 'SW'
        assert lookup({'region': 'West'}) == 'W'

    def test_lookup_handles_missing_values(self, states_db):
        """Test lookup with missing values returns None."""
        cursor = states_db.cursor()
        lookup = TableLookup(
            cursor, 'region_codes',
            key_cols='region',
            return_cols='code'
        )

        assert lookup({'region': 'Invalid Region'}) is None
        assert lookup({'region': ''}) is None
        assert lookup({'region': None}) is None

    def test_lookup_reverse_mapping(self, states_db):
        """Test lookup in reverse direction (code to region)."""
        cursor = states_db.cursor()
        lookup = TableLookup(
            cursor, 'region_codes',
            key_cols='code',
            return_cols='region'
        )

        assert lookup({'code': 'NE'}) == 'Northeast'
        assert lookup({'code': 'SE'}) == 'Southeast'
        assert lookup({'code': 'MW'}) == 'Midwest'
        assert lookup({'code': 'SW'}) == 'Southwest'
        assert lookup({'code': 'W'}) == 'West'

    def test_cache_strategies(self, states_db):
        """Test different cache strategies."""
        cursor = states_db.cursor()

        # No cache
        lookup_none = TableLookup(
            cursor, 'region_codes',
            key_cols='region',
            return_cols='code',
            cache=TableLookup.CACHE_NONE
        )
        assert lookup_none({'region': 'West'}) == 'W'
        assert len(lookup_none._cache) == 0

        # Lazy cache (default)
        lookup_lazy = TableLookup(
            cursor, 'region_codes',
            key_cols='region',
            return_cols='code',
            cache=TableLookup.CACHE_LAZY
        )
        assert lookup_lazy({'region': 'West'}) == 'W'
        assert len(lookup_lazy._cache) == 1  # Only cached one lookup

        # Preload
        lookup_preload = TableLookup(
            cursor, 'region_codes',
            key_cols='region',
            return_cols='code',
            cache=TableLookup.CACHE_PRELOAD
        )
        assert len(lookup_preload._cache) == 5  # All preloaded
        assert lookup_preload({'region': 'West'}) == 'W'


class TestSQLFileExecution:
    """Test cursor.execute_file() and prepare_file() with real SQL files."""

    @pytest.fixture(autouse=True)
    def setup_sql_files(self):
        """Create SQL files for testing."""
        SQL_DIR.mkdir(exist_ok=True)

        # Query 1: Get states by region
        with open(SQL_DIR / 'get_states_by_region.sql', 'w') as f:
            f.write("""
                    SELECT state, code, capital, population
                    FROM states
                    WHERE region = :region
                    ORDER BY state
                    """)

        # Query 2: Insert state (for prepare_file testing)
        with open(SQL_DIR / 'insert_state.sql', 'w') as f:
            f.write("""
                    INSERT INTO states (state, code, capital, population, region)
                    VALUES (:state, :code, :capital, :population, :region)
                    """)

        # Query 3: Get state info
        with open(SQL_DIR / 'get_state_info.sql', 'w') as f:
            f.write("""
                    SELECT state, capital, population, admitted, sales_tax_rate, region
                    FROM states
                    WHERE code = :code
                    """)

        # Query 4: Count states by region with filter
        with open(SQL_DIR / 'count_states_by_region.sql', 'w') as f:
            f.write("""
                    SELECT region,
                           COUNT(*)        as state_count,
                           SUM(population) as total_population
                    FROM states
                    WHERE population > :min_population
                    GROUP BY region
                    ORDER BY total_population DESC
                    """)

        yield

        # Cleanup SQL files
        for sql_file in SQL_DIR.glob('*.sql'):
            sql_file.unlink()

    def test_execute_file_basic(self, states_db):
        """Test basic execute_file functionality."""
        cursor = states_db.cursor()
        cursor.execute_file(
            str(SQL_DIR / 'get_states_by_region.sql'),
            {'region': 'West'}
        )

        states = cursor.fetchall()
        assert len(states) > 0

        # Verify all returned states are in West region
        for state in states:
            cursor.execute("SELECT region FROM states WHERE code = ?", (state['code'],))
            assert cursor.fetchone()['region'] == 'West'

    def test_execute_file_with_results(self, states_db):
        """Test execute_file and verify specific results."""
        cursor = states_db.cursor()
        cursor.execute_file(
            str(SQL_DIR / 'get_state_info.sql'),
            {'code': 'TX'}
        )

        texas = cursor.fetchone()
        assert texas['state'] == 'Texas'
        assert texas['capital'] == 'Austin'
        assert texas['region'] == 'Southwest'
        assert texas['population'] == 29145505

    def test_prepare_file_single_execution(self, states_db):
        """Test prepare_file with single execution."""
        cursor = states_db.cursor()
        stmt = cursor.prepare_file(str(SQL_DIR / 'get_state_info.sql'))

        stmt.execute({'code': 'CA'})
        ca = stmt.fetchone()

        assert ca['state'] == 'California'
        assert ca['capital'] == 'Sacramento'

    def test_prepare_file_multiple_executions(self, states_db):
        """Test prepare_file with multiple executions."""
        cursor = states_db.cursor()
        stmt = cursor.prepare_file(str(SQL_DIR / 'get_state_info.sql'))

        test_states = ['CA', 'TX', 'NY', 'FL']
        results = {}

        for code in test_states:
            stmt.execute({'code': code})
            state_info = stmt.fetchone()
            results[code] = state_info['state']

        assert results['CA'] == 'California'
        assert results['TX'] == 'Texas'
        assert results['NY'] == 'New York'
        assert results['FL'] == 'Florida'

    def test_prepare_file_acts_like_cursor(self, states_db):
        """Test that PreparedStatement can be iterated like a cursor."""
        cursor = states_db.cursor()
        stmt = cursor.prepare_file(str(SQL_DIR / 'get_states_by_region.sql'))

        stmt.execute({'region': 'Northeast'})

        # Iterate over results
        northeast_states = []
        for state in stmt.fetchall():
            northeast_states.append(state['code'])

        assert len(northeast_states) > 0
        assert 'MA' in northeast_states
        assert 'NY' in northeast_states


class TestDatabaseQueries:
    """Test various database queries and aggregations."""

    def test_aggregate_by_region(self, states_db):
        """Test aggregation queries by region."""
        cursor = states_db.cursor()
        cursor.execute("""
                       SELECT region,
                              COUNT(*)            as state_count,
                              SUM(population)     as total_population,
                              AVG(area_sq_mi)     as avg_area,
                              AVG(sales_tax_rate) as avg_tax_rate
                       FROM states
                       GROUP BY region
                       ORDER BY total_population DESC
                       """)

        regions = cursor.fetchall()

        # Should have 5 regions
        assert len(regions) == 5

        # Southeast is the largest region
        assert regions[0]['region'] == 'Southeast'
        assert regions[0]['total_population'] > 80000000
        assert regions[0]['state_count'] == 12

    def test_filter_by_population(self, states_db):
        """Test filtering states by population."""
        cursor = states_db.cursor()
        cursor.execute("""
                       SELECT state, population
                       FROM states
                       WHERE population > 10000000
                       ORDER BY population DESC
                       """)

        large_states = cursor.fetchall()

        # California should be first
        assert large_states[0]['state'] == 'California'

        # All returned states should have population > 10M
        for state in large_states:
            assert state['population'] > 10000000

    def test_states_without_sales_tax(self, states_db):
        """Test finding states with no sales tax."""
        cursor = states_db.cursor()
        cursor.execute("""
                       SELECT state, code
                       FROM states
                       WHERE sales_tax_rate = 0
                          OR sales_tax_rate IS NULL
                       ORDER BY state
                       """)

        no_tax_states = cursor.fetchall()
        codes = [s['code'] for s in no_tax_states]

        # Should include AK, DE, MT, NH, OR
        assert 'AK' in codes
        assert 'DE' in codes
        assert 'MT' in codes
        assert 'NH' in codes
        assert 'OR' in codes

    def test_original_colonies_vs_newer_states(self, states_db):
        """Test comparing original 13 colonies to newer states."""
        cursor = states_db.cursor()

        # Original colonies (NULL admitted date)
        cursor.execute("""
                       SELECT COUNT(*) as cnt, AVG(population) as avg_pop
                       FROM states
                       WHERE admitted IS NULL
                       """)
        original = cursor.fetchone()

        # Newer states
        cursor.execute("""
                       SELECT COUNT(*) as cnt, AVG(population) as avg_pop
                       FROM states
                       WHERE admitted IS NOT NULL
                       """)
        newer = cursor.fetchone()

        assert original['cnt'] == 13
        assert newer['cnt'] == 37

        # Both groups should have reasonable populations
        assert original['avg_pop'] > 0
        assert newer['avg_pop'] > 0


class TestETLTransformations:
    """Test ETL transformations using Table class."""

    def test_table_with_validator(self, states_db):
        """Test Table with TableLookup in validator mode."""
        cursor = states_db.cursor()

        # Use Lookup with return_cols to return validated value
        region_validator = Lookup('valid_regions',
            key_cols='region_name',
            return_cols='region_name'
        )

        # Create temporary test table
        cursor.execute("DROP TABLE IF EXISTS test_cities")
        cursor.execute("""
                       CREATE TABLE test_cities
                       (
                           city       TEXT PRIMARY KEY,
                           state_code TEXT,
                           region     TEXT
                       )
                       """)

        city_table = Table('test_cities', {
            'city': {'field': 'name', 'primary_key': True},
            'state_code': {'field': 'state'},
            'region': {'field': 'region', 'fn': region_validator, 'nullable': False}
        }, cursor=cursor)

        # Valid region
        city_table.set_values({'name': 'Boston', 'state': 'MA', 'region': 'Northeast'})
        assert city_table.reqs_met
        assert city_table.values['region'] == 'Northeast'

        # Invalid region - should fail reqs_met
        city_table.set_values({'name': 'Atlantis', 'state': 'XX', 'region': 'Ocean'})
        assert not city_table.reqs_met
        assert 'region' in city_table.reqs_missing

    def test_table_with_lookup(self, states_db):
        """Test Table with TableLookup transformation."""
        cursor = states_db.cursor()

        region_to_code = Lookup('region_codes', 'region', 'code')

        # Create temporary test table
        cursor.execute("DROP TABLE IF EXISTS test_locations")
        cursor.execute("""
                       CREATE TABLE test_locations
                       (
                           location    TEXT PRIMARY KEY,
                           region_code TEXT
                       )
                       """)

        location_table = Table('test_locations', {
            'location': {'field': 'place', 'primary_key': True},
            'region_code': {'field': 'region', 'fn': region_to_code}
        }, cursor=cursor)

        # Test transformation
        location_table.set_values({'place': 'Fire Island', 'region': 'Northeast'})
        assert location_table.values['region_code'] == 'NE'

        location_table.set_values({'place': 'Yellowstone', 'region': 'West'})
        assert location_table.values['region_code'] == 'W'


class TestCompleteDataCycle:
    """Test a complete data processing cycle."""

    def test_read_transform_bulk_load_query(self, states_db):
        """Test the complete cycle: read CSV -> transform -> bulk load -> query."""
        cursor = states_db.cursor()

        # Create a new table for state summaries
        cursor.execute("DROP TABLE IF EXISTS state_summaries")
        cursor.execute("""
                       CREATE TABLE state_summaries
                       (
                           code         TEXT PRIMARY KEY,
                           state_name   TEXT NOT NULL,
                           region_code  TEXT,
                           large_state  INTEGER,
                           created_date TEXT
                       )
                       """)

        # Set up transformations
        region_lookup = Lookup('region_codes', key_cols='region', return_cols='code')

        def mark_large_state(population):
            """Mark states with >10M population as large."""
            if population and int(population) > 10000000:
                return 1
            return 0

        # Define ETL table with transformations
        summary_table = Table('state_summaries', {
            'code': {'field': 'code', 'primary_key': True},
            'state_name': {'field': 'state', 'nullable': False},
            'region_code': {'field': 'region', 'fn': region_lookup},
            'large_state': {'field': 'population', 'fn': mark_large_state},
            'created_date': {'db_expr': 'date("now")'}
        }, cursor=cursor)

        # Use DataSurge for bulk loading
        surge = DataSurge(summary_table, batch_size=10)

        # Read from existing states table (simulating CSV read)
        cursor.execute("SELECT * FROM states")
        source_data = cursor.fetchall()

        # Bulk insert transformed data
        errors = surge.insert(source_data)
        states_db.commit()

        assert errors == 0
        assert summary_table.counts['insert'] == 50

        # Query and verify transformations worked
        cursor.execute("""
                       SELECT COUNT(*) as cnt
                       FROM state_summaries
                       WHERE large_state = 1
                       """)
        large_count = cursor.fetchone()['cnt']
        assert large_count > 0

        # Verify region codes were translated
        cursor.execute("""
                       SELECT code, region_code
                       FROM state_summaries
                       WHERE code = 'CA'
                       """)
        ca_summary = cursor.fetchone()
        assert ca_summary['region_code'] == 'W'  # California is in West -> W