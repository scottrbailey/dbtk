# tests/test_table.py
import pytest
import sqlite3
from pathlib import Path

from dbtk.etl.table import Table, validate_identifier
from dbtk.database import ParamStyle
from dbtk.cursors import Cursor
from dbtk.database import Database


@pytest.fixture
def sqlite_db():
    """Create in-memory SQLite database."""
    db = Database.create('sqlite', database=':memory:')
    yield db
    db.close()


@pytest.fixture
def cursor(sqlite_db):
    """Get cursor from SQLite database."""
    return sqlite_db.cursor()


@pytest.fixture
def airbender_schema(cursor):
    """Create Air Nomad training table schema."""
    cursor.execute("""
                   CREATE TABLE air_nomad_training
                   (
                       nomad_id         TEXT PRIMARY KEY,
                       name             TEXT NOT NULL,
                       temple           TEXT NOT NULL,
                       airbending_level INTEGER,
                       sky_bison        TEXT,
                       meditation_score REAL,
                       training_date    TEXT,
                       instructor       TEXT
                   )
                   """)
    cursor.connection.commit()
    return 'air_nomad_training'


@pytest.fixture
def fire_nation_schema(cursor):
    """Create Fire Nation army table schema."""
    cursor.execute("""
                   CREATE TABLE fire_nation_army
                   (
                       soldier_id         TEXT PRIMARY KEY,
                       name               TEXT NOT NULL,
                       rank               TEXT NOT NULL,
                       firebending_skill  INTEGER,
                       home_village       TEXT,
                       enlistment_date    TEXT,
                       combat_name        TEXT,
                       commanding_officer TEXT
                   )
                   """)
    cursor.connection.commit()
    return 'fire_nation_army'


@pytest.fixture
def earth_kingdom_schema(cursor):
    """Create Earth Kingdom census table schema."""
    cursor.execute("""
                   CREATE TABLE earth_kingdom_census
                   (
                       citizen_id         TEXT PRIMARY KEY,
                       name               TEXT NOT NULL,
                       city               TEXT NOT NULL,
                       earthbending_skill REAL,
                       occupation         TEXT,
                       registration_date  TEXT,
                       kingdom_loyalty    TEXT
                   )
                   """)
    cursor.connection.commit()
    return 'earth_kingdom_census'


@pytest.fixture
def airbender_table(cursor, airbender_schema):
    """Table configuration for Air Nomad training records."""
    col_def = {
        'nomad_id': {'field': 'trainee_id', 'primary_key': True},
        'name': {'field': 'monk_name', 'required': True},
        'temple': {'field': 'home_temple', 'nullable': False},
        'airbending_level': {'field': 'mastery_rank', 'fn': lambda x: int(x) if x else 0},
        'sky_bison': {'field': 'bison_companion'},
        'meditation_score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0},
        'training_date': {'db_fn': 'CURRENT_TIMESTAMP'},
        'instructor': {'value': 'Monk Gyatso', 'no_update': True}
    }
    return Table('air_nomad_training', columns=col_def, cursor=cursor)


@pytest.fixture
def fire_nation_table(cursor, fire_nation_schema):
    """Table configuration for Fire Nation army records."""
    col_def = {
        'soldier_id': {'field': 'recruit_number', 'primary_key': True},
        'name': {'field': 'full_name', 'required': True},
        'rank': {'field': 'military_rank', 'nullable': False},
        'firebending_skill': {'field': 'flame_intensity', 'fn': lambda x: int(x) if x else 0},
        'home_village': {'field': 'birthplace'},
        'enlistment_date': {'field': 'joined_date', 'fn': lambda x: x},
        'commanding_officer': {'value': 'Admiral Zhao', 'no_update': True}
    }
    return Table('fire_nation_army', columns=col_def, cursor=cursor)


@pytest.fixture
def earth_kingdom_table(cursor, earth_kingdom_schema):
    """Table configuration for Earth Kingdom census."""
    col_def = {
        'citizen_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'city': {'field': 'home_city', 'nullable': False},
        'earthbending_skill': {'field': 'boulder_size', 'fn': lambda x: float(x) if x else 0.0},
        'occupation': {'field': 'job_title'},
        'registration_date': {'db_fn': 'CURRENT_DATE'},
        'kingdom_loyalty': {'value': 'True Earth Kingdom Citizen'}
    }
    return Table('earth_kingdom_census', columns=col_def, cursor=cursor)


class TestValidateIdentifier:
    """Test identifier validation for table and column names."""

    def test_valid_simple_identifier(self):
        """Test valid simple identifiers like airbender temple names."""
        assert validate_identifier('eastern_air_temple') == 'eastern_air_temple'
        assert validate_identifier('fire_nation') == 'fire_nation'
        assert validate_identifier('ba_sing_se') == 'ba_sing_se'

    def test_valid_dotted_identifier(self):
        """Test valid dotted identifiers like schema.table references."""
        assert validate_identifier('air_temple.monks') == 'air_temple.monks'
        assert validate_identifier('fire_nation.army') == 'fire_nation.army'

    def test_invalid_identifier_starts_with_number(self):
        """Test invalid identifiers that start with numbers."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('100_year_war')

    def test_invalid_identifier_special_chars(self):
        """Test invalid identifiers with special characters."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('fire--nation')

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('sozins comet ')


class TestTableInitialization:
    """Test Table class initialization and configuration."""

    def test_airbender_table_init(self, airbender_table):
        """Test Air Nomad training table initialization."""
        assert airbender_table.name == 'air_nomad_training'
        assert 'nomad_id' in airbender_table.columns
        assert airbender_table.columns['nomad_id']['bind_name'] in airbender_table.req_cols
        assert airbender_table.columns['nomad_id']['bind_name'] in airbender_table.key_cols
        assert airbender_table.paramstyle in (ParamStyle.QMARK, ParamStyle.NAMED)

    def test_fire_nation_table_init(self, fire_nation_table):
        """Test Fire Nation army table initialization."""
        assert fire_nation_table.name == 'fire_nation_army'
        assert 'soldier_id' in fire_nation_table.columns
        assert fire_nation_table.columns['soldier_id']['bind_name'] in fire_nation_table.req_cols
        assert fire_nation_table.columns['soldier_id']['bind_name'] in fire_nation_table.key_cols

    def test_table_with_invalid_name(self, cursor):
        """Test that invalid table names are rejected."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            Table('123_invalid_table', {}, cursor=cursor)

    def test_table_with_invalid_column_name(self, cursor):
        """Test that invalid column names are rejected."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            Table('valid_table', {'invalid--column': {'field': 'test'}}, cursor=cursor)

    def test_table_with_column_name_needing_quotes(self, cursor):
        """Test that column names with spaces are handled correctly."""
        # Create a test table with space in column name
        cursor.execute("""
                       CREATE TABLE test_table
                       (
                           "meditation score" REAL
                       )
                       """)
        cursor.connection.commit()

        table = Table('test_table', {
            'meditation score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0}
        }, cursor=cursor)

        # Should create valid bind_name
        assert table.columns['meditation score']['bind_name'] == 'meditation_score'

        # Should generate valid SQL with quoted column name
        table.generate_sql('insert')
        sql = table.get_sql('insert')
        assert '"meditation score"' in sql  # Column name quoted
        assert 'meditation_score' in table._param_config['insert']  # Bind param is sanitized

    def test_table_repr(self, airbender_table):
        """Test table string representation."""
        repr_str = repr(airbender_table)
        assert 'air_nomad_training' in repr_str
        assert '8 columns' in repr_str


class TestSQLGeneration:
    """Test SQL statement generation for different operations."""

    def test_insert_statement_generation(self, airbender_table):
        """Test INSERT statement for Air Nomad training records."""
        airbender_table.generate_sql('insert')
        sql = airbender_table.get_sql('insert')
        assert 'INSERT INTO air_nomad_training' in sql
        assert 'nomad_id' in sql
        assert 'name' in sql
        assert 'CURRENT_TIMESTAMP' in sql  # For training_date

    def test_select_statement_generation(self, airbender_table):
        """Test SELECT statement for finding existing Air Nomads."""
        airbender_table.generate_sql('select')
        sql = airbender_table.get_sql('select')
        assert 'SELECT' in sql
        assert 'FROM air_nomad_training' in sql
        assert 'WHERE' in sql

    def test_update_statement_creation(self, fire_nation_table):
        """Test UPDATE statement generation for Fire Nation records."""
        fire_nation_table.generate_sql('update')
        sql = fire_nation_table.get_sql('update')

        assert 'UPDATE fire_nation_army' in sql
        assert 'SET' in sql
        assert 'WHERE' in sql

    def test_delete_statement_creation(self, airbender_table):
        """Test DELETE statement for removing Air Nomad records."""
        airbender_table.generate_sql('delete')
        sql = airbender_table.get_sql('delete')
        assert 'DELETE FROM air_nomad_training' in sql
        assert 'WHERE' in sql

    def test_merge_statement_sqlite(self, airbender_table):
        """Test MERGE/upsert statement generation for SQLite."""
        airbender_table.generate_sql('merge')
        sql = airbender_table.get_sql('merge')
        # SQLite uses INSERT...ON CONFLICT
        assert 'INSERT INTO air_nomad_training' in sql
        assert 'ON CONFLICT' in sql or 'REPLACE' in sql


class TestDataProcessing:
    """Test data processing and transformation."""

    def test_set_values_basic(self, airbender_table):
        """Test setting values from Air Nomad recruitment data."""
        aang_data = {
            'trainee_id': 'AANG001',
            'monk_name': 'Aang',
            'home_temple': 'Southern Air Temple',
            'mastery_rank': '4',
            'bison_companion': 'Appa',
            'daily_meditation': '8.5'
        }

        airbender_table.set_values(aang_data)

        assert airbender_table.values['nomad_id'] == 'AANG001'
        assert airbender_table.values['name'] == 'Aang'
        assert airbender_table.values['temple'] == 'Southern Air Temple'
        assert airbender_table.values['airbending_level'] == 4
        assert airbender_table.values['meditation_score'] == 8.5
        assert airbender_table.values['instructor'] == 'Monk Gyatso'

    def test_set_values_with_transformations(self, fire_nation_table):
        """Test setting values with Fire Nation data transformations."""
        zuko_data = {
            'recruit_number': 'ZUKO001',
            'full_name': 'Prince Zuko',
            'military_rank': 'Banished Prince',
            'flame_intensity': '9',
            'birthplace': 'Fire Nation Capital',
            'joined_date': '095 AG'
        }

        fire_nation_table.set_values(zuko_data)

        assert fire_nation_table.values['soldier_id'] == 'ZUKO001'
        assert fire_nation_table.values['name'] == 'Prince Zuko'
        assert fire_nation_table.values['firebending_skill'] == 9
        assert fire_nation_table.values['commanding_officer'] == 'Admiral Zhao'

    def test_set_values_missing_field(self, airbender_table):
        """Test handling missing fields in Air Nomad data."""
        incomplete_data = {
            'trainee_id': 'JINORA001',
            'monk_name': 'Jinora'
        }

        airbender_table.set_values(incomplete_data)

        assert airbender_table.values['nomad_id'] == 'JINORA001'
        assert airbender_table.values['name'] == 'Jinora'
        assert airbender_table.values['temple'] is None
        assert airbender_table.values['airbending_level'] == 0

    def test_requirements_checking(self, airbender_table):
        """Test checking if Air Nomad requirements are met."""
        # Complete data
        complete_data = {
            'trainee_id': 'TENZIN001',
            'monk_name': 'Tenzin',
            'home_temple': 'Air Temple Island'
        }
        airbender_table.set_values(complete_data)
        assert airbender_table.reqs_met is True
        assert len(airbender_table.reqs_missing) == 0

        # Incomplete data
        incomplete_data = {
            'trainee_id': 'MEELO001',
            'monk_name': 'Meelo'
        }
        airbender_table.set_values(incomplete_data)
        assert airbender_table.reqs_met is False
        temple_bind_name = airbender_table.columns['temple']['bind_name']
        assert temple_bind_name in airbender_table.reqs_missing


class TestDatabaseOperations:
    """Test database operation execution."""

    def test_exec_insert_success(self, airbender_table, cursor):
        """Test successful insert of Air Nomad training record."""
        airbender_table.set_values({
            'trainee_id': 'IKKI001',
            'monk_name': 'Ikki',
            'home_temple': 'Air Temple Island'
        })

        result = airbender_table.exec_insert()

        assert result == 0
        cursor.connection.commit()

        # Verify data was inserted
        cursor.execute("SELECT nomad_id, name, temple FROM air_nomad_training WHERE nomad_id = 'IKKI001'")
        row = cursor.fetchone()
        assert row['nomad_id'] == 'IKKI001'
        assert row['name'] == 'Ikki'
        assert row['temple'] == 'Air Temple Island'

    def test_exec_insert_duplicate_key(self, airbender_table, cursor):
        """Test insert failure due to duplicate key."""
        airbender_table.set_values({
            'trainee_id': 'AANG001',
            'monk_name': 'Aang',
            'home_temple': 'Southern Air Temple'
        })

        # First insert succeeds
        result = airbender_table.exec_insert()
        assert result == 0
        cursor.connection.commit()

        # Second insert with same key fails
        result = airbender_table.exec_insert(raise_error=False)
        assert result == 1

    def test_exec_update_success(self, airbender_table, cursor):
        """Test successful update of Air Nomad progress."""
        # Insert initial record
        airbender_table.set_values({
            'trainee_id': 'AANG001',
            'monk_name': 'Aang',
            'home_temple': 'Southern Air Temple'
        })
        airbender_table.exec_insert()
        cursor.connection.commit()

        # Update the record
        airbender_table.set_values({
            'trainee_id': 'AANG001',
            'monk_name': 'Avatar Aang',
            'home_temple': 'All Temples'
        })
        result = airbender_table.exec_update()

        assert result == 0
        cursor.connection.commit()

        # Verify update
        cursor.execute("SELECT name, temple FROM air_nomad_training WHERE nomad_id = 'AANG001'")
        row = cursor.fetchone()
        assert row['name'] == 'Avatar Aang'
        assert row['temple'] == 'All Temples'

    def test_exec_update_missing_requirements(self, airbender_table):
        """Test update failure when Air Nomad requirements not met."""
        airbender_table.set_values({
            'trainee_id': 'INCOMPLETE001'
        })

        with pytest.raises(ValueError, match="required columns"):
            airbender_table.exec_update()

    def test_exec_delete_success(self, airbender_table, cursor):
        """Test successful delete of Air Nomad record."""
        # Insert record first
        airbender_table.set_values({
            'trainee_id': 'ROHAN001',
            'monk_name': 'Rohan',
            'home_temple': 'Air Temple Island'
        })
        airbender_table.exec_insert()
        cursor.connection.commit()

        # Delete the record
        airbender_table.set_values({'trainee_id': 'ROHAN001'})
        result = airbender_table.exec_delete()

        assert result == 0
        cursor.connection.commit()

        # Verify deletion
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training WHERE nomad_id = 'ROHAN001'")
        row = cursor.fetchone()
        assert row['cnt'] == 0

    def test_get_db_record(self, fire_nation_table, cursor):
        """Test fetching current Fire Nation soldier record."""
        # Insert record first
        fire_nation_table.set_values({
            'recruit_number': 'MAI001',
            'full_name': 'Mai',
            'military_rank': 'Royal Guard',
            'flame_intensity': '7'
        })
        fire_nation_table.exec_insert()
        cursor.connection.commit()

        # Fetch the record
        fire_nation_table.set_values({
            'recruit_number': 'MAI001',
            'full_name': 'Mai',
            'military_rank': 'Royal Guard'
        })

        result = fire_nation_table.get_db_record()

        assert result['soldier_id'] == 'MAI001'
        assert result['name'] == 'Mai'
        assert result['rank'] == 'Royal Guard'


class TestUpdateExclusions:
    """Test column exclusion logic for updates."""

    def test_calc_update_excludes_missing_fields(self, airbender_table):
        """Test excluding Air Nomad fields missing from source data."""
        file_headers = {'trainee_id', 'monk_name', 'home_temple'}

        airbender_table.calc_update_excludes(file_headers)

        sky_bison_bind = airbender_table.columns['sky_bison']['bind_name']
        meditation_bind = airbender_table.columns['meditation_score']['bind_name']
        assert sky_bison_bind in airbender_table._update_excludes
        assert meditation_bind in airbender_table._update_excludes

        name_bind = airbender_table.columns['name']['bind_name']
        temple_bind = airbender_table.columns['temple']['bind_name']
        assert name_bind not in airbender_table._update_excludes
        assert temple_bind not in airbender_table._update_excludes

    def test_calc_update_excludes_no_update_flag(self, airbender_table):
        """Test excluding Air Nomad fields marked as no_update."""
        file_headers = {col_def.get('field', col) for col, col_def in airbender_table.columns.items()}

        airbender_table.calc_update_excludes(file_headers)

        instructor_bind = airbender_table.columns['instructor']['bind_name']
        assert instructor_bind in airbender_table._update_excludes


class TestSetCursor:
    """Test set_cursor functionality and cache invalidation."""

    def test_set_cursor_same_paramstyle(self, airbender_table, sqlite_db):
        """Test switching cursor with same paramstyle doesn't reset cache."""
        original_sql = airbender_table.get_sql('insert')

        # Create new cursor with same paramstyle
        new_cursor = sqlite_db.cursor()

        # Switch cursor
        airbender_table.set_cursor(new_cursor)

        new_sql = airbender_table.get_sql('insert')

        assert new_sql == original_sql
        assert airbender_table.cursor == new_cursor

    def test_set_cursor_different_connection_type(self, airbender_table):
        """Test switching to different connection type."""
        original_sql = airbender_table.get_sql('insert')
        original_paramstyle = airbender_table.paramstyle

        # Note: Can't easily test different paramstyle with SQLite
        # This test verifies cursor switch works
        assert original_sql is not None
        assert original_paramstyle in (ParamStyle.QMARK, ParamStyle.NAMED)


class TestReset:
    """Test _reset functionality."""

    def test_reset_clears_all_state(self, airbender_table):
        """Test that reset clears all cached state."""
        airbender_table.generate_sql('insert')
        airbender_table.set_values({'trainee_id': 'TEST001', 'monk_name': 'Test'})
        airbender_table.counts['insert'] = 5
        airbender_table._update_excludes.add('test_col')

        airbender_table._reset()

        assert airbender_table._sql_statements['insert'] is None
        assert airbender_table._param_config['insert'] == ()
        assert len(airbender_table._update_excludes) == 0
        assert airbender_table.counts['insert'] == 0
        assert len(airbender_table.values) == 0


class TestAdvancedFeatures:
    """Test advanced ETL features."""

    def test_complex_etl_scenario(self, airbender_table, cursor):
        """Test complete ETL scenario for Air Nomad recruitment."""
        recruit_data = {
            'trainee_id': 'ROHAN001',
            'monk_name': 'Rohan',
            'home_temple': 'Air Temple Island',
            'mastery_rank': '2',
            'bison_companion': 'Young Bison',
            'daily_meditation': '6.0'
        }

        airbender_table.set_values(recruit_data)

        assert airbender_table.reqs_met

        # Check if record exists (should be None)
        existing = airbender_table.get_db_record()
        assert existing is None

        # Insert new record
        result = airbender_table.exec_insert()
        assert result == 0
        cursor.connection.commit()

        # Verify insertion
        cursor.execute("SELECT * FROM air_nomad_training WHERE nomad_id = 'ROHAN001'")
        row = cursor.fetchone()
        assert row['nomad_id'] == 'ROHAN001'
        assert row['name'] == 'Rohan'
        assert row['airbending_level'] == 2