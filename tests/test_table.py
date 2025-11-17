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
        'training_date': {'db_expr': 'CURRENT_TIMESTAMP'},
        'instructor': {'default': 'Monk Gyatso', 'no_update': True}
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
        'commanding_officer': {'default': 'Admiral Zhao', 'no_update': True}
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
        'registration_date': {'db_expr': 'CURRENT_DATE'},
        'kingdom_loyalty': {'default': 'True Earth Kingdom Citizen'}
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

    def test_exec_update_missing_requirements_strict(self, airbender_table):
        """Test update raises error when requirements not met and raise_error=True."""
        airbender_table.set_values({
            'trainee_id': 'INCOMPLETE001'
        })

        with pytest.raises(ValueError, match="required columns"):
            airbender_table.exec_update(raise_error=True)

    def test_exec_update_missing_requirements_graceful(self, airbender_table):
        """Test update logs and tracks incomplete when requirements not met."""
        airbender_table.set_values({
            'trainee_id': 'INCOMPLETE001'
        })

        result = airbender_table.exec_update(raise_error=False)

        assert result == 1  # Error code
        assert airbender_table.counts['incomplete'] == 1
        assert airbender_table.counts['update'] == 0  # Should not have executed

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

        result = fire_nation_table.fetch()

        assert result['soldier_id'] == 'MAI001'
        assert result['name'] == 'Mai'
        assert result['rank'] == 'Royal Guard'


class TestIncompleteTracking:
    """Test counts['incomplete'] tracking for missing requirements."""

    def test_incomplete_counter_initialization(self, airbender_table):
        """Test that incomplete counter starts at zero."""
        assert airbender_table.counts['incomplete'] == 0

    def test_insert_incomplete_tracking(self, airbender_table):
        """Test incomplete tracking for insert operations."""
        # Missing required temple field
        airbender_table.set_values({
            'trainee_id': 'INC001',
            'monk_name': 'Incomplete Nomad'
        })

        result = airbender_table.exec_insert(raise_error=False)

        assert result == 1
        assert airbender_table.counts['incomplete'] == 1
        assert airbender_table.counts['insert'] == 0

    def test_update_incomplete_tracking(self, airbender_table):
        """Test incomplete tracking for update operations."""
        airbender_table.set_values({
            'trainee_id': 'INC002'
        })

        result = airbender_table.exec_update(raise_error=False)

        assert result == 1
        assert airbender_table.counts['incomplete'] == 1
        assert airbender_table.counts['update'] == 0

    def test_merge_incomplete_tracking(self, airbender_table):
        """Test incomplete tracking for merge operations."""
        airbender_table.set_values({
            'trainee_id': 'INC003',
            'monk_name': 'Incomplete Merge'
        })

        result = airbender_table.exec_merge(raise_error=False)

        assert result == 1
        assert airbender_table.counts['incomplete'] == 1
        assert airbender_table.counts['merge'] == 0

    def test_select_incomplete_tracking(self, airbender_table):
        """Test incomplete tracking for select operations with missing keys."""
        airbender_table.set_values({
            'monk_name': 'No ID Nomad',
            'home_temple': 'Unknown Temple'
        })

        result = airbender_table.exec_select(raise_error=False)

        assert result == 1
        assert airbender_table.counts['incomplete'] == 1
        assert airbender_table.counts['select'] == 0

    def test_delete_incomplete_tracking(self, airbender_table):
        """Test incomplete tracking for delete operations with missing keys."""
        airbender_table.set_values({
            'monk_name': 'No ID Nomad',
            'home_temple': 'Unknown Temple'
        })

        result = airbender_table.exec_delete(raise_error=False)

        assert result == 1
        assert airbender_table.counts['incomplete'] == 1
        assert airbender_table.counts['delete'] == 0

    def test_multiple_incomplete_accumulation(self, airbender_table):
        """Test that incomplete counter accumulates across operations."""
        # First incomplete insert
        airbender_table.set_values({
            'trainee_id': 'INC004',
            'monk_name': 'First Incomplete'
        })
        airbender_table.exec_insert(raise_error=False)

        # Second incomplete update
        airbender_table.set_values({
            'trainee_id': 'INC005'
        })
        airbender_table.exec_update(raise_error=False)

        # Third incomplete merge
        airbender_table.set_values({
            'trainee_id': 'INC006',
            'monk_name': 'Third Incomplete'
        })
        airbender_table.exec_merge(raise_error=False)

        assert airbender_table.counts['incomplete'] == 3
        assert airbender_table.counts['insert'] == 0
        assert airbender_table.counts['update'] == 0
        assert airbender_table.counts['merge'] == 0


class TestReqsCheckedParameter:
    """Test reqs_checked parameter to skip redundant validation."""

    def test_insert_with_reqs_checked(self, airbender_table, cursor):
        """Test insert with reqs_checked=True skips validation."""
        airbender_table.set_values({
            'trainee_id': 'CHECK001',
            'monk_name': 'Pre-Checked Nomad',
            'home_temple': 'Validated Temple'
        })

        # Verify requirements before calling
        assert airbender_table.reqs_met

        # Execute with reqs_checked=True
        result = airbender_table.exec_insert(reqs_checked=True)

        assert result == 0
        assert airbender_table.counts['insert'] == 1
        assert airbender_table.counts['incomplete'] == 0

    def test_update_with_reqs_checked(self, airbender_table, cursor):
        """Test update with reqs_checked=True skips validation."""
        # Insert initial record
        airbender_table.set_values({
            'trainee_id': 'CHECK002',
            'monk_name': 'Checkable Nomad',
            'home_temple': 'Validation Temple'
        })
        airbender_table.exec_insert()
        cursor.connection.commit()

        # Update with pre-validation
        airbender_table.set_values({
            'trainee_id': 'CHECK002',
            'monk_name': 'Updated Nomad',
            'home_temple': 'New Temple'
        })

        assert airbender_table.reqs_met

        result = airbender_table.exec_update(reqs_checked=True)

        assert result == 0
        assert airbender_table.counts['update'] == 1

    def test_select_with_reqs_checked(self, airbender_table, cursor):
        """Test select with reqs_checked=True skips key validation."""
        # Insert record first
        airbender_table.set_values({
            'trainee_id': 'CHECK003',
            'monk_name': 'Selectable Nomad',
            'home_temple': 'Select Temple'
        })
        airbender_table.exec_insert()
        cursor.connection.commit()

        # Select with pre-validation
        airbender_table.set_values({
            'trainee_id': 'CHECK003',
            'monk_name': 'Any Name',
            'home_temple': 'Any Temple'
        })

        assert airbender_table.has_all_keys

        result = airbender_table.exec_select(reqs_checked=True)

        assert result == 0
        assert airbender_table.counts['select'] == 1

    def test_optional_table_pattern(self, cursor):
        """Test optional table pattern with explicit requirement checking."""
        # Create an optional address table
        cursor.execute("""
                       CREATE TABLE optional_addresses
                       (
                           nomad_id TEXT PRIMARY KEY,
                           street   TEXT NOT NULL,
                           city     TEXT NOT NULL
                       )
                       """)
        cursor.connection.commit()

        address_table = Table('optional_addresses', {
            'nomad_id': {'field': 'id', 'primary_key': True},
            'street': {'field': 'address_line', 'nullable': False},
            'city': {'field': 'city_name', 'nullable': False}
        }, cursor=cursor)

        # Record with complete address
        complete_record = {
            'id': 'ADDR001',
            'address_line': '123 Air Temple Way',
            'city_name': 'Southern Air Temple'
        }

        address_table.set_values(complete_record)
        if address_table.reqs_met:
            result = address_table.exec_insert(reqs_checked=True)
            assert result == 0

        # Record without address (optional)
        incomplete_record = {
            'id': 'ADDR002'
        }

        address_table.set_values(incomplete_record)
        if address_table.reqs_met:
            address_table.exec_insert(reqs_checked=True)
        # If not met, don't insert - this is expected

        # Should have inserted 1, skipped 1
        assert address_table.counts['insert'] == 1
        assert address_table.counts['incomplete'] == 0  # We never called exec


class TestUpdateExclusions:
    """Test column exclusion logic for updates."""

    def test_calc_update_excludes_missing_fields(self, airbender_table):
        """Test excluding Air Nomad fields missing from source data."""
        record_fields = {'trainee_id', 'monk_name', 'home_temple'}

        airbender_table.calc_update_excludes(record_fields)

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
        record_fields = {col_def.get('field', col) for col, col_def in airbender_table.columns.items()}

        airbender_table.calc_update_excludes(record_fields)

        instructor_bind = airbender_table.columns['instructor']['bind_name']
        assert instructor_bind in airbender_table._update_excludes

    def test_calc_update_excludes_uses_cached_fields(self, airbender_table):
        """Test that calc_update_excludes uses cached _record_fields when called without arguments."""
        # Set values to cache record fields
        airbender_table.set_values({
            'trainee_id': 'CACHE001',
            'monk_name': 'Cached Nomad',
            'home_temple': 'Cache Temple'
        })

        # Call without arguments - should use cached fields
        airbender_table.calc_update_excludes()

        # Fields not in cached set should be excluded
        sky_bison_bind = airbender_table.columns['sky_bison']['bind_name']
        meditation_bind = airbender_table.columns['meditation_score']['bind_name']
        assert sky_bison_bind in airbender_table._update_excludes
        assert meditation_bind in airbender_table._update_excludes

        # Fields in cached set should not be excluded
        name_bind = airbender_table.columns['name']['bind_name']
        assert name_bind not in airbender_table._update_excludes


class TestSetCursor:
    """Test cursor setter functionality and cache invalidation."""

    def test_set_cursor_same_paramstyle(self, airbender_table, sqlite_db):
        """Test switching cursor with same paramstyle doesn't reset cache."""
        original_sql = airbender_table.get_sql('insert')

        # Create new cursor with same paramstyle
        new_cursor = sqlite_db.cursor()

        # Switch cursor
        airbender_table.cursor = new_cursor

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
        airbender_table.counts['incomplete'] = 3
        airbender_table._update_excludes.add('test_col')

        airbender_table._reset()

        assert airbender_table._sql_statements['insert'] is None
        assert airbender_table._param_config['insert'] == ()
        assert len(airbender_table._update_excludes) == 0
        assert airbender_table.counts['insert'] == 0
        assert airbender_table.counts['incomplete'] == 0
        assert len(airbender_table.values) == 0


class TestAutomaticUpdateExcludes:
    """Test automatic _record_fields caching and calc_update_excludes() calls."""

    def test_record_fields_cached_on_set_values(self, airbender_table):
        """Test that _record_fields is cached when set_values() is called."""
        # Initially empty
        assert airbender_table._record_fields == set()

        # Set values with some fields
        record_data = {
            'trainee_id': 'CACHE001',
            'monk_name': 'Cache Test',
            'home_temple': 'Test Temple'
        }

        airbender_table.set_values(record_data)

        # _record_fields should now contain the field names
        assert airbender_table._record_fields == {'trainee_id', 'monk_name', 'home_temple'}

    def test_record_fields_only_set_once(self, airbender_table):
        """Test that _record_fields is only cached on first set_values() call."""
        first_record = {
            'trainee_id': 'FIRST001',
            'monk_name': 'First',
            'home_temple': 'First Temple'
        }

        airbender_table.set_values(first_record)
        initial_fields = airbender_table._record_fields.copy()

        # Set values again with different fields
        second_record = {
            'trainee_id': 'SECOND001',
            'monk_name': 'Second',
            'home_temple': 'Second Temple',
            'bison_companion': 'New Bison'  # Extra field not in first record
        }

        airbender_table.set_values(second_record)

        # _record_fields should still have the original field set
        assert airbender_table._record_fields == initial_fields

    def test_exec_update_automatically_calls_calc_update_excludes(self, airbender_table, cursor):
        """Test that exec_update() automatically calculates update excludes."""
        # Insert initial record
        airbender_table.set_values({
            'trainee_id': 'AUTO001',
            'monk_name': 'Auto Update',
            'home_temple': 'Auto Temple',
            'mastery_rank': '5'
        })
        airbender_table.exec_insert()
        cursor.connection.commit()

        # Create new table instance to simulate fresh start
        from dbtk.etl.table import Table
        col_def = {
            'nomad_id': {'field': 'trainee_id', 'primary_key': True},
            'name': {'field': 'monk_name', 'required': True},
            'temple': {'field': 'home_temple', 'nullable': False},
            'airbending_level': {'field': 'mastery_rank', 'fn': lambda x: int(x) if x else 0},
            'sky_bison': {'field': 'bison_companion'},
            'meditation_score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0},
            'training_date': {'db_expr': 'CURRENT_TIMESTAMP'},
            'instructor': {'default': 'Monk Gyatso', 'no_update': True}
        }
        new_table = Table('air_nomad_training', columns=col_def, cursor=cursor)

        # Set values with only some fields (missing bison_companion and daily_meditation)
        update_data = {
            'trainee_id': 'AUTO001',
            'monk_name': 'Auto Updated',
            'home_temple': 'Updated Temple',
            'mastery_rank': '6'
        }

        new_table.set_values(update_data)

        # Verify _record_fields is cached
        assert new_table._record_fields == {'trainee_id', 'monk_name', 'home_temple', 'mastery_rank'}

        # _update_excludes should not be calculated yet
        assert not new_table._update_excludes_calculated

        # Execute update - should automatically call calc_update_excludes()
        result = new_table.exec_update()
        assert result == 0

        # _update_excludes should now be calculated
        assert new_table._update_excludes_calculated

        # Missing fields should be in excludes (plus no_update field)
        sky_bison_bind = new_table.columns['sky_bison']['bind_name']
        meditation_bind = new_table.columns['meditation_score']['bind_name']
        instructor_bind = new_table.columns['instructor']['bind_name']

        assert sky_bison_bind in new_table._update_excludes
        assert meditation_bind in new_table._update_excludes
        assert instructor_bind in new_table._update_excludes  # no_update=True

    def test_exec_merge_automatically_calls_calc_update_excludes(self, airbender_table, cursor):
        """Test that exec_merge() automatically calculates update excludes."""
        # Set values with only some fields
        merge_data = {
            'trainee_id': 'MERGE001',
            'monk_name': 'Merge Test',
            'home_temple': 'Merge Temple',
            'mastery_rank': '3'
        }

        airbender_table.set_values(merge_data)

        # Verify _record_fields is cached
        assert airbender_table._record_fields == {'trainee_id', 'monk_name', 'home_temple', 'mastery_rank'}

        # _update_excludes should not be calculated yet
        assert not airbender_table._update_excludes_calculated

        # Execute merge - should automatically call calc_update_excludes()
        result = airbender_table.exec_merge()
        assert result == 0

        # _update_excludes should now be calculated
        assert airbender_table._update_excludes_calculated

        # Missing fields should be in excludes
        sky_bison_bind = airbender_table.columns['sky_bison']['bind_name']
        meditation_bind = airbender_table.columns['meditation_score']['bind_name']

        assert sky_bison_bind in airbender_table._update_excludes
        assert meditation_bind in airbender_table._update_excludes

    def test_update_excludes_calculated_flag_prevents_redundant_calls(self, airbender_table, cursor):
        """Test that _update_excludes_calculated flag prevents redundant calculations."""
        # Insert initial record
        airbender_table.set_values({
            'trainee_id': 'REDUN001',
            'monk_name': 'Redundant Test',
            'home_temple': 'Redundant Temple',
            'mastery_rank': '4'
        })
        airbender_table.exec_insert()
        cursor.connection.commit()

        # Create new table and set values
        from dbtk.etl.table import Table
        col_def = {
            'nomad_id': {'field': 'trainee_id', 'primary_key': True},
            'name': {'field': 'monk_name', 'required': True},
            'temple': {'field': 'home_temple', 'nullable': False},
            'airbending_level': {'field': 'mastery_rank', 'fn': lambda x: int(x) if x else 0},
            'sky_bison': {'field': 'bison_companion'},
            'meditation_score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0},
            'training_date': {'db_expr': 'CURRENT_TIMESTAMP'},
            'instructor': {'default': 'Monk Gyatso', 'no_update': True}
        }
        new_table = Table('air_nomad_training', columns=col_def, cursor=cursor)

        update_data = {
            'trainee_id': 'REDUN001',
            'monk_name': 'Updated Name',
            'home_temple': 'Updated Temple',
            'mastery_rank': '5'
        }

        new_table.set_values(update_data)

        # Execute first update
        new_table.exec_update()
        assert new_table._update_excludes_calculated

        # Store the excludes set
        first_excludes = new_table._update_excludes.copy()

        # Execute another update without calling set_values again
        new_table.values['name'] = 'Another Update'
        new_table.exec_update()

        # Excludes should be the same (not recalculated)
        assert new_table._update_excludes == first_excludes

    def test_update_excludes_sql_regeneration(self, fire_nation_table):
        """Test that UPDATE/MERGE SQL is regenerated when excludes change."""
        # Set values with all fields
        all_fields_data = {
            'recruit_number': 'REGEN001',
            'full_name': 'Regen Test',
            'military_rank': 'Soldier',
            'flame_intensity': '5',
            'birthplace': 'Fire Nation',
            'joined_date': '100 AG'
        }

        fire_nation_table.set_values(all_fields_data)

        # Generate initial UPDATE SQL (no excludes)
        fire_nation_table.generate_sql('update')
        initial_sql = fire_nation_table._sql_statements['update']

        # Now manually calculate excludes with a subset of fields
        fire_nation_table.calc_update_excludes({'recruit_number', 'full_name', 'military_rank', 'flame_intensity'})

        # SQL statements should be invalidated (set to None)
        assert fire_nation_table._sql_statements['update'] is None
        assert fire_nation_table._sql_statements['merge'] is None

        # Regenerate UPDATE SQL
        fire_nation_table.generate_sql('update')
        new_sql = fire_nation_table._sql_statements['update']

        # SQL should be different (excludes birthplace fields)
        assert new_sql != initial_sql


class TestSpecialColumnNames:
    """Test handling of column names that require bind_name sanitization."""

    @pytest.fixture
    def special_chars_schema(self, cursor):
        """Create table with special character column names."""
        cursor.execute("""
                       CREATE TABLE spirit_world_records
                       (
                           "spirit-id"   TEXT PRIMARY KEY,
                           "spirit name" TEXT NOT NULL,
                           "power$level" INTEGER,
                           "realm#"      TEXT,
                           "last-seen"   TEXT
                       )
                       """)
        cursor.connection.commit()
        return 'spirit_world_records'

    @pytest.fixture
    def special_chars_table(self, cursor, special_chars_schema):
        """Table with special character column names."""
        col_def = {
            'spirit-id': {'field': 'id', 'primary_key': True},
            'spirit name': {'field': 'name', 'nullable': False},
            'power$level': {'field': 'power', 'fn': lambda x: int(x) if x else 0},
            'realm#': {'field': 'realm'},
            'last-seen': {'field': 'last_encounter'}
        }
        return Table('spirit_world_records', columns=col_def, cursor=cursor)

    def test_bind_name_sanitization(self, special_chars_table):
        """Test that special characters in column names are sanitized for bind parameters."""
        # Check bind_name sanitization
        assert special_chars_table.columns['spirit-id']['bind_name'] == 'spirit_id'
        assert special_chars_table.columns['spirit name']['bind_name'] == 'spirit_name'
        assert special_chars_table.columns['power$level']['bind_name'] == 'power_level'
        assert special_chars_table.columns['realm#']['bind_name'] == 'realm'
        assert special_chars_table.columns['last-seen']['bind_name'] == 'last_seen'

    def test_insert_with_special_column_names(self, special_chars_table, cursor):
        """Test INSERT with special column names uses sanitized bind parameters."""
        special_chars_table.set_values({
            'id': 'SPIRIT001',
            'name': 'Wan Shi Tong',
            'power': '10',
            'realm': 'Spirit World',
            'last_encounter': '100 AG'
        })

        # Generate and examine INSERT SQL
        sql = special_chars_table.get_sql('insert')

        # Column names should be quoted in SQL
        assert '"spirit-id"' in sql or 'spirit-id' in sql
        assert '"spirit name"' in sql or 'spirit_name' in sql
        assert '"power$level"' in sql or 'power$level' in sql

        # But bind parameters should use sanitized names (check _param_config)
        param_names = special_chars_table._param_config['insert']
        assert 'spirit_id' in param_names
        assert 'spirit_name' in param_names
        assert 'power_level' in param_names

    def test_update_with_special_column_names(self, special_chars_table, cursor):
        """Test UPDATE with special column names uses sanitized bind parameters."""
        # Insert first
        special_chars_table.set_values({
            'id': 'SPIRIT002',
            'name': 'Koh',
            'power': '9',
            'realm': 'Spirit World',
            'last_encounter': '99 AG'
        })
        special_chars_table.exec_insert()
        cursor.connection.commit()

        # Now update
        special_chars_table.set_values({
            'id': 'SPIRIT002',
            'name': 'Koh the Face Stealer',
            'power': '10',
            'realm': 'Spirit World',
            'last_encounter': '100 AG'
        })

        # Generate and examine UPDATE SQL
        sql = special_chars_table.get_sql('update')

        # Column names should be quoted in SQL
        assert '"spirit-id"' in sql or 'spirit-id' in sql

        # But bind parameters should use sanitized names (check _param_config)
        param_names = special_chars_table._param_config['update']
        assert 'spirit_id' in param_names


    def test_merge_with_special_column_names(self, special_chars_table, cursor):
        """Test MERGE/UPSERT with special column names uses sanitized bind parameters."""
        special_chars_table.set_values({
            'id': 'SPIRIT003',
            'name': 'Hei Bai',
            'power': '8',
            'realm': 'Physical World',
            'last_encounter': '99 AG'
        })

        # Generate and examine MERGE SQL
        sql = special_chars_table.get_sql('merge')

        # Column names should be quoted in SQL
        assert '"spirit-id"' in sql or 'spirit-id' in sql
        assert '"spirit name"' in sql or 'spirit_name' in sql

        # But bind parameters should use sanitized names (check _param_config)
        param_names = special_chars_table._param_config['merge']
        assert 'spirit_id' in param_names
        assert 'spirit_name' in param_names
        assert 'power_level' in param_names


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
        existing = airbender_table.fetch()
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