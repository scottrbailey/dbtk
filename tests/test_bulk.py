# tests/test_bulk.py
import pytest
from pathlib import Path

from dbtk.etl.bulk import DataSurge
from dbtk.etl.table import Table
from dbtk.database import ParamStyle, Database


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
                       meditation_score REAL
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
                       soldier_id        TEXT PRIMARY KEY,
                       name              TEXT NOT NULL,
                       rank              TEXT NOT NULL,
                       firebending_skill INTEGER
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
                       earthbending_skill REAL
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
        'meditation_score': {'field': 'daily_meditation', 'fn': lambda x: float(x) if x else 0.0}
    }
    return Table('air_nomad_training', columns=col_def, cursor=cursor)


@pytest.fixture
def fire_nation_table(cursor, fire_nation_schema):
    """Table configuration for Fire Nation army records."""
    col_def = {
        'soldier_id': {'field': 'recruit_number', 'primary_key': True},
        'name': {'field': 'full_name', 'required': True},
        'rank': {'field': 'military_rank', 'nullable': False},
        'firebending_skill': {'field': 'flame_intensity', 'fn': lambda x: int(x) if x else 0}
    }
    return Table('fire_nation_army', columns=col_def, cursor=cursor)


@pytest.fixture
def earth_kingdom_table(cursor, earth_kingdom_schema):
    """Table configuration for Earth Kingdom census."""
    col_def = {
        'citizen_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'city': {'field': 'home_city', 'nullable': False},
        'earthbending_skill': {'field': 'boulder_size', 'fn': lambda x: float(x) if x else 0.0}
    }
    return Table('earth_kingdom_census', columns=col_def, cursor=cursor)


@pytest.fixture
def airbender_records():
    """Sample Air Nomad training records."""
    return [
        {
            'trainee_id': 'AANG001',
            'monk_name': 'Aang',
            'home_temple': 'Southern Air Temple',
            'mastery_rank': '4',
            'bison_companion': 'Appa',
            'daily_meditation': '8.5'
        },
        {
            'trainee_id': 'TENZIN001',
            'monk_name': 'Tenzin',
            'home_temple': 'Air Temple Island',
            'mastery_rank': '4',
            'bison_companion': 'Oogi',
            'daily_meditation': '9.0'
        },
        {
            'trainee_id': 'JINORA001',
            'monk_name': 'Jinora',
            'home_temple': 'Air Temple Island',
            'mastery_rank': '3',
            'bison_companion': 'Pepper',
            'daily_meditation': '7.5'
        }
    ]


@pytest.fixture
def fire_nation_records():
    """Sample Fire Nation army records."""
    return [
        {
            'recruit_number': 'ZUKO001',
            'full_name': 'Prince Zuko',
            'military_rank': 'Banished Prince',
            'flame_intensity': '9'
        },
        {
            'recruit_number': 'AZULA001',
            'full_name': 'Princess Azula',
            'military_rank': 'Fire Nation Princess',
            'flame_intensity': '10'
        },
        {
            'recruit_number': 'IROH001',
            'full_name': 'General Iroh',
            'military_rank': 'Dragon of the West',
            'flame_intensity': '10'
        }
    ]


class TestDataSurgeInitialization:
    """Test DataSurge class initialization."""

    def test_initialization_with_table(self, airbender_table):
        """Test DataSurge initialization with Air Nomad table."""
        surge = DataSurge(airbender_table)

        assert surge.table == airbender_table
        assert surge.cursor == airbender_table.cursor
        assert surge.skips == 0
        assert surge._sql_statements == {}

    def test_cursor_inherited_from_table(self, fire_nation_table):
        """Test that DataSurge uses cursor from Fire Nation table."""
        surge = DataSurge(fire_nation_table)

        assert surge.cursor is fire_nation_table.cursor
        assert surge.cursor.connection.database_type == 'sqlite'


class TestGetSQL:
    """Test get_sql method."""

    def test_get_sql_from_table(self, airbender_table):
        """Test getting SQL from Air Nomad table when not modified."""
        surge = DataSurge(airbender_table)

        sql = surge.get_sql('insert')

        assert 'INSERT INTO air_nomad_training' in sql
        assert surge._sql_statements == {}  # No modifications stored

    def test_get_sql_returns_modified_version(self, airbender_table):
        """Test that modified SQL is returned when stored locally."""
        surge = DataSurge(airbender_table)

        # Store modified SQL
        custom_sql = "SELECT * FROM custom_air_temple"
        surge._sql_statements['select'] = custom_sql

        sql = surge.get_sql('select')

        assert sql == custom_sql
        assert sql != airbender_table.get_sql('select')


class TestInsertOperation:
    """Test bulk insert operations."""

    def test_insert_single_batch(self, airbender_table, airbender_records, cursor):
        """Test inserting Air Nomad records in single batch."""
        surge = DataSurge(airbender_table)

        errors = surge.insert(airbender_records)

        assert errors == 0
        assert airbender_table.counts['insert'] == 3
        assert surge.skips == 0

        cursor.connection.commit()

        # Verify all records inserted
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 3

    def test_insert_multiple_batches(self, airbender_table, airbender_records, cursor):
        """Test inserting Air Nomad records across multiple batches."""
        surge = DataSurge(airbender_table, batch_size=2)

        errors = surge.insert(airbender_records)

        assert errors == 0
        assert airbender_table.counts['insert'] == 3

        cursor.connection.commit()

        # Verify all records inserted
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 3

    def test_insert_with_missing_requirements(self, airbender_table, cursor):
        """Test inserting Air Nomad records with missing required fields."""
        incomplete_records = [
            {
                'trainee_id': 'MEELO001',
                'monk_name': 'Meelo'
                # Missing required 'temple' field
            },
            {
                'trainee_id': 'IKKI001',
                'monk_name': 'Ikki',
                'home_temple': 'Air Temple Island',
                'mastery_rank': '2'
            }
        ]

        surge = DataSurge(airbender_table)
        errors = surge.insert(incomplete_records)

        assert errors == 0
        assert airbender_table.counts['insert'] == 1  # Only valid record inserted
        assert surge.skips == 1

        cursor.connection.commit()

        # Verify only valid record was inserted
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 1

    def test_insert_with_duplicate_key_error(self, airbender_table, airbender_records, cursor):
        """Test insert handling duplicate key errors."""
        surge = DataSurge(airbender_table)

        # First insert succeeds
        errors = surge.insert(airbender_records)
        assert errors == 0
        cursor.connection.commit()

        # Second insert with same keys fails
        errors = surge.insert(airbender_records, raise_error=False)

        assert errors == 3  # All 3 records fail due to duplicate keys
        assert airbender_table.counts['insert'] == 3  # Count doesn't increase

    def test_insert_raises_on_error(self, airbender_table, airbender_records, cursor):
        """Test insert raises exception when raise_error=True."""
        surge = DataSurge(airbender_table)

        # First insert
        surge.insert(airbender_records)
        cursor.connection.commit()

        # Second insert should raise
        with pytest.raises(Exception):
            surge.insert(airbender_records, raise_error=True)

    def test_insert_with_transaction(self, airbender_table, airbender_records, cursor):
        """Test insert with transaction wrapping."""
        surge = DataSurge(airbender_table, use_transaction=True)

        errors = surge.insert(airbender_records)

        assert errors == 0
        # Note: Transaction is auto-committed in SQLite when context exits

        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 3


class TestUpdateOperation:
    """Test bulk update operations."""

    def test_update_single_batch(self, fire_nation_table, fire_nation_records, cursor):
        """Test updating Fire Nation records in single batch."""
        surge = DataSurge(fire_nation_table)

        # Insert records first
        surge.insert(fire_nation_records)
        cursor.connection.commit()

        # Update records
        updated_records = [
            {**rec, 'military_rank': rec['military_rank'] + ' (Updated)'}
            for rec in fire_nation_records
        ]

        errors = surge.update(updated_records)

        assert errors == 0
        assert fire_nation_table.counts['update'] == 3
        assert surge.skips == 0

        cursor.connection.commit()

        # Verify updates
        cursor.execute("SELECT rank FROM fire_nation_army WHERE soldier_id = 'ZUKO001'")
        assert '(Updated)' in cursor.fetchone()['rank']

    def test_update_multiple_batches(self, fire_nation_table, fire_nation_records, cursor):
        """Test updating Fire Nation records across multiple batches."""
        surge = DataSurge(fire_nation_table, batch_size=1)

        # Insert records first
        surge.insert(fire_nation_records)
        cursor.connection.commit()

        # Update in small batches
        updated_records = [
            {**rec, 'military_rank': rec['military_rank'] + ' (Updated)'}
            for rec in fire_nation_records
        ]

        errors = surge.update(updated_records)

        assert errors == 0
        assert fire_nation_table.counts['update'] == 3

    def test_update_with_missing_requirements(self, fire_nation_table, cursor):
        """Test updating Fire Nation records with missing required fields."""
        surge = DataSurge(fire_nation_table)

        # Insert one valid record first
        valid_record = {
            'recruit_number': 'TYLEE001',
            'full_name': 'Ty Lee',
            'military_rank': 'Acrobat',
            'flame_intensity': '3'
        }
        surge.insert([valid_record])
        cursor.connection.commit()

        incomplete_records = [
            {
                'recruit_number': 'MAI001'
                # Missing required fields
            },
            {
                'recruit_number': 'TYLEE001',
                'full_name': 'Ty Lee Updated',
                'military_rank': 'Kyoshi Warrior'
            }
        ]

        errors = surge.update(incomplete_records)

        assert errors == 0
        assert fire_nation_table.counts['update'] == 1
        assert surge.skips == 1

    def test_update_with_transaction(self, fire_nation_table, fire_nation_records, cursor):
        """Test update with transaction wrapping."""
        surge = DataSurge(fire_nation_table, use_transaction=True)

        # Insert records first
        surge.insert(fire_nation_records)

        # Update with transaction
        updated_records = [
            {**rec, 'military_rank': rec['military_rank'] + ' (TX)'}
            for rec in fire_nation_records
        ]

        errors = surge.update(updated_records)
        assert errors == 0


class TestDeleteOperation:
    """Test bulk delete operations."""

    def test_delete_single_batch(self, airbender_table, airbender_records, cursor):
        """Test deleting Air Nomad records in single batch."""
        surge = DataSurge(airbender_table)

        # Insert records first
        surge.insert(airbender_records)
        cursor.connection.commit()

        # Delete records
        delete_records = [
            {'trainee_id': rec['trainee_id']}
            for rec in airbender_records
        ]

        errors = surge.delete(delete_records)

        assert errors == 0
        assert airbender_table.counts['delete'] == 3

        cursor.connection.commit()

        # Verify deletion
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 0

    def test_delete_with_missing_keys(self, airbender_table, airbender_records, cursor):
        """Test deleting Air Nomad records with missing key fields."""
        surge = DataSurge(airbender_table)

        # Insert records first
        surge.insert(airbender_records)
        cursor.connection.commit()

        delete_records = [
            {'trainee_id': 'AANG001'},
            {},  # Missing key
            {'trainee_id': 'JINORA001'}
        ]

        errors = surge.delete(delete_records)

        assert errors == 0
        assert airbender_table.counts['delete'] == 2
        assert surge.skips == 1

        cursor.connection.commit()

        # Verify partial deletion
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 1  # Only TENZIN001 remains

    def test_delete_with_transaction(self, airbender_table, airbender_records, cursor):
        """Test delete with transaction wrapping."""
        surge = DataSurge(airbender_table, use_transaction=True)

        # Insert records first
        surge.insert(airbender_records)
        cursor.connection.commit()

        delete_records = [
            {'trainee_id': rec['trainee_id']}
            for rec in airbender_records[:2]
        ]

        errors = surge.delete(delete_records)

        assert errors == 0


class TestMergeWithUpsert:
    """Test merge operations using upsert syntax."""

    def test_merge_sqlite_uses_upsert(self, airbender_table, airbender_records, cursor):
        """Test that SQLite Air Nomad merge uses INSERT...ON CONFLICT."""
        surge = DataSurge(airbender_table)

        errors = surge.merge(airbender_records)

        assert errors == 0
        assert airbender_table.counts['merge'] == 3

        cursor.connection.commit()

        # Verify inserts
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 3

    def test_merge_inserts_new_records(self, airbender_table, airbender_records, cursor):
        """Test merge inserts new Air Nomad records."""
        surge = DataSurge(airbender_table)

        errors = surge.merge(airbender_records)
        cursor.connection.commit()

        assert errors == 0

        # Verify data
        cursor.execute("SELECT name FROM air_nomad_training WHERE nomad_id = 'AANG001'")
        assert cursor.fetchone()['name'] == 'Aang'

    def test_merge_updates_existing_records(self, airbender_table, airbender_records, cursor):
        """Test merge updates existing Air Nomad records."""
        surge = DataSurge(airbender_table)

        # Initial insert
        surge.insert(airbender_records)
        cursor.connection.commit()

        # Merge with updated data
        updated_records = [
            {**rec, 'monk_name': rec['monk_name'] + ' the Great'}
            for rec in airbender_records
        ]

        errors = surge.merge(updated_records)
        cursor.connection.commit()

        assert errors == 0
        assert airbender_table.counts['merge'] == 3

        # Verify updates (not duplicates)
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 3

        cursor.execute("SELECT name FROM air_nomad_training WHERE nomad_id = 'AANG001'")
        assert cursor.fetchone()['name'] == 'Aang the Great'

    def test_merge_mixed_insert_update(self, fire_nation_table, fire_nation_records, cursor):
        """Test merge handles mix of new and existing Fire Nation records."""
        surge = DataSurge(fire_nation_table)

        # Insert first record
        surge.insert([fire_nation_records[0]])
        cursor.connection.commit()

        # Merge all three (1 update, 2 inserts)
        updated_records = [
            {**fire_nation_records[0], 'military_rank': 'Fire Lord'},
            fire_nation_records[1],
            fire_nation_records[2]
        ]

        errors = surge.merge(updated_records)
        cursor.connection.commit()

        assert errors == 0
        assert fire_nation_table.counts['merge'] == 3

        # Verify total count
        cursor.execute("SELECT COUNT(*) as cnt FROM fire_nation_army")
        assert cursor.fetchone()['cnt'] == 3

        # Verify update
        cursor.execute("SELECT rank FROM fire_nation_army WHERE soldier_id = 'ZUKO001'")
        assert cursor.fetchone()['rank'] == 'Fire Lord'

    def test_merge_with_missing_requirements(self, airbender_table, cursor):
        """Test merge skips Air Nomad records with missing fields."""
        merge_records = [
            {
                'trainee_id': 'ROHAN001',
                'monk_name': 'Rohan',
                'home_temple': 'Air Temple Island',
                'mastery_rank': '1'
            },
            {
                'trainee_id': 'MEELO001'
                # Missing required fields
            }
        ]

        surge = DataSurge(airbender_table)
        errors = surge.merge(merge_records)
        cursor.connection.commit()

        assert errors == 0
        assert airbender_table.counts['merge'] == 1
        assert surge.skips == 1

        # Verify only valid record merged
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 1


class TestBatchProcessing:
    """Test batch processing logic."""

    def test_respects_batch_size(self, airbender_table, cursor):
        """Test that Air Nomad records are processed in correct batch sizes."""
        # Create 10 records
        records = [
            {
                'trainee_id': f'NOMAD{i:03d}',
                'monk_name': f'Airbender {i}',
                'home_temple': 'Northern Air Temple',
                'mastery_rank': str(i % 4)
            }
            for i in range(10)
        ]

        surge = DataSurge(airbender_table, batch_size=3)
        errors = surge.insert(records)
        cursor.connection.commit()

        assert errors == 0
        assert airbender_table.counts['insert'] == 10

        # Verify all inserted
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 10

    def test_empty_records_list(self, airbender_table):
        """Test handling empty Air Nomad records list."""
        surge = DataSurge(airbender_table)

        errors = surge.insert([])

        assert errors == 0
        assert airbender_table.counts['insert'] == 0

    def test_all_records_skipped(self, airbender_table, cursor):
        """Test when all Air Nomad records are invalid."""
        invalid_records = [
            {'trainee_id': 'BAD001'},  # Missing required fields
            {'trainee_id': 'BAD002'},
            {'trainee_id': 'BAD003'}
        ]

        surge = DataSurge(airbender_table)
        errors = surge.insert(invalid_records)

        assert errors == 0
        assert airbender_table.counts['insert'] == 0
        assert surge.skips == 3

        # Verify nothing inserted
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 0


class TestErrorHandling:
    """Test error handling across operations."""

    def test_partial_batch_failure(self, fire_nation_table, fire_nation_records, cursor):
        """Test handling when some Fire Nation batches fail."""
        surge = DataSurge(fire_nation_table, batch_size=2)

        # Insert first batch
        surge.insert(fire_nation_records[:2])
        cursor.connection.commit()

        # Try to insert all again (first 2 will fail on duplicate key)
        errors = surge.insert(fire_nation_records, raise_error=False)

        assert errors == 2  # First batch of 2 fails
        # Note: The third one in second batch should succeed
        assert fire_nation_table.counts['insert'] >= 2


class TestTransactionHandling:
    """Test transaction handling."""

    def test_transaction_wraps_all_batches(self, airbender_table, cursor):
        """Test that transaction wraps all Air Nomad batches."""
        records = [
            {
                'trainee_id': f'NOMAD{i:03d}',
                'monk_name': f'Airbender {i}',
                'home_temple': 'Western Air Temple',
                'mastery_rank': str(i % 4)
            }
            for i in range(5)
        ]

        surge = DataSurge(airbender_table, batch_size=2, use_transaction=True)
        errors = surge.insert(records)

        assert errors == 0

        # Verify all committed
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 5

    def test_no_transaction_by_default(self, airbender_table, airbender_records, cursor):
        """Test that transactions are not used by default."""
        surge = DataSurge(airbender_table)

        errors = surge.insert(airbender_records)

        assert errors == 0
        # Manual commit needed without transaction
        cursor.connection.commit()


class TestCountTracking:
    """Test count tracking across operations."""

    def test_counts_accumulate_across_calls(self, airbender_table, airbender_records, cursor):
        """Test that Air Nomad counts accumulate across multiple calls."""
        surge = DataSurge(airbender_table)

        # First insert
        surge.insert(airbender_records[:2])
        cursor.connection.commit()
        assert airbender_table.counts['insert'] == 2

        # Second insert
        surge.insert(airbender_records[2:])
        cursor.connection.commit()
        assert airbender_table.counts['insert'] == 3

    def test_skips_accumulate(self, airbender_table):
        """Test that skipped Air Nomad records accumulate."""
        incomplete_records = [
            {'trainee_id': 'BAD001'},
            {'trainee_id': 'BAD002'}
        ]

        surge = DataSurge(airbender_table)

        surge.insert(incomplete_records)
        assert surge.skips == 2

        surge.update(incomplete_records)
        assert surge.skips == 4  # Accumulated

    def test_separate_operation_counts(self, airbender_table, airbender_records, cursor):
        """Test that different operations have separate counts."""
        surge = DataSurge(airbender_table)

        surge.insert(airbender_records)
        cursor.connection.commit()

        surge.update(airbender_records)
        cursor.connection.commit()

        surge.delete(airbender_records)
        cursor.connection.commit()

        assert airbender_table.counts['insert'] == 3
        assert airbender_table.counts['update'] == 3
        assert airbender_table.counts['delete'] == 3
        assert airbender_table.counts['merge'] == 0


class TestComplexScenarios:
    """Test complex ETL scenarios."""

    def test_mixed_valid_invalid_records(self, airbender_table, cursor):
        """Test processing mix of valid and invalid Air Nomad records."""
        mixed_records = [
            {
                'trainee_id': 'AANG001',
                'monk_name': 'Aang',
                'home_temple': 'Southern Air Temple',
                'mastery_rank': '4'
            },
            {
                'trainee_id': 'BAD001'
                # Missing required fields
            },
            {
                'trainee_id': 'TENZIN001',
                'monk_name': 'Tenzin',
                'home_temple': 'Air Temple Island',
                'mastery_rank': '4'
            },
            {
                'trainee_id': 'BAD002'
                # Missing required fields
            }
        ]

        surge = DataSurge(airbender_table)
        errors = surge.insert(mixed_records)
        cursor.connection.commit()

        assert errors == 0
        assert airbender_table.counts['insert'] == 2
        assert surge.skips == 2

        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 2

    def test_large_dataset_multiple_batches(self, fire_nation_table, cursor):
        """Test processing large Fire Nation dataset across many batches."""
        records = [
            {
                'recruit_number': f'SOLDIER{i:05d}',
                'full_name': f'Fire Nation Soldier {i}',
                'military_rank': 'Private',
                'flame_intensity': str((i % 10) + 1)
            }
            for i in range(100)
        ]

        surge = DataSurge(fire_nation_table)
        errors = surge.insert(records)
        cursor.connection.commit()

        assert errors == 0
        assert fire_nation_table.counts['insert'] == 100

        cursor.execute("SELECT COUNT(*) as cnt FROM fire_nation_army")
        assert cursor.fetchone()['cnt'] == 100

    def test_complete_etl_workflow(self, airbender_table, airbender_records, cursor):
        """Test complete Air Nomad ETL workflow."""
        surge = DataSurge(airbender_table)

        # Initial load
        errors = surge.insert(airbender_records)
        cursor.connection.commit()
        assert errors == 0
        assert airbender_table.counts['insert'] == 3

        # Update existing records
        updated_records = [
            {
                **record,
                'mastery_rank': str(int(record['mastery_rank']) + 1)
            }
            for record in airbender_records
        ]

        errors = surge.update(updated_records)
        cursor.connection.commit()
        assert errors == 0
        assert airbender_table.counts['update'] == 3

        # Merge new and existing
        merge_records = airbender_records + [
            {
                'trainee_id': 'ROHAN001',
                'monk_name': 'Rohan',
                'home_temple': 'Air Temple Island',
                'mastery_rank': '1',
                'bison_companion': 'Young Bison',
                'daily_meditation': '5.0'
            }
        ]

        errors = surge.merge(merge_records)
        cursor.connection.commit()
        assert errors == 0
        assert airbender_table.counts['merge'] == 4

        # Verify final state
        cursor.execute("SELECT COUNT(*) as cnt FROM air_nomad_training")
        assert cursor.fetchone()['cnt'] == 4