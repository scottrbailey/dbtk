# dbtk_tests/conftest.py
"""
Shared test fixtures and configuration for pytest.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import Mock, patch

from dbtk.config import connect
from dbtk.readers import CSVReader
from dbtk.etl import Table, DataSurge


# Set test config file and encryption key for all tests
@pytest.fixture(autouse=True)
def setup_test_config():
    """Automatically set test config file and encryption key for all tests."""
    from dbtk.config import set_config_file

    # Use test.yml in the tests directory
    test_config = Path(__file__).parent / 'test.yml'
    set_config_file(str(test_config))

    with patch.dict(os.environ, {'DBTK_ENCRYPTION_KEY': '2YvTXI9DHQPy4d6-ZC9NxcypvLMsJ94OBdmoHyjmwbM='}):
        yield


@pytest.fixture
def mock_db_cursor():
    """Create a mock database cursor with standard test data."""
    cursor = Mock()
    cursor.description = [
        ('name', None, None, None, None, None, None),
        ('age', None, None, None, None, None, None),
        ('email', None, None, None, None, None, None)
    ]
    cursor.fetchone.return_value = ('John', 25, 'john@example.com')
    cursor.fetchall.return_value = [
        ('John', 25, 'john@example.com'),
        ('Jane', 30, 'jane@example.com')
    ]
    cursor.fetchmany.return_value = [('John', 25, 'john@example.com')]
    cursor.arraysize = 1
    cursor.execute.return_value = None
    cursor.executemany.return_value = None
    return cursor


@pytest.fixture
def mock_connection(mock_db_cursor):
    """Create a mock database connection."""
    connection = Mock()
    connection.interface._paramstyle = 'format'
    connection.placeholder = '%s'
    connection._connection.cursor.return_value = mock_db_cursor
    connection.cursor.return_value = mock_db_cursor
    connection.commit.return_value = None
    connection.rollback.return_value = None
    connection.close.return_value = None
    return connection


@pytest.fixture
def sample_records():
    """Sample test records."""
    return [
        {'name': 'John', 'age': 25, 'email': 'john@example.com', 'signup_date': '2024-01-15'},
        {'name': 'Jane', 'age': 30, 'email': 'jane@example.com', 'signup_date': '2024-01-20'}
    ]


@pytest.fixture(scope='session')
def states_db():
    """Create test database and load states data. Session-scoped for reuse across all tests."""
    # Test database and data paths
    test_dir = Path(__file__).parent
    TEST_DB_PATH = test_dir / 'fixtures' / 'test_states.db'
    TEST_CONFIG_PATH = test_dir / 'test.yml'
    STATES_CSV_PATH = test_dir / 'fixtures' / 'readers' / 'states.csv'

    # Ensure fixtures directory exists
    TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing database
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()

    # Connect using config - use absolute path
    # Since config has relative path, we need to change to tests dir first or use absolute path
    from dbtk.database import Database
    db = Database.create('sqlite', database=str(TEST_DB_PATH))
    cursor = db.cursor()

    # Drop and recreate states table
    cursor.execute("DROP TABLE IF EXISTS states")
    cursor.execute("""
                   CREATE TABLE states
                   (
                       state          TEXT PRIMARY KEY,
                       code           TEXT NOT NULL UNIQUE,
                       capital        TEXT NOT NULL,
                       population     INTEGER,
                       area_sq_mi     INTEGER,
                       admitted       TEXT,
                       sales_tax_rate REAL,
                       region         TEXT
                   )
                   """)
    db.commit()

    # Create validation/lookup tables
    cursor.execute("DROP TABLE IF EXISTS valid_regions")
    cursor.execute("""
                   CREATE TABLE valid_regions
                   (
                       region_name TEXT PRIMARY KEY
                   )
                   """)
    cursor.execute("""
                   INSERT INTO valid_regions (region_name)
                   VALUES ('Northeast'),
                          ('Southeast'),
                          ('Midwest'),
                          ('Southwest'),
                          ('West')
                   """)

    cursor.execute("DROP TABLE IF EXISTS region_codes")
    cursor.execute("""
                   CREATE TABLE region_codes
                   (
                       region TEXT PRIMARY KEY,
                       code   TEXT NOT NULL
                   )
                   """)
    cursor.execute("""
                   INSERT INTO region_codes (region, code)
                   VALUES ('Northeast', 'NE'),
                          ('Southeast', 'SE'),
                          ('Midwest', 'MW'),
                          ('Southwest', 'SW'),
                          ('West', 'W')
                   """)

    db.commit()

    # Load states data using CSVReader, Table, and DataSurge
    states_table = Table('states', {
        'state': {'field': 'state', 'primary_key': True},
        'code': {'field': 'code', 'nullable': False},
        'capital': {'field': 'capital', 'nullable': False},
        'population': {'field': 'population'},
        'area_sq_mi': {'field': 'area_sq_mi'},
        'admitted': {'field': 'admitted'},
        'sales_tax_rate': {'field': 'sales_tax_rate'},
        'region': {'field': 'region'}
    }, cursor=cursor)

    surge = DataSurge(states_table, batch_size=25)

    with open(STATES_CSV_PATH, 'r') as f:
        with CSVReader(f) as reader:
            errors = surge.insert(reader)

    db.commit()

    assert errors == 0
    assert states_table.counts['insert'] == 50  # 50 US states

    yield db

    # Cleanup
    db.close()
    # Optional: Remove database after all tests complete
    # if TEST_DB_PATH.exists():
        # TEST_DB_PATH.unlink()