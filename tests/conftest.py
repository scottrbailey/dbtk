# dbtk_tests/conftest.py
"""
Shared test fixtures and configuration for pytest.
"""

import pytest
import os
from unittest.mock import Mock, patch


# Set test encryption key for all tests
@pytest.fixture(autouse=True)
def set_test_encryption_key():
    """Automatically set test encryption key for all tests."""
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
    connection.interface.__paramstyle = 'format'
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


