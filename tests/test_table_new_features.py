# tests/test_table_new_features.py
"""Tests for new Table features: field='*' and empty dict shorthand."""

import pytest
from dbtk.database import Database
from dbtk.etl.table import Table


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
def test_schema(cursor):
    """Create test table schema."""
    cursor.execute("""
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT,
            vip_status TEXT,
            discount REAL
        )
    """)
    cursor.connection.commit()
    return 'users'


class TestEmptyDictShorthand:
    """Test empty dict shorthand for auto field mapping."""

    def test_empty_dict_creates_field_mapping(self, cursor, test_schema):
        """Test that empty dict {} defaults field to column name."""
        table = Table('users', {
            'user_id': {},  # Should become {'field': 'user_id'}
            'name': {},
            'age': {}
        }, cursor=cursor)

        # Verify field was set correctly
        assert table.columns['user_id']['field'] == 'user_id'
        assert table.columns['name']['field'] == 'name'
        assert table.columns['age']['field'] == 'age'

    def test_empty_dict_with_transformation(self, cursor, test_schema):
        """Test empty dict shorthand works with other options."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'email': {'fn': lambda x: x.lower() if x else None},  # Has fn, no auto-field
            'name': {}  # Empty dict shorthand
        }, cursor=cursor)

        # email should NOT get auto-field (has fn but no field)
        assert 'field' not in table.columns['email']
        # name should get auto-field
        assert table.columns['name']['field'] == 'name'

    def test_empty_dict_extracts_values(self, cursor, test_schema):
        """Test that empty dict shorthand correctly extracts values from records."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'name': {},  # Auto field='name'
            'email': {}  # Auto field='email'
        }, cursor=cursor)

        # Set values using shorthand field names
        table.set_values({
            'id': 'USER001',
            'name': 'Aang',
            'email': 'aang@avatar.com'
        })

        assert table.values['user_id'] == 'USER001'
        assert table.values['name'] == 'Aang'
        assert table.values['email'] == 'aang@avatar.com'


class TestWholeRecordAccess:
    """Test field='*' for passing whole record to functions."""

    def test_field_asterisk_passes_whole_record(self, cursor, test_schema):
        """Test that field='*' passes entire record to function."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'vip_status': {
                'field': '*',
                'fn': lambda record: 'VIP' if record.get('age', 0) > 65 else 'Regular'
            }
        }, cursor=cursor)

        # Test with VIP age
        table.set_values({'id': 'USER001', 'age': 70})
        assert table.values['vip_status'] == 'VIP'

        # Test with non-VIP age
        table.set_values({'id': 'USER002', 'age': 30})
        assert table.values['vip_status'] == 'Regular'

    def test_field_asterisk_in_pipeline(self, cursor, test_schema):
        """Test that field='*' works in function pipelines."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'discount': {
                'field': '*',
                'fn': [
                    lambda record: 0.25 if record.get('age', 0) > 65 else 0.10,  # First: analyze record
                    lambda x: round(x, 2),  # Second: operate on value
                ]
            }
        }, cursor=cursor)

        table.set_values({'id': 'USER001', 'age': 70})
        assert table.values['discount'] == 0.25

        table.set_values({'id': 'USER002', 'age': 30})
        assert table.values['discount'] == 0.10

    def test_field_asterisk_multi_field_logic(self, cursor, test_schema):
        """Test field='*' with multi-field decision logic."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'vip_status': {
                'field': '*',
                'fn': lambda r: 'VIP' if r.get('age', 0) > 65 or r.get('purchases', 0) > 100 else 'Regular'
            }
        }, cursor=cursor)

        # VIP by age
        table.set_values({'id': 'U1', 'age': 70, 'purchases': 10})
        assert table.values['vip_status'] == 'VIP'

        # VIP by purchases
        table.set_values({'id': 'U2', 'age': 30, 'purchases': 150})
        assert table.values['vip_status'] == 'VIP'

        # Not VIP
        table.set_values({'id': 'U3', 'age': 30, 'purchases': 50})
        assert table.values['vip_status'] == 'Regular'

    def test_field_asterisk_with_required(self, cursor, test_schema):
        """Test that field='*' with fn can satisfy required validation."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'vip_status': {
                'field': '*',
                'fn': lambda r: 'VIP' if r.get('age', 0) > 65 else 'Regular',
                'required': True
            }
        }, cursor=cursor)

        table.set_values({'id': 'USER001', 'age': 70})

        # Should have vip_status value
        assert table.values['vip_status'] == 'VIP'
        # Should be ready for insert (required field is populated by fn)
        assert table.is_ready('insert')


class TestCombinedFeatures:
    """Test combination of new features together."""

    def test_empty_dict_and_field_asterisk_together(self, cursor, test_schema):
        """Test using both empty dict shorthand and field='*' in same table."""
        table = Table('users', {
            'user_id': {'field': 'id', 'primary_key': True},
            'name': {},  # Empty dict shorthand
            'email': {},  # Empty dict shorthand
            'vip_status': {
                'field': '*',
                'fn': lambda r: 'VIP' if r.get('age', 0) > 65 else 'Regular'
            }
        }, cursor=cursor)

        table.set_values({
            'id': 'USER001',
            'name': 'Aang',
            'email': 'aang@avatar.com',
            'age': 112
        })

        assert table.values['user_id'] == 'USER001'
        assert table.values['name'] == 'Aang'
        assert table.values['email'] == 'aang@avatar.com'
        assert table.values['vip_status'] == 'VIP'
