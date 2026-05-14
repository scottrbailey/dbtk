# tests/test_validation_collector.py
"""
Tests for ValidationCollector class.
"""

import pytest
from dbtk.etl.managers import ValidationCollector


class TestValidationCollector:
    """Tests for ValidationCollector."""

    def test_contains_in_added(self):
        """Test __contains__ for values in added set."""
        collector = ValidationCollector()

        # Add some values
        collector('value1')
        collector('value2')
        collector('value3')

        # Test membership
        assert 'value1' in collector
        assert 'value2' in collector
        assert 'value3' in collector
        assert 'value4' not in collector

    def test_contains_in_existing(self):
        """Test __contains__ for values in existing dict."""
        collector = ValidationCollector()

        # Manually add to existing (simulating preloaded lookup)
        collector.existing['code1'] = 'Description 1'
        collector.existing['code2'] = 'Description 2'

        # Test membership
        assert 'code1' in collector
        assert 'code2' in collector
        assert 'code3' not in collector

    def test_contains_mixed(self):
        """Test __contains__ with values in both existing and added."""
        collector = ValidationCollector()

        # Add to existing (preloaded)
        collector.existing['code1'] = 'Description 1'

        # Add new values
        collector('code2')
        collector('code3')

        # Test membership for both
        assert 'code1' in collector  # In existing
        assert 'code2' in collector  # In added
        assert 'code3' in collector  # In added
        assert 'code4' not in collector

    def test_get_all_only_added(self):
        """Test get_all() with only added values."""
        collector = ValidationCollector()

        collector('value1')
        collector('value2')
        collector('value3')

        all_codes = collector.get_all()
        assert isinstance(all_codes, set)
        assert all_codes == {'value1', 'value2', 'value3'}

    def test_get_all_only_existing(self):
        """Test get_all() with only existing values."""
        collector = ValidationCollector()

        collector.existing['code1'] = 'Desc 1'
        collector.existing['code2'] = 'Desc 2'

        all_codes = collector.get_all()
        assert isinstance(all_codes, set)
        assert all_codes == {'code1', 'code2'}

    def test_get_all_mixed(self):
        """Test get_all() with both existing and added values."""
        collector = ValidationCollector()

        # Add to existing
        collector.existing['code1'] = 'Desc 1'
        collector.existing['code2'] = 'Desc 2'

        # Add new values
        collector('code3')
        collector('code4')

        all_codes = collector.get_all()
        assert isinstance(all_codes, set)
        assert all_codes == {'code1', 'code2', 'code3', 'code4'}

    def test_get_all_empty(self):
        """Test get_all() with empty collector."""
        collector = ValidationCollector()

        all_codes = collector.get_all()
        assert isinstance(all_codes, set)
        assert all_codes == set()

    def test_get_all_returns_new_set(self):
        """Test that get_all() returns a new set each time."""
        collector = ValidationCollector()

        collector('value1')

        set1 = collector.get_all()
        set2 = collector.get_all()

        assert set1 == set2
        assert set1 is not set2  # Different objects

    def test_contains_with_none(self):
        """Test __contains__ with None value."""
        collector = ValidationCollector()

        collector('value1')

        assert None not in collector

    def test_get_all_with_duplicates(self):
        """Test get_all() handles duplicates correctly."""
        collector = ValidationCollector()

        # Add same value multiple times
        collector('value1')
        collector('value1')
        collector('value2')

        all_codes = collector.get_all()
        assert all_codes == {'value1', 'value2'}

    def test_integration_with_filter(self):
        """Test ValidationCollector with Reader.filter() pattern."""
        collector = ValidationCollector()

        # Simulate collecting valid IDs
        valid_ids = ['id1', 'id2', 'id3', 'id4']
        for id_ in valid_ids:
            collector(id_)

        # Simulate filtering records
        test_records = [
            {'id': 'id1', 'name': 'Alice'},
            {'id': 'id5', 'name': 'Bob'},
            {'id': 'id3', 'name': 'Charlie'},
            {'id': 'id7', 'name': 'Diana'},
        ]

        filtered = [r for r in test_records if r['id'] in collector]

        assert len(filtered) == 2
        assert filtered[0]['name'] == 'Alice'
        assert filtered[1]['name'] == 'Charlie'

    def test_integration_with_polars_pattern(self):
        """Test ValidationCollector with polars is_in() pattern."""
        collector = ValidationCollector()

        # Simulate mixed existing and new
        collector.existing['id1'] = 'Existing 1'
        collector.existing['id2'] = 'Existing 2'
        collector('id3')
        collector('id4')

        # Get all codes for polars filtering
        all_codes = collector.get_all()

        # Simulate polars filter
        test_ids = ['id1', 'id3', 'id5', 'id6']
        filtered_ids = [id_ for id_ in test_ids if id_ in all_codes]

        assert filtered_ids == ['id1', 'id3']

    # ------------------------------------------------------------------
    # annotate_new / get_new_records
    # ------------------------------------------------------------------

    def test_annotate_new_with_kwargs(self):
        """annotate_new attaches extra fields to a new code via kwargs."""
        collector = ValidationCollector()
        collector('CIP01')
        collector.annotate_new('CIP01', desc='Computer Science', active='Y')

        records = collector.get_new_records('code')
        assert len(records) == 1
        assert records[0] == {'code': 'CIP01', 'desc': 'Computer Science', 'active': 'Y'}

    def test_annotate_new_with_dict(self):
        """annotate_new accepts a data dict."""
        collector = ValidationCollector()
        collector('CIP02')
        collector.annotate_new('CIP02', data={'desc': 'Biology'})

        records = collector.get_new_records('code')
        assert records[0]['desc'] == 'Biology'

    def test_annotate_new_merges(self):
        """Successive annotate_new calls merge fields."""
        collector = ValidationCollector()
        collector('CIP03')
        collector.annotate_new('CIP03', desc='Physics')
        collector.annotate_new('CIP03', active='Y')

        records = collector.get_new_records('code')
        assert records[0] == {'code': 'CIP03', 'desc': 'Physics', 'active': 'Y'}

    def test_annotate_new_ignores_existing_codes(self):
        """annotate_new is a no-op for codes not in added."""
        collector = ValidationCollector()
        collector.existing['OLD01'] = 'Old description'
        collector.annotate_new('OLD01', desc='Should not appear')

        assert collector.get_new_records('code') == []

    def test_get_new_records_custom_code_field(self):
        """get_new_records uses the supplied code_field name."""
        collector = ValidationCollector()
        collector('CIP04')
        collector.annotate_new('CIP04', stvcipc_desc='Engineering')

        records = collector.get_new_records('stvcipc_code')
        assert records[0]['stvcipc_code'] == 'CIP04'
        assert records[0]['stvcipc_desc'] == 'Engineering'

    def test_get_new_records_unannotated(self):
        """get_new_records works for codes with no extra annotations."""
        collector = ValidationCollector()
        collector('CIP05')
        collector('CIP06')

        records = collector.get_new_records('code')
        codes = {r['code'] for r in records}
        assert codes == {'CIP05', 'CIP06'}
        assert all(list(r.keys()) == ['code'] for r in records)

    def test_get_new_records_multiple_codes(self):
        """get_new_records returns one dict per new code."""
        collector = ValidationCollector()
        collector('A')
        collector('B')
        collector.annotate_new('A', desc='Alpha')
        collector.annotate_new('B', desc='Beta')

        records = {r['code']: r for r in collector.get_new_records('code')}
        assert records['A']['desc'] == 'Alpha'
        assert records['B']['desc'] == 'Beta'

    def test_added_is_dict_not_set(self):
        """Internal added attribute is a dict keyed by code."""
        collector = ValidationCollector()
        collector('X1')
        assert isinstance(collector.added, dict)
        assert 'X1' in collector.added

    def test_annotate_new_does_not_duplicate_on_repeated_call(self):
        """Calling the collector twice with the same code keeps one entry."""
        collector = ValidationCollector()
        collector('DUP')
        collector('DUP')
        collector.annotate_new('DUP', desc='Duplicate code')

        records = collector.get_new_records('code')
        assert len(records) == 1
