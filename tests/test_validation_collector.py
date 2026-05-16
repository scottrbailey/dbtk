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
    # collect_new / get_new_records
    # ------------------------------------------------------------------

    def test_collect_new_noop_when_not_recently_added(self):
        """collect_new is a no-op when the preceding call found an existing code."""
        collector = ValidationCollector()
        collector.existing['OLD'] = 'Old description'
        collector('OLD')                                   # existing — _recently_added stays False
        collector.collect_new('OLD', desc='Should not appear')

        assert collector.get_new_records('code') == []

    def test_collect_new_annotates_new_code(self):
        """collect_new stores extra fields on a genuinely new code."""
        collector = ValidationCollector()
        collector('CIP01')
        collector.collect_new('CIP01', stvcipc_desc='Computer Science', active='Y')

        records = collector.get_new_records('code')
        assert len(records) == 1
        assert records[0] == {'code': 'CIP01', 'stvcipc_desc': 'Computer Science', 'active': 'Y'}

    def test_collect_new_clears_flag(self):
        """collect_new clears _recently_added so a second call is a no-op."""
        collector = ValidationCollector()
        collector('CIP02')
        collector.collect_new('CIP02', desc='First')
        collector.collect_new('CIP02', desc='Second')   # should be ignored

        assert collector.added['CIP02'] == {'desc': 'First'}

    def test_collect_new_first_annotation_wins(self):
        """Once a code is annotated, subsequent collect_new calls won't overwrite."""
        collector = ValidationCollector()
        collector('CIP03')
        collector.collect_new('CIP03', desc='Physics')
        # Simulate another record with the same code — already in added, not recently_added
        collector('CIP03')                               # existing in added, flag = False
        collector.collect_new('CIP03', desc='Should not overwrite')

        assert collector.added['CIP03'] == {'desc': 'Physics'}

    def test_collect_new_noop_for_existing_code(self):
        """collect_new is a no-op when called for a code in existing (not added)."""
        collector = ValidationCollector()
        collector.existing['E01'] = 'Existing'
        collector('E01')
        collector.collect_new('E01', desc='Should not appear')

        assert collector.get_new_records('code') == []

    def test_recently_added_false_after_existing_code(self):
        """_recently_added is False after processing a known code."""
        collector = ValidationCollector()
        collector.existing['K01'] = 'Known'
        collector('K01')
        assert collector._recently_added is False

    def test_recently_added_true_after_new_code(self):
        """_recently_added is True after processing a new code."""
        collector = ValidationCollector()
        collector('NEW01')
        assert collector._recently_added is True

    def test_recently_added_reset_each_call(self):
        """_recently_added reflects only the most recent __call__."""
        collector = ValidationCollector()
        collector('NEW01')                               # new — True
        assert collector._recently_added is True
        collector.existing['K01'] = 'Known'
        collector('K01')                                 # existing — False
        assert collector._recently_added is False

    def test_get_new_records_unannotated(self):
        """get_new_records returns unannotated codes with only the code field."""
        collector = ValidationCollector()
        collector('CIP05')
        collector('CIP06')

        records = collector.get_new_records('code')
        codes = {r['code'] for r in records}
        assert codes == {'CIP05', 'CIP06'}
        assert all(list(r.keys()) == ['code'] for r in records)

    def test_get_new_records_custom_code_field(self):
        """get_new_records uses the supplied code_field name."""
        collector = ValidationCollector()
        collector('CIP07')
        collector.collect_new('CIP07', stvcipc_desc='Engineering')

        records = collector.get_new_records('stvcipc_code')
        assert records[0]['stvcipc_code'] == 'CIP07'
        assert records[0]['stvcipc_desc'] == 'Engineering'

    def test_get_new_records_multiple_codes(self):
        """get_new_records returns one dict per new code."""
        collector = ValidationCollector()
        collector('A')
        collector.collect_new('A', desc='Alpha')
        collector('B')
        collector.collect_new('B', desc='Beta')

        records = {r['code']: r for r in collector.get_new_records('code')}
        assert records['A']['desc'] == 'Alpha'
        assert records['B']['desc'] == 'Beta'

    def test_added_uses_none_sentinel(self):
        """New codes are stored as None until annotated."""
        collector = ValidationCollector()
        collector('X1')
        assert collector.added['X1'] is None

    def test_collect_new_sets_dict_on_none_sentinel(self):
        """collect_new replaces None sentinel with the fields dict."""
        collector = ValidationCollector()
        collector('X2')
        assert collector.added['X2'] is None
        collector.collect_new('X2', desc='value')
        assert collector.added['X2'] == {'desc': 'value'}

    def test_duplicate_calls_keep_one_entry(self):
        """Calling the collector twice with the same code keeps one added entry."""
        collector = ValidationCollector()
        collector('DUP')
        collector('DUP')

        assert len(collector.added) == 1

    # ------------------------------------------------------------------
    # return_col: existing vs added
    # ------------------------------------------------------------------

    def test_return_col_new_code_returns_none(self):
        """return_col set: new codes return None (field value not yet known)."""
        collector = ValidationCollector(return_col='title')
        result = collector('NEW01')
        assert result is None

    def test_no_return_col_new_code_returns_code(self):
        """return_col=None (default): always returns the raw code."""
        collector = ValidationCollector()
        result = collector('NEW01')
        assert result == 'NEW01'

    def test_return_col_existing_code_returns_field(self):
        """return_col set: existing codes return the named field from the lookup result."""
        collector = ValidationCollector(return_col='title')
        collector.existing['E01'] = {'title': 'Ethnic Studies', 'year': 2020}
        result = collector('E01')
        assert result == 'Ethnic Studies'

    def test_no_return_col_existing_code_returns_code(self):
        """return_col=None: returns raw code even for existing codes."""
        collector = ValidationCollector()
        collector.existing['E01'] = {'title': 'Ethnic Studies'}
        result = collector('E01')
        assert result == 'E01'

    def test_existing_stores_raw_result_not_description(self):
        """existing stores the full lookup result, not just the extracted field."""
        collector = ValidationCollector()
        raw = {'stvcipc_desc': 'Computer Science', 'stvcipc_pub_year': 2020}
        collector.existing['05'] = raw
        assert collector.existing['05'] is raw

    def test_return_col_after_collect_new(self):
        """After collect_new annotates a code, subsequent calls return the named field."""
        collector = ValidationCollector(return_col='title')
        collector('CIP01')                                    # new — returns None
        collector.collect_new('CIP01', title='Computer Science')
        result = collector('CIP01')                           # now annotated
        assert result == 'Computer Science'

    def test_comma_separated_mixed_existing_and_new(self):
        """Comma-separated input with return_col: existing return field, new codes dropped."""
        collector = ValidationCollector(return_col='title')
        collector.existing['A'] = {'title': 'Alpha'}
        collector.existing['B'] = {'title': 'Beta'}
        result = collector('A,NEW,B')
        assert result == 'Alpha,Beta'

    def test_comma_separated_no_return_col(self):
        """return_col=None: all codes returned in comma-separated input."""
        collector = ValidationCollector()
        collector.existing['A'] = {'title': 'Alpha'}
        result = collector('A,NEW,B')
        assert result == 'A,NEW,B'

    def test_get_valid_mapping_extracts_col(self):
        """get_valid_mapping returns the return_col field from each stored result."""
        collector = ValidationCollector(return_col='title')
        collector.existing['05'] = {'title': 'Ethnic Studies', 'year': 2020}
        collector.existing['11'] = {'title': 'Computer Science'}
        mapping = collector.get_valid_mapping()
        assert mapping['05'] == 'Ethnic Studies'
        assert mapping['11'] == 'Computer Science'

    def test_get_all_mapping_parity(self):
        """get_all_mapping: return_col field for existing, None for unannotated new codes."""
        collector = ValidationCollector(return_col='title')
        collector.existing['E'] = {'title': 'Existing'}
        collector('N')
        mapping = collector.get_all_mapping()
        assert mapping['E'] == 'Existing'
        assert mapping['N'] is None

    def test_get_all_mapping_annotated_new_code(self):
        """get_all_mapping returns return_col field for annotated new codes."""
        collector = ValidationCollector(return_col='title')
        collector('N')
        collector.collect_new('N', title='New Thing')
        mapping = collector.get_all_mapping()
        assert mapping['N'] == 'New Thing'
