# tests/test_record.py
"""
Tests for the Record and FixedWidthRecord classes.
"""

import pytest
from dbtk.record import Record, FixedWidthRecord, fixed_record_factory
from dbtk.utils import normalize_field_name, FixedColumn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_record_class(*fields):
    """Create a fresh Record subclass with the given field names."""
    cls = type('R', (Record,), {})
    cls.set_fields(list(fields))
    return cls


def make_record(*fields):
    """Create a Record subclass and return a factory function for instances."""
    cls = make_record_class(*fields)
    return cls


# ---------------------------------------------------------------------------
# set_fields / normalization
# ---------------------------------------------------------------------------

class TestSetFields:

    def test_fields_stored(self):
        R = make_record_class('id', 'name', 'email')
        assert R._fields == ['id', 'name', 'email']

    def test_normalized_fields(self):
        R = make_record_class('Employee ID', 'FULL NAME', 'Start Year')
        assert R._fields_normalized == ['employee_id', 'full_name', 'start_year']

    def test_field_len_cached(self):
        R = make_record_class('a', 'b', 'c')
        assert R._field_len == 3

    def test_duplicate_columns_get_suffix(self):
        R = make_record_class('Status', 'STATUS', 'status!')
        assert R._fields_normalized == ['status', 'status_2', 'status_3']

    def test_duplicate_original_names_renamed(self):
        # Simple case: two identical originals → second gets _2
        R = make_record_class('id', 'id', 'name')
        assert R._fields == ['id', 'id_2', 'name']

    def test_duplicate_original_skips_existing(self):
        # Edge case from user: ['id', 'id', 'id_2'] → ['id', 'id_3', 'id_2']
        # 'id_2' is already taken by the third column, so second 'id' must become 'id_3'
        R = make_record_class('id', 'id', 'id_2')
        assert R._fields == ['id', 'id_3', 'id_2']

    def test_duplicate_original_warns(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger='dbtk.record'):
            make_record_class('x', 'x')
        assert any('x' in msg and 'renamed' in msg for msg in caplog.messages)

    def test_reserved_name_collision_gets_suffix(self):
        R = make_record_class('id', 'values', 'name')
        # 'values' collides with Record.values() — should be renamed
        assert 'values' not in R._fields_normalized
        assert R._fields_normalized[1] == 'values_2'

    def test_reserved_collision_warns(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger='dbtk.record'):
            make_record_class('id', 'get', 'name')
        assert any('get' in msg for msg in caplog.messages)

    def test_leading_underscore_preserved(self):
        R = make_record_class('_internal')
        assert R._fields_normalized == ['_internal']

    def test_synthetic_leading_underscore_stripped(self):
        R = make_record_class('#code')
        assert R._fields_normalized == ['code']

    def test_digit_prefix(self):
        R = make_record_class('2025_sales')
        assert R._fields_normalized == ['n2025_sales']

    def test_subclasses_are_independent(self):
        R1 = make_record_class('a', 'b')
        R2 = make_record_class('x', 'y', 'z')
        assert R1._fields == ['a', 'b']
        assert R2._fields == ['x', 'y', 'z']


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:

    def test_positional_args(self):
        R = make_record_class('id', 'name', 'email')
        r = R(1, 'Alice', 'alice@example.com')
        assert list.__getitem__(r, 0) == 1
        assert list.__getitem__(r, 1) == 'Alice'

    def test_keyword_args(self):
        R = make_record_class('id', 'name', 'email')
        r = R(id=1, name='Alice', email='alice@example.com')
        assert r['id'] == 1
        assert r['name'] == 'Alice'

    def test_keyword_by_normalized_name(self):
        R = make_record_class('Employee ID')
        r = R(employee_id=99)
        assert r['Employee ID'] == 99

    def test_too_many_positional_args_truncated(self):
        R = make_record_class('a', 'b')
        r = R(1, 2, 3, 4)   # extra args silently dropped
        assert r['a'] == 1
        assert r['b'] == 2

    def test_missing_args_default_to_none(self):
        R = make_record_class('a', 'b', 'c')
        r = R(1)
        assert r['b'] is None
        assert r['c'] is None


# ---------------------------------------------------------------------------
# Access
# ---------------------------------------------------------------------------

class TestAccess:

    def setup_method(self):
        R = make_record_class('Employee ID', 'name', 'score')
        self.record = R(42, 'Aang', 9.5)

    def test_getitem_original_name(self):
        assert self.record['Employee ID'] == 42

    def test_getitem_normalized_name(self):
        assert self.record['employee_id'] == 42

    def test_getattr(self):
        assert self.record.employee_id == 42
        assert self.record.name == 'Aang'

    def test_index_access(self):
        assert self.record[0] == 42
        assert self.record[1] == 'Aang'

    def test_negative_index(self):
        assert self.record[-1] == 9.5

    def test_slice(self):
        assert self.record[:2] == [42, 'Aang']

    def test_missing_key_raises(self):
        with pytest.raises(KeyError):
            _ = self.record['nonexistent']

    def test_missing_attr_raises(self):
        with pytest.raises(AttributeError):
            _ = self.record.nonexistent

    def test_get_existing(self):
        assert self.record.get('name') == 'Aang'

    def test_get_missing_returns_default(self):
        assert self.record.get('phone', 'N/A') == 'N/A'

    def test_get_missing_returns_none_by_default(self):
        assert self.record.get('phone') is None

    def test_contains_original(self):
        assert 'Employee ID' in self.record

    def test_contains_normalized(self):
        assert 'employee_id' in self.record

    def test_not_contains(self):
        assert 'phone' not in self.record


# ---------------------------------------------------------------------------
# Dict-like interface
# ---------------------------------------------------------------------------

class TestDictInterface:

    def setup_method(self):
        R = make_record_class('id', 'name', 'city')
        self.R = R
        self.record = R(1, 'Bob', 'Portland')

    def test_keys_original(self):
        assert self.record.keys() == ['id', 'name', 'city']

    def test_keys_normalized(self):
        R = make_record_class('Employee ID', 'Full Name')
        r = R(1, 'Alice')
        assert r.keys(normalized=True) == ['employee_id', 'full_name']

    def test_values(self):
        assert tuple(self.record.values()) == (1, 'Bob', 'Portland')

    def test_items_original(self):
        assert list(self.record.items()) == [('id', 1), ('name', 'Bob'), ('city', 'Portland')]

    def test_items_normalized(self):
        R = make_record_class('Employee ID')
        r = R(99)
        assert list(r.items(normalized=True)) == [('employee_id', 99)]

    def test_to_dict(self):
        assert self.record.to_dict() == {'id': 1, 'name': 'Bob', 'city': 'Portland'}

    def test_to_dict_normalized(self):
        R = make_record_class('Employee ID', 'Full Name')
        r = R(1, 'Alice')
        assert r.to_dict(normalized=True) == {'employee_id': 1, 'full_name': 'Alice'}

    def test_len(self):
        assert len(self.record) == 3

    def test_iter_yields_values(self):
        assert list(self.record) == [1, 'Bob', 'Portland']

    def test_str(self):
        s = str(self.record)
        assert 'Bob' in s
        assert 'Portland' in s

    def test_repr(self):
        r = repr(self.record)
        assert '1' in r
        assert 'Bob' in r


# ---------------------------------------------------------------------------
# Mutation
# ---------------------------------------------------------------------------

class TestMutation:

    def setup_method(self):
        R = make_record_class('id', 'name', 'status')
        self.record = R(1, 'Alice', 'pending')

    def test_setitem_by_original(self):
        self.record['status'] = 'active'
        assert self.record['status'] == 'active'

    def test_setitem_by_normalized(self):
        R = make_record_class('Employee ID')
        r = R(0)
        r['employee_id'] = 99
        assert r['Employee ID'] == 99

    def test_setattr(self):
        self.record.status = 'done'
        assert self.record['status'] == 'done'

    def test_setitem_by_index(self):
        self.record[0] = 999
        assert self.record['id'] == 999

    def test_update_from_dict(self):
        self.record.update({'name': 'Bob', 'status': 'active'})
        assert self.record['name'] == 'Bob'
        assert self.record['status'] == 'active'

    def test_update_from_kwargs(self):
        self.record.update(name='Carol', status='inactive')
        assert self.record['name'] == 'Carol'

    def test_update_from_iterable(self):
        self.record.update([('name', 'Dave'), ('status', 'active')])
        assert self.record['name'] == 'Dave'

    def test_coalesce_fills_none(self):
        R = make_record_class('a', 'b', 'c')
        r = R(1, None, '')
        r.coalesce({'b': 2, 'c': 3})
        assert r['b'] == 2
        assert r['c'] == 3

    def test_coalesce_preserves_existing(self):
        R = make_record_class('a', 'b')
        r = R(1, 'keep')
        r.coalesce({'a': 99, 'b': 'overwrite'})
        assert r['a'] == 1       # not None, unchanged
        assert r['b'] == 'keep'  # not None, unchanged

    def test_coalesce_kwargs(self):
        R = make_record_class('a', 'b')
        r = R(None, None)
        r.coalesce(a=10, b=20)
        assert r['a'] == 10
        assert r['b'] == 20


# ---------------------------------------------------------------------------
# Runtime-added fields
# ---------------------------------------------------------------------------

class TestAddedFields:

    def setup_method(self):
        R = make_record_class('id', 'name')
        self.record = R(1, 'Alice')

    def test_add_new_field_by_key(self):
        self.record['extra'] = 'bonus'
        assert self.record['extra'] == 'bonus'

    def test_add_new_field_by_attr(self):
        self.record.computed = 42
        assert self.record['computed'] == 42

    def test_added_field_appears_in_keys(self):
        self.record['tag'] = 'vip'
        assert 'tag' in self.record.keys()

    def test_added_field_appears_in_values(self):
        self.record['tag'] = 'vip'
        assert 'vip' in self.record.values()

    def test_added_field_appears_in_items(self):
        self.record['tag'] = 'vip'
        assert ('tag', 'vip') in list(self.record.items())

    def test_update_added_field(self):
        self.record['tag'] = 'vip'
        self.record['tag'] = 'premium'
        assert self.record['tag'] == 'premium'

    def test_pop_added_field(self):
        self.record['tag'] = 'vip'
        val = self.record.pop('tag')
        assert val == 'vip'
        assert 'tag' not in self.record


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

class TestDeletion:

    def setup_method(self):
        R = make_record_class('id', 'name', 'temp')
        self.record = R(1, 'Alice', 'throwaway')

    def test_del_by_key(self):
        del self.record['temp']
        assert 'temp' not in self.record

    def test_del_by_attr(self):
        del self.record.temp
        assert 'temp' not in self.record

    def test_pop_returns_value(self):
        val = self.record.pop('temp')
        assert val == 'throwaway'

    def test_pop_missing_with_default(self):
        assert self.record.pop('nonexistent', None) is None

    def test_pop_missing_without_default_raises(self):
        with pytest.raises(KeyError):
            self.record.pop('nonexistent')

    def test_deleted_field_not_in_keys(self):
        del self.record['temp']
        assert 'temp' not in self.record.keys()

    def test_deleted_field_not_in_len(self):
        del self.record['temp']
        assert len(self.record) == 2

    def test_access_deleted_field_raises(self):
        del self.record['temp']
        with pytest.raises(KeyError, match="deleted"):
            _ = self.record['temp']

    def test_revive_deleted_field_by_assignment(self):
        del self.record['temp']
        self.record['temp'] = 'revived'
        assert self.record['temp'] == 'revived'
        assert 'temp' in self.record

    def test_delete_nonexistent_raises(self):
        with pytest.raises(KeyError):
            del self.record['nonexistent']


# ---------------------------------------------------------------------------
# Mutable schema enforcement
# ---------------------------------------------------------------------------

class TestMutableSchema:

    def test_mutable_schema_default_true(self):
        R = make_record_class('a', 'b')
        assert R._mutable_schema is True

    def test_fixed_schema_blocks_add(self):
        R = make_record_class('a', 'b')
        R._mutable_schema = False
        r = R(1, 2)
        with pytest.raises(TypeError, match="_mutable_schema"):
            r['new_field'] = 'x'

    def test_fixed_schema_blocks_delete(self):
        R = make_record_class('a', 'b')
        R._mutable_schema = False
        r = R(1, 2)
        with pytest.raises(TypeError, match="_mutable_schema"):
            del r['a']

    def test_fixed_schema_allows_update_existing(self):
        R = make_record_class('a', 'b')
        R._mutable_schema = False
        r = R(1, 2)
        r['a'] = 99   # updating existing field is always allowed
        assert r['a'] == 99


# ---------------------------------------------------------------------------
# copy()
# ---------------------------------------------------------------------------

class TestCopy:

    def setup_method(self):
        R = make_record_class('id', 'name', 'tags')
        self.record = R(1, 'Alice', ['a', 'b'])
        self.record['extra'] = 'bonus'
        del self.record['tags']

    def test_copy_is_same_class(self):
        assert type(self.record.copy()) is type(self.record)

    def test_copy_values_match(self):
        c = self.record.copy()
        assert c['id'] == 1
        assert c['name'] == 'Alice'

    def test_copy_added_fields_preserved(self):
        c = self.record.copy()
        assert c['extra'] == 'bonus'

    def test_copy_deleted_fields_preserved(self):
        c = self.record.copy()
        assert 'tags' not in c

    def test_copy_is_independent(self):
        c = self.record.copy()
        c['name'] = 'Bob'
        assert self.record['name'] == 'Alice'


# ---------------------------------------------------------------------------
# _get_reserved MRO merging
# ---------------------------------------------------------------------------

class TestGetReserved:

    def test_base_reserved_included(self):
        reserved = Record._get_reserved()
        assert 'get' in reserved
        assert 'values' in reserved
        assert '_fields' in reserved

    def test_subclass_reserved_merges_parent(self):
        reserved = FixedWidthRecord._get_reserved()
        # FixedWidthRecord-specific
        assert 'to_line' in reserved
        assert '_columns' in reserved
        # Inherited from Record
        assert 'get' in reserved
        assert 'values' in reserved

    def test_custom_subclass_merges(self):
        class MyRecord(Record):
            _RESERVED = frozenset({'my_method'})

        reserved = MyRecord._get_reserved()
        assert 'my_method' in reserved
        assert 'get' in reserved     # from Record


# ---------------------------------------------------------------------------
# normalize_field_name
# ---------------------------------------------------------------------------

class TestNormalizeFieldName:

    def test_spaces_replaced(self):
        assert normalize_field_name('Start Year') == 'start_year'

    def test_trailing_special_stripped(self):
        assert normalize_field_name('Start Year!') == 'start_year'

    def test_bang_prefix_stripped(self):
        assert normalize_field_name('!Status') == 'status'

    def test_hash_prefix_stripped(self):
        assert normalize_field_name('#Term Code') == 'term_code'

    def test_dollar_prefix_stripped(self):
        assert normalize_field_name('$Secret_Code!') == 'secret_code'

    def test_explicit_leading_underscore_preserved(self):
        assert normalize_field_name('_Secret_Code!') == '_secret_code'

    def test_plain_underscore_preserved(self):
        assert normalize_field_name('_row_num') == '_row_num'

    def test_double_leading_underscore_becomes_single(self):
        assert normalize_field_name('__id__') == '_id'

    def test_leading_digit_prefixed(self):
        assert normalize_field_name('2025 Sales') == 'n2025_sales'

    def test_empty_string(self):
        assert normalize_field_name('') == 'col'

    def test_uppercase(self):
        assert normalize_field_name('EMPLOYEE_ID') == 'employee_id'

    def test_consecutive_specials_collapse(self):
        assert normalize_field_name('a  --  b') == 'a_b'


# ---------------------------------------------------------------------------
# fixed_record_factory class factory
# ---------------------------------------------------------------------------

class TestFixedRecord:

    def test_returns_fixed_width_record_subclass(self):
        Rec = fixed_record_factory([('code', 2), ('amount', 10)])
        assert issubclass(Rec, FixedWidthRecord)

    def test_default_class_name(self):
        Rec = fixed_record_factory([('code', 2)])
        assert Rec.__name__ == 'FixedRecord'

    def test_custom_class_name(self):
        Rec = fixed_record_factory([('code', 2)], name='AchDetail')
        assert Rec.__name__ == 'AchDetail'

    def test_tuple_positions_sequential(self):
        Rec = fixed_record_factory([('a', 3), ('b', 5), ('c', 2)])
        cols = Rec._columns
        assert cols[0].start_pos == 1 and cols[0].end_pos == 3
        assert cols[1].start_pos == 4 and cols[1].end_pos == 8
        assert cols[2].start_pos == 9 and cols[2].end_pos == 10

    def test_line_len_computed(self):
        Rec = fixed_record_factory([('a', 3), ('b', 5)])
        assert Rec._line_len == 8

    def test_fields_set(self):
        Rec = fixed_record_factory([('record_type', 1), ('amount', 10)])
        assert Rec._fields == ['record_type', 'amount']

    def test_fixed_column_passthrough(self):
        col = FixedColumn('routing', 1, 9, column_type='int')
        Rec = fixed_record_factory([col])
        assert Rec._columns[0] is col
        assert Rec._line_len == 9

    def test_mixed_tuple_and_fixed_column(self):
        # FixedColumn at explicit pos; tuple picks up after it
        col = FixedColumn('record_type', 1, 1)
        Rec = fixed_record_factory([col, ('priority', 2), ('routing', 9)])
        cols = Rec._columns
        assert cols[0].start_pos == 1 and cols[0].end_pos == 1
        assert cols[1].start_pos == 2 and cols[1].end_pos == 3
        assert cols[2].start_pos == 4 and cols[2].end_pos == 12

    def test_to_line_text_left_aligned(self):
        Rec = fixed_record_factory([('name', 10)])
        r = Rec('Alice')
        assert r.to_line() == 'Alice     '

    def test_to_line_int_right_aligned_zero_padded(self):
        Rec = fixed_record_factory([FixedColumn('amount', 1, 10, column_type='int')])
        r = Rec(42)
        assert r.to_line() == '0000000042'

    def test_mutable_schema_false(self):
        Rec = fixed_record_factory([('code', 2)])
        r = Rec('AB')
        with pytest.raises(TypeError):
            r['new_field'] = 'x'

    def test_to_line_full_line(self):
        Rec = fixed_record_factory([
            ('record_type', 1),
            ('priority',    2),
            ('routing',     9),
        ], name='AchTest')
        r = Rec('6', '22', '123456789')
        assert r.to_line() == '622123456789'
