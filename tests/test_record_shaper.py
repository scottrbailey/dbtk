# tests/test_record_shaper.py
"""
Tests for dbtk.record.RecordShaper - chainable, single-pass column shaping.
"""

import pytest
from collections import namedtuple

from dbtk.record import Record, RecordShaper


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def make_record_class(*fields):
    cls = type('R', (Record,), {})
    cls.set_fields(list(fields))
    return cls


@pytest.fixture
def RecordCls():
    return make_record_class('id', 'name', 'email')


@pytest.fixture
def sample_records(RecordCls):
    return [
        RecordCls(1, 'Aang', 'aang@avatar.com'),
        RecordCls(2, 'Katara', 'katara@avatar.com'),
        RecordCls(3, 'Sokka', 'sokka@avatar.com'),
    ]


@pytest.fixture
def sample_dicts():
    return [
        {'id': 1, 'name': 'Aang', 'email': 'aang@avatar.com'},
        {'id': 2, 'name': 'Katara', 'email': 'katara@avatar.com'},
        {'id': 3, 'name': 'Sokka', 'email': 'sokka@avatar.com'},
    ]


@pytest.fixture
def NamedTupleCls():
    return namedtuple('Person', ['id', 'name', 'email'])


@pytest.fixture
def sample_namedtuples(NamedTupleCls):
    return [
        NamedTupleCls(1, 'Aang', 'aang@avatar.com'),
        NamedTupleCls(2, 'Katara', 'katara@avatar.com'),
        NamedTupleCls(3, 'Sokka', 'sokka@avatar.com'),
    ]


@pytest.fixture
def sample_lists():
    return [
        [1, 'Aang', 'aang@avatar.com'],
        [2, 'Katara', 'katara@avatar.com'],
        [3, 'Sokka', 'sokka@avatar.com'],
    ]


class CountingIterator:
    """Wraps an iterator and counts how many times __next__ was called."""
    def __init__(self, source):
        self._it = iter(source)
        self.calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.calls += 1
        return next(self._it)


# ---------------------------------------------------------------------------
# Passthrough / laziness
# ---------------------------------------------------------------------------

class TestPassthrough:

    def test_no_op_yields_original_rows_unchanged(self, sample_dicts):
        out = list(RecordShaper(sample_dicts))
        assert out == sample_dicts

    def test_construction_does_not_touch_source(self, sample_dicts):
        counter = CountingIterator(sample_dicts)
        RecordShaper(counter)
        assert counter.calls == 0

    def test_chained_call_peeks_exactly_once(self, sample_dicts):
        counter = CountingIterator(sample_dicts)
        shaper = RecordShaper(counter).select(['name'])
        assert counter.calls == 1
        list(shaper)
        # +1 for the final __next__ that raises StopIteration and ends the loop
        assert counter.calls == len(sample_dicts) + 1

    def test_columns_property_forces_and_caches_peek(self, sample_dicts):
        counter = CountingIterator(sample_dicts)
        shaper = RecordShaper(counter)
        assert shaper.columns == ['id', 'name', 'email']
        assert counter.calls == 1
        # Accessing again does not re-peek
        _ = shaper.columns
        assert counter.calls == 1
        # The peeked row is not lost
        out = list(shaper)
        assert len(out) == len(sample_dicts)


# ---------------------------------------------------------------------------
# select()
# ---------------------------------------------------------------------------

class TestSelect:

    def test_select_and_reorder_from_records(self, sample_records):
        out = list(RecordShaper(sample_records).select(['name', 'id']))
        assert [r.keys() for r in out][0] == ['name', 'id']
        assert out[0].name == 'Aang'
        assert out[0].id == 1

    def test_select_from_dicts(self, sample_dicts):
        out = list(RecordShaper(sample_dicts).select(['email']))
        assert [r['email'] for r in out] == [d['email'] for d in sample_dicts]

    def test_select_from_namedtuples(self, sample_namedtuples):
        out = list(RecordShaper(sample_namedtuples).select(['name']))
        assert [r['name'] for r in out] == [nt.name for nt in sample_namedtuples]

    def test_select_from_plain_tuples_raises_type_error(self, sample_lists):
        with pytest.raises(TypeError):
            list(RecordShaper(sample_lists).select(['name']))

    def test_empty_col_names_raises(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).select([])

    def test_strict_raises_on_unknown_column(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).select(['not_a_column'])

    def test_non_strict_drops_unknown_columns(self, sample_records):
        out = list(RecordShaper(sample_records).select(['name', 'not_a_column'], strict=False))
        assert out[0].keys() == ['name']

    def test_non_strict_all_unknown_raises(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).select(['nope'], strict=False)

    def test_returns_new_instance_original_unaffected(self, sample_records):
        a = RecordShaper(sample_records)
        b = a.select(['id'])
        assert a is not b
        assert list(b)[0].keys() == ['id']


# ---------------------------------------------------------------------------
# exclude()
# ---------------------------------------------------------------------------

class TestExclude:

    def test_excludes_and_preserves_order(self, sample_records):
        out = list(RecordShaper(sample_records).exclude(['name']))
        assert out[0].keys() == ['id', 'email']

    def test_accepts_set(self, sample_records):
        out = list(RecordShaper(sample_records).exclude({'name'}))
        assert out[0].keys() == ['id', 'email']

    def test_empty_col_names_is_noop(self, sample_records):
        out = list(RecordShaper(sample_records).exclude([]))
        assert [r.keys() for r in out] == [['id', 'name', 'email']] * 3

    def test_strict_raises_on_unknown_column(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).exclude(['not_a_column'])

    def test_non_strict_ignores_unknown_column(self, sample_records):
        out = list(RecordShaper(sample_records).exclude(['name', 'not_a_column'], strict=False))
        assert out[0].keys() == ['id', 'email']

    def test_excluding_all_columns_raises(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).exclude(['id', 'name', 'email'])


# ---------------------------------------------------------------------------
# rename()
# ---------------------------------------------------------------------------

class TestRename:

    def test_renames_and_preserves_position(self, sample_records):
        out = list(RecordShaper(sample_records).rename({'name': 'Name'}))
        assert out[0].keys() == ['id', 'Name', 'email']
        assert out[0]['Name'] == 'Aang'

    def test_falsy_mapping_value_is_noop(self, sample_records):
        out = list(RecordShaper(sample_records).rename({'name': '', 'id': None, 'email': 'Email'}))
        assert out[0].keys() == ['id', 'name', 'Email']

    def test_empty_mapping_is_noop(self, sample_records):
        out = list(RecordShaper(sample_records).rename({}))
        assert out == sample_records

    def test_strict_raises_on_unknown_source_column(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).rename({'not_a_column': 'X'})

    def test_non_strict_ignores_unknown_source_column(self, sample_records):
        out = list(RecordShaper(sample_records).rename({'not_a_column': 'X'}, strict=False))
        assert out[0].keys() == ['id', 'name', 'email']

    def test_rename_from_namedtuples(self, sample_namedtuples):
        out = list(RecordShaper(sample_namedtuples).rename({'name': 'Name'}))
        assert out[0].keys() == ['id', 'Name', 'email']
        assert out[0]['Name'] == 'Aang'


# ---------------------------------------------------------------------------
# Chaining validates against the current (not original) schema
# ---------------------------------------------------------------------------

class TestChainedValidation:

    def test_select_after_rename_uses_new_name(self, sample_records):
        out = list(
            RecordShaper(sample_records)
            .rename({'name': 'Name'})
            .select(['Name'])
        )
        assert out[0].keys() == ['Name']
        assert out[0]['Name'] == 'Aang'

    def test_select_after_rename_rejects_old_name(self, sample_records):
        with pytest.raises(ValueError):
            RecordShaper(sample_records).rename({'name': 'full_name'}).select(['name'])

    def test_select_after_rename_matches_new_names_normalized_form(self, sample_records):
        # 'Name' normalizes to 'name', so the renamed column is still reachable that way
        out = list(RecordShaper(sample_records).rename({'name': 'Name'}).select(['name']))
        assert out[0].keys() == ['Name']

    def test_exclude_then_rename_then_select(self, sample_records):
        out = list(
            RecordShaper(sample_records)
            .exclude(['email'])
            .rename({'name': 'Name'})
            .select(['Name'])
        )
        assert [r['Name'] for r in out] == ['Aang', 'Katara', 'Sokka']


# ---------------------------------------------------------------------------
# Original vs normalized names (Record sources)
# ---------------------------------------------------------------------------

@pytest.fixture
def mixed_case_records():
    cls = make_record_class('ssn', 'perdtot_year', 'perdtot_EMPR_AMT')
    return [cls('111', 2025, 10.5), cls('222', 2026, 20.0)]


class TestNormalizedNames:

    def test_select_by_normalized_name_returns_original_name(self, mixed_case_records):
        out = list(RecordShaper(mixed_case_records).select(['perdtot_empr_amt', 'ssn']))
        assert out[0].keys() == ['perdtot_EMPR_AMT', 'ssn']
        assert out[0]['perdtot_EMPR_AMT'] == 10.5

    def test_select_same_column_by_both_names_raises(self, mixed_case_records):
        with pytest.raises(ValueError, match='more than once'):
            RecordShaper(mixed_case_records).select(['perdtot_EMPR_AMT', 'perdtot_empr_amt'])

    def test_original_name_wins_over_normalized(self):
        # 'Start Year' normalizes to 'start_year', which collides with the second
        # column's original name - so that one gets normalized to 'start_year_2'.
        cls = make_record_class('Start Year', 'start_year')
        rows = [cls('from Start Year', 'from start_year')]
        out = list(RecordShaper(rows).select(['start_year']))
        assert out[0].keys() == ['start_year']
        assert out[0]['start_year'] == 'from start_year'

    def test_exclude_by_normalized_name(self, mixed_case_records):
        out = list(RecordShaper(mixed_case_records).exclude(['perdtot_empr_amt']))
        assert out[0].keys() == ['ssn', 'perdtot_year']

    def test_rename_by_normalized_key(self, mixed_case_records):
        col_map = {'perdtot_year': 'year', 'perdtot_empr_amt': 'empr_amt'}
        out = list(RecordShaper(mixed_case_records).rename(col_map))
        assert out[0].keys() == ['ssn', 'year', 'empr_amt']
        assert out[1]['empr_amt'] == 20.0

    def test_rename_same_column_by_both_names_raises(self, mixed_case_records):
        with pytest.raises(ValueError, match='more than once'):
            RecordShaper(mixed_case_records).rename(
                {'perdtot_EMPR_AMT': 'a', 'perdtot_empr_amt': 'b'})

    def test_normalized_names_work_after_a_chained_step(self, mixed_case_records):
        out = list(RecordShaper(mixed_case_records)
                   .exclude(['ssn'])
                   .select(['perdtot_empr_amt']))
        assert out[0].keys() == ['perdtot_EMPR_AMT']

    def test_from_tuples_supports_normalized_names(self):
        out = list(RecordShaper.from_tuples([(1, 'x')], ['Row ID', 'Label']).select(['row_id']))
        assert out[0].keys() == ['Row ID']

    def test_dict_sources_match_exact_keys_only(self):
        rows = [{'perdtot_EMPR_AMT': 1}]
        with pytest.raises(ValueError, match='not found'):
            RecordShaper(rows).select(['perdtot_empr_amt'])


# ---------------------------------------------------------------------------
# rename() with a function
# ---------------------------------------------------------------------------

class TestRenameFunction:

    def test_strip_prefix_any_case(self, mixed_case_records):
        out = list(RecordShaper(mixed_case_records).rename(
            lambda c: c[8:].lower() if c.lower().startswith('perdtot_') else None))
        assert out[0].keys() == ['ssn', 'year', 'empr_amt']
        assert [r['empr_amt'] for r in out] == [10.5, 20.0]

    def test_function_called_once_per_column_at_rename_time(self, mixed_case_records):
        calls = []

        def fn(col):
            calls.append(col)
            return col.upper()

        shaper = RecordShaper(mixed_case_records).rename(fn)
        assert calls == ['ssn', 'perdtot_year', 'perdtot_EMPR_AMT']
        list(shaper)
        assert len(calls) == 3  # not called again per row

    def test_function_that_renames_nothing_is_noop(self, sample_records):
        out = list(RecordShaper(sample_records).rename(lambda c: None))
        assert out == sample_records

    def test_function_returning_same_name_is_noop(self, sample_records):
        out = list(RecordShaper(sample_records).rename(lambda c: c))
        assert out == sample_records

    def test_function_exception_names_the_column(self, sample_records):
        def fn(col):
            if col == 'email':
                raise KeyError('boom')
            return None

        with pytest.raises(ValueError, match="column 'email'") as exc_info:
            RecordShaper(sample_records).rename(fn)
        assert isinstance(exc_info.value.__cause__, KeyError)

    def test_function_returning_non_string_raises_type_error(self, sample_records):
        with pytest.raises(TypeError, match="column 'id'"):
            RecordShaper(sample_records).rename(lambda c: c == 'id')

    def test_function_on_namedtuples(self, sample_namedtuples):
        out = list(RecordShaper(sample_namedtuples).rename(str.upper))
        assert out[0].keys() == ['ID', 'NAME', 'EMAIL']
        assert out[0]['NAME'] == 'Aang'

    def test_function_on_empty_source_yields_nothing(self):
        assert list(RecordShaper([]).rename(str.upper)) == []


# ---------------------------------------------------------------------------
# rename() never clobbers columns, and no-op stages stay single-pass
# ---------------------------------------------------------------------------

class TestRenameCollisions:

    def test_dict_rename_onto_existing_column_raises(self, sample_records):
        with pytest.raises(ValueError, match="same name"):
            RecordShaper(sample_records).rename({'name': 'email'})

    def test_function_rename_collision_raises(self):
        cls = make_record_class('year', 'perdtot_year')
        rows = [cls(1, 2)]
        with pytest.raises(ValueError, match=r"\['year', 'perdtot_year'\] -> 'year'"):
            RecordShaper(rows).rename(lambda c: c.replace('perdtot_', ''))

    def test_swapping_two_names_is_allowed(self, sample_records):
        out = list(RecordShaper(sample_records).rename({'name': 'email', 'email': 'name'}))
        assert out[0].keys() == ['id', 'email', 'name']
        assert out[0]['email'] == 'Aang'


class TestNoOpStages:

    def test_noop_stage_is_a_new_instance(self, sample_records):
        base = RecordShaper(sample_records)
        assert base.rename({}) is not base
        assert base.exclude([]) is not base

    def test_noop_stage_shares_single_pass_state(self, sample_dicts):
        base = RecordShaper(iter(sample_dicts))
        noop = base.rename({})
        assert list(noop) == sample_dicts
        with pytest.raises(RuntimeError):
            list(base)


# ---------------------------------------------------------------------------
# from_tuples()
# ---------------------------------------------------------------------------

class TestFromTuples:

    def test_basic_naming(self, sample_lists):
        out = list(RecordShaper.from_tuples(sample_lists, ['id', 'name', 'email']))
        assert out[0].keys() == ['id', 'name', 'email']
        assert out[0]['name'] == 'Aang'

    def test_drops_filler_column(self):
        rows = [(1, 'x', 'Aang'), (2, 'y', 'Katara')]
        out = list(RecordShaper.from_tuples(rows, ['id', None, 'name']))
        assert out[0].keys() == ['id', 'name']
        assert out[0]['name'] == 'Aang'

    def test_no_names_raises(self):
        with pytest.raises(ValueError):
            RecordShaper.from_tuples([(1, 2)], [None, None])

    def test_row_length_mismatch_raises_lazily(self):
        shaper = RecordShaper.from_tuples([(1, 2)], ['a', 'b', 'c'])  # no raise yet
        with pytest.raises(ValueError):
            list(shaper)

    def test_does_not_peek(self):
        counter = CountingIterator([(1, 'Aang')])
        shaper = RecordShaper.from_tuples(counter, ['id', 'name'])
        assert counter.calls == 0
        assert shaper.columns == ['id', 'name']
        assert counter.calls == 0

    def test_composes_with_select(self, sample_lists):
        out = list(RecordShaper.from_tuples(sample_lists, ['id', 'name', 'email']).select(['name']))
        assert [r['name'] for r in out] == ['Aang', 'Katara', 'Sokka']


# ---------------------------------------------------------------------------
# Single-pass enforcement
# ---------------------------------------------------------------------------

class TestSinglePass:

    def test_reiterating_same_object_raises(self, sample_records):
        shaper = RecordShaper(sample_records)
        list(shaper)
        with pytest.raises(RuntimeError):
            list(shaper)

    def test_partial_iteration_still_marks_consumed(self, sample_records):
        shaper = RecordShaper(sample_records)
        for _ in shaper:
            break
        with pytest.raises(RuntimeError):
            list(shaper)

    def test_fork_after_intermediate_stage_raises_on_second_branch(self, sample_records):
        a = RecordShaper(sample_records)
        b = a.select(['id', 'name'])
        c = b.rename({'name': 'Name'})

        list(b)  # consume via the intermediate stage
        with pytest.raises(RuntimeError):
            list(c)  # c shares the same lineage - must not silently succeed

    def test_root_iteration_after_deriving_child_raises_on_child(self, sample_records):
        a = RecordShaper(sample_records)
        b = a.select(['id'])

        list(a)  # consume via the root directly
        with pytest.raises(RuntimeError):
            list(b)

    def test_independent_roots_do_not_interfere(self, sample_records, sample_dicts):
        a = RecordShaper(sample_records)
        b = RecordShaper(sample_dicts)

        list(a)
        # b is a completely separate lineage - must be unaffected
        assert list(b) == sample_dicts


# ---------------------------------------------------------------------------
# Empty source
# ---------------------------------------------------------------------------

class TestEmptySource:

    def test_select_on_empty_source_yields_nothing_without_validating(self):
        assert list(RecordShaper([]).select(['whatever'])) == []

    def test_exclude_on_empty_source_yields_nothing(self):
        assert list(RecordShaper([]).exclude(['whatever'])) == []

    def test_rename_on_empty_source_yields_nothing(self):
        assert list(RecordShaper([]).rename({'a': 'b'})) == []

    def test_columns_on_empty_source_is_empty_list(self):
        assert RecordShaper([]).columns == []

    def test_chaining_past_empty_source_stays_empty(self):
        assert list(RecordShaper([]).select(['a']).rename({'a': 'b'})) == []


# ---------------------------------------------------------------------------
# Writer interop (read-only smoke test - no writer code touched by this feature)
# ---------------------------------------------------------------------------

class TestWriterInterop:

    def test_to_csv_accepts_a_record_shaper(self, tmp_path, sample_records):
        from dbtk.writers import to_csv
        from dbtk.readers import CSVReader

        output_file = tmp_path / "shaped.csv"
        shaper = RecordShaper(sample_records).select(['name', 'email'])
        to_csv(shaper, output_file, write_headers=True)

        with open(output_file, encoding='utf-8-sig') as f:
            header = f.readline().strip()
        assert header == 'name,email'
