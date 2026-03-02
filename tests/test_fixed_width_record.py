# tests/test_fixed_width_record.py
"""
Tests for FixedWidthRecord.set_fields() and to_line().

Covers the splice-based to_line() implementation: positional placement,
gap preservation, out-of-order column definitions, alignment/padding,
overflow handling, and reader round-trip fidelity.
"""

import pytest
from pathlib import Path

from dbtk.record import FixedWidthRecord
from dbtk.utils import FixedColumn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_class(columns):
    """Return a fresh FixedWidthRecord subclass configured with *columns*."""
    cls = type('FW', (FixedWidthRecord,), {})
    cls.set_fields(columns)
    return cls


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def contiguous_cols():
    """Three contiguous columns covering positions 1-21."""
    return [
        FixedColumn('code',   1,  1),
        FixedColumn('name',   2, 11),
        FixedColumn('amount', 12, 21, 'int'),
    ]


@pytest.fixture
def contiguous_class(contiguous_cols):
    return make_class(contiguous_cols)


# ---------------------------------------------------------------------------
# set_fields — class attribute tests
# ---------------------------------------------------------------------------

class TestSetFields:
    def test_widths(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        assert cls._widths == [1, 10, 10]

    def test_start_indices(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        assert cls._start_indices == [0, 1, 11]   # 1-based pos → 0-based idx

    def test_line_len(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        assert cls._line_len == 21

    def test_alignment_defaults(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        # text -> 'l', text -> 'l', int -> 'r'
        assert cls._alignment == 'llr'

    def test_pad_chars_defaults(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        # text -> ' ', text -> ' ', int -> '0'
        assert cls._pad_chars == '  0'

    def test_explicit_alignment_override(self):
        cols = [
            FixedColumn('a', 1, 5, alignment='r'),
            FixedColumn('b', 6, 10, alignment='c'),
            FixedColumn('c', 11, 15, alignment='L'),   # case-insensitive
        ]
        cls = make_class(cols)
        assert cls._alignment == 'rcl'

    def test_explicit_pad_char_override(self):
        cols = [FixedColumn('x', 1, 4, pad_char='*')]
        cls = make_class(cols)
        assert cls._pad_chars == '*'

    def test_empty_fields(self):
        cls = make_class([])
        assert cls._widths == []
        assert cls._start_indices == []
        assert cls._line_len == 0
        assert cls._alignment == ''
        assert cls._pad_chars == ''

    def test_subclasses_are_independent(self):
        """Two subclasses must not share class-attribute state."""
        A = make_class([FixedColumn('x', 1, 5)])
        B = make_class([FixedColumn('y', 1, 10)])
        assert A._widths == [5]
        assert B._widths == [10]
        assert A._line_len == 5
        assert B._line_len == 10


# ---------------------------------------------------------------------------
# to_line — basic formatting
# ---------------------------------------------------------------------------

class TestToLineBasic:
    def test_left_align_text(self, contiguous_class):
        r = contiguous_class('A', 'Hi', '42')
        line = r.to_line()
        assert line[1:11] == 'Hi        '    # 'name' is 10 chars wide

    def test_right_align_numeric(self, contiguous_class):
        r = contiguous_class('A', 'Hi', '42')
        line = r.to_line()
        assert line[11:21] == '0000000042'

    def test_exact_fit_value(self, contiguous_class):
        r = contiguous_class('X', 'ABCDEFGHIJ', '1234567890')
        line = r.to_line()
        assert line == 'XABCDEFGHIJ1234567890'

    def test_total_length(self, contiguous_class):
        r = contiguous_class('A', 'B', '0')
        assert len(r.to_line()) == 21

    def test_center_alignment(self):
        cols = [FixedColumn('label', 1, 11, alignment='c')]
        cls = make_class(cols)
        r = cls('hi')
        line = r.to_line()
        assert line == 'hi'.center(11, ' ')   # delegate to Python — avoids off-by-one counting
        assert len(line) == 11

    def test_empty_string_value(self, contiguous_class):
        r = contiguous_class('', '', '')
        line = r.to_line()
        assert line == ' ' + ' ' * 10 + '0' * 10

    def test_none_value_treated_as_empty(self, contiguous_class):
        r = contiguous_class(None, None, None)
        line_none = r.to_line()
        r2 = contiguous_class('', '', '')
        assert line_none == r2.to_line()


# ---------------------------------------------------------------------------
# to_line — gap and positional correctness
# ---------------------------------------------------------------------------

class TestToLineGaps:
    def test_gap_fills_with_spaces(self):
        # cols cover pos 1-2 and 7-10; positions 3-6 are a gap
        cols = [
            FixedColumn('type', 1, 2),
            FixedColumn('data', 7, 10),
        ]
        cls = make_class(cols)
        r = cls('AB', 'XY')
        line = r.to_line()
        assert line == 'AB    XY  '
        assert line[2:6] == '    '     # gap is spaces

    def test_gap_size_matches_line_len(self):
        cols = [
            FixedColumn('a', 1, 3),
            FixedColumn('b', 8, 10),
        ]
        cls = make_class(cols)
        assert cls._line_len == 10
        r = cls('XX', 'Y')
        assert len(r.to_line()) == 10

    def test_first_col_not_at_position_one(self):
        # Column starts at pos 5
        cols = [FixedColumn('id', 5, 8)]
        cls = make_class(cols)
        assert cls._line_len == 8
        r = cls('AB')
        line = r.to_line()
        assert line == '    AB  '
        assert line[:4] == '    '     # leading gap


# ---------------------------------------------------------------------------
# to_line — out-of-order column definitions
# ---------------------------------------------------------------------------

class TestToLineOutOfOrder:
    def test_reversed_definitions_produce_correct_output(self):
        cols = [
            FixedColumn('last',  6, 10),
            FixedColumn('first', 1, 5),
        ]
        cls = make_class(cols)
        # positional record args match column definition order: last, first
        r = cls('Smith', 'Alice')
        line = r.to_line()
        assert line == 'AliceSmith'
        assert line[:5] == 'Alice'
        assert line[5:] == 'Smith'

    def test_three_columns_shuffled(self):
        # Define in reverse order
        cols = [
            FixedColumn('c', 9, 12),
            FixedColumn('a', 1, 4),
            FixedColumn('b', 5, 8),
        ]
        cls = make_class(cols)
        r = cls('CCCC', 'AAAA', 'BBBB')
        line = r.to_line()
        assert line[0:4]  == 'AAAA'
        assert line[4:8]  == 'BBBB'
        assert line[8:12] == 'CCCC'


# ---------------------------------------------------------------------------
# to_line — overflow handling
# ---------------------------------------------------------------------------

class TestToLineOverflow:
    def test_overflow_raises_by_default(self, contiguous_class):
        r = contiguous_class('A', 'B' * 20, '0')    # 'name' only 10 wide
        with pytest.raises(ValueError, match='name'):
            r.to_line()

    def test_overflow_truncates_when_requested(self, contiguous_class):
        r = contiguous_class('A', 'ABCDEFGHIJKLMNOP', '0')
        line = r.to_line(truncate_overflow=True)
        assert line[1:11] == 'ABCDEFGHIJ'

    def test_truncate_preserves_other_columns(self, contiguous_class):
        r = contiguous_class('Z', 'TOOLONGVALUE!!', '99')
        line = r.to_line(truncate_overflow=True)
        assert line[0] == 'Z'
        assert line[11:21] == '0000000099'


# ---------------------------------------------------------------------------
# to_line — reader round-trip
# ---------------------------------------------------------------------------

class TestToLineRoundTrip:
    """to_line() output should reproduce the original fixed-width line."""

    def test_round_trip_from_fixed_reader(self):
        from dbtk.readers import FixedReader

        fixed_file = Path(__file__).parent / 'fixtures' / 'readers' / 'sample_data.txt'
        # The monks fixture uses left-aligned, space-padded numerics, so we must
        # override the int/float defaults (right-aligned, zero-padded) to match.
        cols = [
            FixedColumn('trainee_id',       1,   5, 'int',   alignment='left', pad_char=' '),
            FixedColumn('monk_name',         6,  35, 'text'),
            FixedColumn('home_temple',      36,  65, 'text'),
            FixedColumn('mastery_rank',     66,  70, 'int',   alignment='left', pad_char=' '),
            FixedColumn('bison_companion',  71,  82, 'text'),
            FixedColumn('daily_meditation', 83,  90, 'float', alignment='left', pad_char=' '),
            FixedColumn('birth_date',       91, 102, 'date'),
            FixedColumn('last_training',   103, 122, 'datetime'),
        ]

        raw_lines = fixed_file.read_text(encoding='utf-8').splitlines()

        with FixedReader(open(fixed_file, encoding='utf-8'), cols) as reader:
            RecordClass = None
            for i, record in enumerate(reader):
                if RecordClass is None:
                    RecordClass = record.__class__
                reconstructed = record.to_line()
                original = raw_lines[i]
                assert reconstructed == original, (
                    f'Row {i + 1} mismatch:\n'
                    f'  original:      {repr(original)}\n'
                    f'  reconstructed: {repr(reconstructed)}'
                )


# ---------------------------------------------------------------------------
# ACH padding regression
# ---------------------------------------------------------------------------

class TestACHPadding:
    """immediate_destination and immediate_origin must be space-padded, not zero-padded."""

    def _make_header_record(self, **overrides):
        from dbtk.readers.edi_formats import ACH_COLUMNS
        cls = make_class(ACH_COLUMNS['1'])
        values = {col.name: '' for col in ACH_COLUMNS['1']}
        values.update(overrides)
        return cls(*[values[col.name] for col in ACH_COLUMNS['1']])

    def test_immediate_destination_space_padded(self):
        r = self._make_header_record(immediate_destination='061000104')   # 9 chars → 1 pad
        line = r.to_line()
        assert line[3:13] == ' 061000104', repr(line[3:13])

    def test_immediate_destination_not_zero_padded(self):
        r = self._make_header_record(immediate_destination='61000104')    # 8 chars → 2 pads
        line = r.to_line()
        assert line[3:13] == '  61000104', repr(line[3:13])
        assert not line[3:5].replace(' ', ''), 'leading chars should be spaces, not zeros'

    def test_immediate_origin_space_padded(self):
        r = self._make_header_record(immediate_origin='123456789')        # 9 chars → 1 pad
        line = r.to_line()
        assert line[13:23] == ' 123456789', repr(line[13:23])

    def test_immediate_destination_exact_fit(self):
        r = self._make_header_record(immediate_destination='1234567890')  # 10 chars → no pad
        line = r.to_line()
        assert line[3:13] == '1234567890', repr(line[3:13])
