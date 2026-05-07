# tests/test_fixed_width_record.py
"""
Tests for FixedWidthRecord.set_fields(), to_line(), pprint(), and visualize().

Covers the splice-based to_line() implementation: positional placement,
gap preservation, out-of-order column definitions, alignment/padding,
overflow handling, and reader round-trip fidelity.
Also covers the pprint() override and its add_comments parameter,
and the visualize() diagnostic method.
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
    def test_columns_stored(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        assert cls._columns == contiguous_cols

    def test_columns_is_a_copy(self, contiguous_cols):
        """Mutating the original list must not affect the class."""
        cls = make_class(contiguous_cols)
        contiguous_cols.append(FixedColumn('extra', 22, 25))
        assert len(cls._columns) == 3

    def test_line_len(self, contiguous_cols):
        cls = make_class(contiguous_cols)
        assert cls._line_len == 21

    def test_empty_fields(self):
        cls = make_class([])
        assert cls._columns == []
        assert cls._line_len == 0

    def test_subclasses_are_independent(self):
        """Two subclasses must not share class-attribute state."""
        A = make_class([FixedColumn('x', 1, 5)])
        B = make_class([FixedColumn('y', 1, 10)])
        assert len(A._columns) == 1
        assert len(B._columns) == 1
        assert A._line_len == 5
        assert B._line_len == 10

    def test_width_kwarg_computes_end_pos(self):
        col = FixedColumn('amount', 5, width=10)
        assert col.end_pos == 14
        assert col.width == 10

    def test_width_kwarg_equivalent_to_end_pos(self):
        by_end = FixedColumn('amount', 5, 14)
        by_width = FixedColumn('amount', 5, width=10)
        assert by_width.start_pos == by_end.start_pos
        assert by_width.end_pos == by_end.end_pos
        assert by_width.width == by_end.width

    def test_width_and_end_pos_both_raises(self):
        with pytest.raises(ValueError):
            FixedColumn('amount', 5, 14, width=10)


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
        cols = [FixedColumn('label', 1, 11, align='c')]
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

    def test_alignment_defaults_via_output(self, contiguous_cols):
        """text→left+space, int→right+zero, verified through to_line() output."""
        cls = make_class(contiguous_cols)
        r = cls('X', 'hi', '7')
        line = r.to_line()
        assert line[1:11] == 'hi        '   # text: left-aligned, space-padded
        assert line[11:21] == '0000000007'   # int: right-aligned, zero-padded

    def test_explicit_alignment_override(self):
        cols = [
            FixedColumn('a', 1,  5, align='r'),
            FixedColumn('b', 6,  10, align='c'),
            FixedColumn('c', 11, 15, align='l'),
        ]
        cls = make_class(cols)
        r = cls('X', 'X', 'X')
        line = r.to_line()
        assert line[0:5]  == '    X'    # right
        assert line[5:10] == '  X  '    # center
        assert line[10:15] == 'X    '   # left

    def test_explicit_pad_char_override(self):
        cols = [FixedColumn('x', 1, 4, pad_char='*')]
        cls = make_class(cols)
        r = cls('A')
        assert r.to_line() == 'A***'


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
            FixedColumn('trainee_id',       1,   5, 'int',   align='left', pad_char=' '),
            FixedColumn('monk_name',         6,  35, 'text'),
            FixedColumn('home_temple',      36,  65, 'text'),
            FixedColumn('mastery_rank',     66,  70, 'int',   align='left', pad_char=' '),
            FixedColumn('bison_companion',  71,  82, 'text'),
            FixedColumn('daily_meditation', 83,  90, 'float', align='left', pad_char=' '),
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
        from dbtk.formats.edi import ACH_COLUMNS
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


# ---------------------------------------------------------------------------
# pprint() override
# ---------------------------------------------------------------------------

class TestPprint:
    """FixedWidthRecord.pprint() with add_comments parameter."""

    @pytest.fixture()
    def cls_with_comments(self):
        return make_class([
            FixedColumn('code',   1,  1,  comment='Record type'),
            FixedColumn('name',   2, 11,  comment='Vendor name'),
            FixedColumn('amount', 12, 21, 'int'),   # no comment
        ])

    @pytest.fixture()
    def cls_no_comments(self):
        return make_class([
            FixedColumn('code',   1,  1),
            FixedColumn('name',   2, 11),
            FixedColumn('amount', 12, 21, 'int'),
        ])

    def test_default_no_comments(self, cls_with_comments, capsys):
        r = cls_with_comments('A', 'Acme', '100')
        r.pprint()
        out = capsys.readouterr().out
        assert 'Record type' not in out
        assert 'code   : A' in out

    def test_add_comments_false_explicit(self, cls_with_comments, capsys):
        r = cls_with_comments('A', 'Acme', '100')
        r.pprint(add_comments=False)
        out = capsys.readouterr().out
        assert 'Record type' not in out

    def test_add_comments_shows_comment_text(self, cls_with_comments, capsys):
        r = cls_with_comments('A', 'Acme', '100')
        r.pprint(add_comments=True)
        out = capsys.readouterr().out
        assert '# Record type' in out
        assert '# Vendor name' in out

    def test_add_comments_blank_for_missing_comment(self, cls_with_comments, capsys):
        r = cls_with_comments('A', 'Acme', '100')
        r.pprint(add_comments=True)
        lines = capsys.readouterr().out.splitlines()
        amount_line = next(l for l in lines if l.startswith('amount'))
        # trailing content after value should be blank (no comment for amount)
        assert 'amount' in amount_line
        assert amount_line.rstrip() == amount_line.rstrip()  # no spurious text
        assert not any(w for w in amount_line.split('100', 1)[1].split() if w)

    def test_add_comments_all_blank_falls_through_to_base(self, cls_no_comments, capsys):
        """When no column has a comment, output should look like base pprint."""
        r = cls_no_comments('A', 'Acme', '100')
        r.pprint(add_comments=True)
        lines_with = capsys.readouterr().out.splitlines()

        r.pprint(add_comments=False)
        lines_without = capsys.readouterr().out.splitlines()

        assert lines_with == lines_without

    def test_values_aligned_in_column(self, cls_with_comments, capsys):
        r = cls_with_comments('A', 'Acme Corp', '999')
        r.pprint(add_comments=True)
        lines = capsys.readouterr().out.splitlines()
        # All ' : ' separators should be at the same position
        positions = [l.index(' : ') for l in lines]
        assert len(set(positions)) == 1


# ---------------------------------------------------------------------------
# from_line — parsing a fixed-width string into a record
# ---------------------------------------------------------------------------

class TestFromLine:
    """FixedWidthRecord.from_line() — the corollary to to_line()."""

    @pytest.fixture
    def contiguous_class(self, contiguous_cols):
        return make_class(contiguous_cols)

    # -- basic parsing -------------------------------------------------------

    def test_returns_instance_of_class(self, contiguous_class):
        r = contiguous_class.from_line('A' + 'Hi        ' + '0000000042')
        assert isinstance(r, contiguous_class)

    def test_text_field_parsed(self, contiguous_class):
        r = contiguous_class.from_line('A' + 'Hi        ' + '0000000042')
        assert r.code == 'A'
        assert r.name == 'Hi'        # auto_trim strips trailing spaces

    def test_int_field_parsed(self, contiguous_class):
        r = contiguous_class.from_line('A' + 'Hi        ' + '0000000042')
        assert r.amount == 42

    def test_zero_padded_int(self, contiguous_class):
        r = contiguous_class.from_line('X' + 'ABCDEFGHIJ' + '0000012345')
        assert r.amount == 12345

    def test_empty_int_becomes_none(self, contiguous_class):
        r = contiguous_class.from_line('X' + 'Name      ' + '          ')
        assert r.amount is None

    # -- auto_trim -----------------------------------------------------------

    def test_auto_trim_true_strips_text(self, contiguous_class):
        r = contiguous_class.from_line('A' + 'Hi        ' + '0000000042', auto_trim=True)
        assert r.name == 'Hi'

    def test_auto_trim_false_preserves_padding(self, contiguous_class):
        r = contiguous_class.from_line('A' + 'Hi        ' + '0000000042', auto_trim=False)
        assert r.name == 'Hi        '

    # -- type columns --------------------------------------------------------

    def test_float_field_parsed(self):
        cols = [FixedColumn('ratio', 1, 10, 'float')]
        cls = make_class(cols)
        r = cls.from_line('  3.14159 ')
        assert abs(r.ratio - 3.14159) < 1e-6

    def test_empty_float_becomes_none(self):
        cols = [FixedColumn('ratio', 1, 10, 'float')]
        cls = make_class(cols)
        r = cls.from_line('          ')
        assert r.ratio is None

    def test_date_field_parsed(self):
        import datetime
        cols = [FixedColumn('dob', 1, 10, 'date')]
        cls = make_class(cols)
        r = cls.from_line('2024-01-15')
        assert r.dob == datetime.date(2024, 1, 15)

    def test_unparseable_value_falls_back_to_string(self):
        cols = [FixedColumn('amount', 1, 5, 'int')]
        cls = make_class(cols)
        r = cls.from_line('ABC  ')   # not a valid int
        assert r.amount == 'ABC'     # auto_trim applied to fallback

    # -- gaps and positional accuracy ----------------------------------------

    def test_gap_columns_sliced_correctly(self):
        cols = [
            FixedColumn('type', 1, 2),
            FixedColumn('data', 7, 10),
        ]
        cls = make_class(cols)
        r = cls.from_line('AB    XY  ')
        assert r.type == 'AB'
        assert r.data == 'XY'

    def test_first_col_not_at_position_one(self):
        cols = [FixedColumn('id', 5, 8)]
        cls = make_class(cols)
        r = cls.from_line('    AB  ')
        assert r.id == 'AB'

    # -- round-trip ----------------------------------------------------------

    def test_round_trip_text(self, contiguous_class):
        original = 'Z' + 'Round Trip' + '0000056789'
        r = contiguous_class.from_line(original)
        assert r.to_line() == original

    def test_round_trip_via_to_line(self, contiguous_class):
        r1 = contiguous_class('B', 'TestValue', 999)
        line = r1.to_line()
        r2 = contiguous_class.from_line(line)
        assert r2.to_line() == line

    def test_round_trip_from_fixed_reader(self):
        """Records parsed with from_line() must round-trip the same as reader records."""
        from pathlib import Path
        from dbtk.readers import FixedReader

        fixed_file = Path(__file__).parent / 'fixtures' / 'readers' / 'sample_data.txt'
        cols = [
            FixedColumn('trainee_id',       1,   5, 'int',   align='left', pad_char=' '),
            FixedColumn('monk_name',         6,  35, 'text'),
            FixedColumn('home_temple',      36,  65, 'text'),
            FixedColumn('mastery_rank',     66,  70, 'int',   align='left', pad_char=' '),
            FixedColumn('bison_companion',  71,  82, 'text'),
            FixedColumn('daily_meditation', 83,  90, 'float', align='left', pad_char=' '),
            FixedColumn('birth_date',       91, 102, 'date'),
            FixedColumn('last_training',   103, 122, 'datetime'),
        ]
        RecordClass = type('FW', (FixedWidthRecord,), {})
        RecordClass.set_fields(cols)

        raw_lines = fixed_file.read_text(encoding='utf-8').splitlines()
        for raw in raw_lines:
            r = RecordClass.from_line(raw)
            assert r.to_line() == raw, (
                f'Round-trip mismatch:\n'
                f'  original: {repr(raw)}\n'
                f'  result:   {repr(r.to_line())}'
            )

    # -- out-of-order column definitions ------------------------------------

    def test_out_of_order_columns_sliced_by_position(self):
        """from_line() must use positional slicing, not definition order."""
        cols = [
            FixedColumn('last',  6, 10),
            FixedColumn('first', 1,  5),
        ]
        cls = make_class(cols)
        r = cls.from_line('AliceSmith')
        # Values are stored in definition order: last first, first second
        assert r.last  == 'Smith'
        assert r.first == 'Alice'


class TestVisualize:
    """Tests for FixedWidthRecord.visualize()."""

    @pytest.fixture
    def cls_3col(self):
        """A simple 3-column, 12-char schema."""
        cls = type('R', (FixedWidthRecord,), {})
        cls.set_fields([
            FixedColumn('code',   1,  2),
            FixedColumn('amount', 3, 12, 'int', pad_char='0'),
        ])
        return cls

    def test_returns_string(self, cls_3col):
        out = cls_3col('AB', 42).visualize()
        assert isinstance(out, str)

    def test_four_lines(self, cls_3col):
        lines = cls_3col('AB', 42).visualize().splitlines()
        assert len(lines) == 4

    def test_rulers_match_line_length(self, cls_3col):
        lines = cls_3col('AB', 42).visualize().splitlines()
        line_len = cls_3col._line_len   # 12
        assert len(lines[0]) == line_len   # tens ruler
        assert len(lines[1]) == line_len   # ones ruler

    def test_boundary_markers_at_column_starts(self, cls_3col):
        boundary = cls_3col('AB', 42).visualize().splitlines()[2]
        for col in cls_3col._columns:
            assert boundary[col.start_idx] == '├', (
                f"Expected '├' at position {col.start_idx} for column '{col.name}'"
            )

    def test_boundary_length_matches_rulers(self, cls_3col):
        lines = cls_3col('AB', 42).visualize().splitlines()
        assert len(lines[2]) == len(lines[1])   # boundary == ones ruler length

    def test_last_line_is_to_line(self, cls_3col):
        r = cls_3col('AB', 42)
        lines = r.visualize().splitlines()
        assert lines[3] == r.to_line()

    def test_ones_ruler_content(self, cls_3col):
        ones = cls_3col('AB', 42).visualize().splitlines()[1]
        # positions 1-9 → digits 1-9, position 10 → '0', 11 → '1', 12 → '2'
        assert ones == '1234567890' + ''.join(str(i % 10) for i in range(11, cls_3col._line_len + 1))

    def test_no_extra_trailing_chars(self, cls_3col):
        """All four output lines must have exactly _line_len characters."""
        line_len = cls_3col._line_len
        for line in cls_3col('AB', 42).visualize().splitlines():
            assert len(line) == line_len
