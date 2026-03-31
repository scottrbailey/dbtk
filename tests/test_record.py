# tests/test_record.py
"""
Tests for the Record and FixedWidthRecord classes.
"""

import pytest
from dbtk.record import Record, FixedWidthRecord
from dbtk.utils import normalize_field_name, FixedColumn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_record_class(*fields):
    """Create a fresh Record subclass with the given field names."""
    cls = type('R', (Record,), {})
    cls.set_fields(list(fields))
    return cls


# ---------------------------------------------------------------------------
# _RESERVED completeness
# ---------------------------------------------------------------------------

class TestReservedCompleteness:
    """Ensure _RESERVED stays in sync with the Record interface."""

    def test_all_record_names_are_reserved(self):
        """
        Every non-dunder name on Record, after normalization, must appear in
        _RESERVED.  If this test fails, a new method or attribute was added
        without updating _RESERVED — meaning a database column with that name
        would silently shadow it.
        """
        non_dunder = {
            name for name in dir(Record)
            if not (name.startswith('__') and name.endswith('__'))
        }
        normalized = {normalize_field_name(name) for name in non_dunder}
        missing = normalized - Record._RESERVED
        assert not missing, (
            "The following names are in dir(Record) but missing from _RESERVED "
            "(add them so field-name collision detection stays accurate): "
            + ", ".join(sorted(missing))
        )

    def test_fixed_width_record_names_are_reserved(self):
        """
        Same check for FixedWidthRecord — _get_reserved() must cover every
        non-dunder name on the subclass.
        """
        non_dunder = {
            name for name in dir(FixedWidthRecord)
            if not (name.startswith('__') and name.endswith('__'))
        }
        normalized = {normalize_field_name(name) for name in non_dunder}
        missing = normalized - FixedWidthRecord._get_reserved()
        assert not missing, (
            "The following names are in dir(FixedWidthRecord) but missing from "
            "_get_reserved(): " + ", ".join(sorted(missing))
        )
