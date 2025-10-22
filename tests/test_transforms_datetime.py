# tests/test_transforms_datetime.py
"""
Tests for dbtk.etl.transforms.datetime module.
"""

import pytest
from datetime import date, time, datetime, timezone, timedelta

from dbtk.etl.transforms.datetime import (
    parse_date,
    parse_time,
    parse_datetime,
    parse_timestamp,
    set_default_timezone,
    get_default_timezone,
    datePattern,
    dateLongPattern,
    timePattern,
    isoPattern,
    MONTHS_SHORT,
    MONTHS_LONG,
    TIMEZONE_OFFSETS
)


class TestParseDate:
    """Test parse_date function for various date formats."""

    def test_parse_date_iso_format(self):
        """Test parsing ISO format dates (Sozin's Comet appearance)."""
        assert parse_date("2024-01-15") == date(2024, 1, 15)
        assert parse_date("1999-12-31") == date(1999, 12, 31)
        assert parse_date("2023-06-21") == date(2023, 6, 21)

    def test_parse_date_slash_format_ymd(self):
        """Test parsing YYYY/MM/DD format."""
        assert parse_date("2024/01/15") == date(2024, 1, 15)
        assert parse_date("2023/12/25") == date(2023, 12, 25)

    def test_parse_date_slash_format_mdy(self):
        """Test parsing MM/DD/YYYY format (Avatar Aang's birthday)."""
        assert parse_date("01/15/2024") == date(2024, 1, 15)
        assert parse_date("12/25/2023") == date(2023, 12, 25)
        assert parse_date("6/21/2023") == date(2023, 6, 21)

    def test_parse_date_dash_format_mdy(self):
        """Test parsing MM-DD-YYYY format."""
        assert parse_date("01-15-2024") == date(2024, 1, 15)
        assert parse_date("12-25-2023") == date(2023, 12, 25)

    def test_parse_date_long_format(self):
        """Test parsing long date formats (Harmonic Convergence dates)."""
        assert parse_date("January 15, 2024") == date(2024, 1, 15)
        assert parse_date("Dec 25, 2023") == date(2023, 12, 25)
        assert parse_date("Jun 21 2023") == date(2023, 6, 21)

    def test_parse_date_long_format_day_first(self):
        """Test parsing day-first long formats."""
        assert parse_date("15 January 2024") == date(2024, 1, 15)
        assert parse_date("25 Dec 2023") == date(2023, 12, 25)

    def test_parse_date_from_datetime_object(self):
        """Test extracting date from datetime object."""
        dt = datetime(2024, 1, 15, 14, 30, 0)
        assert parse_date(dt) == date(2024, 1, 15)

    def test_parse_date_already_date(self):
        """Test that date objects pass through unchanged."""
        d = date(2024, 1, 15)
        assert parse_date(d) == d

    def test_parse_date_invalid(self):
        """Test parsing invalid dates returns None."""
        assert parse_date("not a date") is None
        assert parse_date("99/99/9999") is None

    def test_parse_date_none_or_empty(self):
        """Test parsing None or empty strings."""
        assert parse_date(None) is None
        assert parse_date("") is None
        assert parse_date("   ") is None


class TestParseTime:
    """Test parse_time function for various time formats."""

    def test_parse_time_24hour_format(self):
        """Test parsing 24-hour format (Fire Nation military time)."""
        assert parse_time("14:30:00") == time(14, 30, 0)
        assert parse_time("09:15:30") == time(9, 15, 30)
        assert parse_time("23:59:59") == time(23, 59, 59)

    def test_parse_time_without_seconds(self):
        """Test parsing time without seconds (meditation schedule)."""
        assert parse_time("14:30") == time(14, 30, 0)
        assert parse_time("09:15") == time(9, 15, 0)

    def test_parse_time_12hour_am(self):
        """Test parsing 12-hour AM format (sunrise at Air Temple)."""
        assert parse_time("6:30 AM") == time(6, 30, 0)
        assert parse_time("9:15 AM") == time(9, 15, 0)
        assert parse_time("12:00 AM") == time(0, 0, 0)

    def test_parse_time_12hour_pm(self):
        """Test parsing 12-hour PM format (sunset meditation)."""
        assert parse_time("2:30 PM") == time(14, 30, 0)
        assert parse_time("11:45 PM") == time(23, 45, 0)
        assert parse_time("12:00 PM") == time(12, 0, 0)

    def test_parse_time_with_microseconds(self):
        """Test parsing time with fractional seconds."""
        t = parse_time("14:30:00.123456")
        assert t.hour == 14
        assert t.minute == 30
        assert t.second == 0
        assert t.microsecond == 123456

    def test_parse_time_with_timezone_offset(self):
        """Test parsing time with UTC offset (Water Tribe time)."""
        t = parse_time("14:30:00-05:00")
        assert t.hour == 14
        assert t.minute == 30
        assert t.tzinfo == timezone(timedelta(hours=-5))

    def test_parse_time_with_timezone_name(self):
        """Test parsing time with timezone abbreviation."""
        t = parse_time("14:30:00 EST")
        assert t.hour == 14
        assert t.minute == 30
        assert t.tzinfo == TIMEZONE_OFFSETS['EST']

        t = parse_time("14:30:00 UTC")
        assert t.tzinfo == timezone.utc

    def test_parse_time_from_datetime_object(self):
        """Test extracting time from datetime object."""
        dt = datetime(2024, 1, 15, 14, 30, 45)
        assert parse_time(dt) == time(14, 30, 45)

    def test_parse_time_already_time(self):
        """Test that time objects pass through unchanged."""
        t = time(14, 30, 0)
        assert parse_time(t) == t

    def test_parse_time_invalid(self):
        """Test parsing invalid times returns None."""
        assert parse_time("not a time") is None
        assert parse_time("25:99:99") is None

    def test_parse_time_none_or_empty(self):
        """Test parsing None or empty strings."""
        assert parse_time(None) is None
        assert parse_time("") is None


class TestParseDatetime:
    """Test parse_datetime function for combined date/time."""

    def test_parse_datetime_iso_format(self):
        """Test parsing ISO 8601 format (Spirit World portal opening)."""
        dt = parse_datetime("2024-01-15T14:30:00")
        assert dt == datetime(2024, 1, 15, 14, 30, 0)

    def test_parse_datetime_iso_with_timezone(self):
        """Test parsing ISO format with timezone."""
        dt = parse_datetime("2024-01-15T14:30:00Z")
        assert dt == datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)

        dt = parse_datetime("2024-01-15T14:30:00-05:00")
        assert dt == datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone(timedelta(hours=-5)))

    def test_parse_datetime_iso_with_microseconds(self):
        """Test parsing ISO format with microseconds."""
        dt = parse_datetime("2024-01-15T14:30:00.123456")
        assert dt == datetime(2024, 1, 15, 14, 30, 0, 123456)

    def test_parse_datetime_space_separator(self):
        """Test parsing with space instead of T separator."""
        dt = parse_datetime("2024-01-15 14:30:00")
        assert dt == datetime(2024, 1, 15, 14, 30, 0)

    def test_parse_datetime_with_timezone_name(self):
        """Test parsing with timezone abbreviation."""
        dt = parse_datetime("2024-01-15 14:30:00 EST")
        assert dt.tzinfo == TIMEZONE_OFFSETS['EST']

    def test_parse_datetime_from_date_only(self):
        """Test parsing date string creates datetime at midnight."""
        dt = parse_datetime("2024-01-15")
        assert dt == datetime(2024, 1, 15, 0, 0, 0)

    def test_parse_datetime_from_time_only(self):
        """Test parsing time string uses today's date."""
        dt = parse_datetime("14:30:00")
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.date() == date.today()

    def test_parse_datetime_already_datetime(self):
        """Test that datetime objects pass through unchanged."""
        dt = datetime(2024, 1, 15, 14, 30, 0)
        assert parse_datetime(dt) == dt

    def test_parse_datetime_from_date_object(self):
        """Test converting date object to datetime."""
        d = date(2024, 1, 15)
        dt = parse_datetime(d)
        assert dt == datetime(2024, 1, 15, 0, 0, 0)

    def test_parse_datetime_invalid(self):
        """Test parsing invalid datetime returns None."""
        assert parse_datetime("not a datetime") is None

    def test_parse_datetime_none_or_empty(self):
        """Test parsing None or empty strings."""
        assert parse_datetime(None) is None
        assert parse_datetime("") is None


class TestParseTimestamp:
    """Test parse_timestamp function for Unix timestamps."""

    def test_parse_timestamp_unix_epoch(self):
        """Test parsing Unix timestamp (end of Hundred Year War)."""
        # Unix timestamp for 2024-01-15 14:30:00 UTC
        dt = parse_timestamp("1705329000")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.tzinfo == timezone.utc

    def test_parse_timestamp_with_decimals(self):
        """Test parsing Unix timestamp with fractional seconds."""
        dt = parse_timestamp("1705329000.123")
        assert dt.year == 2024
        assert dt.tzinfo == timezone.utc

    def test_parse_timestamp_datetime_string(self):
        """Test that regular datetime strings work (falls back to parse_datetime)."""
        dt = parse_timestamp("2024-01-15T14:30:00Z")
        assert dt == datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)

    def test_parse_timestamp_invalid(self):
        """Test parsing invalid timestamp returns None."""
        assert parse_timestamp("not a timestamp") is None

    def test_parse_timestamp_none_or_empty(self):
        """Test parsing None or empty strings."""
        assert parse_timestamp(None) is None
        assert parse_timestamp("") is None


class TestTimezoneManagement:
    """Test timezone setting and getting functions."""

    def test_set_get_default_timezone(self):
        """Test setting and getting default timezone."""
        original_tz = get_default_timezone()

        # Set to EST (Fire Nation time)
        set_default_timezone('EST')
        assert get_default_timezone() == TIMEZONE_OFFSETS['EST']

        # Set to PST (Air Temple time)
        set_default_timezone('PST')
        assert get_default_timezone() == TIMEZONE_OFFSETS['PST']

        # Restore original
        if original_tz:
            set_default_timezone(str(original_tz))
        else:
            set_default_timezone(None)

    def test_set_timezone_with_offset(self):
        """Test setting timezone using offset string."""
        set_default_timezone('+05:00')
        tz = get_default_timezone()
        assert tz == timezone(timedelta(hours=5))

        set_default_timezone('-08:00')
        tz = get_default_timezone()
        assert tz == timezone(timedelta(hours=-8))

        # Reset
        set_default_timezone(None)


class TestTimezoneOffsets:
    """Test TIMEZONE_OFFSETS constant."""

    def test_timezone_offsets_utc(self):
        """Test UTC timezone definitions."""
        assert TIMEZONE_OFFSETS['Z'] == timezone.utc
        assert TIMEZONE_OFFSETS['UTC'] == timezone.utc
        assert TIMEZONE_OFFSETS['GMT'] == timezone.utc

    def test_timezone_offsets_us_eastern(self):
        """Test US Eastern timezone (Fire Nation Capital)."""
        assert TIMEZONE_OFFSETS['EST'] == timezone(timedelta(hours=-5))
        assert TIMEZONE_OFFSETS['EDT'] == timezone(timedelta(hours=-4))

    def test_timezone_offsets_us_central(self):
        """Test US Central timezone."""
        assert TIMEZONE_OFFSETS['CST'] == timezone(timedelta(hours=-6))
        assert TIMEZONE_OFFSETS['CDT'] == timezone(timedelta(hours=-5))

    def test_timezone_offsets_us_mountain(self):
        """Test US Mountain timezone (Earth Kingdom regions)."""
        assert TIMEZONE_OFFSETS['MST'] == timezone(timedelta(hours=-7))
        assert TIMEZONE_OFFSETS['MDT'] == timezone(timedelta(hours=-6))

    def test_timezone_offsets_us_pacific(self):
        """Test US Pacific timezone (Western Air Temple)."""
        assert TIMEZONE_OFFSETS['PST'] == timezone(timedelta(hours=-8))
        assert TIMEZONE_OFFSETS['PDT'] == timezone(timedelta(hours=-7))


class TestMonthConstants:
    """Test month name constants."""

    def test_months_short_length(self):
        """Test MONTHS_SHORT has 13 entries (empty + 12 months)."""
        assert len(MONTHS_SHORT) == 13
        assert MONTHS_SHORT[0] == ''

    def test_months_short_values(self):
        """Test MONTHS_SHORT contains correct abbreviations."""
        assert MONTHS_SHORT[1] == 'JAN'
        assert MONTHS_SHORT[6] == 'JUN'
        assert MONTHS_SHORT[12] == 'DEC'

    def test_months_long_length(self):
        """Test MONTHS_LONG has 13 entries."""
        assert len(MONTHS_LONG) == 13
        assert MONTHS_LONG[0] == ''

    def test_months_long_values(self):
        """Test MONTHS_LONG contains correct names."""
        assert MONTHS_LONG[1] == 'JANUARY'
        assert MONTHS_LONG[6] == 'JUNE'
        assert MONTHS_LONG[12] == 'DECEMBER'


class TestDatePatterns:
    """Test date regex patterns."""

    def test_date_pattern_iso_format(self):
        """Test datePattern matches ISO dates."""
        assert datePattern.search("2024-01-15")
        assert datePattern.search("1999/12/31")

    def test_date_pattern_mdy_format(self):
        """Test datePattern matches MM/DD/YYYY."""
        assert datePattern.search("01/15/2024")
        assert datePattern.search("12-25-2023")

    def test_date_long_pattern_month_first(self):
        """Test dateLongPattern matches month-first formats."""
        assert dateLongPattern.search("January 15, 2024")
        assert dateLongPattern.search("Dec 25 2023")

    def test_date_long_pattern_day_first(self):
        """Test dateLongPattern matches day-first formats."""
        assert dateLongPattern.search("15 January 2024")
        assert dateLongPattern.search("25 Dec 2023")


class TestTimePatterns:
    """Test time regex patterns."""

    def test_time_pattern_24hour(self):
        """Test timePattern matches 24-hour format."""
        assert timePattern.search("14:30:00")
        assert timePattern.search("09:15:30")

    def test_time_pattern_with_am_pm(self):
        """Test timePattern matches 12-hour format."""
        assert timePattern.search("2:30 PM")
        assert timePattern.search("9:15 AM")

    def test_time_pattern_with_timezone(self):
        """Test timePattern matches times with timezones."""
        assert timePattern.search("14:30:00-05:00")
        assert timePattern.search("14:30:00 EST")
        assert timePattern.search("14:30:00Z")


class TestISOPattern:
    """Test ISO 8601 datetime pattern."""

    def test_iso_pattern_basic(self):
        """Test isoPattern matches basic ISO format."""
        assert isoPattern.search("2024-01-15T14:30:00")
        assert isoPattern.search("2024-01-15 14:30:00")

    def test_iso_pattern_with_timezone(self):
        """Test isoPattern matches ISO with timezone."""
        assert isoPattern.search("2024-01-15T14:30:00Z")
        assert isoPattern.search("2024-01-15T14:30:00-05:00")
        assert isoPattern.search("2024-01-15T14:30:00+05:30")

    def test_iso_pattern_with_microseconds(self):
        """Test isoPattern matches ISO with microseconds."""
        assert isoPattern.search("2024-01-15T14:30:00.123456")
        assert isoPattern.search("2024-01-15T14:30:00.123456Z")


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_leap_year_dates(self):
        """Test parsing leap year dates (Spirit World alignment)."""
        assert parse_date("2024-02-29") == date(2024, 2, 29)
        assert parse_date("2020-02-29") == date(2020, 2, 29)
        assert parse_date("2023-02-29") is None  # Not a leap year

    def test_midnight_datetime(self):
        """Test datetime at midnight."""
        dt = parse_datetime("2024-01-15 00:00:00")
        assert dt == datetime(2024, 1, 15, 0, 0, 0)

    def test_end_of_day(self):
        """Test datetime at end of day."""
        dt = parse_datetime("2024-01-15 23:59:59")
        assert dt == datetime(2024, 1, 15, 23, 59, 59)

    def test_single_digit_dates(self):
        """Test parsing dates with single digit months/days."""
        assert parse_date("2024-1-5") == date(2024, 1, 5)
        assert parse_date("1/5/2024") == date(2024, 1, 5)

    def test_timezone_preservation(self):
        """Test that explicit timezones override defaults."""
        set_default_timezone('EST')

        # Explicit timezone should override default
        dt = parse_datetime("2024-01-15T14:30:00Z")
        assert dt.tzinfo == timezone.utc

        # Reset
        set_default_timezone(None)

    def test_century_handling(self):
        """Test proper century handling in dates."""
        assert parse_date("1999-12-31") == date(1999, 12, 31)
        assert parse_date("2000-01-01") == date(2000, 1, 1)
        assert parse_date("2099-12-31") == date(2099, 12, 31)

    def test_mixed_format_strings(self):
        """Test parsing dates/times from strings with extra text."""
        # These should fail gracefully or extract the date
        dt = parse_datetime("Event at 2024-01-15T14:30:00")
        # Depending on implementation, this might work or return None
        # The important thing is it doesn't crash

    def test_type_coercion(self):
        """Test that various types are handled."""
        # Integer timestamp
        dt = parse_timestamp(1705329000)
        assert dt is not None

        # String date
        d = parse_date("2024-01-15")
        assert isinstance(d, date)