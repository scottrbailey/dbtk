# dbtk/etl/transforms/datetime.py
"""
Date and time parsing and transformation functions.

Supports various date/time formats with timezone awareness.
Uses dateutil parser when available for robust parsing, with custom
parsing as fallback.
"""

import datetime as dt
import re
from typing import Any, Optional

from ...defaults import settings

# Check for optional dateutil library
try:
    from dateutil import parser as dateutil_parser

    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False
    dateutil_parser = None

# Module-level timezone variable
_default_timezone = None

# Enhanced regex patterns with timezone support
datePattern = re.compile(
    r'((?P<y1>\d{4})[\-|\/|\.](?P<m1>\d{1,2})[\-|\/|\.](?P<d1>\d{1,2}))|'
    r'((?P<m2>\d{1,2})[\-|\/|\.](?P<d2>\d{1,2})[\-|\/|\.](?P<y2>\d{4}))'
)

dateLongPattern = re.compile(
    r'((?P<m1>[a-z]{3,9})[ |\-|\.]+(?P<d1>\d{1,2})[st|nd|rd|th]*[ |\-|\,]+(?P<y1>\d{4}))|'
    r'((?P<d2>\d{1,2})*[ |\-|\.]*(?P<m2>[a-z]{3,9})[ |\-|\.|\,]+(?P<y2>\d{4}))',
    re.I
)

timePattern = re.compile(
    r'(?P<hr>[0-2]?\d):(?P<mi>[0-6]\d):?(?P<sec>[0-6]\d)?(?P<fsec>\.\d{1,9})?'
    r'(?P<am> ?[A|P]M)?'
    r'(?P<tz>[ ]?(?P<offset>[+-]\d{2}:?\d{2})|[ ]?(?P<tzname>Z|UTC|GMT|EST|CST|MST|PST|EDT|CDT|MDT|PDT))?',
    re.I
)

# ISO 8601 datetime pattern with timezone
isoPattern = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})[T ]'
    r'(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})'
    r'(?P<microsecond>\.\d{1,6})?'
    r'(?P<timezone>Z|[+-]\d{2}:?\d{2})?'
)

# Month name constants
MONTHS_SHORT = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

MONTHS_LONG = ['', 'JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
               'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']

# Timezone mappings
TIMEZONE_OFFSETS = {
    'Z': dt.timezone.utc,
    'UTC': dt.timezone.utc,
    'GMT': dt.timezone.utc,
    'EST': dt.timezone(dt.timedelta(hours=-5)),
    'EDT': dt.timezone(dt.timedelta(hours=-4)),
    'CST': dt.timezone(dt.timedelta(hours=-6)),
    'CDT': dt.timezone(dt.timedelta(hours=-5)),
    'MST': dt.timezone(dt.timedelta(hours=-7)),
    'MDT': dt.timezone(dt.timedelta(hours=-6)),
    'PST': dt.timezone(dt.timedelta(hours=-8)),
    'PDT': dt.timezone(dt.timedelta(hours=-7)),
}
# Timezone mapping for dateutil
TZINFOS = {k: int(v.utcoffset(None).total_seconds()) for k, v in TIMEZONE_OFFSETS.items()}


def set_default_timezone(timezone_name: str):
    """
    Set the default timezone for date/time parsing.

    Args:
        timezone_name: Timezone name (e.g., 'UTC', 'EST', 'America/New_York')
                      or offset string (e.g., '+05:00', '-08:00')

    Raises:
        ValueError: If timezone format is unrecognized

    Examples:
        set_default_timezone('UTC')
        set_default_timezone('EST')
        set_default_timezone('+05:00')
        set_default_timezone('America/New_York')  # Requires pytz or dateutil
    """
    global _default_timezone
    if timezone_name and timezone_name.upper() in TIMEZONE_OFFSETS:
        _default_timezone = TIMEZONE_OFFSETS[timezone_name.upper()]
    elif timezone_name:
        # Try to parse as offset like +05:00 or -0800
        try:
            offset_match = re.match(r'([+-])(\d{2}):?(\d{2})', timezone_name)
            if offset_match:
                sign = 1 if offset_match.group(1) == '+' else -1
                hours = int(offset_match.group(2))
                minutes = int(offset_match.group(3))
                total_minutes = sign * (hours * 60 + minutes)
                _default_timezone = dt.timezone(dt.timedelta(minutes=total_minutes))
            else:
                raise ValueError(f"Unknown timezone format: {timezone_name}")
        except Exception:
            # Try dateutil/pytz as fallback if available
            try:
                import pytz
                _default_timezone = pytz.timezone(timezone_name)
            except ImportError:
                try:
                    from dateutil.tz import gettz
                    _default_timezone = gettz(timezone_name)
                except ImportError:
                    raise ValueError(f"Unknown timezone: {timezone_name}")
    else:
        _default_timezone = None


def get_default_timezone():
    """
    Get the current default timezone.

    Returns:
        Current default timezone object or None
    """
    return _default_timezone


def _normalize_timezone(tz):
    """Convert dateutil timezone objects to stdlib timezone objects."""
    if tz is None:
        return None

    # Already a stdlib timezone
    if isinstance(tz, dt.timezone):
        return tz

    # Convert dateutil timezone to stdlib
    try:
        # Get the UTC offset
        offset = tz.utcoffset(None)
        if offset is None:
            return None

        # Check if it's UTC
        if offset.total_seconds() == 0:
            return dt.timezone.utc

        # Create stdlib timezone with same offset
        return dt.timezone(offset)
    except (AttributeError, TypeError):
        return tz

def _parse_timezone_offset(tz_str: str) -> Optional[dt.timezone]:
    """
    Parse timezone offset string into timezone object.

    Args:
        tz_str: Timezone string (e.g., 'Z', 'UTC', '+05:00', '-0800')

    Returns:
        Timezone object or None if parsing fails
    """
    if not tz_str:
        return None

    tz_str = tz_str.strip().upper()

    # Check known timezone abbreviations
    if tz_str in TIMEZONE_OFFSETS:
        return TIMEZONE_OFFSETS[tz_str]

    # Parse offset format like +05:00, -0800, +05:30
    offset_match = re.match(r'([+-])(\d{2}):?(\d{2})', tz_str)
    if offset_match:
        sign = 1 if offset_match.group(1) == '+' else -1
        hours = int(offset_match.group(2))
        minutes = int(offset_match.group(3))
        total_minutes = sign * (hours * 60 + minutes)
        return dt.timezone(dt.timedelta(minutes=total_minutes))

    return None


def parse_date(val: Any, default_tz: Optional[str] = None) -> Optional[dt.date]:
    """
    Parse various date formats to date object.

    Args:
        val: Date string, datetime object, or other value
        default_tz: Default timezone (not used for dates, kept for consistency)

    Returns:
        date object or None if parsing fails

    Examples:
        parse_date("2024-01-15")      # -> date(2024, 1, 15)
        parse_date("01/15/2024")      # -> date(2024, 1, 15)
        parse_date("15 Jan 2024")     # -> date(2024, 1, 15)
    """
    if not val or val == '':
        return None

    # Check datetime first (it's a subclass of date)
    if isinstance(val, dt.datetime):
        return val.date()
    if isinstance(val, dt.date):
        return val

    val_str = str(val).strip()
    if not val_str:
        return None

    # Try dateutil first if available
    if HAS_DATEUTIL:
        try:
            parsed = dateutil_parser.parse(val_str, default=dt.datetime(1900, 1, 1))
            return parsed.date()
        except (ValueError, TypeError):
            pass

    # Fall back to custom parsing
    # Try standard date patterns first
    match = datePattern.search(val_str)
    if not match:
        match = dateLongPattern.search(val_str)

    if match:
        mdict = match.groupdict()
        yr = int(mdict.get('y1') or mdict.get('y2'))
        mon = str(mdict.get('m1') or mdict.get('m2')).upper()
        dy = int(mdict.get('d1') or mdict.get('d2') or 1)

        if mon.isdigit():
            mon = int(mon)
        elif mon in MONTHS_SHORT:
            mon = MONTHS_SHORT.index(mon)
        elif mon in MONTHS_LONG:
            mon = MONTHS_LONG.index(mon)
        else:
            return None

        if yr and mon and dy:
            try:
                return dt.date(yr, mon, dy)
            except ValueError:
                return None

    # Try ISO format
    iso_match = isoPattern.search(val_str)
    if iso_match:
        try:
            year = int(iso_match.group('year'))
            month = int(iso_match.group('month'))
            day = int(iso_match.group('day'))
            return dt.date(year, month, day)
        except ValueError:
            return None

    return None


def parse_time(val: Any) -> Optional[dt.time]:
    """
    Parse various time formats to time object.

    Args:
        val: Time string or other value

    Returns:
        time object or None if parsing fails

    Examples:
        parse_time("14:30:00")        # -> time(14, 30, 0)
        parse_time("2:30 PM")         # -> time(14, 30, 0)
        parse_time("14:30:00-05:00")  # -> time(14, 30, 0, tzinfo=...)
        parse_time("14:30:00 EST")    # -> time(14, 30, 0, tzinfo=EST)
    """
    if not val or val == '':
        return None

    if isinstance(val, dt.time):
        return val
    if isinstance(val, dt.datetime):
        return val.time()

    val_str = str(val).strip()
    if not val_str:
        return None

    # Try dateutil first if available
    if HAS_DATEUTIL:
        try:
            parsed = dateutil_parser.parse(val_str, default=dt.datetime(1900, 1, 1), tzinfos=TZINFOS)
            normalized_tz = _normalize_timezone(parsed.tzinfo)
            result_time = parsed.time().replace(tzinfo=normalized_tz)
            return result_time
        except (ValueError, TypeError):
            pass

    # Fall back to custom parsing
    match = timePattern.search(val_str)
    if match:
        mdict = match.groupdict()
        hr = int(mdict.get('hr') or 0)
        mi = int(mdict.get('mi') or 0)
        sec = int(mdict.get('sec') or 0)

        # Handle fractional seconds
        fsec_str = mdict.get('fsec') or '0'
        if fsec_str.startswith('.'):
            fsec_str = fsec_str[1:]
        # Pad or truncate to 6 digits (microseconds)
        fsec_str = fsec_str.ljust(6, '0')[:6]
        msec = int(fsec_str)

        # Handle AM/PM
        am_pm = (mdict.get('am') or '').strip().upper()
        if am_pm == 'PM' and hr < 12:
            hr += 12
        elif am_pm == 'AM' and hr == 12:
            hr = 0

        # Handle timezone
        tz_info = None
        tz_str = mdict.get('tz')
        if tz_str:
            tz_info = _parse_timezone_offset(tz_str)

        try:
            return dt.time(hr, mi, sec, msec, tzinfo=tz_info)
        except ValueError:
            return None

    return None


def parse_datetime(val: Any) -> Optional[dt.datetime]:
    """
    Parse various datetime formats to datetime object.

    Preserves timezone if present in the input string, otherwise returns naive datetime.
    Use parse_datetimetz() to automatically apply default timezone from settings.

    Args:
        val: Datetime string, date object, or other value

    Returns:
        datetime object or None if parsing fails

    Examples:
        parse_datetime("2024-01-15 14:30:00")         # -> naive datetime
        parse_datetime("2024-01-15T14:30:00Z")        # -> datetime with UTC
        parse_datetime("01/15/2024 2:30 PM EST")      # -> datetime with EST
    """
    if not val or val == '':
        return None

    if isinstance(val, dt.datetime):
        return val
    if isinstance(val, dt.date):
        return dt.datetime.combine(val, dt.time.min)

    val_str = str(val).strip()
    if not val_str:
        return None

    # Try dateutil first if available
    if HAS_DATEUTIL:
        try:
            parsed = dateutil_parser.parse(val_str, tzinfos=TZINFOS)

            # Normalize timezone from dateutil to stdlib
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=_normalize_timezone(parsed.tzinfo))

            return parsed
        except (ValueError, TypeError):
            pass

    # Fall back to custom parsing
    # Try ISO 8601 format first
    iso_match = isoPattern.search(val_str)
    if iso_match:
        try:
            year = int(iso_match.group('year'))
            month = int(iso_match.group('month'))
            day = int(iso_match.group('day'))
            hour = int(iso_match.group('hour'))
            minute = int(iso_match.group('minute'))
            second = int(iso_match.group('second'))

            # Handle microseconds
            microsecond = 0
            if iso_match.group('microsecond'):
                msec_str = iso_match.group('microsecond')[1:]  # Remove the dot
                msec_str = msec_str.ljust(6, '0')[:6]  # Pad or truncate to 6 digits
                microsecond = int(msec_str)

            # Handle timezone
            tz_info = None
            if iso_match.group('timezone'):
                tz_info = _parse_timezone_offset(iso_match.group('timezone'))

            result = dt.datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tz_info)
            return result
        except ValueError:
            pass

    # Try combining date and time parsing
    d = parse_date(val_str)
    t = parse_time(val_str)

    if d and t:
        result = dt.datetime.combine(d, t.replace(tzinfo=None))
        # Add timezone info from time if it had any
        if t.tzinfo:
            result = result.replace(tzinfo=t.tzinfo)
        return result
    elif d:
        result = dt.datetime.combine(d, dt.time.min)
        return result

    return None


def parse_timestamp(val: Any) -> Optional[dt.datetime]:
    """
    Parse timestamp with timezone support.

    Args:
        val: Timestamp string or other value

    Returns:
        timezone-aware datetime object or None if parsing fails

    Examples:
        parse_timestamp("2024-01-15 14:30:00+00:00")     # -> datetime with UTC
        parse_timestamp("2024-01-15T14:30:00Z")          # -> datetime with UTC
        parse_timestamp("1642262200")                    # -> datetime from Unix timestamp
    """
    if not val or val == '':
        return None

    val_str = str(val).strip()

    # Check if it's a Unix timestamp (10 digits for seconds, more for milliseconds)
    if re.match(r'^\d{10}(\.\d+)?$', val_str):
        try:
            timestamp = float(val_str)
            return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
        except (ValueError, OSError):
            pass

    # Use parse_datetime for everything else
    return parse_datetime(val)


def parse_datetimetz(val: Any) -> Optional[dt.datetime]:
    """
    Parse datetime and apply default timezone from settings if datetime is naive.

    This is a convenience function that applies the timezone set via
    set_default_timezone() to naive datetimes. If the datetime already has
    a timezone, it is preserved.

    Args:
        val: Datetime string, date object, or other value

    Returns:
        datetime object or None if parsing fails

    Examples:
        set_default_timezone('UTC')
        parse_datetimetz("2024-01-15 14:30:00")  # -> datetime with UTC
        parse_datetimetz("2024-01-15T14:30:00Z") # -> datetime with UTC (preserved)
    """
    dt_obj = parse_datetime(val)

    if dt_obj and dt_obj.tzinfo is None and _default_timezone:
        dt_obj = dt_obj.replace(tzinfo=_default_timezone)

    return dt_obj


def parse_timestamp(val: Any, default_tz: Optional[str] = None) -> Optional[dt.datetime]:
    """
    Parse timestamp with timezone support (alias for parse_datetime).

    Args:
        val: Timestamp string or other value
        default_tz: Default timezone name if not specified in string

    Returns:
        timezone-aware datetime object or None if parsing fails

    Examples:
        parse_timestamp("2024-01-15 14:30:00+00:00")     # -> datetime with UTC
        parse_timestamp("2024-01-15 14:30:00", "UTC")    # -> datetime with UTC
        parse_timestamp("1642262200")                     # -> datetime from Unix timestamp
    """
    if not val or val == '':
        return None

    val_str = str(val).strip()

    # Check if it's a Unix timestamp (10 digits for seconds, more for milliseconds)
    if re.match(r'^\d{10}(\.\d+)?$', val_str):
        try:
            timestamp = float(val_str)
            return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
        except (ValueError, OSError):
            pass

    # Use parse_datetime for everything else
    return parse_datetime(val)


# Initialize default timezone from settings if present
_default_tz_setting = settings.get('default_timezone', None)
if _default_tz_setting:
    set_default_timezone(_default_tz_setting)