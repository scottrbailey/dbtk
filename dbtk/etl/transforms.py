# dbtk/etl/transforms.py

import datetime as dt
import re

from typing import Tuple, List, Any, Optional
from ..defaults import settings
from ..utils import ParamStyle, quote_identifier, validate_identifier
from ..cursors import DictCursor

try:
    import phonenumbers
    from phonenumbers import NumberParseException, PhoneNumberFormat
    HAS_PHONENUMBERS = True
except ImportError:
    HAS_PHONENUMBERS = False

_default_country = settings.get('default_country', 'US')
_default_timezone = settings.get('default_timezone', None)

# Enhanced regex patterns with timezone support
datePattern = re.compile(
    r'((?P<y1>\d{4})[\-|\/|\.](?P<m1>\d{1,2})[\-|\/|\.](?P<d1>\d{1,2}))|((?P<m2>\d{1,2})[\-|\/|\.](?P<d2>\d{1,2})[\-|\/|\.](?P<y2>\d{4}))')

dateLongPattern = re.compile(
    r'((?P<m1>[a-z]{3,9})[ |\-|\.]+(?P<d1>\d{1,2})[st|nd|rd|th]*[ |\-|\,]+(?P<y1>\d{4}))|((?P<d2>\d{1,2})*[ |\-|\.]*(?P<m2>[a-z]{3,9})[ |\-|\.|\,]+(?P<y2>\d{4}))',
    re.I)

timePattern = re.compile(
    r'(?P<hr>[0-2]?\d):(?P<mi>[0-6]\d):?(?P<sec>[0-6]\d)?(?P<fsec>\.\d{1,9})?(?P<am> ?[A|P]M)?(?P<tz>[ ]?(?P<offset>[+-]\d{2}:?\d{2})|[ ]?(?P<tzname>Z|UTC|GMT|EST|CST|MST|PST|EDT|CDT|MDT|PDT))?',
    re.I)

# ISO 8601 datetime pattern with timezone
isoPattern = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})[T ](?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(?P<microsecond>\.\d{1,6})?(?P<timezone>Z|[+-]\d{2}:?\d{2})?')

# Phone number regex patterns
phonePattern = re.compile(
    r'(?:(\+?1?[-.\s]?))?'           # Optional country code
    r'\(?(\d{3})\)?[-.\s]?'          # Area code (required)
    r'(\d{3})[-.\s]?'                # Exchange (required) 
    r'(\d{4})'                       # Number (required)
    r'(?:\s?(?:ext?|x|extension)\.?\s?(\d{1,6}))?'  # Optional extension
)

# International pattern for non-US numbers
intlPhonePattern = re.compile(
    r'(\+\d{1,3})[-.\s]?'            # Country code (required)
    r'(\d{1,4})[-.\s]?'              # Area/city code
    r'(\d{4,10})'                    # Main number
    r'(?:\s?(?:ext?|x|extension)\.?\s?(\d{1,6}))?'  # Optional extension
)

# Email validation regex - practical but not RFC-compliant
emailPattern = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
)

# Number regex patterns
intsOnlyPattern = re.compile(r'^[\-\+]?\d+$')
numbersOnlyPattern = re.compile(r'^[\-\+]?\d+(\.\d+)?$')

MONTHS_SHORT = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

MONTHS_LONG = ['', 'JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
               'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']


class CodeValidator:
    """
    CodeValidator is a class designed to validate input values against a specified column in a database table.

    This class provides efficient methods for validating values by querying the database or using preloaded
    valid codes, depending on the size of the dataset. It supports optional case sensitivity and uses
    a configurable threshold to determine if the codes should be preloaded into memory. The validation logic
    is handled dynamically based on the database's paramstyle.

    Examples:
        temple_validator = CodeValidator(cursor, table='temples', column='temple_id', case_sensitive=False)

        air_bender_table = Table('air_benders', {
                                'temple': {'field': 'home_temple', 'nullable': False, 'fn': temple_validator}},
                                cursor=cursor)
    """

    PRELOAD_THRESHOLD = 1000

    def __init__(self, cursor, table: str, column: str,
                 case_sensitive: bool = False):
        """
        Initializes a new instance of the validator class by setting up required table and column names,
        and optionally enabling case sensitivity. Automatically determines whether to preload all valid
        codes based on the size of the table and a configurable threshold.

        Parameters:
            cursor (cursor): A database cursor instance used to execute queries.
            table (str): The name of the table to validate against.
            column (str): The name of the column to validate against.
            case_sensitive (bool): Whether the validation is case-sensitive.
        """
        if isinstance(cursor, DictCursor):
            self._cursor = cursor.connection.cursor()
        else:
            self._cursor = cursor
        validate_identifier(table)
        validate_identifier(column)
        self.table = quote_identifier(table)
        self.column = quote_identifier(column)
        self.case_sensitive = case_sensitive
        self.threshold = settings.get('validator_preload_threshold', self.PRELOAD_THRESHOLD)
        self._valid_codes = set()
        self._preloaded = False

        # Get paramstyle from connection
        self._paramstyle = self._cursor.connection.interface.paramstyle
        self._placeholder = ParamStyle.get_placeholder(self._paramstyle)

        # Auto-decide whether to preload based on table size
        row_count = self._get_row_count()
        if row_count <= self.threshold:
            self._load_all_codes()
            self._preloaded = True

    def _get_row_count(self) -> int:
        """Get count of distinct values in validation column."""
        sql = f"SELECT COUNT(DISTINCT {self.column}) FROM {self.table}"
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def _load_all_codes(self):
        """Preload all valid codes into memory."""
        sql = f"SELECT DISTINCT {self.column} FROM {self.table}"
        self._cursor.execute(sql)
        # Filter out None and empty strings, handle case sensitivity
        codes = {row[0] for row in self._cursor.fetchall()
                 if row[0] not in (None, '')}
        self._valid_codes = {c if self.case_sensitive else str(c).upper() for c in codes}

    def _is_valid(self, value) -> bool:
        """Check if value is valid."""
        check_val = value if self.case_sensitive else str(value).upper()

        if self._preloaded:
            return check_val in self._valid_codes

        if check_val in self._valid_codes:
            return True

        # Query database using correct paramstyle
        sql = f"SELECT {self.column} FROM {self.table} WHERE {self.column} = {self._placeholder}"

        if self._paramstyle in ParamStyle.positional_styles():
            self._cursor.execute(sql, (value,))
        else:
            self._cursor.execute(sql, {'val': value})

        if self._cursor.fetchone():
            self._valid_codes.add(check_val)
            return True

        return False

    def __call__(self, value):
        """Validate value against database table."""
        if value is None or value == '':
            return value
        return value if self._is_valid(value) else None


class CodeLookup:
    """
    Lookup/translate codes using a database reference table.

    Automatically decides whether to preload based on table size.

    Examples:
        # State abbreviation to full name
        state_lookup = CodeLookup(cursor, 'states', 'abbrev', 'full_name')
        state_lookup('CA')  # -> 'California'

        # Country code to name
        country_lookup = CodeLookup(cursor, 'countries', 'iso_code', 'name')

        # Use in table config
        table = Table('citizens', {
            'state_name': {'field': 'state_code', 'fn': state_lookup}
        }, cursor=cursor)
    """

    PRELOAD_THRESHOLD = 500

    def __init__(self, cursor, table: str, from_column: str, to_column: str,
                 case_sensitive: bool = False, default: Any = None):
        """
        Initializes an instance of a lookup system which maps values from one column to another
        within a specific database table, using a provided database cursor for operation.

        Parameters:
            cursor: A database cursor instance used to execute queries.
            table (str): The name of the database table being accessed.
            from_column (str): The source column being used as the key.
            to_column (str): The target column being used to retrieve mapped values.
            case_sensitive (bool): Optional; specifies if the lookup is case-sensitive. Defaults to False.
            default (Any): Optional; the default value returned if no match is located in the table.
        """
        # Create new RecordCursor if passed a DictCursor
        if isinstance(cursor, DictCursor):
            self._cursor = cursor.connection.cursor()
        else:
            self._cursor = cursor

        # Validate and quote identifiers
        validate_identifier(table)
        validate_identifier(from_column)
        validate_identifier(to_column)
        self.table = quote_identifier(table)
        self.from_column = quote_identifier(from_column)
        self.to_column = quote_identifier(to_column)
        self.case_sensitive = case_sensitive
        self.default = default
        self._cache = {}
        self._preloaded = False

        # Get paramstyle from connection
        self._paramstyle = self._cursor.connection.interface.paramstyle
        self._placeholder = ParamStyle.get_placeholder(self._paramstyle)

        # Get threshold from settings or use default
        threshold = settings.get('lookup_preload_threshold', self.PRELOAD_THRESHOLD)

        # Auto-decide whether to preload
        row_count = self._get_row_count()
        if row_count <= threshold:
            self._load_all()
            self._preloaded = True

    def _get_row_count(self) -> int:
        """Get count of rows in lookup table."""
        sql = f"SELECT COUNT(*) FROM {self.table}"
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def _load_all(self):
        """Preload entire lookup table into cache."""
        sql = f"SELECT {self.from_column}, {self.to_column} FROM {self.table}"
        self._cursor.execute(sql)
        for row in self._cursor.fetchall():
            key = row[0]
            # Skip rows with null/empty keys
            if key in (None, ''):
                continue
            if not self.case_sensitive:
                key = str(key).upper()
            self._cache[key] = row[1]

    def _lookup(self, value):
        """Lookup value in database."""
        sql = f"SELECT {self.to_column} FROM {self.table} WHERE {self.from_column} = {self._placeholder}"

        if self._paramstyle in ParamStyle.positional_styles():
            self._cursor.execute(sql, (value,))
        else:
            self._cursor.execute(sql, {'val': value})

        row = self._cursor.fetchone()
        return row[0] if row else None

    def __call__(self, value):
        """Lookup value in reference table."""
        if value is None or value == '':
            return self.default

        lookup_key = value if self.case_sensitive else str(value).upper()

        if lookup_key not in self._cache:
            result = self._lookup(value)
            self._cache[lookup_key] = result if result is not None else self.default

        return self._cache[lookup_key]

class PhoneFormat:
    """Phone number formatting styles."""
    NATIONAL = 'national'  # (555) 123-4567
    INTERNATIONAL = 'international'  # +1 (555) 123-4567
    E164 = 'e164'  # +15551234567
    DIGITS = 'digits'  # 5551234567
    RFC3966 = 'rfc3966' # +1-555-123-4567


class Phone:
    """
    Phone number parser and formatter with international support.

    Uses the phonenumbers library when available for proper international
    phone number parsing and validation. Falls back to basic North American
    parsing when phonenumbers is not installed.

    Examples:
        # US number
        phone = Phone("(555) 123-4567")
        print(phone.format(PhoneFormat.INTERNATIONAL))  # +1 555 123 4567

        # International number (requires phonenumbers library)
        phone = Phone("+44 20 7946 0958", "GB")
        print(phone.is_valid)  # True
        print(phone.format(PhoneFormat.NATIONAL))  # 020 7946 0958

        # Extension support
        phone = Phone("555-123-4567 ext 123")
        print(phone.extension)  # "123"
    """

    def __init__(self, value: str, country: Optional[str] = None):
        """
        Parse phone number from string.

        Args:
            value: Phone number string to parse
            country: ISO 3166-1 alpha-2 country code (e.g., 'US', 'GB', 'FR')
                    Uses config default if None
        """
        self.raw = str(value) if value else ''

        # Initialize all attributes
        self.country_code = None
        self.area_code = None
        self.exchange = None
        self.number = None
        self.extension = None
        self._parsed_number = None
        self._country = country or _default_country

        if HAS_PHONENUMBERS:
            self._parse_with_phonenumbers()
        else:
            self._parse_basic()

    def _parse_with_phonenumbers(self):
        """Parse using phonenumbers library for international support."""
        if not self.raw.strip():
            return

        try:
            # Parse the number
            self._parsed_number = phonenumbers.parse(self.raw, self._country)

            # Extract components
            self.country_code = f"+{self._parsed_number.country_code}"

            # Format national number to extract area code and local number
            national = phonenumbers.format_number(
                self._parsed_number, PhoneNumberFormat.NATIONAL
            ).strip()

            # Handle extension
            if self._parsed_number.extension:
                self.extension = self._parsed_number.extension
            else:
                # Try to extract extension from original string
                self.extension = self._extract_extension_from_raw()

            # For North American numbers, extract traditional components
            if self._parsed_number.country_code == 1:  # NANP
                self._extract_nanp_components(national)
            else:
                # For international numbers, store the national number
                # Remove parentheses, spaces, hyphens for consistent storage
                clean_national = re.sub(r'[^\d]', '', national)
                if len(clean_national) >= 4:
                    self.area_code = clean_national[:-7] if len(clean_national) > 7 else clean_national[:-4]
                    remaining = clean_national[-7:] if len(clean_national) > 7 else clean_national[-4:]
                    if len(remaining) >= 4:
                        self.exchange = remaining[:-4] if len(remaining) > 4 else remaining
                        self.number = remaining[-4:]
                else:
                    self.exchange = clean_national

        except NumberParseException:
            # Fall back to basic parsing if phonenumbers can't parse it
            self._parse_basic()

    def _extract_nanp_components(self, national_format: str):
        """Extract area code, exchange, and number for North American numbers."""
        # Remove non-digits except for potential extension markers
        digits_only = re.sub(r'[^\d]', '', national_format)

        if len(digits_only) == 10:
            self.area_code = digits_only[:3]
            self.exchange = digits_only[3:6]
            self.number = digits_only[6:]
        elif len(digits_only) == 7:
            # Local number only
            self.exchange = digits_only[:3]
            self.number = digits_only[3:]

    def _extract_extension_from_raw(self) -> Optional[str]:
        """Extract extension from the raw input string."""
        ext_patterns = [
            r'(?:ext?|extension|x)\.?\s*(\d{1,6})$',
            r'#(\d{1,6})$'
        ]

        for pattern in ext_patterns:
            match = re.search(pattern, self.raw, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _parse_basic(self):
        """Fallback parsing for North American numbers when phonenumbers unavailable."""
        if not self.raw.strip():
            return

        val_clean = self.raw.strip()

        # North American phone pattern
        na_pattern = re.compile(
            r'(?:(\+?1?[-.\s]?))?'  # Optional country code
            r'\(?(\d{3})\)?[-.\s]?'  # Area code
            r'(\d{3})[-.\s]?'  # Exchange
            r'(\d{4})'  # Number
            r'(?:\s?(?:ext?|x|extension|#)\.?\s?(\d{1,6}))?'  # Extension
        )

        match = na_pattern.search(val_clean)
        if match:
            country, area, exchange, number, ext = match.groups()

            # Clean country code
            if country:
                country = re.sub(r'[^\d+]', '', country)
                if country and not country.startswith('+'):
                    country = '+' + country
            elif self._country in ('US', 'CA'):
                country = '+1'

            self.country_code = country
            self.area_code = area
            self.exchange = exchange
            self.number = number
            self.extension = ext

    @property
    def is_valid(self) -> bool:
        """Check if phone number is valid."""
        if HAS_PHONENUMBERS and self._parsed_number:
            return phonenumbers.is_valid_number(self._parsed_number)
        else:
            # Basic validation for North American numbers
            return bool(self.exchange and self.number and
                        len(self.exchange) == 3 and len(self.number) == 4)

    @property
    def is_possible(self) -> bool:
        """Check if phone number is possibly valid (less strict than is_valid)."""
        if HAS_PHONENUMBERS and self._parsed_number:
            return phonenumbers.is_possible_number(self._parsed_number)
        else:
            return self.is_valid

    @property
    def country(self) -> Optional[str]:
        """Get the detected country code."""
        if HAS_PHONENUMBERS and self._parsed_number:
            return phonenumbers.region_code_for_number(self._parsed_number)
        elif self.country_code == '+1':
            return self._country if self._country in ('US', 'CA') else 'US'
        return None

    @property
    def number_type(self) -> Optional[str]:
        """Get the type of phone number (mobile, fixed_line, etc.)."""
        if HAS_PHONENUMBERS and self._parsed_number:
            nt = phonenumbers.number_type(self._parsed_number)
            type_names = {
                0: 'fixed_line',
                1: 'mobile',
                2: 'fixed_line_or_mobile',
                3: 'toll_free',
                4: 'premium_rate',
                5: 'shared_cost',
                6: 'voip',
                7: 'personal_number',
                8: 'pager',
                9: 'uan',
                10: 'voicemail'
            }
            return type_names.get(nt, 'unknown')
        return None

    def format(self, style=PhoneFormat.NATIONAL) -> str:
        """
        Format phone number in specified style.

        Args:
            style: PhoneFormat value

        Returns:
            Formatted phone number string
        """
        if not self.is_valid:
            return self.raw

        if HAS_PHONENUMBERS and self._parsed_number and style != PhoneFormat.RFC3966:
            return self._format_with_phonenumbers(style)
        else:
            return self._format_basic(style)

    def _format_with_phonenumbers(self, style: PhoneFormat) -> str:
        """Format using phonenumbers library."""
        if style == PhoneFormat.NATIONAL:
            formatted = phonenumbers.format_number(self._parsed_number, PhoneNumberFormat.NATIONAL)
        elif style == PhoneFormat.INTERNATIONAL:
            formatted = phonenumbers.format_number(self._parsed_number, PhoneNumberFormat.INTERNATIONAL)
        elif style == PhoneFormat.E164:
            formatted = phonenumbers.format_number(self._parsed_number, PhoneNumberFormat.E164)
        elif style == PhoneFormat.DIGITS:
            # Remove all non-digits except country code
            if self._parsed_number.country_code == 1:  # North America
                formatted = f"{self.area_code or ''}{self.exchange or ''}{self.number or ''}"
            else:
                formatted = phonenumbers.format_number(self._parsed_number, PhoneNumberFormat.E164).lstrip('+')
        else:
            formatted = phonenumbers.format_number(self._parsed_number, PhoneNumberFormat.NATIONAL)

        # Add extension if present
        if self.extension:
            if style == PhoneFormat.RFC3966:
                formatted += f";ext={self.extension}"
            else:
                formatted += f" ext. {self.extension}"

        return formatted

    def _format_basic(self, style: PhoneFormat) -> str:
        """Fallback formatting for North American numbers."""
        if not (self.exchange and self.number):
            return self.raw

        if style == PhoneFormat.NATIONAL and self.area_code:
            result = f"({self.area_code}) {self.exchange}-{self.number}"
        elif style == PhoneFormat.NATIONAL:
            result = f"{self.exchange}-{self.number}"
        elif style == PhoneFormat.INTERNATIONAL:
            country = self.country_code or '+1'
            if self.area_code:
                result = f"{country} {self.area_code} {self.exchange} {self.number}"
            else:
                result = f"{country} {self.exchange} {self.number}"
        elif style == PhoneFormat.E164:
            country = (self.country_code or '+1').lstrip('+')
            area = self.area_code or ''
            result = f"+{country}{area}{self.exchange}{self.number}"
        elif style == PhoneFormat.DIGITS:
            area = self.area_code or ''
            result = f"{area}{self.exchange}{self.number}"
        elif style == PhoneFormat.RFC3966:
            country = (self.country_code or '+1').lstrip('+')
            area = self.area_code or ''
            result = f"tel:+{country}-{area}-{self.exchange}-{self.number}"
        else:
            # Default to national
            return self.format(PhoneFormat.NATIONAL)

        # Add extension
        if self.extension:
            if style == PhoneFormat.RFC3966:
                result += f";ext={self.extension}"
            else:
                result += f" ext. {self.extension}"

        return result

    def __str__(self) -> str:
        """Default string representation (national format)."""
        return self.format(PhoneFormat.NATIONAL)


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


def set_default_timezone(timezone_name: str):
    """Set the default timezone for date/time parsing."""
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
    """Get the current default timezone."""
    return _default_timezone


def _parse_timezone_offset(tz_str: str) -> Optional[dt.timezone]:
    """Parse timezone offset string into timezone object."""
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

    if isinstance(val, dt.date):
        return val
    if isinstance(val, dt.datetime):
        return val.date()

    val_str = str(val).strip()
    if not val_str:
        return None

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

    # Fallback to dateutil if available
    try:
        from dateutil import parser as dateutil_parser
        parsed = dateutil_parser.parse(val_str, default=dt.datetime(1900, 1, 1))
        return parsed.date()
    except (ImportError, ValueError, TypeError):
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

    # Fallback to dateutil
    try:
        from dateutil import parser as dateutil_parser
        parsed = dateutil_parser.parse(val_str, default=dt.datetime(1900, 1, 1))
        return parsed.time()
    except (ImportError, ValueError, TypeError):
        return None


def parse_datetime(val: Any, default_tz: Optional[str] = None) -> Optional[dt.datetime]:
    """
    Parse various datetime formats to datetime object.

    Args:
        val: Datetime string, date object, or other value
        default_tz: Default timezone name if not specified in string

    Returns:
        datetime object or None if parsing fails

    Examples:
        parse_datetime("2024-01-15 14:30:00")         # -> datetime(2024, 1, 15, 14, 30)
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

            # Apply default timezone if no timezone was specified
            if tz_info is None and default_tz:
                default_tz_obj = None
                if default_tz.upper() in TIMEZONE_OFFSETS:
                    default_tz_obj = TIMEZONE_OFFSETS[default_tz.upper()]
                else:
                    default_tz_obj = _parse_timezone_offset(default_tz)
                if default_tz_obj:
                    result = result.replace(tzinfo=default_tz_obj)
            elif tz_info is None and _default_timezone:
                result = result.replace(tzinfo=_default_timezone)

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
        elif default_tz:
            default_tz_obj = None
            if default_tz.upper() in TIMEZONE_OFFSETS:
                default_tz_obj = TIMEZONE_OFFSETS[default_tz.upper()]
            else:
                default_tz_obj = _parse_timezone_offset(default_tz)
            if default_tz_obj:
                result = result.replace(tzinfo=default_tz_obj)
        elif _default_timezone:
            result = result.replace(tzinfo=_default_timezone)
        return result
    elif d:
        result = dt.datetime.combine(d, dt.time.min)
        if default_tz:
            default_tz_obj = None
            if default_tz.upper() in TIMEZONE_OFFSETS:
                default_tz_obj = TIMEZONE_OFFSETS[default_tz.upper()]
            else:
                default_tz_obj = _parse_timezone_offset(default_tz)
            if default_tz_obj:
                result = result.replace(tzinfo=default_tz_obj)
        elif _default_timezone:
            result = result.replace(tzinfo=_default_timezone)
        return result
    elif t:
        # Time only - use today's date
        today = dt.date.today()
        result = dt.datetime.combine(today, t.replace(tzinfo=None))
        if t.tzinfo:
            result = result.replace(tzinfo=t.tzinfo)
        return result

    # Fallback to dateutil
    try:
        from dateutil import parser as dateutil_parser
        parsed = dateutil_parser.parse(val_str)
        if parsed.tzinfo is None and default_tz:
            default_tz_obj = None
            if default_tz.upper() in TIMEZONE_OFFSETS:
                default_tz_obj = TIMEZONE_OFFSETS[default_tz.upper()]
            else:
                default_tz_obj = _parse_timezone_offset(default_tz)
            if default_tz_obj:
                parsed = parsed.replace(tzinfo=default_tz_obj)
        elif parsed.tzinfo is None and _default_timezone:
            parsed = parsed.replace(tzinfo=_default_timezone)
        return parsed
    except (ImportError, ValueError, TypeError):
        return None


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
    return parse_datetime(val, default_tz)


def capitalize(val):
    """Title case a string, but not if it is already in mixed-case"""
    if val and hasattr(val, 'title'):
        if val in (val.upper(), val.lower()):
            return val.title()
    return val


def coalesce(vals: Tuple[List[Any]]):
    """
    Returns the first non-empty and non-None value from a list or tuple.
    """
    if isinstance(vals, (list, tuple)):
        for v in vals:
            if v not in ('', None):
                return v
    return vals


def indicator(val, true_val='Y', false_val=None):
    """ Convert a value to a boolean indicator. """
    if val and str(val).upper() in ('T', 'TRUE', 'YES', 'Y', '1'):
        return true_val
    return false_val


def get_int(val) -> int:
    if val:
        return int(float(val))


def get_float(val) -> float:
    if val:
        return float(val)


def get_item_from_delimited_list(val, index, delimiter=','):
    """ Get an item from a delimited list."""
    if val:
        values = val.split(delimiter)
        if index < len(values):
            return values[index].strip()


def phone_clean(val: str, country: Optional[str] = None) -> str:
    """
    Clean and format phone number in national format.

    Args:
        val: Phone number string to clean
        country: ISO country code (e.g., 'US', 'GB', 'FR')

    Returns:
        Cleaned phone number in national format, or original string if invalid

    Examples:
        phone_clean("5551234567")  # "(555) 123-4567"
        phone_clean("+44 20 7946 0958", "GB")  # "020 7946 0958"
    """
    phone = Phone(val, country)
    return phone.format(PhoneFormat.NATIONAL) if phone.is_valid else str(val)


def phone_format(val: str, style=PhoneFormat.NATIONAL, country: Optional[str] = None) -> str:
    """
    Format phone number in specified style.

    Args:
        val: Phone number string to format
        style: PhoneFormat value or string
        country: ISO country code

    Returns:
        Formatted phone number or original string if invalid

    Examples:
        phone_format("5551234567", PhoneFormat.E164)  # "+15551234567"
        phone_format("+44 20 7946 0958", "international")  # "+44 20 7946 0958"
    """
    # Handle string style values for backward compatibility
    if isinstance(style, str):
        style_map = {
            'national': PhoneFormat.NATIONAL,
            'international': PhoneFormat.INTERNATIONAL,
            'e164': PhoneFormat.E164,
            'rfc3966': PhoneFormat.RFC3966,
            'digits': PhoneFormat.DIGITS
        }
        style = style_map.get(style.lower(), PhoneFormat.NATIONAL)

    phone = Phone(val, country)
    return phone.format(style) if phone.is_valid else str(val)


def phone_get_area_code(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Extract area code from phone number.

    Args:
        val: Phone number string
        country: ISO country code

    Returns:
        Area code string or None if not found/applicable

    Examples:
        phone_get_area_code("(555) 123-4567")  # "555"
        phone_get_area_code("+44 20 7946 0958", "GB")  # "20"
    """
    return Phone(val, country).area_code


def phone_get_exchange(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Extract exchange (first 3 digits of local number) from phone number.

    Note: This concept primarily applies to North American numbering.
    For international numbers, may return part of the local number.

    Args:
        val: Phone number string
        country: ISO country code

    Returns:
        Exchange string or None if not found
    """
    return Phone(val, country).exchange


def phone_get_number(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Extract the last 4 digits of local number from phone number.

    Note: This concept primarily applies to North American numbering.
    For international numbers, may return part of the local number.

    Args:
        val: Phone number string
        country: ISO country code

    Returns:
        Number string or None if not found
    """
    return Phone(val, country).number


def phone_get_extension(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Extract extension from phone number.

    Args:
        val: Phone number string
        country: ISO country code

    Returns:
        Extension string or None if not found

    Examples:
        phone_get_extension("555-123-4567 ext 123")  # "123"
        phone_get_extension("555-123-4567 x456")     # "456"
        phone_get_extension("555-123-4567#789")      # "789"
    """
    return Phone(val, country).extension


def phone_get_country_code(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Extract country code from phone number.

    Args:
        val: Phone number string
        country: ISO country code for parsing context

    Returns:
        Country code with + prefix or None if not found

    Examples:
        phone_get_country_code("+1 555 123 4567")     # "+1"
        phone_get_country_code("+44 20 7946 0958")    # "+44"
    """
    return Phone(val, country).country_code


def phone_get_country(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Get the detected country for a phone number.

    Requires phonenumbers library for reliable international detection.

    Args:
        val: Phone number string
        country: ISO country code for parsing context

    Returns:
        ISO country code or None if not detected

    Examples:
        phone_get_country("+1 555 123 4567")     # "US"
        phone_get_country("+44 20 7946 0958")    # "GB"
    """
    return Phone(val, country).country


def phone_get_type(val: str, country: Optional[str] = None) -> Optional[str]:
    """
    Get the type of phone number (mobile, fixed_line, etc.).

    Requires phonenumbers library. Returns None if library not available.

    Args:
        val: Phone number string
        country: ISO country code

    Returns:
        Phone number type or None

    Possible types: 'fixed_line', 'mobile', 'fixed_line_or_mobile',
                   'toll_free', 'premium_rate', 'shared_cost', 'voip',
                   'personal_number', 'pager', 'uan', 'voicemail'
    """
    return Phone(val, country).number_type


def phone_validate(val: str, country: Optional[str] = None) -> bool:
    """
    Check if phone number is valid.

    Uses phonenumbers library for international validation when available.
    Falls back to basic North American validation otherwise.

    Args:
        val: Phone number string to validate
        country: ISO country code

    Returns:
        True if phone number is valid, False otherwise

    Examples:
        phone_validate("(555) 123-4567")        # True
        phone_validate("+44 20 7946 0958")      # True (if phonenumbers installed)
        phone_validate("123")                   # False
    """
    return Phone(val, country).is_valid


def email_clean(val: str) -> str:
    """
    Clean and normalize email address.

    Args:
        val: Email address string

    Returns:
        Cleaned email address (lowercase, stripped) or empty string if invalid
    """
    if not val:
        return ''

    val_clean = str(val).strip().lower()

    if email_validate(val_clean):
        return val_clean
    else:
        return ''


def email_validate(val: str) -> bool:
    """
    Validate email address format.

    Uses a practical regex that catches most valid emails and rejects
    obviously invalid ones. Not RFC 5322 compliant but good enough
    for most data validation needs.

    Args:
        val: Email address string to validate

    Returns:
        True if email appears valid, False otherwise

    Examples:
        email_validate("user@example.com")     # True
        email_validate("user.name@domain.co")  # True
        email_validate("invalid.email")        # False
        email_validate("@domain.com")          # False
    """
    if not val:
        return False

    val_clean = str(val).strip()
    if not val_clean:
        return False

    return bool(emailPattern.match(val_clean))