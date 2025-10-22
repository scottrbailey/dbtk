# dbtk/etl/transforms/address.py
"""
Address parsing, validation, and standardization.

Uses usaddress library for US address parsing with custom normalization.
Supports both generic international addresses and US-specific validation.

Required dependency:
    pip install usaddress
"""

import re
from typing import Any, Dict, Optional

try:
    import usaddress
    from usaddress import RepeatedLabelError

    HAS_USADDRESS = True
except ImportError:
    HAS_USADDRESS = False
    RepeatedLabelError = Exception

# US state codes for validation
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'AS', 'GU', 'MP', 'PR', 'VI'  # Territories
}

# Zip code patterns
ZIP_PATTERN = re.compile(r'^\d{5}(-\d{4})?$')

# Normalization mappings for USPS abbreviations
STREET_TYPES = {
    'ALLEY': 'Aly', 'ANNEX': 'Anx', 'ARCADE': 'Arc', 'AVENUE': 'Ave',
    'BAYOU': 'Byu', 'BEACH': 'Bch', 'BEND': 'Bnd', 'BLUFF': 'Blf',
    'BOTTOM': 'Btm', 'BOULEVARD': 'Blvd', 'BRANCH': 'Br', 'BRIDGE': 'Brg',
    'BROOK': 'Brk', 'BURG': 'Bg', 'BYPASS': 'Byp', 'CAMP': 'Cp',
    'CANYON': 'Cyn', 'CAPE': 'Cpe', 'CAUSEWAY': 'Cswy', 'CENTER': 'Ctr',
    'CIRCLE': 'Cir', 'CLIFF': 'Clf', 'CLUB': 'Clb', 'COMMON': 'Cmn',
    'CORNER': 'Cor', 'COURSE': 'Crse', 'COURT': 'Ct', 'COVE': 'Cv',
    'CREEK': 'Crk', 'CRESCENT': 'Cres', 'CROSSING': 'Xing', 'DALE': 'Dl',
    'DAM': 'Dm', 'DIVIDE': 'Dv', 'DRIVE': 'Dr', 'ESTATE': 'Est',
    'EXPRESSWAY': 'Expy', 'EXTENSION': 'Ext', 'FALL': 'Fall', 'FERRY': 'Fry',
    'FIELD': 'Fld', 'FLAT': 'Flt', 'FORD': 'Frd', 'FOREST': 'Frst',
    'FORGE': 'Frg', 'FORK': 'Frk', 'FORT': 'Ft', 'FREEWAY': 'Fwy',
    'GARDEN': 'Gdn', 'GATEWAY': 'Gtwy', 'GLEN': 'Gln', 'GREEN': 'Grn',
    'GROVE': 'Grv', 'HARBOR': 'Hbr', 'HAVEN': 'Hvn', 'HEIGHTS': 'Hts',
    'HIGHWAY': 'Hwy', 'HILL': 'Hl', 'HOLLOW': 'Holw', 'INLET': 'Inlt',
    'ISLAND': 'Is', 'ISLE': 'Isle', 'JUNCTION': 'Jct', 'KEY': 'Ky',
    'KNOLL': 'Knl', 'LAKE': 'Lk', 'LAND': 'Land', 'LANDING': 'Lndg',
    'LANE': 'Ln', 'LIGHT': 'Lgt', 'LOAF': 'Lf', 'LOCK': 'Lck',
    'LODGE': 'Ldg', 'LOOP': 'Loop', 'MALL': 'Mall', 'MANOR': 'Mnr',
    'MEADOW': 'Mdw', 'MILL': 'Ml', 'MISSION': 'Msn', 'MOUNT': 'Mt',
    'MOUNTAIN': 'Mtn', 'NECK': 'Nck', 'ORCHARD': 'Orch', 'PARK': 'Park',
    'PARKWAY': 'Pkwy', 'PASS': 'Pass', 'PATH': 'Path', 'PIKE': 'Pike',
    'PINE': 'Pne', 'PLACE': 'Pl', 'PLAIN': 'Pln', 'PLAZA': 'Plz',
    'POINT': 'Pt', 'PORT': 'Prt', 'PRAIRIE': 'Pr', 'RADIAL': 'Radl',
    'RANCH': 'Rnch', 'RAPID': 'Rpd', 'REST': 'Rst', 'RIDGE': 'Rdg',
    'RIVER': 'Riv', 'ROAD': 'Rd', 'ROUTE': 'Rte', 'ROW': 'Row',
    'RUN': 'Run', 'SHOAL': 'Shl', 'SHORE': 'Shr', 'SPRING': 'Spg',
    'SPUR': 'Spur', 'SQUARE': 'Sq', 'STATION': 'Sta', 'STREAM': 'Strm',
    'STREET': 'St', 'SUMMIT': 'Smt', 'TERRACE': 'Ter', 'TRACE': 'Trce',
    'TRACK': 'Trak', 'TRAIL': 'Trl', 'TUNNEL': 'Tunl', 'TURNPIKE': 'Tpke',
    'UNDERPASS': 'Upas', 'UNION': 'Un', 'VALLEY': 'Vly', 'VIADUCT': 'Via',
    'VIEW': 'Vw', 'VILLAGE': 'Vlg', 'VILLE': 'Vl', 'VISTA': 'Vis',
    'WALK': 'Walk', 'WALL': 'Wall', 'WAY': 'Way', 'WELL': 'Wl'
}

DIRECTIONALS = {
    'NORTH': 'N', 'SOUTH': 'S', 'EAST': 'E', 'WEST': 'W',
    'NORTHEAST': 'NE', 'NORTHWEST': 'NW', 'SOUTHEAST': 'SE', 'SOUTHWEST': 'SW'
}

OCCUPANCY_TYPES = {
    'APARTMENT': 'Apt', 'BASEMENT': 'Bsmt', 'BUILDING': 'Bldg',
    'DEPARTMENT': 'Dept', 'FLOOR': 'Fl', 'FRONT': 'Frnt',
    'HANGAR': 'Hngr', 'LOBBY': 'Lbby', 'LOT': 'Lot',
    'LOWER': 'Lowr', 'OFFICE': 'Ofc', 'PENTHOUSE': 'Ph',
    'PIER': 'Pier', 'REAR': 'Rear', 'ROOM': 'Rm',
    'SIDE': 'Side', 'SLIP': 'Slip', 'SPACE': 'Spc',
    'STOP': 'Stop', 'SUITE': 'Ste', 'TRAILER': 'Trlr',
    'UNIT': 'Unit', 'UPPER': 'Uppr'
}


def _check_usaddress():
    """Raise helpful error if usaddress not installed."""
    if not HAS_USADDRESS:
        raise ImportError(
            "Address functionality requires the usaddress library. "
            "Install with: pip install usaddress"
        )


def _normalize_component(value: str, mapping: Dict[str, str]) -> str:
    """Normalize a component using the provided mapping."""
    if not value:
        return value

    upper_value = value.upper().strip()
    return mapping.get(upper_value, value.title())


def _build_address_line(components: Dict[str, str]) -> str:
    """Build address line 1 from parsed components."""
    parts = []

    # Address number prefix (rare)
    if components.get('AddressNumberPrefix'):
        parts.append(components['AddressNumberPrefix'])

    # Address number
    if components.get('AddressNumber'):
        parts.append(components['AddressNumber'])

    # Address number suffix (rare)
    if components.get('AddressNumberSuffix'):
        parts.append(components['AddressNumberSuffix'])

    # Street name pre-directional
    if components.get('StreetNamePreDirectional'):
        parts.append(_normalize_component(
            components['StreetNamePreDirectional'], DIRECTIONALS))

    # Street name pre-modifier (rare)
    if components.get('StreetNamePreModifier'):
        parts.append(components['StreetNamePreModifier'].title())

    # Street name pre-type (rare, e.g., "Avenue A")
    if components.get('StreetNamePreType'):
        parts.append(_normalize_component(
            components['StreetNamePreType'], STREET_TYPES))

    # Street name
    if components.get('StreetName'):
        parts.append(components['StreetName'].title())

    # Street name post-type
    if components.get('StreetNamePostType'):
        parts.append(_normalize_component(
            components['StreetNamePostType'], STREET_TYPES))

    # Street name post-directional
    if components.get('StreetNamePostDirectional'):
        parts.append(_normalize_component(
            components['StreetNamePostDirectional'], DIRECTIONALS))

    return ' '.join(parts)


def _build_address_line_2(components: Dict[str, str]) -> Optional[str]:
    """Build address line 2 from parsed components."""
    parts = []

    # Building name
    if components.get('BuildingName'):
        parts.append(components['BuildingName'].title())

    # Occupancy type and identifier
    if components.get('OccupancyType'):
        occ_type = _normalize_component(
            components['OccupancyType'], OCCUPANCY_TYPES)
        parts.append(occ_type)

        if components.get('OccupancyIdentifier'):
            parts.append(components['OccupancyIdentifier'])

    # Subaddress type and identifier (alternative to occupancy)
    elif components.get('SubaddressType'):
        sub_type = _normalize_component(
            components['SubaddressType'], OCCUPANCY_TYPES)
        parts.append(sub_type)

        if components.get('SubaddressIdentifier'):
            parts.append(components['SubaddressIdentifier'])

    # USPS Box
    if components.get('USPSBoxType'):
        parts.append(components['USPSBoxType'])
        if components.get('USPSBoxID'):
            parts.append(components['USPSBoxID'])

    return ' '.join(parts) if parts else None


class Address:
    """
    Generic address parser and formatter.

    Parses address strings into components and provides standardized formatting.
    Works with any address format but optimized for US addresses.

    Examples:
        addr = Address("123 Main St, Springfield, IL 62701")
        print(addr.street_number)  # "123"
        print(addr.street_name)    # "Main"
        print(addr.street_type)    # "St"
        print(addr.city)           # "Springfield"
        print(addr.state)          # "IL"
        print(addr.postal_code)    # "62701"
        print(addr.format())       # Standardized format

        # From components
        addr = Address({
            'address_line_1': '123 Main St',
            'city': 'Springfield',
            'state': 'IL',
            'postal_code': '62701'
        })
    """

    def __init__(self, value: Any):
        """
        Parse address from string or dictionary.

        Args:
            value: Address string or dictionary of components
        """
        _check_usaddress()

        self.raw = value if isinstance(value, str) else None
        self._components = {}
        self._parsed_components = {}
        self._parsed = False

        if isinstance(value, str):
            self._parse_string(value)
        elif isinstance(value, dict):
            self._components = value.copy()
            self._parsed = True
        else:
            self.raw = str(value) if value else ''
            self._parse_string(self.raw)

    def _parse_string(self, address_string: str):
        """Parse address string into components."""
        if not address_string or not address_string.strip():
            return

        try:
            # Use usaddress to parse
            tagged, address_type = usaddress.tag(address_string)
            self._parsed_components = dict(tagged)

            # Build normalized components
            self._components = {
                'address_line_1': _build_address_line(self._parsed_components),
                'address_line_2': _build_address_line_2(self._parsed_components),
                'city': self._parsed_components.get('PlaceName', '').title() if self._parsed_components.get(
                    'PlaceName') else None,
                'state': self._parsed_components.get('StateName', '').upper() if self._parsed_components.get(
                    'StateName') else None,
                'postal_code': self._parsed_components.get('ZipCode'),
            }

            # Remove None values
            self._components = {k: v for k, v in self._components.items() if v}

            self._parsed = True

        except RepeatedLabelError:
            # If parsing fails with ambiguous results, store as-is
            self._components = {'address_line_1': address_string}
            self._parsed = False

    @property
    def address_line_1(self) -> Optional[str]:
        """First line of address (street)."""
        return self._components.get('address_line_1')

    @property
    def address_line_2(self) -> Optional[str]:
        """Second line of address (unit, apt, etc.)."""
        return self._components.get('address_line_2')

    @property
    def city(self) -> Optional[str]:
        """City name."""
        return self._components.get('city')

    @property
    def state(self) -> Optional[str]:
        """State/province/region code."""
        return self._components.get('state')

    @property
    def postal_code(self) -> Optional[str]:
        """Postal/zip code."""
        return self._components.get('postal_code')

    @property
    def country(self) -> Optional[str]:
        """Country code or name."""
        return self._components.get('country')

    # Component properties
    @property
    def street_number(self) -> Optional[str]:
        """Street number/building number."""
        return self._parsed_components.get('AddressNumber')

    @property
    def street_name(self) -> Optional[str]:
        """Street name without number or type."""
        name = self._parsed_components.get('StreetName')
        return name.title() if name else None

    @property
    def street_type(self) -> Optional[str]:
        """Street type (St, Ave, Rd, etc.)."""
        street_type = self._parsed_components.get('StreetNamePostType')
        if street_type:
            return _normalize_component(street_type, STREET_TYPES)
        return None

    @property
    def unit_number(self) -> Optional[str]:
        """Unit/apartment/suite number."""
        return (self._parsed_components.get('OccupancyIdentifier') or
                self._parsed_components.get('SubaddressIdentifier'))

    @property
    def components(self) -> Dict[str, str]:
        """Get all address components as dictionary."""
        return self._components.copy()

    def format(self, style: str = 'standard') -> str:
        """
        Format address in specified style.

        Args:
            style: Format style ('standard', 'single_line', 'multiline')

        Returns:
            Formatted address string

        Examples:
            addr.format('standard')      # "123 Main St, Springfield, IL 62701"
            addr.format('single_line')   # "123 Main St Springfield IL 62701"
            addr.format('multiline')     # Multi-line format
        """
        if not self._parsed or not self._components:
            return self.raw or ''

        line1 = self.address_line_1 or ''
        line2 = self.address_line_2 or ''
        city = self.city or ''
        state = self.state or ''
        postal = self.postal_code or ''

        if style == 'single_line':
            parts = [line1, line2, city, state, postal]
            return ' '.join(p for p in parts if p)

        elif style == 'multiline':
            lines = []
            if line1:
                lines.append(line1)
            if line2:
                lines.append(line2)
            if city or state or postal:
                city_line = ', '.join(p for p in [city, state, postal] if p)
                lines.append(city_line)
            return '\n'.join(lines)

        else:  # standard
            parts = [line1]
            if line2:
                parts.append(line2)
            if city or state or postal:
                city_line = f"{city}, {state} {postal}".strip()
                parts.append(city_line)
            return ', '.join(p for p in parts if p)

    def __str__(self) -> str:
        """Default string representation (standard format)."""
        return self.format('standard')


class USAddress(Address):
    """
    US-specific address with validation.

    Extends Address with US-specific validation rules including:
    - Valid 2-letter state code
    - Valid ZIP code format (5 digits or 5+4)
    - Required components (street, city, state, zip)

    Examples:
        addr = USAddress("123 Main St, Springfield, IL 62701")
        print(addr.is_valid)  # True

        addr = USAddress("123 Main St, Springfield, XX 12345")
        print(addr.is_valid)  # False (invalid state)
        print(addr.validation_errors)  # ["Invalid state code: XX"]

        # Check individual components
        addr = USAddress("Springfield, IL")
        print(addr.has_street)   # False
        print(addr.has_city)     # True
        print(addr.has_state)    # True
        print(addr.has_zip)      # False
    """

    def __init__(self, value: Any):
        """
        Parse and validate US address.

        Args:
            value: Address string or dictionary of components
        """
        super().__init__(value)
        self._validation_errors = None

    @property
    def has_street(self) -> bool:
        """Check if address has street component."""
        return bool(self.address_line_1)

    @property
    def has_city(self) -> bool:
        """Check if address has city component."""
        return bool(self.city)

    @property
    def has_state(self) -> bool:
        """Check if address has state component."""
        return bool(self.state)

    @property
    def has_zip(self) -> bool:
        """Check if address has postal code component."""
        return bool(self.postal_code)

    @property
    def is_valid_state(self) -> bool:
        """Check if state code is valid US state."""
        if not self.state:
            return False
        return self.state.upper() in US_STATES

    @property
    def is_valid_zip(self) -> bool:
        """Check if ZIP code format is valid."""
        if not self.postal_code:
            return False
        return bool(ZIP_PATTERN.match(self.postal_code))

    @property
    def validation_errors(self) -> list:
        """Get list of validation errors."""
        if self._validation_errors is None:
            self._validation_errors = self._validate()
        return self._validation_errors

    def _validate(self) -> list:
        """Perform validation and return list of errors."""
        errors = []

        if not self.has_street:
            errors.append("Missing street address")

        if not self.has_city:
            errors.append("Missing city")

        if not self.has_state:
            errors.append("Missing state")
        elif not self.is_valid_state:
            errors.append(f"Invalid state code: {self.state}")

        if not self.has_zip:
            errors.append("Missing ZIP code")
        elif not self.is_valid_zip:
            errors.append(f"Invalid ZIP code format: {self.postal_code}")

        return errors

    @property
    def is_valid(self) -> bool:
        """Check if address is valid according to US rules."""
        return len(self.validation_errors) == 0

    def validate(self) -> bool:
        """
        Validate the address.

        Returns:
            True if valid, False otherwise

        Note: Use validation_errors property to see specific issues
        """
        return self.is_valid


# Convenience functions

def parse_address(address: str) -> Dict[str, str]:
    """
    Parse address string into components.

    Args:
        address: Address string to parse

    Returns:
        Dictionary of address components

    Examples:
        components = parse_address("123 Main St, Springfield, IL 62701")
        # {'address_line_1': '123 Main St', 'city': 'Springfield', ...}
    """
    addr = Address(address)
    return addr.components


def standardize_address(address: str, style: str = 'standard') -> str:
    """
    Standardize address formatting.

    Args:
        address: Address string to standardize
        style: Format style ('standard', 'single_line', 'multiline')

    Returns:
        Standardized address string

    Examples:
        standardize_address("123 main street, springfield il 62701")
        # "123 Main St, Springfield, IL 62701"
    """
    addr = Address(address)
    return addr.format(style)


def validate_us_address(address: str) -> bool:
    """
    Validate US address format and components.

    Args:
        address: Address string to validate

    Returns:
        True if valid US address, False otherwise

    Examples:
        validate_us_address("123 Main St, Springfield, IL 62701")  # True
        validate_us_address("123 Main St, Springfield, XX 12345")  # False
    """
    addr = USAddress(address)
    return addr.is_valid