# tests/test_transforms_core.py
"""
Tests for dbtk.etl.transforms.core module.

Testing core transformation functions with Avatar: The Last Airbender themed data.
Like a master of all elements, these functions bend data into the forms we need.
"""

import pytest
from dbtk.etl.transforms.core import (
    capitalize,
    coalesce,
    get_bool,
    indicator,
    get_digits,
    to_number,
    get_int,
    get_float,
    normalize_whitespace,
    format_number,
    parse_list,
    get_list_item,
    intsOnlyPattern,
    numbersOnlyPattern
)


class TestCapitalize:
    """Test capitalize function for proper title casing."""

    def test_capitalize_all_uppercase(self):
        """Test capitalizing all uppercase names."""
        assert capitalize("AVATAR AANG") == "Avatar Aang"
        assert capitalize("FIRE LORD OZAI") == "Fire Lord Ozai"
        assert capitalize("EARTH KINGDOM") == "Earth Kingdom"

    def test_capitalize_all_lowercase(self):
        """Test capitalizing all lowercase names."""
        assert capitalize("avatar aang") == "Avatar Aang"
        assert capitalize("fire lord ozai") == "Fire Lord Ozai"
        assert capitalize("earth kingdom") == "Earth Kingdom"

    def test_capitalize_mixed_case_unchanged(self):
        """Test that mixed case strings remain unchanged."""
        assert capitalize("Avatar Aang") == "Avatar Aang"
        assert capitalize("Fire Lord Ozai") == "Fire Lord Ozai"
        assert capitalize("Ba Sing Se") == "Ba Sing Se"

    def test_capitalize_none(self):
        """Test capitalizing None returns None."""
        assert capitalize(None) is None

    def test_capitalize_non_string(self):
        """Test capitalizing non-string values."""
        assert capitalize(123) == 123
        assert capitalize(45.67) == 45.67


class TestCoalesce:
    """Test coalesce function for finding first non-empty value."""

    def test_coalesce_first_non_empty(self):
        """Test coalesce returns first non-empty value."""
        assert coalesce([None, '', 'Aang', 'Katara']) == 'Aang'
        assert coalesce([None, None, 'Water Tribe']) == 'Water Tribe'
        assert coalesce(['Fire Nation', 'Earth Kingdom']) == 'Fire Nation'

    def test_coalesce_with_zero(self):
        """Test that zero is considered a valid value."""
        assert coalesce([None, '', 0, 5]) == 0
        assert coalesce([0, 1, 2]) == 0

    def test_coalesce_all_empty(self):
        """Test coalesce when all values are empty."""
        assert coalesce([None, None, '']) == None
        assert coalesce(['', None, '']) == None

    def test_coalesce_tuple(self):
        """Test coalesce works with tuples."""
        assert coalesce((None, '', 'Southern Air Temple')) == 'Southern Air Temple'

    def test_coalesce_non_list(self):
        """Test coalesce with non-list returns the value."""
        assert coalesce('Appa') == None
        assert coalesce(42) == None


class TestGetBool:
    """Test get_bool function for boolean parsing."""

    def test_get_bool_true_values(self):
        """Test parsing truthy string values."""
        assert get_bool('T') is True
        assert get_bool('TRUE') is True
        assert get_bool('true') is True
        assert get_bool('Yes') is True
        assert get_bool('Y') is True
        assert get_bool('1') is True

    def test_get_bool_false_values(self):
        """Test parsing falsy string values."""
        assert get_bool('F') is False
        assert get_bool('FALSE') is False
        assert get_bool('false') is False
        assert get_bool('No') is False
        assert get_bool('N') is False
        assert get_bool('0') is False
        assert get_bool('') is False

    def test_get_bool_boolean_type(self):
        """Test parsing actual boolean values."""
        assert get_bool(True) is True
        assert get_bool(False) is False

    def test_get_bool_numeric_types(self):
        """Test parsing numeric values."""
        assert get_bool(1) is True
        assert get_bool(100) is True
        assert get_bool(1.0) is True
        assert get_bool(0) is False
        assert get_bool(0.0) is False

    def test_get_bool_none(self):
        """Test parsing None returns None."""
        assert get_bool(None) is None

    def test_get_bool_other_strings(self):
        """Test parsing other string values as truthy."""
        assert get_bool('Aang') is True
        assert get_bool('Avatar') is True


class TestIndicator:
    """Test indicator function for boolean indicators."""

    def test_indicator_true_values(self):
        """Test indicator returns true_val for truthy values."""
        assert indicator(True) == 'Y'
        assert indicator('yes') == 'Y'
        assert indicator(1) == 'Y'
        assert indicator('T') == 'Y'

    def test_indicator_false_values(self):
        """Test indicator returns false_val for falsy values."""
        assert indicator(False) is None
        assert indicator('no') is None
        assert indicator(0) is None
        assert indicator('F') is None

    def test_indicator_custom_values(self):
        """Test indicator with custom true/false values."""
        assert indicator(True, 'Fire', 'Water') == 'Fire'
        assert indicator(False, 'Fire', 'Water') == 'Water'
        assert indicator(1, 'Bender', 'Non-Bender') == 'Bender'
        assert indicator(0, 'Bender', 'Non-Bender') == 'Non-Bender'

    def test_indicator_none(self):
        """Test indicator with None returns None."""
        assert indicator(None) is None
        assert indicator(None, 'Y', 'N') is None


class TestGetDigits:
    """Test get_digits function for extracting numeric digits."""

    def test_get_digits_phone_number(self):
        """Test extracting digits from Fire Nation phone numbers."""
        assert get_digits("(555) 123-4567") == "5551234567"
        assert get_digits("+1-800-FIRE-LORD") == "1800"
        assert get_digits("555.123.4567") == "5551234567"

    def test_get_digits_ssn(self):
        """Test extracting digits from Earth Kingdom ID numbers."""
        assert get_digits("012-34-5678") == "012345678"
        assert get_digits("123-45-6789") == "123456789"

    def test_get_digits_currency(self):
        """Test extracting digits from currency values."""
        assert get_digits("$-42.35") == "4235"
        assert get_digits("$1,234.56") == "123456"
        assert get_digits("+$500") == "500"

    def test_get_digits_leading_zeros(self):
        """Test that leading zeros are preserved."""
        assert get_digits("001234") == "001234"
        assert get_digits("0800") == "0800"

    def test_get_digits_empty(self):
        """Test extracting digits from empty values."""
        assert get_digits(None) == None
        assert get_digits('') == None

    def test_get_digits_no_digits(self):
        """Test extracting from strings with no digits."""
        assert get_digits("Avatar Aang") == None


class TestToNumber:
    """Test to_number function for converting to float."""

    def test_to_number_currency(self):
        """Test converting currency to numbers."""
        assert to_number("$42.35") == 42.35
        assert to_number("$-42.35") == -42.35
        assert to_number("$1,234.56") == 1234.56

    def test_to_number_with_commas(self):
        """Test converting numbers with thousand separators."""
        assert to_number("1,000") == 1000.0
        assert to_number("100,000") == 100000.0
        assert to_number("1,234,567.89") == 1234567.89

    def test_to_number_plain_numbers(self):
        """Test converting plain number strings."""
        assert to_number("42") == 42.0
        assert to_number("3.14159") == 3.14159
        assert to_number("-17.5") == -17.5

    def test_to_number_with_text(self):
        """Test converting numbers embedded in text."""
        assert to_number("Population: 100,000") == 100000.0
        assert to_number("Cost: $50.00") == 50.0

    def test_to_number_invalid(self):
        """Test converting invalid values returns None."""
        assert to_number("Avatar") is None
        assert to_number("N/A") is None
        assert to_number(None) is None
        assert to_number('') is None


class TestGetInt:
    """Test get_int function for integer conversion."""

    def test_get_int_string_numbers(self):
        """Test converting string numbers to integers."""
        assert get_int("123") == 123
        assert get_int("456") == 456
        assert get_int("-789") == -789

    def test_get_int_with_decimals(self):
        """Test converting decimal strings to integers."""
        assert get_int("123.45") == 123
        assert get_int("999.99") == 999

    def test_get_int_with_currency(self):
        """Test converting currency to integers."""
        assert get_int("$123.45") == 123
        assert get_int("$1,000") == 1000

    def test_get_int_float_type(self):
        """Test converting float values to integers."""
        assert get_int(123.45) == 123
        assert get_int(999.99) == 999

    def test_get_int_none(self):
        """Test converting None returns None."""
        assert get_int(None) is None
        assert get_int('') is None


class TestGetFloat:
    """Test get_float function for float conversion."""

    def test_get_float_string_numbers(self):
        """Test converting string numbers to floats."""
        assert get_float("123.45") == 123.45
        assert get_float("456") == 456.0
        assert get_float("-789.12") == -789.12

    def test_get_float_with_currency(self):
        """Test converting currency to floats."""
        assert get_float("$123.45") == 123.45
        assert get_float("$1,000.50") == 1000.50

    def test_get_float_none(self):
        """Test converting None returns None."""
        assert get_float(None) is None
        assert get_float('') is None


class TestNormalizeWhitespace:
    """Test normalize_whitespace function."""

    def test_normalize_multiple_spaces(self):
        """Test collapsing multiple spaces."""
        assert normalize_whitespace("Avatar  Aang") == "Avatar Aang"
        assert normalize_whitespace("Fire   Nation   Capital") == "Fire Nation Capital"

    def test_normalize_tabs(self):
        """Test converting tabs to single spaces."""
        assert normalize_whitespace("Aang\t\tKatara") == "Aang Katara"
        assert normalize_whitespace("Water\tTribe") == "Water Tribe"

    def test_normalize_newlines(self):
        """Test converting newlines to single spaces."""
        assert normalize_whitespace("Northern\n\nWater\nTribe") == "Northern Water Tribe"
        assert normalize_whitespace("Ba Sing\nSe") == "Ba Sing Se"

    def test_normalize_leading_trailing(self):
        """Test stripping leading and trailing whitespace."""
        assert normalize_whitespace("  Avatar Aang  ") == "Avatar Aang"
        assert normalize_whitespace("\t\tFire Nation\n\n") == "Fire Nation"

    def test_normalize_empty(self):
        """Test normalizing empty values."""
        assert normalize_whitespace(None) == ''
        assert normalize_whitespace('') == ''
        assert normalize_whitespace('   ') == ''


class TestFormatNumber:
    """Test format_number function for pattern formatting."""

    def test_format_phone_number(self):
        """Test formatting Fire Nation phone numbers."""
        assert format_number('8001234567', '(###) ###-####') == '(800) 123-4567'
        assert format_number('5551234567', '###.###.####') == '555.123.4567'

    def test_format_ssn(self):
        """Test formatting Earth Kingdom citizen IDs."""
        assert format_number('012345678', '###-##-####') == '012-34-5678'
        assert format_number('123456789', '###-##-####') == '123-45-6789'

    def test_format_with_non_numeric(self):
        """Test formatting extracts digits from input."""
        assert format_number('(800) 123-4567', '###.###.####') == '800.123.4567'
        assert format_number('012-34-5678', '### ## ####') == '012 34 5678'

    def test_format_wrong_length(self):
        """Test formatting with wrong digit count returns original."""
        assert format_number('12345', '###-##-####') == '12345'
        assert format_number('123456789012', '(###) ###-####') == '123456789012'

    def test_format_empty(self):
        """Test formatting empty values."""
        assert format_number(None, '###-##-####') == ''
        assert format_number('', '###-##-####') == ''


class TestParseList:
    """Test parse_list function for splitting delimited strings."""

    def test_parse_list_comma(self):
        """Test parsing comma-delimited Avatar team members."""
        assert parse_list("Aang,Katara,Sokka,Toph") == ["Aang", "Katara", "Sokka", "Toph"]
        assert parse_list("Water,Earth,Fire,Air") == ["Water", "Earth", "Fire", "Air"]

    def test_parse_list_with_spaces(self):
        """Test parsing strips whitespace from items."""
        assert parse_list("Aang, Katara, Sokka") == ["Aang", "Katara", "Sokka"]
        assert parse_list("Fire Nation , Earth Kingdom , Water Tribe") == ["Fire Nation", "Earth Kingdom", "Water Tribe"]

    def test_parse_list_tab(self):
        """Test parsing tab-delimited lists."""
        result = parse_list("Aang\tKatara\tSokka")
        assert result == ["Aang", "Katara", "Sokka"]

    def test_parse_list_pipe(self):
        """Test parsing pipe-delimited lists."""
        assert parse_list("Fire|Water|Earth|Air", "|") == ["Fire", "Water", "Earth", "Air"]

    def test_parse_list_auto_detect_comma(self):
        """Test auto-detecting comma delimiter."""
        result = parse_list("North,South,East,West")
        assert result == ["North", "South", "East", "West"]

    def test_parse_list_auto_detect_tab(self):
        """Test auto-detecting tab delimiter."""
        result = parse_list("Fire\tWater\tEarth")
        assert result == ["Fire", "Water", "Earth"]

    def test_parse_list_multiple_delimiters_error(self):
        """Test error when multiple delimiter types found."""
        with pytest.raises(ValueError, match="Multiple delimiters found"):
            parse_list("Aang,Katara|Sokka")

    def test_parse_list_no_delimiter(self):
        """Test single item when no delimiter found."""
        assert parse_list("Avatar Aang") == ["Avatar Aang"]

    def test_parse_list_empty(self):
        """Test parsing empty values."""
        assert parse_list(None) == []
        assert parse_list('') == []


class TestGetListItem:
    """Test get_list_item function for extracting list items."""

    def test_get_list_item_first(self):
        """Test getting first item from Avatar team."""
        assert get_list_item("Aang,Katara,Sokka,Toph", 0) == "Aang"
        assert get_list_item("Fire,Water,Earth,Air", 0) == "Fire"

    def test_get_list_item_middle(self):
        """Test getting middle items."""
        assert get_list_item("Aang,Katara,Sokka,Toph", 1) == "Katara"
        assert get_list_item("Aang,Katara,Sokka,Toph", 2) == "Sokka"

    def test_get_list_item_last(self):
        """Test getting last item."""
        assert get_list_item("Aang,Katara,Sokka,Toph", 3) == "Toph"

    def test_get_list_item_custom_delimiter(self):
        """Test getting item with custom delimiter."""
        assert get_list_item("Fire|Water|Earth|Air", 1, "|") == "Water"
        assert get_list_item("North-South-East-West", 2, "-") == "East"

    def test_get_list_item_strips_whitespace(self):
        """Test that whitespace is stripped from items."""
        assert get_list_item("Aang , Katara , Sokka", 1) == "Katara"

    def test_get_list_item_out_of_range(self):
        """Test getting item beyond list length returns None."""
        assert get_list_item("Aang,Katara", 5) is None
        assert get_list_item("Fire,Water", 10) is None

    def test_get_list_item_empty(self):
        """Test getting item from empty values."""
        assert get_list_item(None, 0) is None
        assert get_list_item('', 0) is None


class TestRegexPatterns:
    """Test regex patterns for number validation."""

    def test_ints_only_pattern_valid(self):
        """Test intsOnlyPattern matches valid integers."""
        assert intsOnlyPattern.match("123")
        assert intsOnlyPattern.match("-456")
        assert intsOnlyPattern.match("+789")
        assert intsOnlyPattern.match("0")

    def test_ints_only_pattern_invalid(self):
        """Test intsOnlyPattern rejects non-integers."""
        assert not intsOnlyPattern.match("12.34")
        assert not intsOnlyPattern.match("abc")
        assert not intsOnlyPattern.match("12a")
        assert not intsOnlyPattern.match("")

    def test_numbers_only_pattern_valid(self):
        """Test numbersOnlyPattern matches valid numbers."""
        assert numbersOnlyPattern.match("123")
        assert numbersOnlyPattern.match("123.45")
        assert numbersOnlyPattern.match("-456")
        assert numbersOnlyPattern.match("+789.12")
        assert numbersOnlyPattern.match("0.5")

    def test_numbers_only_pattern_invalid(self):
        """Test numbersOnlyPattern rejects invalid numbers."""
        assert not numbersOnlyPattern.match("12.34.56")
        assert not numbersOnlyPattern.match("abc")
        assert not numbersOnlyPattern.match("12a")
        assert not numbersOnlyPattern.match("")


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_unicode_handling(self):
        """Test handling of unicode characters in names."""
        assert capitalize("TOPH BEIFONG") == "Toph Beifong"
        assert normalize_whitespace("Iroh's   Tea") == "Iroh's Tea"

    def test_very_long_strings(self):
        """Test handling very long strings."""
        long_list = ",".join([f"Member{i}" for i in range(1000)])
        result = parse_list(long_list)
        assert len(result) == 1000
        assert result[0] == "Member0"
        assert result[999] == "Member999"

    def test_special_characters_in_numbers(self):
        """Test handling special characters."""
        assert get_digits("(Fire) Nation: 555-1234") == "5551234"
        assert to_number("Price: ¥1,234.56") == 1234.56

    def test_chaining_transformations(self):
        """Test chaining multiple transformations."""
        # Raw -> normalize -> capitalize -> extract
        raw = "  FIRE   NATION  "
        normalized = normalize_whitespace(raw)
        capitalized = capitalize(normalized)
        assert capitalized == "Fire Nation"

    def test_type_preservation(self):
        """Test that appropriate types are preserved."""
        assert isinstance(get_int("123"), int)
        assert isinstance(get_float("123.45"), float)
        assert isinstance(parse_list("a,b,c"), list)
        assert isinstance(get_digits("123"), str)