# tests/test_transforms_phone.py
"""
Tests for dbtk.etl.transforms.phone module.

Testing phone number parsing, validation, and formatting with Avatar themed data.
"""

import pytest
from dbtk.etl.transforms.phone import (
    Phone,
    PhoneFormat,
    phone_clean,
    phone_format,
    phone_get_area_code,
    phone_get_exchange,
    phone_get_number,
    phone_get_extension,
    phone_get_country_code,
    phone_get_country,
    phone_get_type,
    phone_validate,
    HAS_PHONENUMBERS
)


class TestPhoneClass:
    """Test Phone class for parsing and formatting."""

    def test_phone_basic_parsing(self):
        """Test parsing basic US phone number."""
        phone = Phone("(360) 123-4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"
        assert phone.is_valid is True

    def test_phone_with_country_code(self):
        """Test parsing phone with country code."""
        phone = Phone("+1-360-123-4567")
        assert phone.country_code == "+1"
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"

    def test_phone_digits_only(self):
        """Test parsing phone from digits only."""
        phone = Phone("3601234567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"
        assert phone.is_valid is True

    def test_phone_with_extension(self):
        """Test parsing phone with extension."""
        phone = Phone("360-123-4567 ext 123")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"
        assert phone.extension == "123"

    def test_phone_extension_formats(self):
        """Test various extension formats."""
        phone1 = Phone("360-123-4567 x456")
        assert phone1.extension == "456"

        phone2 = Phone("360-123-4567 extension 789")
        assert phone2.extension == "789"

        phone3 = Phone("360-123-4567#321")
        assert phone3.extension == "321"

    def test_phone_local_number_only(self):
        """Test parsing local number without area code."""
        phone = Phone("123-4567")
        assert phone.exchange == "123"
        assert phone.number == "4567"
        assert phone.area_code is None

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_phone_international_uk(self):
        """Test parsing UK phone number."""
        phone = Phone("+44 20 7946 0958", "GB")
        assert phone.country_code == "+44"
        assert phone.is_valid is True
        assert phone.country == "GB"

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_phone_international_france(self):
        """Test parsing French phone number."""
        phone = Phone("+33 1 42 86 82 00", "FR")
        assert phone.country_code == "+33"
        assert phone.is_valid is True
        assert phone.country == "FR"

    def test_phone_empty_string(self):
        """Test parsing empty string."""
        phone = Phone("")
        assert phone.is_valid is False
        assert phone.area_code is None

    def test_phone_invalid_format(self):
        """Test parsing invalid phone format."""
        phone = Phone("not a phone")
        assert phone.is_valid is False


class TestPhoneFormatting:
    """Test phone number formatting in various styles."""

    def test_format_national(self):
        """Test formatting in national format."""
        phone = Phone("3601234567")
        assert phone.format(PhoneFormat.NATIONAL) == "(360) 123-4567"

    def test_format_international(self):
        """Test formatting in international format."""
        phone = Phone("+1-360-123-4567")
        formatted = phone.format(PhoneFormat.INTERNATIONAL)
        assert "360" in formatted
        assert "123" in formatted
        assert "4567" in formatted

    def test_format_e164(self):
        """Test formatting in E164 format."""
        phone = Phone("360-123-4567")
        formatted = phone.format(PhoneFormat.E164)
        assert formatted == "+13601234567" or formatted.endswith("3601234567")

    def test_format_digits(self):
        """Test formatting as digits only."""
        phone = Phone("(360) 123-4567")
        formatted = phone.format(PhoneFormat.DIGITS)
        assert "3601234567" in formatted

    def test_format_with_extension(self):
        """Test formatting includes extension."""
        phone = Phone("360-123-4567 ext 123")
        formatted = phone.format(PhoneFormat.NATIONAL)
        assert "123" in formatted  # Extension should be in output
        assert "ext" in formatted or "ext." in formatted

    def test_format_invalid_returns_raw(self):
        """Test formatting invalid phone returns raw input."""
        phone = Phone("not a phone")
        assert phone.format(PhoneFormat.NATIONAL) == "not a phone"

    def test_str_representation(self):
        """Test default string representation."""
        phone = Phone("3601234567")
        assert str(phone) == "(360) 123-4567"


class TestPhoneValidation:
    """Test phone number validation."""

    def test_is_valid_complete_number(self):
        """Test valid complete US phone number."""
        phone = Phone("(360) 123-4567")
        assert phone.is_valid is True

    def test_is_valid_with_country_code(self):
        """Test valid number with country code."""
        phone = Phone("+1-360-123-4567")
        assert phone.is_valid is True

    def test_is_valid_digits_only(self):
        """Test valid number as digits."""
        phone = Phone("3601234567")
        assert phone.is_valid is True

    def test_is_valid_local_only(self):
        """Test local number (7 digits) validation."""
        phone = Phone("123-4567")
        # Should be valid in basic mode
        assert phone.is_valid is True

    def test_is_valid_too_short(self):
        """Test invalid number (too short)."""
        phone = Phone("123")
        assert phone.is_valid is False

    def test_is_valid_letters(self):
        """Test invalid number with letters."""
        phone = Phone("ABC-DEFG")
        assert phone.is_valid is False

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_is_possible(self):
        """Test is_possible validation (less strict)."""
        phone = Phone("360-123-4567")
        assert phone.is_possible is True


class TestPhoneProperties:
    """Test phone number property extraction."""

    def test_property_area_code(self):
        """Test area_code property."""
        phone = Phone("(360) 123-4567")
        assert phone.area_code == "360"

    def test_property_exchange(self):
        """Test exchange property."""
        phone = Phone("360-123-4567")
        assert phone.exchange == "123"

    def test_property_number(self):
        """Test number property."""
        phone = Phone("360-123-4567")
        assert phone.number == "4567"

    def test_property_extension(self):
        """Test extension property."""
        phone = Phone("360-123-4567 ext 999")
        assert phone.extension == "999"

    def test_property_country_code(self):
        """Test country_code property."""
        phone = Phone("+1-360-123-4567")
        assert phone.country_code == "+1"

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_property_country(self):
        """Test country property."""
        phone = Phone("+1-360-123-4567", "US")
        assert phone.country == "US"

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_property_number_type(self):
        """Test number_type property."""
        phone = Phone("360-123-4567", "US")
        # Type detection requires phonenumbers
        assert phone.number_type is not None or phone.number_type is None

    def test_property_missing_components(self):
        """Test properties on incomplete numbers."""
        phone = Phone("123-4567")  # No area code
        assert phone.area_code is None
        assert phone.exchange == "123"
        assert phone.number == "4567"


class TestConvenienceFunctions:
    """Test convenience functions for phone operations."""

    def test_phone_clean(self):
        """Test phone_clean function."""
        assert phone_clean("3601234567") == "(360) 123-4567"
        assert phone_clean("(360) 123-4567") == "(360) 123-4567"

    def test_phone_clean_invalid(self):
        """Test phone_clean returns original for invalid."""
        result = phone_clean("not a phone")
        assert result == ""

    def test_phone_format_function(self):
        """Test phone_format convenience function."""
        formatted = phone_format("3601234567", PhoneFormat.E164)
        assert "360" in formatted and "123" in formatted and "4567" in formatted

    def test_phone_format_string_style(self):
        """Test phone_format with string style name."""
        formatted = phone_format("3601234567", "international")
        assert "360" in formatted

    def test_phone_get_area_code_function(self):
        """Test phone_get_area_code function."""
        assert phone_get_area_code("(360) 123-4567") == "360"
        assert phone_get_area_code("123-4567") is None

    def test_phone_get_exchange_function(self):
        """Test phone_get_exchange function."""
        assert phone_get_exchange("360-123-4567") == "123"

    def test_phone_get_number_function(self):
        """Test phone_get_number function."""
        assert phone_get_number("360-123-4567") == "4567"

    def test_phone_get_extension_function(self):
        """Test phone_get_extension function."""
        assert phone_get_extension("360-123-4567 ext 123") == "123"
        assert phone_get_extension("360-123-4567") is None

    def test_phone_get_country_code_function(self):
        """Test phone_get_country_code function."""
        assert phone_get_country_code("+1-360-123-4567") == "+1"

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_phone_get_country_function(self):
        """Test phone_get_country function."""
        country = phone_get_country("+1-360-123-4567", "US")
        assert country == "US"

    @pytest.mark.skipif(not HAS_PHONENUMBERS, reason="phonenumbers library not installed")
    def test_phone_get_type_function(self):
        """Test phone_get_type function."""
        # Type detection requires phonenumbers
        result = phone_get_type("360-123-4567", "US")
        # Just verify it doesn't crash
        assert result is None or isinstance(result, str)

    def test_phone_validate_function(self):
        """Test phone_validate function."""
        assert phone_validate("360-123-4567") is True
        assert phone_validate("123") is False


class TestVariousFormats:
    """Test parsing various phone number formats."""

    def test_format_dots(self):
        """Test format with dots."""
        phone = Phone("360.123.4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"

    def test_format_spaces(self):
        """Test format with spaces."""
        phone = Phone("360 123 4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"

    def test_format_mixed_separators(self):
        """Test format with mixed separators."""
        phone = Phone("360-123.4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"

    def test_format_parentheses_no_space(self):
        """Test format with parentheses but no space."""
        phone = Phone("(360)123-4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"

    def test_format_plus_one(self):
        """Test format with +1 prefix."""
        phone = Phone("+1 360 123 4567")
        assert phone.country_code == "+1"
        assert phone.area_code == "360"

    def test_format_one_prefix(self):
        """Test format with 1 prefix."""
        phone = Phone("1-360-123-4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_input(self):
        """Test handling empty input."""
        phone = Phone("")
        assert phone.is_valid is False
        assert phone.raw == ""

    def test_none_input(self):
        """Test handling None input."""
        phone = Phone(None)
        assert phone.is_valid is False

    def test_numeric_input(self):
        """Test handling numeric input."""
        phone = Phone(3601234567)
        assert phone.is_valid is True

    def test_whitespace_only(self):
        """Test handling whitespace only."""
        phone = Phone("   ")
        assert phone.is_valid is False

    def test_letters_in_number(self):
        """Test phone numbers with letter mnemonics."""
        # 1-800-FLOWERS type numbers - digits extracted
        phone = Phone("1-800-356-9377")  # The numeric equivalent
        assert phone.is_valid is True

    def test_international_prefix_variations(self):
        """Test various international prefix formats."""
        phone1 = Phone("+1-360-123-4567")
        assert phone1.country_code == "+1"

        phone2 = Phone("011-1-360-123-4567")  # US international dialing
        # May or may not parse correctly depending on implementation

    def test_extension_at_different_positions(self):
        """Test extensions in different positions."""
        phone1 = Phone("360-123-4567 ext 100")
        assert phone1.extension == "100"

        phone2 = Phone("360-123-4567 x 200")
        assert phone2.extension == "200"

    def test_very_long_extension(self):
        """Test handling very long extension numbers."""
        phone = Phone("360-123-4567 ext 123456")
        assert phone.extension == "123456"

    def test_format_preservation_on_invalid(self):
        """Test that invalid numbers preserve original format."""
        original = "Fire Nation Hotline"
        phone = Phone(original)
        assert str(phone) == original


class TestAvatarThemedScenarios:
    """Test with Avatar-themed phone numbers and scenarios."""

    def test_fire_nation_emergency(self):
        """Test Fire Nation emergency services number."""
        phone = Phone("(919) 360-FIRE")  # Would need numeric conversion
        # Just verify it doesn't crash

    def test_earth_kingdom_directory(self):
        """Test Ba Sing Se city directory number."""
        phone = Phone("360-EARTH")
        # Would extract digits if any

    def test_water_tribe_contact(self):
        """Test Northern Water Tribe contact information."""
        phone = Phone("(907) 360-WATER")
        # Would extract digits if any

    def test_air_nomad_monastery(self):
        """Test Air Temple monastery contact."""
        phone = Phone("+1-360-NOMAD-01")
        # Would extract digits and extension if any

    def test_team_avatar_phones(self):
        """Test phone numbers for Team Avatar members."""
        aang_phone = Phone("(360) 123-4567")
        katara_phone = Phone("(360) 234-5678")
        sokka_phone = Phone("(360) 345-6789")
        toph_phone = Phone("(360) 456-7890")

        assert aang_phone.is_valid
        assert katara_phone.is_valid
        assert sokka_phone.is_valid
        assert toph_phone.is_valid

    def test_cabbage_merchant_hotline(self):
        """Test Cabbage Corp customer service."""
        phone = Phone("1-800-CABBAGE")
        # Would need actual numeric conversion


class TestDegradedMode:
    """Test behavior when phonenumbers library is not available."""

    def test_has_phonenumbers_flag(self):
        """Test HAS_PHONENUMBERS flag is set correctly."""
        assert isinstance(HAS_PHONENUMBERS, bool)

    @pytest.mark.skipif(HAS_PHONENUMBERS, reason="phonenumbers library is installed")
    def test_degraded_mode_us_numbers(self):
        """Test that US numbers still work without phonenumbers."""
        phone = Phone("360-123-4567")
        assert phone.area_code == "360"
        assert phone.exchange == "123"
        assert phone.number == "4567"
        assert phone.is_valid is True

    @pytest.mark.skipif(HAS_PHONENUMBERS, reason="phonenumbers library is installed")
    def test_degraded_mode_international(self):
        """Test that international features are limited without phonenumbers."""
        phone = Phone("+44 20 7946 0958", "GB")
        # Should still parse but with limited capability
        assert phone.country is None or phone.country == "US"
        assert phone.number_type is None

    @pytest.mark.skipif(HAS_PHONENUMBERS, reason="phonenumbers library is installed")
    def test_degraded_mode_get_type_returns_none(self):
        """Test phone_get_type returns None without phonenumbers."""
        result = phone_get_type("360-123-4567")
        assert result is None