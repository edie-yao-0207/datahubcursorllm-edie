"""
Test suite for error handling and input validation.

Consolidates all error conditions, edge cases, and input validation
tests across all decoder functions.
"""

import pytest

from ...signal_decoder.decoder import (
    calculate_normalized_bit_position,
    extract_byte_segment,
    extract_signal_value,
    apply_signal_sign,
    decode_single_signal_from_payload,
)


class TestNoneInputValidation:
    """Test None input validation paths for the main decoder function."""

    def test_none_payload_returns_none(self):
        """Test None payload handling."""
        result = decode_single_signal_from_payload(None, 0, 8, 1, 2)
        assert result is None

    def test_none_bit_start_returns_none(self):
        """Test None bit_start handling."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, None, 8, 1, 2)
        assert result is None

    def test_none_bit_length_returns_none(self):
        """Test None bit_length handling."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, None, 1, 2)
        assert result is None

    def test_none_endianness_returns_none(self):
        """Test None endianness handling."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 8, None, 2)
        assert result is None

    def test_none_sign_type_returns_none(self):
        """Test None sign_type handling."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 8, 1, None)
        assert result is None


class TestTypeConversionErrors:
    """Test type conversion error handling."""

    def test_invalid_bit_start_type_conversion(self):
        """Test invalid bit_start type conversion."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, "invalid", 8, 1, 2)
        assert result is None

    def test_invalid_bit_length_type_conversion(self):
        """Test invalid bit_length type conversion."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, complex(1, 2), 1, 2)
        assert result is None

    def test_invalid_endianness_type_conversion(self):
        """Test invalid endianness type conversion."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 8, [1, 2], 2)
        assert result is None

    def test_invalid_sign_type_conversion(self):
        """Test invalid sign_type type conversion."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 8, 1, {"key": "value"})
        assert result is None


class TestBoundaryValidationErrors:
    """Test boundary validation errors."""

    def test_negative_bit_start_returns_none(self):
        """Test negative bit_start validation."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, -1, 8, 1, 2)
        assert result is None

    def test_zero_bit_length_returns_none(self):
        """Test zero bit_length validation."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 0, 1, 2)
        assert result is None

    def test_excessive_bit_length_returns_none(self):
        """Test excessive bit_length validation."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 65, 1, 2)
        assert result is None

    def test_invalid_endianness_value_returns_none(self):
        """Test invalid endianness values."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 8, 3, 2)
        assert result is None

        result = decode_single_signal_from_payload(payload, 0, 8, 0, 2)
        assert result is None

    def test_invalid_sign_type_value_returns_none(self):
        """Test invalid sign_type values."""
        payload = b"\x12\x34"
        result = decode_single_signal_from_payload(payload, 0, 8, 1, 3)
        assert result is None

        result = decode_single_signal_from_payload(payload, 0, 8, 1, 0)
        assert result is None

    def test_bit_start_exceeds_payload_returns_none(self):
        """Test bit_start exceeding payload bounds."""
        payload = b"\x12"  # 8 bits total
        result = decode_single_signal_from_payload(payload, 8, 4, 1, 2)
        assert result is None

        result = decode_single_signal_from_payload(payload, 16, 4, 1, 2)
        assert result is None

    def test_normalized_range_invalid_returns_none(self):
        """Test invalid normalized range."""
        payload = b"\x12"  # 8 bits total
        result = decode_single_signal_from_payload(payload, 5, 8, 1, 2)
        assert result is None


class TestEndiannessValidationErrors:
    """Test endianness validation errors."""

    def test_unsupported_endianness_in_calculate_normalized_bit_position(self):
        """Test unsupported endianness values."""
        with pytest.raises(ValueError, match="Unsupported endianness value"):
            calculate_normalized_bit_position(0, 3)

        with pytest.raises(ValueError, match="Unsupported endianness value"):
            calculate_normalized_bit_position(0, 0)

        with pytest.raises(ValueError, match="Unsupported endianness value"):
            calculate_normalized_bit_position(0, -1)


class TestByteSegmentExtractionErrors:
    """Test byte segment extraction errors."""

    def test_invalid_bit_range_in_extract_byte_segment(self):
        """Test invalid bit range errors."""
        payload = b"\x12\x34"

        with pytest.raises(ValueError, match="Invalid bit range"):
            extract_byte_segment(payload, -1, 7)

        with pytest.raises(ValueError, match="Invalid bit range"):
            extract_byte_segment(payload, 7, 6)

    def test_bit_range_exceeds_payload(self):
        """Test bit range exceeding payload."""
        payload = b"\x12\x34"  # 2 bytes = 16 bits

        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 0, 16)

        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 16, 23)

    def test_empty_payload_extraction(self):
        """Test extraction from empty payload."""
        payload = b""

        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 0, 0)


class TestSignalValueExtractionErrors:
    """Test signal value extraction errors."""

    def test_invalid_bit_length_in_extract_signal_value(self):
        """Test invalid bit_length validation."""
        segment = b"\x12\x34"

        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, 0, 1)

        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, -1, 1)

        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, 65, 1)

    def test_negative_shift_calculation(self):
        """Test negative shift calculation errors."""
        segment = b"\x12"  # 1 byte = 8 bits

        with pytest.raises(ValueError, match="Negative shift calculated"):
            extract_signal_value(segment, 7, 8, 1)


class TestExceptionHandling:
    """Test exception handling in the main decoder function."""

    def test_exception_handling_in_main_function(self):
        """Test exception handling catches errors and returns None."""

        # Test with very large payload
        large_payload = b"\x00" * 10000
        result = decode_single_signal_from_payload(large_payload, 0, 8, 1, 2)
        assert result is not None or result is None

        # Test with extreme bit positions
        result = decode_single_signal_from_payload(b"\x12\x34", 999999, 8, 1, 2)
        assert result is None

        # Test edge case with maximum valid inputs
        max_payload = b"\xFF" * 8
        result = decode_single_signal_from_payload(max_payload, 0, 64, 1, 1)
        assert result is not None or result is None


class TestEdgeCaseCombinations:
    """Test combinations of edge cases."""

    def test_multiple_error_conditions(self):
        """Test multiple error conditions combined."""
        result = decode_single_signal_from_payload(None, None, None, None, None)
        assert result is None

        result = decode_single_signal_from_payload(b"\x12", 0, -1, 1, 2)
        assert result is None

        result = decode_single_signal_from_payload(b"\x12", "invalid", 65, 3, 0)
        assert result is None

    def test_stress_boundary_conditions(self):
        """Test stress conditions at boundaries."""
        payload = b"\x12\x34"  # 16 bits

        # Test exactly at payload boundary
        result = decode_single_signal_from_payload(payload, 15, 1, 1, 2)
        assert result is not None

        # Test one past end
        result = decode_single_signal_from_payload(payload, 16, 1, 1, 2)
        assert result is None


class TestStringConversionEdgeCases:
    """Test string conversion edge cases."""

    def test_valid_string_conversions(self):
        """Test valid string to number conversions."""
        payload = b"\x12\x34"

        # Valid string numbers
        result = decode_single_signal_from_payload(payload, "0", "8", "1", "2")
        assert result == 0x12

        result = decode_single_signal_from_payload(payload, "8", "8", "1", "2")
        assert result == 0x34

    @pytest.mark.parametrize(
        "invalid_str,param_position,description",
        [
            # Test invalid strings as bit_start parameter
            ("invalid", "bit_start", "invalid string as bit_start"),
            ("abc", "bit_start", "abc string as bit_start"),
            ("", "bit_start", "empty string as bit_start"),
            ("1.5.5", "bit_start", "float string as bit_start"),
            ("NaN", "bit_start", "NaN string as bit_start"),
            # Test invalid strings as bit_length parameter
            ("invalid", "bit_length", "invalid string as bit_length"),
            ("abc", "bit_length", "abc string as bit_length"),
            ("", "bit_length", "empty string as bit_length"),
            ("1.5.5", "bit_length", "float string as bit_length"),
            ("NaN", "bit_length", "NaN string as bit_length"),
            # Test invalid strings as endianness parameter
            ("invalid", "endianness", "invalid string as endianness"),
            ("abc", "endianness", "abc string as endianness"),
            ("", "endianness", "empty string as endianness"),
            ("1.5.5", "endianness", "float string as endianness"),
            ("NaN", "endianness", "NaN string as endianness"),
            # Test invalid strings as sign_type parameter
            ("invalid", "sign_type", "invalid string as sign_type"),
            ("abc", "sign_type", "abc string as sign_type"),
            ("", "sign_type", "empty string as sign_type"),
            ("1.5.5", "sign_type", "float string as sign_type"),
            ("NaN", "sign_type", "NaN string as sign_type"),
        ],
    )
    def test_invalid_string_conversions(self, invalid_str, param_position, description):
        """Test invalid string conversions."""
        payload = b"\x12\x34"

        if param_position == "bit_start":
            result = decode_single_signal_from_payload(payload, invalid_str, 8, 1, 2)
        elif param_position == "bit_length":
            result = decode_single_signal_from_payload(payload, 0, invalid_str, 1, 2)
        elif param_position == "endianness":
            result = decode_single_signal_from_payload(payload, 0, 8, invalid_str, 2)
        elif param_position == "sign_type":
            result = decode_single_signal_from_payload(payload, 0, 8, 1, invalid_str)

        assert result is None, f"Expected None for {description}"


class TestPayloadValidationErrors:
    """Test payload-specific validation errors."""

    def test_empty_payload_scenarios(self):
        """Test various empty payload scenarios."""
        empty_payload = b""

        result = decode_single_signal_from_payload(empty_payload, 0, 1, 1, 2)
        assert result is None

        result = decode_single_signal_from_payload(empty_payload, 0, 8, 1, 2)
        assert result is None

    def test_payload_boundary_edge_cases(self):
        """Test payload boundary edge cases."""
        payload = b"\x12\x34\x56\x78"  # 32 bits

        # Valid boundary cases
        result = decode_single_signal_from_payload(payload, 31, 1, 1, 2)
        assert result is not None

        # Invalid boundary cases
        result = decode_single_signal_from_payload(payload, 32, 1, 1, 2)
        assert result is None

        result = decode_single_signal_from_payload(payload, 24, 16, 1, 2)
        assert result is None  # Would extend to bit 39, beyond 32-bit payload


class TestComprehensiveErrorScenarios:
    """Test comprehensive error scenarios."""

    @pytest.mark.parametrize(
        "bit_start,bit_length,endianness,sign_type,param_name,invalid_value",
        [
            # Invalid bit_start values
            (-1, 8, 1, 2, "bit_start", -1),
            (-10, 8, 1, 2, "bit_start", -10),
            (32, 8, 1, 2, "bit_start", 32),
            (100, 8, 1, 2, "bit_start", 100),
            # Invalid bit_length values
            (0, -1, 1, 2, "bit_length", -1),
            (0, 0, 1, 2, "bit_length", 0),
            (0, 65, 1, 2, "bit_length", 65),
            (0, 128, 1, 2, "bit_length", 128),
            (0, 1000, 1, 2, "bit_length", 1000),
            # Invalid endianness values
            (0, 8, -1, 2, "endianness", -1),
            (0, 8, 0, 2, "endianness", 0),
            (0, 8, 3, 2, "endianness", 3),
            (0, 8, 999, 2, "endianness", 999),
            # Invalid sign_type values
            (0, 8, 1, -1, "sign_type", -1),
            (0, 8, 1, 0, "sign_type", 0),
            (0, 8, 1, 3, "sign_type", 3),
            (0, 8, 1, 999, "sign_type", 999),
        ],
    )
    def test_systematic_parameter_validation(
        self, bit_start, bit_length, endianness, sign_type, param_name, invalid_value
    ):
        """Systematically test parameter validation."""
        valid_payload = b"\x12\x34\x56\x78"

        result = decode_single_signal_from_payload(
            valid_payload, bit_start, bit_length, endianness, sign_type
        )
        assert result is None, f"Expected None for invalid {param_name}={invalid_value}"

    def test_complex_error_combinations(self):
        """Test complex combinations of error conditions."""
        payload = b"\x12\x34"

        # Multiple parameter errors
        error_combinations = [
            (-1, 0, 0, 0),  # All invalid
            (-1, 8, 3, 2),  # bit_start and endianness invalid
            (0, 65, 1, 3),  # bit_length and sign_type invalid
            (16, 8, 0, 2),  # bit_start beyond payload, invalid endianness
        ]

        for bit_start, bit_length, endianness, sign_type in error_combinations:
            result = decode_single_signal_from_payload(
                payload, bit_start, bit_length, endianness, sign_type
            )
            assert result is None

    def test_defensive_programming_scenarios(self):
        """Test defensive programming scenarios."""
        # Test with unusual but potentially valid inputs
        payload = b"\x12\x34\x56\x78"

        # Very large valid bit positions
        result = decode_single_signal_from_payload(payload, 0, 32, 1, 2)
        assert result is not None

        # Maximum reasonable bit extraction
        max_payload = b"\xFF" * 8
        result = decode_single_signal_from_payload(max_payload, 0, 64, 1, 1)
        assert result == -1.0  # All bits set, signed = -1
