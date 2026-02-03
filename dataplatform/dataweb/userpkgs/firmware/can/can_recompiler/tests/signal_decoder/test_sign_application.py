"""
Test suite for signal sign application functionality.

Tests the apply_signal_sign() function which applies signed/unsigned
interpretation to extracted signal values.
"""

import pytest

from ...signal_decoder.decoder import apply_signal_sign


class TestApplySignalSign:
    """Comprehensive tests for signal sign application."""

    @pytest.mark.parametrize(
        "value,bit_length,sign_type,expected",
        [
            # 8-bit signed tests (sign_type=1)
            (127, 8, 1, 127),  # Positive maximum (0x7F)
            (128, 8, 1, -128),  # Negative maximum (0x80 -> -128)
            (255, 8, 1, -1),  # All bits set (0xFF -> -1)
            # 8-bit unsigned tests (sign_type=2)
            (127, 8, 2, 127),  # Same as signed for positive
            (128, 8, 2, 128),  # Different from signed
            (255, 8, 2, 255),  # Different from signed
            # 16-bit signed tests (sign_type=1)
            (32767, 16, 1, 32767),  # Positive maximum (0x7FFF)
            (32768, 16, 1, -32768),  # Negative maximum (0x8000 -> -32768)
            (65535, 16, 1, -1),  # All bits set (0xFFFF -> -1)
            # 16-bit unsigned tests (sign_type=2)
            (32767, 16, 2, 32767),  # Same as signed for positive
            (32768, 16, 2, 32768),  # Different from signed
            (65535, 16, 2, 65535),  # Different from signed
            # Edge cases
            (1, 8, 1, 1),  # Small positive value
            (254, 8, 1, -2),  # Large signed value (0xFE -> -2)
        ],
    )
    def test_sign_application(self, value: int, bit_length: int, sign_type: int, expected: int):
        """Test sign application for various value/bit length combinations."""
        result = apply_signal_sign(value, bit_length, sign_type)
        assert result == expected, f"value={value}, bit_length={bit_length}, sign_type={sign_type}"

    @pytest.mark.parametrize(
        "bit_length,max_positive,min_negative_input,min_negative_output,max_unsigned",
        [
            (1, 0, 1, -1, 1),
            (2, 1, 2, -2, 3),
            (3, 3, 4, -4, 7),
            (4, 7, 8, -8, 15),
            (5, 15, 16, -16, 31),
            (6, 31, 32, -32, 63),
            (7, 63, 64, -64, 127),
            (8, 127, 128, -128, 255),
            (12, 2047, 2048, -2048, 4095),
            (16, 32767, 32768, -32768, 65535),
            (24, 8388607, 8388608, -8388608, 16777215),
            (32, 2147483647, 2147483648, -2147483648, 4294967295),
        ],
    )
    def test_sign_edge_cases_various_lengths(
        self,
        bit_length: int,
        max_positive: int,
        min_negative_input: int,
        min_negative_output: int,
        max_unsigned: int,
    ):
        """Test sign application edge cases for various bit lengths."""

        # Test maximum positive value (sign bit not set)
        result_signed = apply_signal_sign(max_positive, bit_length, 1)
        result_unsigned = apply_signal_sign(max_positive, bit_length, 2)
        assert result_signed == max_positive
        assert result_unsigned == max_positive

        # Test minimum negative value (sign bit set, all other bits 0)
        result_signed = apply_signal_sign(min_negative_input, bit_length, 1)
        result_unsigned = apply_signal_sign(min_negative_input, bit_length, 2)
        assert result_signed == min_negative_output
        assert result_unsigned == min_negative_input

        # Test maximum unsigned value (all bits set)
        result_signed = apply_signal_sign(max_unsigned, bit_length, 1)
        result_unsigned = apply_signal_sign(max_unsigned, bit_length, 2)
        assert result_signed == -1
        assert result_unsigned == max_unsigned

    def test_single_bit_sign_application(self):
        """Test sign application for single-bit values."""
        # 1-bit signed: 0 -> 0, 1 -> -1
        assert apply_signal_sign(0, 1, 1) == 0
        assert apply_signal_sign(1, 1, 1) == -1

        # 1-bit unsigned: 0 -> 0, 1 -> 1
        assert apply_signal_sign(0, 1, 2) == 0
        assert apply_signal_sign(1, 1, 2) == 1

    def test_two_bit_sign_application(self):
        """Test sign application for 2-bit values."""
        # 2-bit signed: 00=0, 01=1, 10=-2, 11=-1
        assert apply_signal_sign(0, 2, 1) == 0  # 00
        assert apply_signal_sign(1, 2, 1) == 1  # 01
        assert apply_signal_sign(2, 2, 1) == -2  # 10
        assert apply_signal_sign(3, 2, 1) == -1  # 11

        # 2-bit unsigned: 00=0, 01=1, 10=2, 11=3
        assert apply_signal_sign(0, 2, 2) == 0
        assert apply_signal_sign(1, 2, 2) == 1
        assert apply_signal_sign(2, 2, 2) == 2
        assert apply_signal_sign(3, 2, 2) == 3

    def test_four_bit_sign_application(self):
        """Test sign application for 4-bit values (common nibble size)."""
        # 4-bit signed range: -8 to +7
        # Positive values (0-7): sign bit not set
        for i in range(8):
            assert apply_signal_sign(i, 4, 1) == i
            assert apply_signal_sign(i, 4, 2) == i

        # Negative values (8-15): sign bit set
        expected_signed = [-8, -7, -6, -5, -4, -3, -2, -1]
        for i, expected in enumerate(expected_signed, 8):
            assert apply_signal_sign(i, 4, 1) == expected
            assert apply_signal_sign(i, 4, 2) == i  # Unsigned keeps original

    def test_comprehensive_8bit_values(self):
        """Test comprehensive 8-bit value conversions."""
        # Test all possible 8-bit values
        for value in range(256):
            signed_result = apply_signal_sign(value, 8, 1)
            unsigned_result = apply_signal_sign(value, 8, 2)

            # Unsigned should always equal original value
            assert unsigned_result == value

            # Signed should follow two's complement rules
            if value < 128:  # Sign bit not set (0-127)
                assert signed_result == value
            else:  # Sign bit set (128-255)
                expected_signed = value - 256  # Two's complement
                assert signed_result == expected_signed

    def test_large_bit_lengths(self):
        """Test sign application for larger bit lengths."""
        # Test 32-bit values
        test_cases_32 = [
            (0x7FFFFFFF, 32, 1, 0x7FFFFFFF),  # Max positive 32-bit
            (0x80000000, 32, 1, -0x80000000),  # Min negative 32-bit
            (0xFFFFFFFF, 32, 1, -1),  # All bits set
            (0x7FFFFFFF, 32, 2, 0x7FFFFFFF),  # Unsigned max positive
            (0x80000000, 32, 2, 0x80000000),  # Unsigned large value
            (0xFFFFFFFF, 32, 2, 0xFFFFFFFF),  # Unsigned max value
        ]

        for value, bit_length, sign_type, expected in test_cases_32:
            result = apply_signal_sign(value, bit_length, sign_type)
            assert result == expected

    def test_sign_type_validation(self):
        """Test that function handles both valid sign types."""
        value = 128
        bit_length = 8

        # Valid sign types (1=SIGNED, 2=UNSIGNED)
        signed_result = apply_signal_sign(value, bit_length, 1)
        unsigned_result = apply_signal_sign(value, bit_length, 2)

        assert signed_result == -128  # Signed interpretation
        assert unsigned_result == 128  # Unsigned interpretation

        # Note: Invalid sign types are not explicitly validated in the function
        # The function assumes valid inputs (defensive programming elsewhere)

    def test_zero_and_one_values(self):
        """Test sign application for zero and one values."""
        # Test across different bit lengths
        bit_lengths = [1, 2, 4, 8, 16, 32]

        for bit_length in bit_lengths:
            # Zero should always remain zero
            assert apply_signal_sign(0, bit_length, 1) == 0
            assert apply_signal_sign(0, bit_length, 2) == 0

            # One behavior depends on bit length
            if bit_length == 1:
                # For 1-bit signed, 1 has sign bit set -> -1
                assert apply_signal_sign(1, bit_length, 1) == -1
                assert apply_signal_sign(1, bit_length, 2) == 1
            else:
                # For multi-bit, 1 is positive
                assert apply_signal_sign(1, bit_length, 1) == 1
                assert apply_signal_sign(1, bit_length, 2) == 1

    def test_boundary_values_systematic(self):
        """Systematically test boundary values for common bit lengths."""
        common_bit_lengths = [8, 16, 24, 32]

        for bit_length in common_bit_lengths:
            max_value = (1 << bit_length) - 1
            sign_bit = 1 << (bit_length - 1)

            # Test 0 (minimum value)
            assert apply_signal_sign(0, bit_length, 1) == 0
            assert apply_signal_sign(0, bit_length, 2) == 0

            # Test sign bit boundary (positive maximum for signed)
            positive_max = sign_bit - 1
            assert apply_signal_sign(positive_max, bit_length, 1) == positive_max
            assert apply_signal_sign(positive_max, bit_length, 2) == positive_max

            # Test sign bit set (negative minimum for signed)
            assert apply_signal_sign(sign_bit, bit_length, 1) == -sign_bit
            assert apply_signal_sign(sign_bit, bit_length, 2) == sign_bit

            # Test maximum value (all bits set)
            assert apply_signal_sign(max_value, bit_length, 1) == -1
            assert apply_signal_sign(max_value, bit_length, 2) == max_value


class TestSignApplicationSpecialCases:
    """Test special cases and edge conditions for sign application."""

    def test_sign_bit_detection(self):
        """Test that sign bit detection works correctly."""
        # Create test cases where sign bit is specifically tested
        test_cases = [
            # (value, bit_length, has_sign_bit)
            (0b01111111, 8, False),  # 127: sign bit not set
            (0b10000000, 8, True),  # 128: sign bit set
            (0b10000001, 8, True),  # 129: sign bit set
            (0b11111111, 8, True),  # 255: sign bit set
            (0b0111111111111111, 16, False),  # 32767: sign bit not set
            (0b1000000000000000, 16, True),  # 32768: sign bit set
            (0b1111111111111111, 16, True),  # 65535: sign bit set
        ]

        for value, bit_length, has_sign_bit in test_cases:
            signed_result = apply_signal_sign(value, bit_length, 1)
            unsigned_result = apply_signal_sign(value, bit_length, 2)

            # Unsigned should always equal input
            assert unsigned_result == value

            # Signed should be negative only if sign bit is set
            if has_sign_bit:
                assert signed_result < 0
            else:
                assert signed_result >= 0
                assert signed_result == value

    def test_twos_complement_calculation(self):
        """Test that two's complement calculation is correct."""
        # Test specific two's complement conversions
        test_cases = [
            # (unsigned_value, bit_length, expected_signed)
            (0x80, 8, -128),  # 128 -> -128
            (0xFF, 8, -1),  # 255 -> -1
            (0xFE, 8, -2),  # 254 -> -2
            (0x81, 8, -127),  # 129 -> -127
            (0x8000, 16, -32768),  # 32768 -> -32768
            (0xFFFF, 16, -1),  # 65535 -> -1
            (0x8001, 16, -32767),  # 32769 -> -32767
        ]

        for unsigned_value, bit_length, expected_signed in test_cases:
            result = apply_signal_sign(unsigned_value, bit_length, 1)
            assert result == expected_signed

    def test_sign_application_consistency(self):
        """Test that sign application is consistent across calls."""
        # Test that repeated calls with same inputs give same results
        test_values = [0, 1, 127, 128, 255, 32767, 32768, 65535]
        bit_lengths = [8, 16]
        sign_types = [1, 2]

        for value in test_values:
            for bit_length in bit_lengths:
                for sign_type in sign_types:
                    if value < (1 << bit_length):  # Value fits in bit length
                        result1 = apply_signal_sign(value, bit_length, sign_type)
                        result2 = apply_signal_sign(value, bit_length, sign_type)
                        assert result1 == result2
