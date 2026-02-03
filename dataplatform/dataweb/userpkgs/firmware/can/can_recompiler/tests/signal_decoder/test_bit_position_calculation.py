"""
Test suite for bit position calculation functionality.

Tests the calculate_normalized_bit_position() function which handles
endianness-specific bit position normalization for CAN signal decoding.
"""

import pytest

from ...signal_decoder.decoder import calculate_normalized_bit_position


class TestCalculateNormalizedBitPosition:
    """Comprehensive tests for bit position normalization."""

    @pytest.mark.parametrize(
        "bit_start,endianness,expected",
        [
            # Little endian cases (endianness=1)
            (0, 1, 0),  # First bit of first byte
            (1, 1, 1),  # Second bit of first byte
            (7, 1, 7),  # Last bit of first byte
            (8, 1, 8),  # First bit of second byte
            (15, 1, 15),  # Last bit of second byte
            (16, 1, 16),  # First bit of third byte
            (24, 1, 24),  # First bit of fourth byte
            (31, 1, 31),  # Last bit of fourth byte
            # Big endian cases (endianness=2)
            (0, 2, 7),  # First bit becomes last bit of byte
            (1, 2, 6),  # Second bit becomes 6th bit of byte
            (7, 2, 0),  # Last bit becomes first bit of byte
            (8, 2, 15),  # First bit of second byte
            (15, 2, 8),  # Last bit of second byte
            (16, 2, 23),  # First bit of third byte
            (24, 2, 31),  # First bit of fourth byte
            (31, 2, 24),  # Last bit of fourth byte
        ],
    )
    def test_normalized_bit_positions(self, bit_start, endianness, expected):
        """Test normalized bit position calculations for various scenarios."""
        result = calculate_normalized_bit_position(bit_start, endianness)
        assert result == expected, f"bit_start={bit_start}, endianness={endianness}"

    @pytest.mark.parametrize("invalid_endianness", [0, 3, -1, 999])
    def test_invalid_endianness_raises_error(self, invalid_endianness):
        """Test that invalid endianness values raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported endianness value"):
            calculate_normalized_bit_position(0, invalid_endianness)

    @pytest.mark.parametrize(
        "bit_start,endianness,expected",
        [
            # Little endian - positions should remain the same
            (32, 1, 32),
            (63, 1, 63),
            (100, 1, 100),
            # Big endian - positions should be transformed
            (32, 2, 39),  # 32 -> byte 4, bit 0 -> bit 7
            (63, 2, 56),  # 63 -> byte 7, bit 7 -> bit 0
        ],
    )
    def test_large_bit_positions(self, bit_start, endianness, expected):
        """Test normalization with large bit positions."""
        result = calculate_normalized_bit_position(bit_start, endianness)
        assert result == expected

    @pytest.mark.parametrize("bit_pos", [0, 8, 16, 24, 32, 40, 48, 56])
    def test_little_endian_consistency(self, bit_pos):
        """Test that little endian doesn't change positions."""
        assert calculate_normalized_bit_position(bit_pos, 1) == bit_pos

    @pytest.mark.parametrize(
        "original,expected",
        [
            (0, 7),
            (1, 6),
            (2, 5),
            (3, 4),
            (4, 3),
            (5, 2),
            (6, 1),
            (7, 0),
            (8, 15),
            (9, 14),
            (10, 13),
            (11, 12),
            (12, 11),
            (13, 10),
            (14, 9),
            (15, 8),
        ],
    )
    def test_big_endian_pattern(self, original, expected):
        """Test that big endian follows expected pattern."""
        result = calculate_normalized_bit_position(original, 2)
        assert result == expected, f"Big endian: {original} -> {result}, expected {expected}"

    @pytest.mark.parametrize("byte_num", [0, 1, 2, 3])
    def test_byte_boundary_calculations(self, byte_num):
        """Test calculations at byte boundaries."""
        first_bit = byte_num * 8
        last_bit = byte_num * 8 + 7

        # Little endian: should remain unchanged
        assert calculate_normalized_bit_position(first_bit, 1) == first_bit
        assert calculate_normalized_bit_position(last_bit, 1) == last_bit

        # Big endian: should be swapped within byte
        expected_first = byte_num * 8 + 7
        expected_last = byte_num * 8 + 0
        assert calculate_normalized_bit_position(first_bit, 2) == expected_first
        assert calculate_normalized_bit_position(last_bit, 2) == expected_last


class TestBitPositionEdgeCases:
    """Test edge cases and boundary conditions for bit position calculation."""

    def test_zero_bit_position(self):
        """Test bit position 0 for both endiannesses."""
        assert calculate_normalized_bit_position(0, 1) == 0  # Little endian
        assert calculate_normalized_bit_position(0, 2) == 7  # Big endian

    def test_maximum_reasonable_bit_positions(self):
        """Test with maximum reasonable bit positions for CAN frames."""
        # CAN frames are typically 8 bytes (64 bits) maximum
        max_bit = 63

        # Little endian
        assert calculate_normalized_bit_position(max_bit, 1) == max_bit

        # Big endian - bit 63 is the last bit of byte 7
        # In big endian, this becomes the first bit of byte 7
        expected_big = 7 * 8 + 0  # 56
        assert calculate_normalized_bit_position(max_bit, 2) == expected_big

    @pytest.mark.parametrize("endianness,expected", [(1, 0), (2, 7)])
    def test_valid_endianness_values(self, endianness, expected):
        """Test valid endianness parameter values."""
        assert calculate_normalized_bit_position(0, endianness) == expected

    @pytest.mark.parametrize("invalid_endianness", [0, 3, -1, 999, -999])
    def test_invalid_endianness_type_validation(self, invalid_endianness):
        """Test validation of invalid endianness parameter types."""
        with pytest.raises(ValueError, match="Unsupported endianness value"):
            calculate_normalized_bit_position(0, invalid_endianness)
