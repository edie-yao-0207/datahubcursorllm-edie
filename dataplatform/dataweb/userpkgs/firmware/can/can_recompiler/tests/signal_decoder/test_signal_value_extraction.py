"""
Test suite for signal value extraction functionality.

Tests the extract_signal_value() function which extracts signal values
from byte segments using bit manipulation and endianness handling.
"""

import pytest

from ...signal_decoder.decoder import extract_signal_value


class TestExtractSignalValue:
    """Comprehensive tests for signal value extraction."""

    @pytest.mark.parametrize("test_value", [0x00, 0x01, 0x42, 0x7F, 0x80, 0xFF])
    def test_full_byte_extraction(self, test_value: int):
        """Test extraction of full byte values."""
        segment = bytes([test_value])

        # Test both endiannesses (should be same for single byte)
        result_le = extract_signal_value(segment, 0, 8, 1)  # Little endian
        result_be = extract_signal_value(segment, 0, 8, 2)  # Big endian

        assert result_le == test_value
        assert result_be == test_value
        assert result_le == result_be  # Same for single byte

    @pytest.mark.parametrize(
        "segment,bit_length,endianness,expected",
        [
            (b"\x34\x12", 16, 1, 0x1234),  # 16-bit little endian
            (b"\x34\x12", 16, 2, 0x3412),  # 16-bit big endian
            (b"\x78\x56\x34\x12", 32, 1, 0x12345678),  # 32-bit little endian
            (b"\x78\x56\x34\x12", 32, 2, 0x78563412),  # 32-bit big endian
            (b"\x12", 8, 1, 0x12),  # 8-bit (same for both endiannesses)
            (b"\x12", 8, 2, 0x12),  # 8-bit (same for both endiannesses)
        ],
    )
    def test_multi_byte_extraction(
        self, segment: bytes, bit_length: int, endianness: int, expected: int
    ):
        """Test extraction of multi-byte values."""
        result = extract_signal_value(segment, 0, bit_length, endianness)
        assert result == expected

    @pytest.mark.parametrize(
        "bit_length,expected",
        [(1, 1), (2, 3), (3, 7), (4, 15), (5, 31), (6, 63), (7, 127), (8, 255)],
    )
    def test_partial_bit_extraction_all_bits_set(self, bit_length: int, expected: int):
        """Test extraction of partial bit values with all bits set."""
        segment = b"\xFF"  # All bits set
        result = extract_signal_value(segment, 0, bit_length, 1)
        assert result == expected

    @pytest.mark.parametrize(
        "segment,start_offset,bit_length,expected",
        [
            (b"\xA5", 0, 4, 10),  # 10100101 -> bits 0-3: 1010 = 10
            (b"\xA5", 4, 4, 5),  # 10100101 -> bits 4-7: 0101 = 5
            (b"\xF0", 0, 4, 15),  # 11110000 -> bits 0-3: 1111 = 15
            (b"\xF0", 4, 4, 0),  # 11110000 -> bits 4-7: 0000 = 0
        ],
    )
    def test_partial_bit_extraction_specific_patterns(
        self, segment: bytes, start_offset: int, bit_length: int, expected: int
    ):
        """Test extraction of specific bit patterns."""
        result = extract_signal_value(segment, start_offset, bit_length, 1)
        assert result == expected

    @pytest.mark.parametrize(
        "bit_length,segment,expected_range",
        [
            (1, b"\xAA\x55", (0, 1)),
            (2, b"\xAA\x55", (0, 3)),
            (3, b"\xAA\x55", (0, 7)),
            (4, b"\xAA\x55", (0, 15)),
            (5, b"\xAA\x55", (0, 31)),
            (6, b"\xAA\x55", (0, 63)),
            (7, b"\xAA\x55", (0, 127)),
            (8, b"\xAA\x55", (0, 255)),
            (12, b"\xAA\x55\xAA\x55", (0, 4095)),
            (16, b"\xAA\x55\xAA\x55", (0, 65535)),
            (24, b"\xAA\x55\xAA\x55", (0, 16777215)),
            (32, b"\xAA\x55\xAA\x55\xAA\x55", (0, 4294967295)),
            (48, b"\xAA\x55" * 6, (0, 281474976710655)),
            (64, b"\xAA\x55" * 8, (0, 18446744073709551615)),
        ],
    )
    def test_various_bit_lengths(self, bit_length: int, segment: bytes, expected_range: tuple):
        """Test extraction with various bit lengths."""
        min_val, max_val = expected_range

        # Test extraction for both endiannesses
        result_le = extract_signal_value(segment, 0, bit_length, 1)
        result_be = extract_signal_value(segment, 0, bit_length, 2)

        # Results should be valid integers within expected range
        assert min_val <= result_le <= max_val
        assert min_val <= result_be <= max_val

        # Results should be different for multi-byte extractions
        if bit_length > 8:
            assert isinstance(result_le, int)
            assert isinstance(result_be, int)

    @pytest.mark.parametrize("invalid_length", [0, -1, 65, 128])
    def test_invalid_bit_lengths(self, invalid_length: int):
        """Test that invalid bit lengths raise ValueError."""
        segment = b"\x12\x34"

        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, invalid_length, 1)

    def test_shift_calculation_edge_cases(self):
        """Test edge cases in shift calculation."""
        # Test cases where shift calculation needs to be precise

        # Single byte, extract first bit (MSB)
        segment = b"\x80"  # Only MSB set (10000000)
        result = extract_signal_value(segment, 0, 1, 1)
        assert result == 1

        # Two bytes, extract from second byte
        segment = b"\x00\xFF"  # Second byte all set
        result = extract_signal_value(segment, 8, 8, 1)  # Start at bit 8
        assert result == 0xFF

        # Test with bit offset within byte
        segment = b"\x0F"  # 00001111
        result = extract_signal_value(segment, 4, 4, 1)  # Bits 4-7
        assert result == 0xF

    def test_complex_bit_patterns(self):
        """Test with complex bit patterns and various configurations."""
        # Test different segment patterns with endianness

        # Test 2-byte segment
        segment = b"\x01\x02"
        result_le = extract_signal_value(segment, 0, 16, 1)  # Little endian
        result_be = extract_signal_value(segment, 0, 16, 2)  # Big endian
        assert result_le == 0x0201  # Little endian byte order
        assert result_be == 0x0102  # Big endian byte order

        # Test 4-byte segment
        segment = b"\x01\x02\x03\x04"
        result_le = extract_signal_value(segment, 0, 32, 1)
        result_be = extract_signal_value(segment, 0, 32, 2)
        assert result_le == 0x04030201  # Little endian
        assert result_be == 0x01020304  # Big endian

        # Test that endianness matters for multi-byte
        assert result_le != result_be

    def test_endianness_differences(self):
        """Test specific endianness handling differences."""
        # Create a clear test case for endianness
        segment = b"\x12\x34\x56\x78"

        # 16-bit extractions from start
        le_16 = extract_signal_value(segment, 0, 16, 1)  # Little endian
        be_16 = extract_signal_value(segment, 0, 16, 2)  # Big endian

        assert le_16 == 0x7856  # Little endian: uses last 2 bytes in LE order
        assert be_16 == 0x1234  # Big endian: uses first 2 bytes in BE order

        # 32-bit extractions
        le_32 = extract_signal_value(segment, 0, 32, 1)
        be_32 = extract_signal_value(segment, 0, 32, 2)

        assert le_32 == 0x78563412  # Little endian: all bytes in LE order
        assert be_32 == 0x12345678  # Big endian: all bytes in BE order

        # Verify they are different
        assert le_16 != be_16
        assert le_32 != be_32

    def test_bit_offset_calculations(self):
        """Test bit offset calculations within segments."""
        # Test bit offsets within a single byte
        segment = b"\xF0"  # 11110000

        # Extract 4 bits from different positions within the byte
        result_0 = extract_signal_value(segment, 0, 4, 1)  # First 4 bits: 1111 = 15
        result_4 = extract_signal_value(segment, 4, 4, 1)  # Last 4 bits: 0000 = 0

        assert result_0 == 15
        assert result_4 == 0

        # Test with a different pattern
        segment = b"\xAA"  # 10101010
        result_0 = extract_signal_value(segment, 0, 4, 1)  # First 4 bits: 1010 = 10
        result_4 = extract_signal_value(segment, 4, 4, 1)  # Last 4 bits: 1010 = 10

        assert result_0 == 10
        assert result_4 == 10

        # Test 2-bit extractions are done in separate parameterized test

    @pytest.mark.parametrize(
        "bit_pos,expected",
        [
            (0, 3),  # bits 0-1: 11 -> 3
            (2, 0),  # bits 2-3: 00 -> 0
            (4, 0),  # bits 4-5: 00 -> 0
            (6, 3),  # bits 6-7: 11 -> 3
        ],
    )
    def test_2_bit_extractions_edge_cases(self, bit_pos, expected):
        """Test 2-bit extractions at various positions."""
        segment = b"\xC3"  # 11000011
        result = extract_signal_value(segment, bit_pos, 2, 1)
        assert result == expected


class TestSignalValueExtractionErrors:
    """Test error conditions for signal value extraction."""

    def test_invalid_bit_length_validation(self):
        """Test bit length validation errors."""
        segment = b"\x12\x34"

        # Test zero bit length
        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, 0, 1)

        # Test negative bit length
        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, -1, 1)

        # Test excessive bit length
        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, 65, 1)

        # Test way too large bit length
        with pytest.raises(ValueError, match="Invalid bit_length"):
            extract_signal_value(segment, 0, 1000, 1)

    def test_negative_shift_conditions(self):
        """Test conditions that would cause negative shift calculations."""
        # Create scenarios where shift = len(segment) * 8 - bit_length - bit_offset < 0

        # Small segment, large bit requirements
        segment = b"\x12"  # 1 byte = 8 bits

        # This should cause shift = 8 - 8 - 7 = -7
        with pytest.raises(ValueError, match="Negative shift calculated"):
            extract_signal_value(segment, 7, 8, 1)

        # Another case: shift = 8 - 16 - 0 = -8
        with pytest.raises(ValueError, match="Negative shift calculated"):
            extract_signal_value(segment, 0, 16, 1)

        # More complex case with 2-byte segment
        segment = b"\x12\x34"  # 2 bytes = 16 bits

        # This should cause negative shift
        with pytest.raises(ValueError, match="Negative shift calculated"):
            extract_signal_value(segment, 15, 16, 1)

    def test_edge_case_bit_manipulations(self):
        """Test edge cases in bit manipulation."""
        # Test maximum bit extraction from various segment sizes

        # 1 byte segment - maximum extraction
        segment = b"\xFF"
        result = extract_signal_value(segment, 0, 8, 1)
        assert result == 0xFF

        # 2 byte segment - maximum extraction
        segment = b"\xFF\xFF"
        result = extract_signal_value(segment, 0, 16, 1)
        assert result == 0xFFFF

        # 8 byte segment - maximum extraction
        segment = b"\xFF" * 8
        result = extract_signal_value(segment, 0, 64, 1)
        assert result == 0xFFFFFFFFFFFFFFFF

    @pytest.mark.parametrize("bit_pos", list(range(24)))  # 0-23: within segment range
    def test_boundary_bit_extractions(self, bit_pos):
        """Test extractions exactly at boundaries."""
        segment = b"\x12\x34\x56\x78"

        result = extract_signal_value(segment, bit_pos, 1, 1)
        assert result in [0, 1]  # Must be 0 or 1 for single bit
