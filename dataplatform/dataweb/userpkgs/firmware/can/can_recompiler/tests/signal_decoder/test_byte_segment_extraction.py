"""
Test suite for byte segment extraction functionality.

Tests the extract_byte_segment() function which extracts relevant
byte segments from payload data for signal decoding.
"""

import pytest

from ...signal_decoder.decoder import extract_byte_segment


class TestExtractByteSegment:
    """Comprehensive tests for byte segment extraction."""

    def test_single_byte_extraction(self):
        """Test extraction of single byte segments."""
        payload = b"\x12\x34\x56\x78"

        # Extract first byte (bits 0-7)
        result = extract_byte_segment(payload, 0, 7)
        assert result == b"\x12"

        # Extract second byte (bits 8-15)
        result = extract_byte_segment(payload, 8, 15)
        assert result == b"\x34"

        # Extract third byte (bits 16-23)
        result = extract_byte_segment(payload, 16, 23)
        assert result == b"\x56"

        # Extract fourth byte (bits 24-31)
        result = extract_byte_segment(payload, 24, 31)
        assert result == b"\x78"

    def test_multi_byte_extraction(self):
        """Test extraction of multi-byte segments."""
        payload = b"\x12\x34\x56\x78\xAB\xCD\xEF\x00"

        # Extract two bytes (bits 0-15)
        result = extract_byte_segment(payload, 0, 15)
        assert result == b"\x12\x34"

        # Extract three bytes (bits 8-31)
        result = extract_byte_segment(payload, 8, 31)
        assert result == b"\x34\x56\x78"

        # Extract four bytes (bits 16-47)
        result = extract_byte_segment(payload, 16, 47)
        assert result == b"\x56\x78\xAB\xCD"

        # Extract entire payload
        result = extract_byte_segment(payload, 0, 63)
        assert result == payload

    def test_partial_byte_boundaries(self):
        """Test extraction across partial byte boundaries."""
        payload = b"\x12\x34\x56\x78"

        # Extract partial segments that span bytes
        # Bits 4-11 (partial first byte + partial second byte)
        result = extract_byte_segment(payload, 4, 11)
        assert result == b"\x12\x34"  # Should include both bytes

        # Bits 6-17 (partial first byte + full second byte + partial third byte)
        result = extract_byte_segment(payload, 6, 17)
        assert result == b"\x12\x34\x56"  # Should include three bytes

        # Single bit extractions at byte boundaries
        result = extract_byte_segment(payload, 7, 7)  # Last bit of first byte
        assert result == b"\x12"

        result = extract_byte_segment(payload, 8, 8)  # First bit of second byte
        assert result == b"\x34"

    @pytest.mark.parametrize(
        "start,end,payload,should_raise",
        [
            (-1, 7, b"\x12\x34", True),  # Negative start
            (7, 6, b"\x12\x34", True),  # End < start
            (0, 16, b"\x12", True),  # End beyond payload
            (16, 23, b"\x12", True),  # Both beyond payload
        ],
    )
    def test_invalid_bit_ranges(self, start, end, payload, should_raise):
        """Test invalid bit range handling."""
        if should_raise:
            with pytest.raises(ValueError):
                extract_byte_segment(payload, start, end)
        else:
            # Should not raise exception
            result = extract_byte_segment(payload, start, end)
            assert isinstance(result, bytes)

    def test_empty_payload(self):
        """Test behavior with empty payload."""
        payload = b""

        # Any bit range should raise an error with empty payload
        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 0, 0)

        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 0, 7)

    def test_exact_payload_boundaries(self):
        """Test extraction at exact payload boundaries."""
        payload = b"\x12\x34"  # 16 bits (0-15)

        # Valid boundary cases
        result = extract_byte_segment(payload, 0, 15)  # Entire payload
        assert result == payload

        result = extract_byte_segment(payload, 15, 15)  # Single last bit
        assert result == b"\x34"

        # Invalid boundary cases
        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 0, 16)  # One bit beyond

        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, 16, 23)  # Completely beyond

    @pytest.mark.parametrize(
        "start,end,expected",
        [
            (0, 7, b"\x12"),  # Byte 0
            (8, 15, b"\x34"),  # Byte 1
            (16, 23, b"\x56"),  # Byte 2
            (24, 31, b"\x78"),  # Byte 3
            (32, 39, b"\xAB"),  # Byte 4
            (40, 47, b"\xCD"),  # Byte 5
            (0, 15, b"\x12\x34"),  # Bytes 0-1
            (8, 23, b"\x34\x56"),  # Bytes 1-2
            (16, 31, b"\x56\x78"),  # Bytes 2-3
            (0, 23, b"\x12\x34\x56"),  # Bytes 0-2
            (8, 31, b"\x34\x56\x78"),  # Bytes 1-3
        ],
    )
    def test_byte_alignment_calculations(self, start, end, expected):
        """Test that byte calculations are correct."""
        payload = b"\x12\x34\x56\x78\xAB\xCD"
        result = extract_byte_segment(payload, start, end)
        assert (
            result == expected
        ), f"Range {start}-{end}: got {result.hex()}, expected {expected.hex()}"


class TestByteSegmentErrorConditions:
    """Test error conditions and edge cases for byte segment extraction."""

    @pytest.mark.parametrize(
        "start_bit,end_bit,description",
        [
            (-1, 7, "negative start position"),  # Test negative start positions
            (-10, 0, "large negative start"),  # Test negative start positions
            (10, 5, "end < start (10 > 5)"),  # Test end < start
            (15, 8, "end < start (15 > 8)"),  # Test end < start
        ],
    )
    def test_bit_range_validation_errors(self, start_bit, end_bit, description):
        """Test specific bit range validation errors."""
        payload = b"\x12\x34\x56\x78"

        with pytest.raises(ValueError, match="Invalid bit range"):
            extract_byte_segment(payload, start_bit, end_bit)

    @pytest.mark.parametrize(
        "start_bit,end_bit,description",
        [
            (0, 16, "end_bit=16 -> end_byte=2"),  # end_byte calculation exceeds payload
            (8, 23, "end_bit=23 -> end_byte=2"),  # end_byte calculation exceeds payload
            (16, 23, "start beyond payload"),  # start beyond payload
        ],
    )
    def test_payload_bounds_errors(self, start_bit, end_bit, description):
        """Test payload boundary validation errors."""
        payload = b"\x12\x34"  # 2 bytes = 16 bits (0-15)

        with pytest.raises(ValueError, match="Bit range exceeds payload"):
            extract_byte_segment(payload, start_bit, end_bit)

    @pytest.mark.parametrize("bit_pos", list(range(32)))  # 4 bytes = 32 bits
    def test_edge_case_single_bit_extractions(self, bit_pos):
        """Test single bit extractions at various positions."""
        payload = b"\xFF\x00\xFF\x00"

        result = extract_byte_segment(payload, bit_pos, bit_pos)
        expected_byte_index = bit_pos // 8
        expected_byte = payload[expected_byte_index : expected_byte_index + 1]
        assert result == expected_byte
        assert len(result) == 1

    @pytest.mark.parametrize(
        "start_bit,end_bit,expected_length,slice_start,slice_end",
        [
            (0, 63, 8, 0, 8),  # 8 bytes from beginning
            (0, 255, 32, 0, 32),  # 32 bytes from beginning
            (128, 255, 16, 16, 32),  # 16 bytes from middle
        ],
    )
    def test_maximum_segment_sizes(
        self, start_bit, end_bit, expected_length, slice_start, slice_end
    ):
        """Test extraction of maximum reasonable segment sizes."""
        # Create a large payload (typical CAN frame size)
        payload = bytes(range(64))  # 64 bytes

        result = extract_byte_segment(payload, start_bit, end_bit)
        assert len(result) == expected_length
        assert result == payload[slice_start:slice_end]

    @pytest.mark.parametrize("bit", list(range(32)))  # 4 bytes = 32 bits
    def test_single_bit_boundary_stress(self, bit):
        """Stress test single-bit boundary conditions."""
        payload = b"\x12\x34\x56\x78"
        result = extract_byte_segment(payload, bit, bit)
        assert len(result) == 1

    @pytest.mark.parametrize(
        "start_byte,end_byte,description",
        [
            (0, 0, "byte 0 only"),
            (0, 1, "bytes 0-1"),
            (0, 2, "bytes 0-2"),
            (0, 3, "bytes 0-3"),
            (1, 1, "byte 1 only"),
            (1, 2, "bytes 1-2"),
            (1, 3, "bytes 1-3"),
            (2, 2, "byte 2 only"),
            (2, 3, "bytes 2-3"),
            (3, 3, "byte 3 only"),
        ],
    )
    def test_byte_aligned_boundary_stress(self, start_byte, end_byte, description):
        """Stress test byte-aligned boundary conditions."""
        payload = b"\x12\x34\x56\x78"

        start_bit = start_byte * 8
        end_bit = end_byte * 8 + 7
        result = extract_byte_segment(payload, start_bit, end_bit)
        expected_length = end_byte - start_byte + 1
        assert len(result) == expected_length
