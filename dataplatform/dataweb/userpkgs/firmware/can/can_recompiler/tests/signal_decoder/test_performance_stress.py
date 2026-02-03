"""
Test suite for performance and stress testing.

Tests the decoder functions under various performance scenarios
and stress conditions to ensure robustness and efficiency.
"""

import pytest
import time
from typing import List

from ...signal_decoder.decoder import (
    calculate_normalized_bit_position,
    extract_byte_segment,
    extract_signal_value,
    apply_signal_sign,
    decode_single_signal_from_payload,
)


class TestPerformanceAndStress:
    """Performance and stress testing for signal decoder."""

    def test_large_payloads(self):
        """Test decoder performance with large payloads."""
        # Create large payload (1KB)
        large_payload = bytes(range(256)) * 4  # 1024 bytes

        # Test extraction from various positions
        positions = [0, 100, 500, 800, 1000]

        for pos in positions:
            if pos + 8 <= len(large_payload) * 8:  # Ensure we don't exceed bounds
                result = decode_single_signal_from_payload(large_payload, pos, 8, 1, 2)
                assert result is not None
                assert isinstance(result, float)

    def test_many_sequential_extractions(self):
        """Test performance with many sequential signal extractions."""
        payload = b"\x12\x34\x56\x78\xAB\xCD\xEF\x00" * 100  # 800 bytes

        results = []

        # Extract 100 signals sequentially
        for i in range(100):
            bit_start = i * 8
            if bit_start + 8 <= len(payload) * 8:
                result = decode_single_signal_from_payload(payload, bit_start, 8, 1, 2)
                results.append(result)

        # Verify all extractions succeeded
        assert len(results) > 50  # Should extract many signals
        assert all(isinstance(r, float) for r in results)

    @pytest.mark.parametrize("payload_size", [1, 2, 4, 8, 16, 32, 64])
    def test_various_payload_sizes(self, payload_size: int):
        """Test decoder with various payload sizes."""
        payload = bytes(range(256))[:payload_size]
        max_bits = payload_size * 8

        if max_bits > 0:
            # Test single bit extraction
            result = decode_single_signal_from_payload(payload, 0, 1, 1, 2)
            assert result in [0.0, 1.0]

            # Test full byte extraction if possible
            if max_bits >= 8:
                result = decode_single_signal_from_payload(payload, 0, 8, 1, 2)
                assert isinstance(result, float)

            # Test maximum extraction for this payload
            if max_bits >= 16:
                result = decode_single_signal_from_payload(payload, 0, min(16, max_bits), 1, 2)
                assert isinstance(result, float)

    def test_stress_bit_position_calculations(self):
        """Stress test bit position calculations."""
        # Test many bit position calculations
        test_positions = list(range(0, 1000, 7))  # Every 7th position up to 1000

        for pos in test_positions:
            # Little endian
            result_le = calculate_normalized_bit_position(pos, 1)
            assert result_le == pos

            # Big endian
            result_be = calculate_normalized_bit_position(pos, 2)
            expected_be = (pos // 8) * 8 + (7 - (pos % 8))
            assert result_be == expected_be

    def test_stress_byte_segment_extraction(self):
        """Stress test byte segment extraction."""
        # Create a large payload for stress testing
        payload = bytes(range(256)) + bytes(range(256))  # 512 bytes

        # Test many different segment extractions
        test_ranges = [
            (0, 7),
            (8, 15),
            (16, 31),
            (32, 63),
            (64, 127),
            (100, 200),
            (200, 300),
            (300, 400),
            (400, 500),
        ]

        for start, end in test_ranges:
            if end < len(payload) * 8:
                segment = extract_byte_segment(payload, start, end)
                assert isinstance(segment, bytes)
                assert len(segment) > 0

    @pytest.mark.parametrize(
        "segment,bit_length,endianness,description",
        [
            # 1 byte tests - all bit lengths that fit
            (b"\xFF", 1, 1, "1-byte LE 1-bit"),
            (b"\xFF", 1, 2, "1-byte BE 1-bit"),
            (b"\xFF", 2, 1, "1-byte LE 2-bit"),
            (b"\xFF", 2, 2, "1-byte BE 2-bit"),
            (b"\xFF", 4, 1, "1-byte LE 4-bit"),
            (b"\xFF", 4, 2, "1-byte BE 4-bit"),
            (b"\xFF", 8, 1, "1-byte LE 8-bit"),
            (b"\xFF", 8, 2, "1-byte BE 8-bit"),
            # 4 byte tests - more bit lengths
            (b"\xAA\x55\xAA\x55", 1, 1, "4-byte LE 1-bit"),
            (b"\xAA\x55\xAA\x55", 1, 2, "4-byte BE 1-bit"),
            (b"\xAA\x55\xAA\x55", 2, 1, "4-byte LE 2-bit"),
            (b"\xAA\x55\xAA\x55", 2, 2, "4-byte BE 2-bit"),
            (b"\xAA\x55\xAA\x55", 4, 1, "4-byte LE 4-bit"),
            (b"\xAA\x55\xAA\x55", 4, 2, "4-byte BE 4-bit"),
            (b"\xAA\x55\xAA\x55", 8, 1, "4-byte LE 8-bit"),
            (b"\xAA\x55\xAA\x55", 8, 2, "4-byte BE 8-bit"),
            (b"\xAA\x55\xAA\x55", 12, 1, "4-byte LE 12-bit"),
            (b"\xAA\x55\xAA\x55", 12, 2, "4-byte BE 12-bit"),
            (b"\xAA\x55\xAA\x55", 16, 1, "4-byte LE 16-bit"),
            (b"\xAA\x55\xAA\x55", 16, 2, "4-byte BE 16-bit"),
            (b"\xAA\x55\xAA\x55", 24, 1, "4-byte LE 24-bit"),
            (b"\xAA\x55\xAA\x55", 24, 2, "4-byte BE 24-bit"),
            (b"\xAA\x55\xAA\x55", 32, 1, "4-byte LE 32-bit"),
            (b"\xAA\x55\xAA\x55", 32, 2, "4-byte BE 32-bit"),
            # 8 byte tests - all bit lengths
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 1, 1, "8-byte LE 1-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 1, 2, "8-byte BE 1-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 2, 1, "8-byte LE 2-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 2, 2, "8-byte BE 2-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 4, 1, "8-byte LE 4-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 4, 2, "8-byte BE 4-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 8, 1, "8-byte LE 8-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 8, 2, "8-byte BE 8-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 12, 1, "8-byte LE 12-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 12, 2, "8-byte BE 12-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 16, 1, "8-byte LE 16-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 16, 2, "8-byte BE 16-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 24, 1, "8-byte LE 24-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 24, 2, "8-byte BE 24-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 32, 1, "8-byte LE 32-bit"),
            (b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0", 32, 2, "8-byte BE 32-bit"),
        ],
    )
    def test_stress_signal_value_extraction(self, segment, bit_length, endianness, description):
        """Stress test signal value extraction."""
        result = extract_signal_value(segment, 0, bit_length, endianness)

        assert isinstance(result, int)
        assert 0 <= result < (1 << bit_length)

    @pytest.mark.parametrize(
        "bit_length,value,sign_type,description",
        [
            # 1-bit tests
            (1, 0, 1, "1-bit min signed"),
            (1, 0, 2, "1-bit min unsigned"),
            (1, 1, 1, "1-bit max signed"),
            (1, 1, 2, "1-bit max unsigned"),
            # 2-bit tests
            (2, 0, 1, "2-bit min signed"),
            (2, 0, 2, "2-bit min unsigned"),
            (2, 1, 1, "2-bit small signed"),
            (2, 1, 2, "2-bit small unsigned"),
            (2, 1, 1, "2-bit max pos signed"),
            (2, 1, 2, "2-bit max pos unsigned"),
            (2, 2, 1, "2-bit min neg signed"),
            (2, 2, 2, "2-bit min neg unsigned"),
            (2, 3, 1, "2-bit max signed"),
            (2, 3, 2, "2-bit max unsigned"),
            # 4-bit tests
            (4, 0, 1, "4-bit min signed"),
            (4, 0, 2, "4-bit min unsigned"),
            (4, 1, 1, "4-bit small signed"),
            (4, 1, 2, "4-bit small unsigned"),
            (4, 7, 1, "4-bit max pos signed"),
            (4, 7, 2, "4-bit max pos unsigned"),
            (4, 8, 1, "4-bit min neg signed"),
            (4, 8, 2, "4-bit min neg unsigned"),
            (4, 15, 1, "4-bit max signed"),
            (4, 15, 2, "4-bit max unsigned"),
            # 8-bit tests
            (8, 0, 1, "8-bit min signed"),
            (8, 0, 2, "8-bit min unsigned"),
            (8, 1, 1, "8-bit small signed"),
            (8, 1, 2, "8-bit small unsigned"),
            (8, 127, 1, "8-bit max pos signed"),
            (8, 127, 2, "8-bit max pos unsigned"),
            (8, 128, 1, "8-bit min neg signed"),
            (8, 128, 2, "8-bit min neg unsigned"),
            (8, 255, 1, "8-bit max signed"),
            (8, 255, 2, "8-bit max unsigned"),
            # 16-bit tests
            (16, 0, 1, "16-bit min signed"),
            (16, 0, 2, "16-bit min unsigned"),
            (16, 1, 1, "16-bit small signed"),
            (16, 1, 2, "16-bit small unsigned"),
            (16, 32767, 1, "16-bit max pos signed"),
            (16, 32767, 2, "16-bit max pos unsigned"),
            (16, 32768, 1, "16-bit min neg signed"),
            (16, 32768, 2, "16-bit min neg unsigned"),
            (16, 65535, 1, "16-bit max signed"),
            (16, 65535, 2, "16-bit max unsigned"),
            # 32-bit tests (sample)
            (32, 0, 1, "32-bit min signed"),
            (32, 0, 2, "32-bit min unsigned"),
            (32, 1, 1, "32-bit small signed"),
            (32, 1, 2, "32-bit small unsigned"),
            (32, 2147483647, 1, "32-bit max pos signed"),
            (32, 2147483647, 2, "32-bit max pos unsigned"),
            (32, 2147483648, 1, "32-bit min neg signed"),
            (32, 2147483648, 2, "32-bit min neg unsigned"),
            (32, 4294967295, 1, "32-bit max signed"),
            (32, 4294967295, 2, "32-bit max unsigned"),
        ],
    )
    def test_stress_sign_application(self, bit_length, value, sign_type, description):
        """Stress test sign application."""
        result = apply_signal_sign(value, bit_length, sign_type)

        assert isinstance(result, int)
        if sign_type == 2:  # Unsigned
            assert result == value  # Unsigned should match input

    def test_performance_timing_benchmark(self):
        """Basic performance timing benchmark."""
        payload = b"\x12\x34\x56\x78\xAB\xCD\xEF\x00"
        iterations = 1000

        start_time = time.time()

        for i in range(iterations):
            # Vary the bit start to prevent caching effects
            bit_start = i % 56  # Up to bit 56 (8-bit signal fits in 64-bit payload)
            result = decode_single_signal_from_payload(payload, bit_start, 8, 1, 2)
            assert result is not None

        end_time = time.time()
        elapsed = end_time - start_time

        # Performance should be reasonable (less than 1 second for 1000 operations)
        assert elapsed < 1.0, f"Performance too slow: {elapsed:.3f}s for {iterations} operations"

        # Calculate operations per second
        ops_per_second = iterations / elapsed
        print(f"Performance: {ops_per_second:.0f} operations/second")

    def test_memory_efficiency_large_payloads(self):
        """Test memory efficiency with large payloads."""
        # Test with progressively larger payloads
        payload_sizes = [64, 128, 256, 512, 1024]  # bytes

        for size in payload_sizes:
            payload = bytes(range(256))[:256] * (size // 256 + 1)
            payload = payload[:size]

            # Extract a signal from the middle
            middle_bit = (size * 8) // 2
            if middle_bit + 16 <= size * 8:
                result = decode_single_signal_from_payload(payload, middle_bit, 16, 1, 2)
                assert isinstance(result, float)

    def test_concurrent_operation_simulation(self):
        """Simulate concurrent operations on different payload sections."""
        # Large payload simulating concurrent CAN frames
        payload = bytes(range(256)) * 4  # 1024 bytes

        # Simulate processing multiple signals concurrently
        signal_definitions = [
            (0, 8, 1, 2),  # Signal 1: byte 0
            (64, 16, 2, 1),  # Signal 2: bytes 8-9, big endian, signed
            (128, 12, 1, 2),  # Signal 3: partial bytes, little endian
            (200, 24, 2, 1),  # Signal 4: 3 bytes, big endian, signed
            (300, 32, 1, 2),  # Signal 5: 4 bytes, little endian
        ]

        results = []
        for bit_start, bit_length, endianness, sign_type in signal_definitions:
            if bit_start + bit_length <= len(payload) * 8:
                result = decode_single_signal_from_payload(
                    payload, bit_start, bit_length, endianness, sign_type
                )
                results.append(result)

        # All signals should decode successfully
        assert len(results) == len(signal_definitions)
        assert all(isinstance(r, float) for r in results)

    def test_edge_case_performance(self):
        """Test performance with edge cases."""
        # Test maximum bit length extraction
        max_payload = b"\xFF" * 8  # 64 bits

        # Maximum extraction (64 bits)
        result = decode_single_signal_from_payload(max_payload, 0, 64, 1, 2)
        assert result == float(0xFFFFFFFFFFFFFFFF)

        # Test minimum extraction (1 bit) at various positions
        for bit_pos in range(0, 64, 8):
            result = decode_single_signal_from_payload(max_payload, bit_pos, 1, 1, 2)
            assert result == 1.0  # All bits are set

    def test_robustness_under_stress(self):
        """Test robustness under stress conditions."""
        # Rapid-fire extractions with varying parameters
        payload = b"\x12\x34\x56\x78\xAB\xCD\xEF\x00"

        success_count = 0
        total_attempts = 200

        for i in range(total_attempts):
            # Vary parameters systematically
            bit_start = (i * 3) % 56  # Vary bit start
            bit_length = [1, 2, 4, 8, 16][i % 5]  # Vary bit length
            endianness = [1, 2][i % 2]  # Vary endianness
            sign_type = [1, 2][i % 2]  # Vary sign type

            if bit_start + bit_length <= 64:  # Ensure valid range
                result = decode_single_signal_from_payload(
                    payload, bit_start, bit_length, endianness, sign_type
                )
                if result is not None:
                    success_count += 1

        # Should have high success rate for valid parameters
        success_rate = success_count / total_attempts
        assert success_rate > 0.8, f"Success rate too low: {success_rate:.2%}"


class TestStressErrorConditions:
    """Stress test error conditions and recovery."""

    def test_rapid_error_condition_handling(self):
        """Test rapid error condition handling."""
        payload = b"\x12\x34"
        error_conditions = [
            (-1, 8, 1, 2),  # Invalid bit_start
            (0, 0, 1, 2),  # Invalid bit_length
            (0, 8, 0, 2),  # Invalid endianness
            (0, 8, 1, 0),  # Invalid sign_type
            (20, 8, 1, 2),  # bit_start beyond payload
        ]

        for _ in range(100):  # Repeat many times
            for bit_start, bit_length, endianness, sign_type in error_conditions:
                result = decode_single_signal_from_payload(
                    payload, bit_start, bit_length, endianness, sign_type
                )
                assert result is None  # Should handle gracefully

    def test_boundary_stress_testing(self):
        """Stress test boundary conditions."""
        payloads = [
            b"",  # Empty
            b"\x00",  # Single byte
            b"\xFF" * 8,  # Maximum typical CAN frame
            b"\xAA" * 64,  # Large payload
        ]

        for payload in payloads:
            max_bits = len(payload) * 8

            if max_bits > 0:
                # Test valid boundary
                result = decode_single_signal_from_payload(payload, 0, 1, 1, 2)
                if len(payload) > 0:
                    assert result is not None

                # Test invalid boundary
                result = decode_single_signal_from_payload(payload, max_bits, 1, 1, 2)
                assert result is None

    def test_parameter_validation_stress(self):
        """Stress test parameter validation."""
        payload = b"\x12\x34\x56\x78"

        # Test extreme parameter values
        extreme_values = [-999999, -1, 0, 999999]

        for extreme in extreme_values:
            # Test each parameter with extreme values
            result = decode_single_signal_from_payload(payload, extreme, 8, 1, 2)
            if extreme < 0 or extreme >= 32:  # Invalid for this payload
                assert result is None

            if extreme > 0 and extreme <= 64:
                result = decode_single_signal_from_payload(payload, 0, extreme, 1, 2)
                # May be valid or invalid depending on payload size


class TestRealisticWorkloadSimulation:
    """Simulate realistic automotive CAN signal processing workloads."""

    def test_automotive_signal_processing_simulation(self):
        """Simulate realistic automotive signal processing."""
        # Simulate multiple CAN frames being processed
        can_frames = [
            b"\x00\x64\x13\x88\x27\x10\xFF\x00",  # Vehicle data frame
            b"\x12\x34\x56\x78\x9A\xBC\xDE\xF0",  # Engine data frame
            b"\xFF\x00\xAA\x55\x33\xCC\x69\x96",  # Sensor data frame
        ]

        # Define typical automotive signals
        signal_definitions = [
            # (frame_idx, bit_start, bit_length, endianness, sign_type, description)
            (0, 8, 16, 2, 2, "Vehicle Speed"),  # Big endian, unsigned
            (0, 24, 16, 1, 2, "Engine RPM"),  # Little endian, unsigned
            (0, 40, 8, 1, 1, "Coolant Temp"),  # Signed temperature
            (1, 0, 32, 1, 2, "Odometer"),  # 32-bit odometer
            (1, 32, 12, 2, 2, "Fuel Level"),  # 12-bit fuel level
            (2, 16, 8, 1, 1, "Steering Angle"),  # Signed steering
        ]

        results = []

        for frame_idx, bit_start, bit_length, endianness, sign_type, desc in signal_definitions:
            if frame_idx < len(can_frames):
                payload = can_frames[frame_idx]
                result = decode_single_signal_from_payload(
                    payload, bit_start, bit_length, endianness, sign_type
                )
                results.append((desc, result))

        # All signals should decode successfully
        assert len(results) == len(signal_definitions)
        assert all(result is not None for _, result in results)

    def test_high_frequency_signal_processing(self):
        """Test high-frequency signal processing scenario."""
        # Simulate processing 1000 CAN frames per second
        frame_template = b"\x12\x34\x56\x78\xAB\xCD\xEF\x00"
        frames_per_second = 1000

        start_time = time.time()

        for frame_num in range(frames_per_second):
            # Vary the payload slightly to simulate real data
            payload = bytes([(b + frame_num) % 256 for b in frame_template])

            # Extract multiple signals per frame
            signals = [
                (0, 8, 1, 2),  # Signal 1
                (16, 16, 2, 1),  # Signal 2
                (32, 12, 1, 2),  # Signal 3
            ]

            for bit_start, bit_length, endianness, sign_type in signals:
                result = decode_single_signal_from_payload(
                    payload, bit_start, bit_length, endianness, sign_type
                )
                assert result is not None

        end_time = time.time()
        processing_time = end_time - start_time

        # Should process efficiently
        print(f"Processed {frames_per_second} frames in {processing_time:.3f}s")
        assert processing_time < 2.0  # Should be fast enough for real-time processing
