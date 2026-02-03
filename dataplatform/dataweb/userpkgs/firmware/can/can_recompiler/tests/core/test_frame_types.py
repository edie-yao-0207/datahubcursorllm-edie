"""
Tests for core CAN frame types and utilities.

This module tests pure domain objects and utilities without any infrastructure
dependencies. Tests that require Spark/DataWeb infrastructure should be in
tests/infra/spark/test_frame_types.py.
"""

import pytest

from ...core.frame_types import CANTransportFrame


class TestCANTransportFrame:
    """Test CANTransportFrame creation and properties."""

    @pytest.mark.parametrize(
        "arb_id,data,transport_id,is_extended,description",
        [
            (0x7E0, b"\x22\xF1\x90", 1, False, "ISO-TP standard frame"),
            (0x18DA0F01, b"\xDF\x03\x22\xF1\x90", 1, True, "ISO-TP extended frame"),
            (0x18FEF100, b"\x01\x02\x03\x04\x05\x06\x07\x08", 2, True, "J1939 standard frame"),
            (
                0x18EC0000,
                b"\x20\x0A\x00\x02\x00\xF1\xFE\x00",
                2,
                True,
                "J1939 transport protocol frame",
            ),
        ],
    )
    def test_can_transport_frame_creation(
        self, arb_id: int, data: bytes, transport_id: int, is_extended: bool, description: str
    ):
        """Test CANTransportFrame creation for different frame types."""
        frame = CANTransportFrame(
            arbitration_id=arb_id, payload=data, start_timestamp_unix_us=1000000
        )

        assert frame.arbitration_id == arb_id, f"Arbitration ID mismatch for {description}"
        assert frame.payload == data, f"Payload mismatch for {description}"
        assert frame.start_timestamp_unix_us == 1000000, f"Timestamp mismatch for {description}"
        assert (
            frame.is_extended_frame == is_extended
        ), f"Extended frame flag mismatch for {description}"

    @pytest.mark.parametrize(
        "invalid_id,error_pattern",
        [
            (-1, "Invalid CAN ID: -1. Must be non-negative integer"),
            (0x20000000, "CAN ID 20000000 exceeds 29-bit limit"),
        ],
    )
    def test_invalid_can_id_validation(self, invalid_id: int, error_pattern: str):
        """Test validation of invalid CAN IDs on CANTransportFrame construction."""
        with pytest.raises(ValueError, match=error_pattern):
            CANTransportFrame(
                arbitration_id=invalid_id, payload=b"\x22\xF1\x90", start_timestamp_unix_us=1000000
            )

    @pytest.mark.parametrize(
        "timestamp_value,should_raise,description",
        [
            (0, False, "minimum valid timestamp"),
            (1000000, False, "normal timestamp"),
            (4294967296, False, "large valid timestamp"),
            (-1, True, "negative timestamp"),
            (9223372036854775808, True, "extremely large timestamp"),
        ],
    )
    def test_timestamp_validation_comprehensive(self, timestamp_value, should_raise, description):
        """Test timestamp validation on CANTransportFrame construction."""
        if should_raise:
            with pytest.raises(ValueError):
                CANTransportFrame(
                    arbitration_id=0x7E0,
                    payload=b"\x22\xF1\x90",
                    start_timestamp_unix_us=timestamp_value,
                )
        else:
            frame = CANTransportFrame(
                arbitration_id=0x7E0,
                payload=b"\x22\xF1\x90",
                start_timestamp_unix_us=timestamp_value,
            )
            assert frame.start_timestamp_unix_us == timestamp_value

    @pytest.mark.parametrize(
        "can_id_value,should_raise,description",
        [
            (0, False, "minimum CAN ID"),
            (291, False, "normal standard ID"),
            (2047, False, "maximum standard ID"),
            (2048, False, "minimum extended ID"),
            (536870911, False, "maximum valid CAN ID"),
            (-1, True, "negative CAN ID"),
            (536870912, True, "CAN ID too large"),
        ],
    )
    def test_can_id_validation_comprehensive(self, can_id_value, should_raise, description):
        """Test CAN ID validation on CANTransportFrame construction."""
        if should_raise:
            with pytest.raises(ValueError):
                CANTransportFrame(
                    arbitration_id=can_id_value,
                    payload=b"\x22\xF1\x90",
                    start_timestamp_unix_us=1000000,
                )
        else:
            frame = CANTransportFrame(
                arbitration_id=can_id_value,
                payload=b"\x22\xF1\x90",
                start_timestamp_unix_us=1000000,
            )
            assert frame.arbitration_id == can_id_value
