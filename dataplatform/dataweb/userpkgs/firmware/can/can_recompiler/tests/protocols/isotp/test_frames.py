"""
Tests for ISO-TP transport frames.

This module contains tests for ISO-TP frame creation and properties.
"""

import pytest
from typing import Dict, Any

from ....protocols.isotp.frames import ISOTPFrame

TRANSPORT_FRAME_TEST_CASES = [
    pytest.param(
        {
            "source_address": 0x7E0,
            "payload": b"\x22\xF1\x90",
            "expected_transport": "ISO-TP",
        },
        id="isotp_standard_addressing",
    ),
    pytest.param(
        {
            "source_address": 0x18DA0F01,
            "payload": b"\x22\xF1\x90",  # Normal addressing with 29-bit CAN ID
            "expected_transport": "ISO-TP",
        },
        id="isotp_extended_can_id",
    ),
]


class TestISOTPFrames:
    """Test ISO-TP transport frame creation and properties."""

    @pytest.mark.parametrize("test_case", TRANSPORT_FRAME_TEST_CASES)
    def test_isotp_frame_creation(self, test_case: Dict[str, Any]):
        """Test ISO-TP frame creation with different addressing modes."""
        # Create ISO-TP frame
        isotp_frame = ISOTPFrame(
            source_address=test_case["source_address"],
            payload=test_case["payload"],
            start_timestamp_unix_us=1000000,
            frame_count=1,
            expected_length=len(test_case["payload"]),
        )

        # Verify ISO-TP frame properties
        assert isotp_frame.source_address == test_case["source_address"]
        assert isotp_frame.payload == test_case["payload"]
        assert isotp_frame.expected_length == len(test_case["payload"])

    def test_isotp_frame_to_dict(self):
        """Test ISO-TP frame to_dict conversion."""
        isotp_frame = ISOTPFrame(
            source_address=0x7E0,
            payload=b"\x22\xF1\x90",
            start_timestamp_unix_us=1000000,
            frame_count=1,
            expected_length=3,
        )

        # Convert to output dictionary using ISO-TP adapter
        from userpkgs.firmware.can.can_recompiler.infra.spark.adapters import ISOTPFrameSparkAdapter

        adapter = ISOTPFrameSparkAdapter(isotp_frame)
        output_dict = adapter.to_spark_dict()

        # Verify transport-level structure (no nested metadata/application)
        assert "source_address" in output_dict
        assert "payload" in output_dict
        assert "start_timestamp_unix_us" in output_dict

        # Verify transport-level data
        assert output_dict["source_address"] == 0x7E0
        assert output_dict["payload"] == b"\x22\xF1\x90"
        assert output_dict["start_timestamp_unix_us"] == 1000000
        assert output_dict["frame_count"] == 1
        assert output_dict["expected_length"] == 3
