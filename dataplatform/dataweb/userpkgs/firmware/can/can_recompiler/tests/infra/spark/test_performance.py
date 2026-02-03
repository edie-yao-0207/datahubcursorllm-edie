"""
Performance tests for protocol processing.

This module contains performance benchmarks for protocol processing operations.
"""

import pytest
import time

from ....protocols.uds.frames import UDSMessage
from ....protocols.isotp.frames import ISOTPFrame
from ....infra.spark.frame_types import PartitionMetadata
from ....protocols.uds.enums import UDSDataIdentifierType


@pytest.fixture
def sample_metadata():
    """Sample partition metadata for testing."""
    return PartitionMetadata(
        trace_uuid="test-uuid-123",
        date="2024-01-15",
        org_id=42,
        device_id=1001,
        source_interface="can0",
        direction=1,
    )


@pytest.fixture
def sample_isotp_frame(sample_metadata):
    """Sample ISO-TP frame for testing."""
    return ISOTPFrame(
        source_address=0x7E0,
        payload=b"\x22\xF1\x90",
        start_timestamp_unix_us=1000000,
        frame_count=1,
        expected_length=3,
    )


class TestProtocolPerformance:
    """Performance tests for protocol-specific operations."""

    @pytest.mark.parametrize("message_count", [1, 10, 100, 1000])
    def test_message_conversion_performance(
        self, message_count: int, sample_isotp_frame: ISOTPFrame
    ):
        """Test performance of message conversion at different scales."""
        # Create test messages
        messages = []
        for i in range(message_count):
            # Create transport frame for performance test
            transport_frame = ISOTPFrame(
                source_address=0x7E0 + i,  # Vary the address for different messages
                payload=b"\x22" + (0xF190 + i).to_bytes(2, "big"),
                start_timestamp_unix_us=1000000 + i,
            )

            messages.append(UDSMessage(transport_frame=transport_frame))

        # Time the conversions using adapters
        from userpkgs.firmware.can.can_recompiler.infra.spark.adapters import UDSMessageSparkAdapter

        start_time = time.time()
        for message in messages:
            adapter = UDSMessageSparkAdapter(message, sample_isotp_frame)
            adapter.to_consolidated_dict()
        end_time = time.time()

        # Performance assertions (adjust thresholds as needed)
        total_time = end_time - start_time
        time_per_message = total_time / message_count

        # Should be able to process at least 1000 messages per second
        assert time_per_message < 0.001, f"Conversion too slow: {time_per_message:.6f}s per message"
