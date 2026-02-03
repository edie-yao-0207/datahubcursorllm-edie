"""
Tests for constants.py module.

This module contains tests to verify that constants are correctly defined
and used throughout the package.
"""

from ...core.constants import (
    # CAN frame constants
    CAN_FRAME_MAX_DATA_LENGTH,
    CAN_BYTE_SHIFT,
    CAN_ID_STANDARD_FRAME_LIMIT,
    CAN_ID_MASK_29BIT,
    BYTE_MASK,
    # Common constants
    MAX_CONCURRENT_SESSIONS,
    SESSION_CLEANUP_INTERVAL,
)

# Import protocol-specific constants from their packages
from ...protocols.isotp.constants import (
    ISOTP_SINGLE_FRAME_MAX_LENGTH,
    ISOTP_SINGLE_FRAME_PRACTICAL_MAX,
    ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH,
    ISOTP_SESSION_TIMEOUT_US,
)

from ...protocols.j1939.constants import (
    J1939_BAM_TIMEOUT_US,
    J1939_SESSION_TIMEOUT_US,
    J1939_MAX_PACKET_COUNT,
    J1939_MAX_TOTAL_SIZE,
    J1939_BROADCAST_DESTINATION,
    J1939_INITIAL_SEQUENCE_NUMBER,
    J1939_TP_DT_SEQUENCE_NUMBER_MIN,
    J1939_TP_DT_SEQUENCE_NUMBER_MAX,
)


def test_constants_usage() -> None:
    """Test that constants are correctly defined."""
    # Test CAN frame constants
    assert CAN_FRAME_MAX_DATA_LENGTH == 8
    assert CAN_BYTE_SHIFT == 8
    assert CAN_ID_STANDARD_FRAME_LIMIT == 0x7FF
    assert CAN_ID_MASK_29BIT == 0x1FFFFFFF
    assert BYTE_MASK == 0xFF

    # Test ISO-TP constants
    assert ISOTP_SINGLE_FRAME_MAX_LENGTH == 15
    assert ISOTP_SINGLE_FRAME_PRACTICAL_MAX == 7
    assert ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH == 4095
    assert ISOTP_SESSION_TIMEOUT_US == 2_000_000  # 2 seconds

    # Test J1939 constants
    assert J1939_BAM_TIMEOUT_US == 750_000  # 750ms
    assert J1939_SESSION_TIMEOUT_US == 2_000_000  # 2 seconds
    assert J1939_MAX_PACKET_COUNT == 255
    assert J1939_MAX_TOTAL_SIZE == 65535
    assert J1939_BROADCAST_DESTINATION == 0xFF
    assert J1939_INITIAL_SEQUENCE_NUMBER == 1
    assert J1939_TP_DT_SEQUENCE_NUMBER_MIN == 1
    assert J1939_TP_DT_SEQUENCE_NUMBER_MAX == 255

    # Test common constants
    assert MAX_CONCURRENT_SESSIONS == 1000
    assert SESSION_CLEANUP_INTERVAL == 100

    # Test relationships between constants
    assert CAN_ID_STANDARD_FRAME_LIMIT < CAN_ID_MASK_29BIT
    assert ISOTP_SINGLE_FRAME_PRACTICAL_MAX < CAN_FRAME_MAX_DATA_LENGTH
    assert J1939_TP_DT_SEQUENCE_NUMBER_MIN < J1939_TP_DT_SEQUENCE_NUMBER_MAX
    assert J1939_MAX_TOTAL_SIZE > ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH
