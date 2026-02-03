"""
Tests for J1939 protocol detection.

This module tests J1939 specific detection logic including:
- PGN range detection
- High PF value handling
- Reserved bit checking
"""

import pytest

from ....protocols.j1939.detection import can_detect_j1939


@pytest.mark.parametrize(
    "arb_id,pgn_range",
    [
        # J1939 diagnostic PGN range
        (0x18F00000, "diagnostic"),  # Within diagnostic range
        (0x18F0FF00, "diagnostic"),  # Within diagnostic range
        # J1939 normal PGN range
        (0x18FE0000, "normal"),  # Within normal range
        (0x18FEFF00, "normal"),  # Within normal range
        # J1939 proprietary PGN range
        (0x18FF0000, "proprietary"),  # Within proprietary range
        (0x18FFFF00, "proprietary"),  # Within proprietary range
    ],
)
def test_j1939_pgn_range_detection(arb_id: int, pgn_range: str) -> None:
    """Test J1939 protocol detection for specific PGN ranges."""
    assert can_detect_j1939(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,expected_pgn",
    [
        (0x18010000, 0x0100),  # PF=0x01, PGN=0x0100 (standard range)
        (0x18050000, 0x0500),  # PF=0x05, PGN=0x0500 (standard range)
        (0x18800000, 0x8000),  # PF=0x80, PGN=0x8000 (standard range)
        (0x18DF0000, 0xDF00),  # PF=0xDF, PGN=0xDF00 (standard range)
    ],
)
def test_j1939_pgn_range_checks_specific(arb_id: int, expected_pgn: int) -> None:
    """Test specific J1939 PGN range checks that require precise conditions."""
    assert can_detect_j1939(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,expected_pgn",
    [
        (0x18010000, 0x0100),  # PF=0x01, PGN=0x0100 (standard range)
        (0x18EF0000, 0xEF00),  # PF=0xEF, PGN=0xEF00 (standard range)
    ],
)
def test_j1939_specific_pgn_ranges_low_pf(arb_id: int, expected_pgn: int) -> None:
    """Test J1939 PGN range detection for low PF values (< 240)."""
    assert can_detect_j1939(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,pgn_range,description",
    [
        # J1939_DIAGNOSTIC_PGN_MIN <= pgn <= J1939_DIAGNOSTIC_PGN_MAX (0xF000-0xF0FF)
        (0x18F00000, "diagnostic", "PF=0xF0, PGN=0xF000"),
        (0x18F0FF00, "diagnostic", "PF=0xF0, PGN=0xF0FF"),
        # J1939_NORMAL_PGN_MIN <= pgn <= J1939_NORMAL_PGN_MAX (0xFE00-0xFEFF)
        (0x18FE0000, "normal", "PF=0xFE, PGN=0xFE00"),
        (0x18FEFF00, "normal", "PF=0xFE, PGN=0xFEFF"),
        # J1939_PROPRIETARY_PGN_MIN <= pgn <= J1939_PROPRIETARY_PGN_MAX (0xFF00-0xFFFF)
        (0x18FF0000, "proprietary", "PF=0xFF, PGN=0xFF00"),
        (0x18FFFF00, "proprietary", "PF=0xFF, PGN=0xFFFF"),
    ],
)
def test_j1939_high_pf_pgn_ranges(arb_id: int, pgn_range: str, description: str) -> None:
    """Test J1939 PGN range detection for high PF values (>= 240)."""
    assert can_detect_j1939(arb_id) is True


def test_j1939_high_pf_catch_all() -> None:
    """Test J1939 catch-all for high PF values not in specific ranges."""
    # Use PF=0xF1 (241) which creates PGN outside all defined ranges
    arb_id = 0x18F10000  # PF=0xF1, PS=0x00, PGN=0xF100
    assert can_detect_j1939(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,should_detect",
    [
        # Non-J1939 IDs
        (0x7E0, False),  # Standard UDS request
        (0x100, False),  # Standard ID non-protocol
        # J1939 IDs
        (0x18FEF100, True),  # J1939 EEC1
        (0x18ECFF00, True),  # J1939 TP.CM
        (0x18FF0000, True),  # J1939 proprietary
    ],
)
def test_j1939_detection_accuracy(arb_id: int, should_detect: bool) -> None:
    """Test J1939 detection accuracy for both positive and negative cases."""
    assert can_detect_j1939(arb_id) is should_detect


def test_reserved_bit_handling() -> None:
    """Test that reserved bit detection is properly handled."""
    # Test with reserved bit set (bit 25)
    reserved_bit_id = 0x1A000000  # Has reserved bit set
    assert can_detect_j1939(reserved_bit_id) is False


@pytest.mark.parametrize(
    "can_id_range,protocol_expectation,description",
    [
        # Test ranges that should detect as J1939
        (range(0x18FE0000, 0x18FE0010, 1), "j1939_likely", "J1939 application range"),
        (range(0x1CEC0000, 0x1CEC0010, 1), "j1939_likely", "J1939 transport management"),
        (range(0x18F00000, 0x18F00010, 1), "j1939_likely", "J1939 diagnostic range"),
        (range(0x18FF0000, 0x18FF0010, 1), "j1939_likely", "J1939 proprietary range"),
    ],
)
def test_j1939_detection_ranges(can_id_range, protocol_expectation, description) -> None:
    """Test J1939 protocol detection across various CAN ID ranges."""
    for can_id in can_id_range:
        result = can_detect_j1939(can_id)
        assert isinstance(result, bool), f"Failed for {description} at ID {can_id:X}"
        if protocol_expectation == "j1939_likely":
            assert result is True, f"Expected J1939 detection for {description} at ID {can_id:X}"
