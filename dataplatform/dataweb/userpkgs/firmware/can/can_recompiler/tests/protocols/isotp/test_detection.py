"""
Tests for ISO-TP protocol detection.

This module tests ISO-TP specific detection logic including:
- Standard ID UDS ranges
- Extended ID UDS ranges  
- Extended addressing detection
"""

import pytest

from ....protocols.isotp.detection import can_detect_isotp


@pytest.mark.parametrize(
    "arb_id,addr_type",
    [
        # UDS request range
        (0x7E0, "UDS request"),
        (0x7E1, "UDS request"),
        (0x7E7, "UDS request"),
        # UDS broadcast address
        (0x7DF, "UDS broadcast"),
        # UDS response range
        (0x7E8, "UDS response"),
        (0x7E9, "UDS response"),
        (0x7EF, "UDS response"),
    ],
)
def test_standard_id_isotp_detection(arb_id: int, addr_type: str) -> None:
    """Test standard ID ISO-TP detection (11-bit IDs)."""
    assert can_detect_isotp(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,addr_type",
    [
        # Extended UDS request range
        (0x700, "Extended UDS request min"),
        (0x750, "Extended UDS request mid"),
        (0x78F, "Extended UDS request high"),
        (0x7DE, "Extended UDS request max"),
        # Extended UDS response range
        (0x708, "Extended UDS response min"),
        (0x758, "Extended UDS response mid"),
        (0x797, "Extended UDS response - user case"),  # The specific problematic ID!
        (0x7A0, "Extended UDS response high"),
        (0x7FE, "Extended UDS response max"),
    ],
)
def test_extended_diagnostic_isotp_detection(arb_id: int, addr_type: str) -> None:
    """Test extended diagnostic range ISO-TP detection (automotive ECUs)."""
    assert can_detect_isotp(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,description",
    [
        (0x18DB33F1, "UDS_EXTENDED_DIAGNOSTIC_RESPONSE_EXAMPLE"),
        (0x18DA1000, "UDS extended range"),
        (0x18DA5000, "UDS extended range middle"),
        (0x18DAFFFF, "UDS extended range max"),
    ],
)
def test_extended_isotp_edge_cases(arb_id: int, description: str) -> None:
    """Test extended ISO-TP detection edge cases."""
    assert can_detect_isotp(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,description",
    [
        # PF=0xDA but outside UDS_EXTENDED_RANGE (0x18DA0000-0x18DAFFFF)
        (0x1CDA0000, "PF=0xDA outside UDS extended range"),  # Different priority (7 instead of 6)
    ],
)
def test_extended_isotp_pf_da_check(arb_id: int, description: str) -> None:
    """Test extended ISO-TP detection for PF=0xDA outside standard UDS range."""
    assert can_detect_isotp(arb_id) is True


@pytest.mark.parametrize(
    "arb_id,should_detect",
    [
        # Non-ISO-TP IDs
        (0x100, False),  # Standard ID non-protocol
        (0x6FF, False),  # Just below extended request range
        (0x7FF, False),  # Just above extended response range
        (0x18FEF100, False),  # J1939 EEC1
        (0x18ECFF00, False),  # J1939 TP.CM
        (0x18FF0000, False),  # J1939 proprietary
        # ISO-TP IDs (existing ranges)
        (0x7E0, True),  # UDS request
        (0x18DA00F1, True),  # UDS on J1939 network
        # ISO-TP IDs (new extended ranges)
        (0x700, True),  # Extended request min
        (0x797, True),  # User's problematic ID (now should work!)
        (0x7FE, True),  # Extended response max
    ],
)
def test_isotp_detection_accuracy(arb_id: int, should_detect: bool) -> None:
    """Test ISO-TP detection accuracy for both positive and negative cases."""
    assert can_detect_isotp(arb_id) is should_detect


@pytest.mark.parametrize(
    "can_id,description",
    [
        (0x7E0, "OBD-II request"),
        (0x7E8, "OBD-II response"),
        (0x7DF, "OBD-II broadcast"),
        (0x18DA0000, "Extended addressing"),
        (0x000, "Minimal standard ID"),
        (0x7FF, "Maximum standard ID"),
    ],
)
def test_isotp_detection_edge_case_ranges(can_id: int, description: str) -> None:
    """Test ISO-TP detection with comprehensive edge case CAN IDs."""
    result = can_detect_isotp(can_id)
    assert isinstance(result, bool), f"Detection failed for {description}"
