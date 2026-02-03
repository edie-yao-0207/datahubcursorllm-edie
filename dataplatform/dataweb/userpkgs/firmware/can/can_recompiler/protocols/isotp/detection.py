"""
ISO-TP Protocol Detection

This module contains ISO-TP specific protocol detection logic.
"""

from ...core.constants import CAN_ID_MASK_29BIT, CAN_ID_STANDARD_FRAME_LIMIT
from .constants import (
    UDS_REQUEST_ADDR_MIN,
    UDS_REQUEST_ADDR_MAX,
    UDS_BROADCAST_ADDR,
    UDS_RESPONSE_ADDR_MIN,
    UDS_RESPONSE_ADDR_MAX,
    UDS_EXTENDED_REQUEST_MIN,
    UDS_EXTENDED_REQUEST_MAX,
    UDS_EXTENDED_RESPONSE_MIN,
    UDS_EXTENDED_RESPONSE_MAX,
    UDS_EXTENDED_RANGE_MIN,
    UDS_EXTENDED_RANGE_MAX,
)


def can_detect_isotp(arbitration_id: int) -> bool:
    """
    Check if the arbitration ID can be processed as an ISO-TP frame.

    Args:
        arbitration_id: CAN frame arbitration ID

    Returns:
        True if the frame can be processed as ISO-TP, False otherwise
    """
    arbitration_id = arbitration_id & CAN_ID_MASK_29BIT
    is_extended_id = arbitration_id > CAN_ID_STANDARD_FRAME_LIMIT

    if not is_extended_id:
        # === 11-bit standard identifiers ===
        # Apply ChatGPT's ISO-TP classification logic for 11-bit IDs

        # Standard OBD-II ranges
        if arbitration_id == UDS_BROADCAST_ADDR:  # 0x7DF
            return True  # Functional broadcast request
        if UDS_REQUEST_ADDR_MIN <= arbitration_id <= UDS_REQUEST_ADDR_MAX:  # 0x7E0-0x7E7
            return True  # Physical request (tester→ECU)
        if UDS_RESPONSE_ADDR_MIN <= arbitration_id <= UDS_RESPONSE_ADDR_MAX:  # 0x7E8-0x7EF
            return True  # Physical response (ECU→tester)

        # Extended diagnostic ranges (covers user's 0x797 case)
        if UDS_EXTENDED_REQUEST_MIN <= arbitration_id <= UDS_EXTENDED_REQUEST_MAX:
            return True  # Extended diagnostic request
        if UDS_EXTENDED_RESPONSE_MIN <= arbitration_id <= UDS_EXTENDED_RESPONSE_MAX:
            return True  # Extended diagnostic response

        # Note: Other 11-bit IDs (like 0x481, 0x4C0) require OEM-specific mapping
        # and are likely broadcast/proprietary frames, not ISO-TP
        return False

    # === 29-bit extended identifiers ===
    # Apply ChatGPT's ISO-TP classification logic for 29-bit IDs
    # Extract ISO-TP addressing fields
    pf = (arbitration_id >> 16) & 0xFF  # Parameter Format
    ps = (arbitration_id >> 8) & 0xFF  # Parameter Specific
    sa = arbitration_id & 0xFF  # Source Address

    # === Classic UDS (ISO-15765 "DA" mapping) ===
    if pf == 0xDA:  # PDU1 format - PS is destination address
        # All PF=0xDA frames are UDS diagnostic (not just tester-related)
        # This covers physical addressing for UDS over CAN
        return True  # Classic UDS diagnostic traffic

    elif pf == 0xDB:  # Functional addressing
        if sa == 0xF1:  # Tester as source
            return True  # Functional UDS request

    # === WWH-OBD/J1939-style UDS ===
    elif pf in [0xFC, 0xFD, 0xFE]:  # PDU2 format - PS is group extension
        # Be more specific: only certain priority ranges are WWH-OBD, not J1939
        # User's confirmed WWH-OBD: 0x17FE**** (priority=5, PF=0xFE)
        # J1939 EEC1: 0x18FEF100 (priority=6, PF=0xFE) - should NOT be detected
        priority = (arbitration_id >> 26) & 0x7  # Extract 3-bit priority

        # WWH-OBD typically uses priority 5 (0x17FE****), not priority 6 (0x18FE****)
        if priority == 5:  # 0x17FE**** range confirmed as WWH-OBD by user data
            return True  # WWH-OBD diagnostic traffic
        # Could add other confirmed priority ranges here if needed
        # For now, be conservative and only accept priority 5

    # === Legacy range check for backward compatibility ===
    elif UDS_EXTENDED_RANGE_MIN <= arbitration_id <= UDS_EXTENDED_RANGE_MAX:
        return True  # Fallback for any 18DA**** not caught above

    return False
