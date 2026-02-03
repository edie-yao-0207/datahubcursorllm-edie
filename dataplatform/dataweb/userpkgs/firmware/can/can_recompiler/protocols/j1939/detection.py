"""
J1939 Protocol Detection

This module contains J1939 specific protocol detection logic.
"""

from ...core.constants import (
    CAN_ID_MASK_29BIT,
    CAN_ID_STANDARD_FRAME_LIMIT,
    BYTE_MASK,
    CAN_BYTE_SHIFT,
)
from .constants import (
    J1939_PF_EXTRACTION_SHIFT,
    J1939_RESERVED_BIT_SHIFT,
    J1939_TP_ACK_PGN_MASK,
    J1939_TP_DT_PGN_MASK,
    J1939_TP_CM_PGN_MASK,
    J1939_DIAGNOSTIC_PGN_MIN,
    J1939_DIAGNOSTIC_PGN_MAX,
    J1939_NORMAL_PGN_MIN,
    J1939_NORMAL_PGN_MAX,
    J1939_PROPRIETARY_PGN_MIN,
    J1939_PROPRIETARY_PGN_MAX,
    J1939_STANDARD_PGN_MIN,
    J1939_STANDARD_PGN_MAX,
    J1939_PF_PS_THRESHOLD,
)


def can_detect_j1939(arbitration_id: int) -> bool:
    """
    Check if the arbitration ID can be processed as a J1939 frame.

    Args:
        arbitration_id: CAN frame arbitration ID

    Returns:
        True if the frame can be processed as J1939, False otherwise
    """
    arbitration_id = arbitration_id & CAN_ID_MASK_29BIT
    is_extended_id = arbitration_id > CAN_ID_STANDARD_FRAME_LIMIT

    # J1939 only uses 29-bit extended IDs
    if not is_extended_id:
        return False

    # === 29-bit extended identifiers ===
    reserved = (arbitration_id >> J1939_RESERVED_BIT_SHIFT) & 0x1
    pf = (arbitration_id >> J1939_PF_EXTRACTION_SHIFT) & BYTE_MASK
    ps = (arbitration_id >> CAN_BYTE_SHIFT) & BYTE_MASK
    sa = arbitration_id & BYTE_MASK
    pgn = (pf << CAN_BYTE_SHIFT) | (ps if pf >= J1939_PF_PS_THRESHOLD else 0x00)

    # Basic validation - reserved bit must be 0
    if reserved != 0:
        return False

    # === J1939 validation ===
    # Check J1939 PGN ranges
    if J1939_DIAGNOSTIC_PGN_MIN <= pgn <= J1939_DIAGNOSTIC_PGN_MAX:
        return True
    if J1939_NORMAL_PGN_MIN <= pgn <= J1939_NORMAL_PGN_MAX:
        return True
    if J1939_PROPRIETARY_PGN_MIN <= pgn <= J1939_PROPRIETARY_PGN_MAX:
        return True
    if pgn in {J1939_TP_CM_PGN_MASK, J1939_TP_DT_PGN_MASK, J1939_TP_ACK_PGN_MASK}:
        return True
    if J1939_STANDARD_PGN_MIN <= pgn <= J1939_STANDARD_PGN_MAX:
        return True

    # Check if it's a J1939 message (catch-all for high PF values)
    if pf >= J1939_PF_PS_THRESHOLD:
        return True

    return False
