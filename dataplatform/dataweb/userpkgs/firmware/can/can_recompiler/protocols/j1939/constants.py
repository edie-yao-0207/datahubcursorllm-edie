"""
J1939 Protocol Constants

Constants specific to J1939 Transport Protocol implementation for heavy-duty
vehicle communications, including PGN ranges, frame sizes, and timeouts.
"""

# === J1939 Transport Protocol PGN Masks ===
J1939_TP_ACK_PGN_MASK = 0xEA00  # Transport Protocol Acknowledgment
J1939_TP_DT_PGN_MASK = 0xEB00  # Transport Protocol Data Transfer
J1939_TP_CM_PGN_MASK = 0xEC00  # Transport Protocol Connection Management

# Alternative naming for direct PGN usage (same values)
J1939_TP_CM_PGN = 0xEC00  # Transport Protocol Connection Management
J1939_TP_DT_PGN = 0xEB00  # Transport Protocol Data Transfer
J1939_TP_ACK_PGN = 0xEA00  # Transport Protocol Acknowledgment

# === J1939 Transport Protocol Control Bytes ===
J1939_TP_CM_RTS_CONTROL_BYTE = 16  # Request to Send control byte (point-to-point)
J1939_TP_CM_BAM_CONTROL_BYTE = 32  # Broadcast Announce Message control byte (broadcast)

# Additional TP.CM Control Bytes
J1939_TP_CM_RTS = 16  # Request To Send
J1939_TP_CM_CTS = 17  # Clear To Send
J1939_TP_CM_ENDOFMSGACK = 19  # End of Message Acknowledgment
J1939_TP_CM_BAM = 32  # Broadcast Announce Message
J1939_TP_CM_ABORT = 255  # Connection Abort

# === J1939 TP.ACK Control Bytes ===
J1939_TP_ACK_ACK = 0  # Acknowledgment
J1939_TP_ACK_NACK = 1  # Negative Acknowledgment

# === J1939 Frame Size Limits ===
J1939_MAX_SINGLE_FRAME_SIZE = 8

# === J1939 PGN Ranges ===
J1939_STANDARD_PGN_MIN = 0x0001  # Standard J1939 PGN range lower bound
J1939_STANDARD_PGN_MAX = 0xEFFF  # Standard J1939 PGN range upper bound
J1939_DIAGNOSTIC_PGN_MIN = 0xF000  # J1939-73 Diagnostic PGNs
J1939_DIAGNOSTIC_PGN_MAX = 0xF0FF
J1939_NORMAL_PGN_MIN = 0xFE00  # J1939-71 Normal PGNs
J1939_NORMAL_PGN_MAX = 0xFEFF
J1939_PROPRIETARY_PGN_MIN = 0xFF00  # Proprietary PGNs
J1939_PROPRIETARY_PGN_MAX = 0xFFFF

# === J1939 PGN Processing ===
J1939_PGN_HIGH_BYTE_MASK = 0xFF00  # Upper byte of PGN for classification
J1939_PF_PS_THRESHOLD = 240  # PF value threshold for PS inclusion in PGN
J1939_BROADCAST_DESTINATION = 0xFF  # Global broadcast destination address

# === J1939 Session Management ===
J1939_INITIAL_SEQUENCE_NUMBER = 1  # Starting sequence number for multi-packet
# Import shared timeout from core constants to avoid duplication
from ...core.constants import SESSION_TIMEOUT_US

J1939_SESSION_TIMEOUT_US = SESSION_TIMEOUT_US  # 2 second timeout in microseconds

# === J1939 BAM (Broadcast Announce Message) ===
J1939_BAM_TIMEOUT_US = 750_000  # 750ms timeout for BAM sessions (stricter than RTS)
J1939_BAM_PACKET_INTERVAL_US = 50_000  # 50ms minimum interval between BAM packets

# === J1939 Frame Size Limits ===
J1939_TP_CM_FRAME_MIN_SIZE = 8  # Minimum TP.CM frame size in bytes
J1939_TP_CM_FRAME_REQUIRED_SIZE = 8  # TP.CM frames must be exactly 8 bytes
J1939_TP_DT_FRAME_MIN_SIZE = 2  # Minimum TP.DT frame size in bytes (seq + 1 data byte)
J1939_TP_DT_FRAME_MAX_PAYLOAD = 7  # Maximum payload per TP.DT packet (8-byte frame - 1 seq byte)
J1939_SINGLE_FRAME_MAX_PAYLOAD = 8  # Maximum payload for single-frame J1939 messages

# === J1939 Message Size Limits ===
J1939_MAX_TOTAL_SIZE = 65535  # Maximum total message size (16-bit field)
J1939_MIN_TOTAL_SIZE = 9  # Minimum size requiring multi-packet (> single frame)
J1939_MAX_PACKET_COUNT = 255  # Maximum packet count (8-bit field)
J1939_MIN_PACKET_COUNT = 1  # Minimum packet count for valid TP.CM

# === J1939 TP.DT Sequence Numbers ===
J1939_TP_DT_SEQUENCE_NUMBER_MIN = 1  # Minimum valid TP.DT sequence number
J1939_TP_DT_SEQUENCE_NUMBER_MAX = 255  # Maximum valid TP.DT sequence number

# === J1939 TP.CM Frame Layout ===
J1939_TP_CM_CONTROL_BYTE_POS = 0  # Control byte position
J1939_TP_CM_TOTAL_SIZE_LOW_POS = 1  # Total size low byte position
J1939_TP_CM_TOTAL_SIZE_HIGH_POS = 2  # Total size high byte position
J1939_TP_CM_PACKET_COUNT_POS = 3  # Packet count position
J1939_TP_CM_PGN_LOW_POS = 5  # PGN low byte position
J1939_TP_CM_PGN_MID_POS = 6  # PGN middle byte position
J1939_TP_CM_PGN_HIGH_POS = 7  # PGN high byte position

# === J1939 TP.DT Frame Layout ===
J1939_TP_DT_SEQUENCE_POS = 0  # Sequence number position
J1939_TP_DT_PAYLOAD_OFFSET = 1  # Payload starts at byte 1

# === J1939 Arbitration ID Processing ===
J1939_PF_EXTRACTION_SHIFT = 16  # Extract Parameter Format (PF) byte from J1939 arbitration ID
J1939_RESERVED_BIT_SHIFT = 25  # Extract reserved bit from J1939 arbitration ID (bit 25)

# === J1939 Application PGN Ranges (merged from applications/j1939) ===
J1939_APP_STANDARD_PGN_MIN = 0x0001  # Standard J1939 PGN range lower bound
J1939_APP_STANDARD_PGN_MAX = 0xEFFF  # Standard J1939 PGN range upper bound
J1939_APP_DIAGNOSTIC_PGN_MIN = 0xF000  # J1939-73 Diagnostic PGNs
J1939_APP_DIAGNOSTIC_PGN_MAX = 0xF0FF
J1939_APP_NORMAL_PGN_MIN = 0xFE00  # J1939-71 Normal PGNs
J1939_APP_NORMAL_PGN_MAX = 0xFEFF
J1939_APP_PROPRIETARY_PGN_MIN = 0xFF00  # Proprietary PGNs
J1939_APP_PROPRIETARY_PGN_MAX = 0xFFFF

# === J1939 PGN Classification Constants ===

# Transport Protocol range bounds
J1939_TRANSPORT_PGN_MIN = 0xEA00  # Transport Protocol range start
J1939_TRANSPORT_PGN_MAX = 0xEC00  # Transport Protocol range end

# Standard J1939 range (below transport protocols)
J1939_STANDARD_DATA_PGN_MIN = 0x0001  # Standard J1939 data range start
J1939_STANDARD_DATA_PGN_MAX = 0xDFFF  # Standard J1939 data range end (before transport protocols)

# Diagnostic Message (DM) PGNs - specific known DM messages
J1939_DM_MESSAGE_PGNS = {
    # Standard DM messages in J1939-71 range
    65226,  # 0xFECA - DM1: Active Diagnostic Trouble Codes
    65227,  # 0xFECB - DM2: Previously Active Diagnostic Trouble Codes
    65228,  # 0xFECC - DM3: Clear/Reset Previously Active DTCs (also in command set)
    65229,  # 0xFECD - DM4: Freeze Frame Parameters
    65230,  # 0xFECE - DM5: Diagnostic Readiness 1
    65231,  # 0xFECF - DM6: Pending Diagnostic Trouble Codes
    65232,  # 0xFED0 - DM8: Test Results for Non-continuously Monitored Test
    65235,  # 0xFED3 - DM11: Clear/Reset Active DTCs (also in command set)
    65236,  # 0xFED4 - DM12: Emissions-related Active DTCs
    65237,  # 0xFED5 - DM14: Memory Access Response
    65238,  # 0xFED6 - DM15: Memory Access Response
    65239,  # 0xFED7 - DM16: Binary Data Transfer
    65240,  # 0xFED8 - DM17: Memory Access Request
    65241,  # 0xFED9 - DM18: Memory Access Request
    65242,  # 0xFEDA - DM19: Calibration Information
    65243,  # 0xFEDB - DM20: Monitor Performance Ratio
    65244,  # 0xFEDC - DM21: Diagnostic Readiness 2
    65245,  # 0xFEDD - DM22: Individual Clear/Reset of Active and Previously Active DTCs
    65246,  # 0xFEDE - DM23: Emissions-related Previously Active DTCs
    65247,  # 0xFEDF - DM24: SPN Support
    65248,  # 0xFEE0 - DM25: Expanded Freeze Frame
    65249,  # 0xFEE1 - DM26: Diagnostic Readiness 3
    65250,  # 0xFEE2 - DM27: All Pending DTCs
    65251,  # 0xFEE3 - DM28: Permanent DTCs
    65252,  # 0xFEE4 - DM29: Regulated DTC Counts
    65253,  # 0xFEE5 - DM30: Scaled Test Results
    65254,  # 0xFEE6 - DM31: DTC to Lamp Association
    65256,  # 0xFEE8 - DM33: Emissions-related Active DTCs for WWH-OBD vehicles
    65257,  # 0xFEE9 - DM34: Emissions-related Previously Active DTCs for WWH-OBD
    65258,  # 0xFEEA - DM35: Emissions-related Permanent DTCs for WWH-OBD
    # DM messages in other ranges
    57344,  # 0xE000 - DM7: Command Non-continuously Monitored Test (also in command set)
    57088,  # 0xDF00 - DM13: Stop Start Broadcast
}
