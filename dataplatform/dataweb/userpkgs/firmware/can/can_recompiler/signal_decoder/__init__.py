"""
CAN Signal Decoder

Utilities for decoding signal values from CAN frame payloads.
Provides bit-level signal extraction with proper endianness and signedness handling.
"""

from .decoder import (
    decode_single_signal_from_payload,
    decode_signals_batch,
    calculate_normalized_bit_position,
    extract_byte_segment,
    extract_signal_value,
    apply_signal_sign,
)

from .enums import EndiannessType, SignType, DecodeReasonType

__all__ = [
    "decode_single_signal_from_payload",
    "decode_signals_batch",
    "calculate_normalized_bit_position",
    "extract_byte_segment",
    "extract_signal_value",
    "apply_signal_sign",
    "EndiannessType",
    "SignType",
    "DecodeReasonType",
]
