"""
CAN Signal Decoding Utilities

This module provides utilities for extracting signal values from CAN frame payloads
using bit-level manipulation with proper endianness and signedness handling.
"""

from typing import Optional


def calculate_normalized_bit_position(bit_start: int, endianness: int) -> int:
    """
    Calculate normalized bit position based on endianness.

    Args:
        bit_start: Starting bit position in signal definition
        endianness: Endianness type (1=LITTLE, 2=BIG)

    Returns:
        Normalized bit start position

    Raises:
        ValueError: If endianness is not supported
    """
    byte_index = bit_start // 8
    bit_in_byte = bit_start % 8

    # Use integer constants to avoid enum serialization issues
    # EndiannessType.BIG = 2, EndiannessType.LITTLE = 1
    if endianness == 2:  # EndiannessType.BIG
        return byte_index * 8 + (7 - bit_in_byte)
    elif endianness == 1:  # EndiannessType.LITTLE
        return byte_index * 8 + bit_in_byte
    else:
        raise ValueError(f"Unsupported endianness value: {endianness} (expected 1=LITTLE or 2=BIG)")


def extract_byte_segment(payload: bytes, normalized_start: int, normalized_end: int) -> bytes:
    """
    Extract relevant byte segment from payload.

    Args:
        payload: Binary payload data
        normalized_start: Normalized starting bit position
        normalized_end: Normalized ending bit position

    Returns:
        Byte segment containing the signal data

    Raises:
        ValueError: If segment is empty or bounds are invalid
    """
    if normalized_start < 0 or normalized_end < normalized_start:
        raise ValueError(f"Invalid bit range: start={normalized_start}, end={normalized_end}")

    start_byte = normalized_start // 8
    end_byte = normalized_end // 8

    if end_byte >= len(payload):
        raise ValueError(
            f"Bit range exceeds payload: end_byte={end_byte}, payload_len={len(payload)}"
        )

    segment = payload[start_byte : end_byte + 1]

    if len(segment) == 0:
        raise ValueError(
            f"Empty segment: start_byte={start_byte}, end_byte={end_byte}, "
            f"payload={payload.hex()}"
        )

    return segment


def extract_signal_value(
    segment: bytes, normalized_start: int, bit_length: int, endianness: int
) -> int:
    """
    Extract integer value from byte segment.

    Args:
        segment: Byte segment containing signal data
        normalized_start: Normalized starting bit position
        bit_length: Length of signal in bits
        endianness: Endianness type for byte order (1=LITTLE, 2=BIG)

    Returns:
        Extracted unsigned integer value

    Raises:
        ValueError: If bit shifting calculations are invalid
    """
    if bit_length <= 0 or bit_length > 64:
        raise ValueError(f"Invalid bit_length: {bit_length} (must be 1-64)")

    # Convert bytes to integer using appropriate byte order
    # Use integer constant to avoid enum serialization issues (EndiannessType.BIG = 2)
    byte_order = "big" if endianness == 2 else "little"
    val = int.from_bytes(segment, byteorder=byte_order)

    # Calculate bit offset within the segment
    bit_offset = normalized_start % 8

    # Calculate shift - same for both endiannesses, byte order handles the difference
    shift = len(segment) * 8 - bit_length - bit_offset

    if shift < 0:
        raise ValueError(
            f"Negative shift calculated: shift={shift}, segment_len={len(segment)}, "
            f"bit_offset={bit_offset}, bit_length={bit_length}"
        )

    # Extract the signal bits using bit masking
    return (val >> shift) & ((1 << bit_length) - 1)


def apply_signal_sign(value: int, bit_length: int, sign_type: int) -> int:
    """
    Apply signedness to extracted signal value.

    Args:
        value: Unsigned integer value
        bit_length: Length of signal in bits
        sign_type: Sign type (1=SIGNED, 2=UNSIGNED)

    Returns:
        Signed integer value if sign_type is SIGNED, otherwise unsigned
    """
    # Use integer constant to avoid enum serialization issues (SignType.SIGNED = 1)
    if sign_type == 1 and value & (1 << (bit_length - 1)):  # SignType.SIGNED
        return value - (1 << bit_length)
    return value


def decode_single_signal_from_payload(
    payload: Optional[bytes],
    bit_start: Optional[int],
    bit_length: Optional[int],
    endianness: Optional[int],
    sign_type: Optional[int],
) -> Optional[float]:
    """
    Decode a single signal value from CAN payload bytes.

    Args:
        payload: Binary payload data
        bit_start: Starting bit position of signal
        bit_length: Length of signal in bits
        endianness: Endianness type (1=LITTLE, 2=BIG)
        sign_type: Sign type (1=SIGNED, 2=UNSIGNED)

    Returns:
        Decoded signal value as float, or None if decoding fails

    Raises:
        ValueError: If parameters are invalid
        IndexError: If bit range is out of bounds
    """
    # Validate inputs
    if payload is None:
        return None

    if bit_start is None or bit_length is None:
        return None

    if endianness is None or sign_type is None:
        return None

    # Convert to integers and validate
    try:
        bit_start_int = int(bit_start)
        bit_length_int = int(bit_length)
        endianness_int = int(endianness)
        sign_type_int = int(sign_type)
    except (ValueError, TypeError):
        return None

    if bit_start_int < 0 or bit_length_int <= 0 or bit_length_int > 64:
        return None

    if endianness_int not in (1, 2) or sign_type_int not in (1, 2):
        return None

    # Validate bit bounds
    total_bits = len(payload) * 8
    if bit_start_int >= total_bits:
        return None

    try:
        # Calculate normalized bit positions
        normalized_start = calculate_normalized_bit_position(bit_start_int, endianness_int)
        normalized_end = normalized_start + bit_length_int - 1

        # Validate bit range
        if normalized_start < 0 or normalized_end >= total_bits:
            return None

        # Extract byte segment
        segment = extract_byte_segment(payload, normalized_start, normalized_end)

        # Extract signal value
        unsigned_value = extract_signal_value(
            segment, normalized_start, bit_length_int, endianness_int
        )

        # Apply signedness
        signed_value = apply_signal_sign(unsigned_value, bit_length_int, sign_type_int)

        return float(signed_value)

    except (ValueError, IndexError):
        # Return None for any decoding errors to match SQL UDF behavior
        return None


def decode_signals_batch(iterator):
    """
    Decode CAN signals for a batch of frames using mapInPandas.

    This function is designed to be used directly with Spark's mapInPandas
    without any external dependencies beyond pandas, making it serializable to workers.

    Args:
        iterator: Iterator of pandas DataFrames (from mapInPandas)

    Expected input DataFrame columns:
    - payload: bytes
    - payload_length: int
    - rules_array: list of dicts with signal definitions
    - All frame identity columns (preserved in output)

    Yields:
        DataFrame with added 'decoded_signals' column containing array of decoded signal dicts

    Note: Uses integer literals for enums to avoid serialization issues:
        - decode_reason: 0=OK, 1=INVALID_BIT_LENGTH, 4=PAYLOAD_TOO_SMALL
        - endian: 1=LITTLE, 2=BIG
        - sign: 1=SIGNED, 2=UNSIGNED
    """
    import pandas as pd

    # Process each batch of rows from the iterator
    for pdf in iterator:
        decoded_signals_list = []

        for idx, row in pdf.iterrows():
            payload = row.get("payload")
            length = row.get("payload_length")
            rules = row.get("rules_array")

            if payload is None or rules is None or not rules:
                decoded_signals_list.append([])
                continue

            decoded_signals = []

            # Trim payload to actual length
            trimmed_payload = payload[:length] if length and length > 0 else payload

            for rule in rules:
                try:
                    # Use existing decoder function
                    raw_value = decode_single_signal_from_payload(
                        trimmed_payload,
                        rule.get("bit_start"),
                        rule.get("bit_length"),
                        rule.get("endian"),
                        rule.get("sign"),
                    )

                    # Determine decode success
                    can_apply = raw_value is not None
                    # Use integer literals instead of enum to avoid serialization issues
                    decode_reason = 0 if can_apply else 4  # 0=OK, 4=PAYLOAD_TOO_SMALL

                    # Apply scale and offset
                    decoded_value = None
                    if raw_value is not None:
                        scale = rule.get("scale", 1.0) if rule.get("scale") is not None else 1.0
                        offset = rule.get("offset", 0.0) if rule.get("offset") is not None else 0.0
                        decoded_value = raw_value * scale + offset

                    # Check range if min/max are specified
                    is_out_of_range = False
                    if decoded_value is not None:
                        min_val = rule.get("minimum")
                        max_val = rule.get("maximum")
                        if min_val is not None and decoded_value < min_val:
                            is_out_of_range = True
                        if max_val is not None and decoded_value > max_val:
                            is_out_of_range = True

                    decoded_signals.append(
                        {
                            "signal_catalog_id": rule.get("signal_catalog_id"),
                            "data_identifier": rule.get("data_identifier"),
                            "obd_value": rule.get("obd_value"),
                            "bit_start": rule.get("bit_start"),
                            "bit_length": rule.get("bit_length"),
                            "endian": rule.get("endian"),
                            "sign": rule.get("sign"),
                            "scale": scale,
                            "offset": offset,
                            "minimum": rule.get("minimum"),
                            "maximum": rule.get("maximum"),
                            "unit": rule.get("unit"),
                            "source_id": rule.get("source_id"),
                            "decoded_value": decoded_value,
                            "can_apply_rule": can_apply,
                            "decode_reason": decode_reason,
                            "is_out_of_range": is_out_of_range,
                            "is_standard": rule.get("is_standard", False),
                            "is_broadcast": rule.get("is_broadcast", False),
                        }
                    )
                except Exception:
                    # On error, add entry with null decoded_value
                    decoded_signals.append(
                        {
                            "signal_catalog_id": rule.get("signal_catalog_id"),
                            "data_identifier": rule.get("data_identifier"),
                            "obd_value": rule.get("obd_value"),
                            "bit_start": rule.get("bit_start"),
                            "bit_length": rule.get("bit_length"),
                            "endian": rule.get("endian"),
                            "sign": rule.get("sign"),
                            "scale": rule.get("scale", 1.0),
                            "offset": rule.get("offset", 0.0),
                            "minimum": rule.get("minimum"),
                            "maximum": rule.get("maximum"),
                            "unit": rule.get("unit"),
                            "source_id": rule.get("source_id"),
                            "decoded_value": None,
                            "can_apply_rule": False,
                            "decode_reason": 1,  # INVALID_BIT_LENGTH
                            "is_out_of_range": False,
                            "is_standard": rule.get("is_standard", False),
                            "is_broadcast": rule.get("is_broadcast", False),
                        }
                    )

            decoded_signals_list.append(decoded_signals)

        # Add decoded_signals column to the dataframe
        pdf["decoded_signals"] = decoded_signals_list

        # Yield the processed batch
        yield pdf
