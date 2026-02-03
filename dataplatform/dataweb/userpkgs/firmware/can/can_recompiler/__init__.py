"""
CAN Protocol Processing Framework

A clean, modular framework for processing and reassembling CAN bus frames from 
automotive diagnostic protocols including ISO-TP (UDS/OBD-II) and J1939.

This framework provides a professional, maintainable architecture organized by
automotive standards:
- protocols/isotp: ISO-TP transport protocol
- protocols/uds: UDS diagnostic protocol  
- protocols/j1939: J1939 heavy-duty vehicle protocol
- core: Framework foundation and interfaces
- infra/spark: Spark/DataWeb integration layer
"""

import logging
import os

# Import from clean core structure (pure domain logic)
from .core import (
    # Core types
    CANTransportFrame,
    # Enums
    TransportProtocolType,
    ApplicationProtocolType,
    # Constants
    CAN_ID_MASK_29BIT,
    CAN_FRAME_MAX_DATA_LENGTH,
)

# Import Spark-specific functionality from infra
from .infra.spark import (
    # Interfaces
    TransportFrame,
    ApplicationMessage,
    # Main Spark processor
    SparkCANProtocolProcessor,
    # Spark schema generation
    get_topological_schema,
    # Spark-specific types
    PartitionMetadata,
    InputFrameData,
    # Schema utilities
    create_empty_dataframe_with_schema,
    # Trace processing utilities
    create_passthrough_dataframe,
    process_frames_distributed,
    create_decode_traces_udf,
    # UDF utilities
    register_detect_protocol_udf,
)

# Import protocol detection from routing package
from .protocols.routing import detect_protocol

# Import protocol-specific frame types from their packages
from .protocols.isotp.frames import ISOTPFrame
from .protocols.j1939.frames import J1939TransportMessage
from .protocols.uds.frames import UDSMessage

# Import protocol-specific enums
from .protocols.uds.enums import UDSDataIdentifierType
from .protocols.j1939.enums import J1939DataIdentifierType, J1939TransportProtocolType

# Union type for protocol messages
from typing import Union

ProtocolMessage = Union[UDSMessage, J1939TransportMessage]

# Import signal decoder functions and enums for distribution to Spark workers
from .signal_decoder import (
    calculate_normalized_bit_position,
    extract_byte_segment,
    extract_signal_value,
    apply_signal_sign,
    decode_single_signal_from_payload,
    decode_signals_batch,  # Batch decoder for mapInPandas
    # Signal processing enums
    EndiannessType,
    SignType,
    DecodeReasonType,
)

# Configure package-level logging
logger = logging.getLogger("can_recompiler")
logger.setLevel(
    logging.DEBUG
    if os.getenv("CAN_RECOMPILER_DEBUG", "false").lower() == "true"
    else logging.WARNING
)

# Create console handler if it doesn't exist
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Version information
__version__ = "1.0.0"
__author__ = "Firmware VDP Team"
__description__ = "CAN bus frame processing and reassembly library"

# Public API - Clean and Professional
__all__ = [
    # 🔧 Core Framework
    "SparkCANProtocolProcessor",  # Main processor
    # 📋 Core Interfaces
    "TransportFrame",  # Transport layer interface
    "ApplicationMessage",  # Application layer interface
    # 🔄 Schema Generation
    "get_topological_schema",  # Schema generation
    "create_empty_dataframe_with_schema",  # Empty DataFrame utilities
    # 🔄 Trace Processing
    "create_passthrough_dataframe",  # Passthrough DataFrame creation
    "process_frames_distributed",  # Distributed CAN processing
    "create_decode_traces_udf",  # UDF factory function
    # ⚡ UDF Utilities
    "register_detect_protocol_udf",  # UDF registration for DETECT_PROTOCOL
    # 📊 Core Types
    "PartitionMetadata",  # Partition metadata
    "CANTransportFrame",  # CAN transport frame
    "InputFrameData",  # Input frame data
    # 🚛 Transport Protocol Frames
    "ISOTPFrame",  # ISO-TP frames
    "J1939TransportMessage",  # J1939 transport
    # 📱 Application Protocol Messages
    "UDSMessage",  # UDS messages
    "Message",
    "ProtocolMessage",  # Union type
    # 🏷️ Enumerations
    "TransportProtocolType",  # Transport protocol types
    "ApplicationProtocolType",  # Application protocol types
    # Protocol-Specific Enums
    "UDSDataIdentifierType",  # UDS data identifier types
    "DataIdentifierType",
    "J1939DataIdentifierType",  # J1939 data identifier types
    "J1939TransportProtocolType",  # J1939 transport protocol types
    # 🔧 Constants
    "CAN_ID_MASK_29BIT",  # CAN ID mask
    "CAN_FRAME_MAX_DATA_LENGTH",  # Max CAN data length
    # 🔍 Protocol Detection
    "detect_protocol",  # Protocol detection function
    # Signal decoder functions (for Spark distribution)
    "calculate_normalized_bit_position",
    "extract_byte_segment",
    "extract_signal_value",
    "apply_signal_sign",
    "decode_single_signal_from_payload",
    "decode_signals_batch",  # Batch decoder for mapInPandas
    # Signal processing enums
    "EndiannessType",
    "SignType",
    "DecodeReasonType",
]
