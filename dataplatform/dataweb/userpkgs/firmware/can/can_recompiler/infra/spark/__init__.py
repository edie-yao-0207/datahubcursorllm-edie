"""
Spark infrastructure adapters for CAN protocol processing.

This package contains all Spark-specific logic, schemas, and conversions,
keeping the core domain logic free from Spark dependencies.
"""

from .processor import SparkCANProtocolProcessor
from .schema import get_topological_schema
from .frame_types import PartitionMetadata, InputFrameData
from .adapters import (
    UDSMessageSparkAdapter,
    J1939ApplicationSparkAdapter,
    ISOTPFrameSparkAdapter,
    J1939TransportSparkAdapter,
)
from .schema_utils import create_empty_dataframe_with_schema
from .trace_processing import (
    create_passthrough_dataframe,
    process_frames_distributed,
    create_decode_traces_udf,
)
from .udfs import register_detect_protocol_udf

# Pure domain framework
from .frame_types import TransportFrame, ApplicationMessage

__all__ = [
    # Interfaces
    "TransportFrame",
    "ApplicationMessage",
    "SparkCANProtocolProcessor",
    "get_topological_schema",
    "PartitionMetadata",
    "InputFrameData",
    "UDSMessageSparkAdapter",
    "J1939ApplicationSparkAdapter",
    "ISOTPFrameSparkAdapter",
    "J1939TransportSparkAdapter",
    "create_empty_dataframe_with_schema",
    # Trace processing utilities
    "create_passthrough_dataframe",
    "process_frames_distributed",
    "create_decode_traces_udf",
    # UDF utilities
    "register_detect_protocol_udf",
]
