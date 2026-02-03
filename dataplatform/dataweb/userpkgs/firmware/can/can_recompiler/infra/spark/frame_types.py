"""
CAN Frame Data Types

This module contains dataclasses and frame representations used in CAN frame processing.
These types provide structured interfaces for frame data, parsing results, and protocol
information.
"""

from dataclasses import dataclass
from typing import Optional, TypedDict, Dict, Any, Protocol as ProtocolInterface
from abc import abstractmethod

from ...core.frame_types import CANTransportFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Type aliases for abstract protocol interfaces
class TransportFrame(ProtocolInterface):
    """Protocol interface for transport layer frames."""

    @property
    @abstractmethod
    def start_timestamp_unix_us(self) -> int:
        ...


class ApplicationMessage(ProtocolInterface):
    """Protocol interface for application layer messages."""

    @property
    @abstractmethod
    def start_timestamp_unix_us(self) -> Optional[int]:
        ...


@dataclass
class PartitionMetadata:
    """
    Partition and context metadata for DataWeb pipeline integration.

    Contains all non-CAN fields needed for output frame construction,
    making it easy to pass through the processing pipeline with Spark-specific
    serialization capabilities.
    """

    trace_uuid: str = ""
    date: str = ""
    org_id: int = 0
    device_id: int = 0
    source_interface: str = "default"
    direction: Optional[int] = None
    mmyef_id: Optional[int] = None  # Vehicle MMYEF hash for signal catalog lookups
    end_timestamp_unix_us: Optional[int] = None  # End timestamp for deduplication
    duplicate_count: Optional[int] = None  # Duplicate count for deduplication

    def to_dict(self) -> Dict[str, Any]:
        """Convert partition metadata to dictionary representation."""
        return {
            "date": str(self.date),
            "trace_uuid": str(self.trace_uuid),
            "org_id": int(self.org_id),
            "device_id": int(self.device_id),
            "source_interface": str(self.source_interface),
            "direction": int(self.direction) if self.direction is not None else 0,
            "mmyef_id": int(self.mmyef_id) if self.mmyef_id is not None else None,
            "end_timestamp_unix_us": int(self.end_timestamp_unix_us)
            if self.end_timestamp_unix_us is not None
            else None,
            "duplicate_count": int(self.duplicate_count)
            if self.duplicate_count is not None
            else None,
        }

    @classmethod
    def get_spark_schema(cls):
        """
        Get the Spark SQL schema for this class.

        Explicitly defined schema matching to_dict() output.

        Returns:
            StructType representing the Spark SQL schema
        """
        return StructType(
            [
                StructField("date", StringType(), True),
                StructField("trace_uuid", StringType(), True),
                StructField("org_id", LongType(), True),
                StructField("device_id", LongType(), True),
                StructField("source_interface", StringType(), True),
                StructField("direction", IntegerType(), True),
                StructField("mmyef_id", LongType(), True),
                StructField("end_timestamp_unix_us", LongType(), True),
                StructField("duplicate_count", LongType(), True),
            ]
        )


class InputFrameData(TypedDict, total=False):
    """
    Typed interface for input frame data from pandas DataFrame.

    The processor handles both CAN protocol processing and partition field
    passthrough to produce complete, self-contained results.

    CAN processing: id, payload, start_timestamp_unix_us, direction, source_interface, transport_id
    Partition passthrough: date, org_id, device_id, trace_uuid, mmyef_id
    """

    # Core CAN frame data (for protocol processing)
    id: int
    payload: bytes
    start_timestamp_unix_us: int
    direction: int
    source_interface: Optional[str]
    transport_id: Optional[int]  # Pre-classified transport protocol (if available)

    # Partition fields (passed through to output)
    date: str
    org_id: int
    device_id: int
    trace_uuid: str
    mmyef_id: int  # Vehicle MMYEF hash for signal catalog lookups


def convert_input_to_processing_frame(
    input_data: InputFrameData,
) -> tuple["CANTransportFrame", PartitionMetadata]:
    """
    Convert input data to processing frame and extract metadata for Spark processing.

    Args:
        input_data: Raw input frame data from DataWeb/Spark pipeline

    Returns:
        Tuple of (CANTransportFrame, PartitionMetadata) for processing pipeline

    Raises:
        ValueError: If required fields are missing or invalid
    """

    # Validate required CAN fields
    required_fields = ["id", "payload", "start_timestamp_unix_us"]
    for field in required_fields:
        if field not in input_data:
            raise ValueError(f"Required field '{field}' missing")

    # Create CANTransportFrame with validation
    processing_frame = CANTransportFrame(
        arbitration_id=input_data["id"],
        payload=input_data["payload"],
        start_timestamp_unix_us=input_data["start_timestamp_unix_us"],
        direction=input_data.get("direction"),
        source_interface=input_data.get("source_interface", "default"),
        transport_id=input_data.get(
            "transport_id"
        ),  # Pass through pre-classified transport protocol
    )

    # Create metadata with defaults
    metadata = PartitionMetadata(
        date=input_data.get("date", ""),
        org_id=input_data.get("org_id", 0),
        device_id=input_data.get("device_id", 0),
        trace_uuid=input_data.get("trace_uuid", ""),
        source_interface=input_data.get("source_interface", "default"),
        direction=input_data.get("direction"),
        mmyef_id=input_data.get("mmyef_id"),  # Extract MMYEF hash for signal catalog lookups
        end_timestamp_unix_us=input_data.get(
            "end_timestamp_unix_us"
        ),  # Extract deduplication metadata
        duplicate_count=input_data.get("duplicate_count"),  # Extract deduplication metadata
    )

    return processing_frame, metadata
