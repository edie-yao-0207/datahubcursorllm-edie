"""
Spark-Compatible CAN Protocol Processor

This module provides the main processor for DataWeb Spark integration with direct 
protocol processing. Designed to process CAN frame data from Spark DataFrames and 
produce protocol-specific dictionaries compatible with the topological Spark schema 
for structured storage and analysis.
"""

import logging
from typing import Optional, Dict, Any

from .adapters import UDSMessageSparkAdapter, J1939ApplicationSparkAdapter
from .frame_types import TransportFrame, ApplicationMessage
from .frame_types import InputFrameData, convert_input_to_processing_frame
from .frame_types import PartitionMetadata as SparkPartitionMetadata
from ...core.enums import TransportProtocolType, ApplicationProtocolType
from ...protocols.routing import create_universal_router, RoutingResult
from ...protocols.uds.frames import UDSMessage
from ...protocols.j1939.frames import J1939TransportMessage

logger = logging.getLogger("can_recompiler.protocol_processor")


class SparkCANProtocolProcessor:
    """
    Spark-compatible CAN protocol processor for DataWeb integration.

    Designed for Spark DataFrame processing with direct protocol processing:
    - Input: DataWeb CAN frame data (from Spark DataFrame rows)
    - Processing: Direct transport→application protocol pipeline
    - Output: Protocol-specific dictionaries compatible with Spark schemas

    This processor bridges DataWeb's Spark-based data processing with
    protocol-specific CAN frame analysis and structured output generation.
    """

    def __init__(self):
        # Use universal router for ALL protocol routing and processing
        self._universal_router = create_universal_router()

    def cleanup(self, start_timestamp_unix_us: int) -> None:
        """Clean up expired sessions in all transport processors."""
        self._universal_router.cleanup(start_timestamp_unix_us)

    def process_frame_to_dict(self, input_data: InputFrameData) -> Optional[Dict[str, Any]]:
        """
        Process a CAN frame and return a Spark-compatible dictionary.

        This method provides direct Spark DataFrame integration by processing
        a CAN frame and returning a dictionary that matches the topological
        Spark schema structure for immediate DataFrame row creation.

        Args:
            input_data: Raw CAN frame data from DataWeb/Spark DataFrame row

        Returns:
            Dictionary matching topological Spark schema if processing successful, None otherwise
        """
        # Step 1: Convert input to CANTransportFrame and extract metadata
        processing_frame, metadata = convert_input_to_processing_frame(input_data)

        # Step 2: Use universal router for all protocol detection and processing
        routing_result: RoutingResult = self._universal_router.route_and_process(processing_frame)

        # Return None for frames with no usable data (efficient filtering)
        if (
            not routing_result.success
            or routing_result.transport_protocol == TransportProtocolType.NONE
            or routing_result.application_protocol == ApplicationProtocolType.NONE
        ):
            return None

        # Note: Request filtering is now handled at the application layer in protocol processors
        # Step 3: Assemble the complete topological structure from routing result
        return self._assemble_topological_dict(
            routing_result.transport_frame,
            routing_result.application_message,
            metadata,
            routing_result.transport_protocol,
            routing_result.application_protocol,
            input_data.get("id"),  # Pass arbitration ID from input
        )

    def _assemble_topological_dict(
        self,
        transport_frame: TransportFrame,
        application_message: Optional[ApplicationMessage] = None,
        metadata=None,  # Accept any PartitionMetadata type
        transport_protocol: TransportProtocolType = TransportProtocolType.NONE,
        application_protocol: ApplicationProtocolType = ApplicationProtocolType.NONE,
        arbitration_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Assemble the complete topological dictionary structure.

        Args:
            transport_frame: The transport layer frame (ISO-TP or J1939)
            application_message: Optional application layer message (UDS, or J1939)
            metadata: Optional partition metadata for the frame
            transport_protocol: Detected transport protocol type
            application_protocol: Detected application protocol type

        Returns:
            Complete topological dictionary with explicit protocols, metadata, and consolidated application sections
        """
        # Convert core metadata to Spark metadata if needed, or handle missing metadata
        if metadata is None:
            metadata_dict = {}
        elif hasattr(metadata, "to_dict"):
            # Already a Spark-enhanced PartitionMetadata
            metadata_dict = metadata.to_dict()
        else:
            # Core PartitionMetadata - convert to Spark version
            spark_metadata = SparkPartitionMetadata(
                date=getattr(metadata, "date", ""),
                org_id=getattr(metadata, "org_id", 0),
                device_id=getattr(metadata, "device_id", 0),
                trace_uuid=getattr(metadata, "trace_uuid", ""),
                source_interface=getattr(metadata, "source_interface", "default"),
                direction=getattr(metadata, "direction", None),
                frame_id=getattr(metadata, "frame_id", None),
            )
            metadata_dict = spark_metadata.to_dict()

        # Get consolidated application data (includes embedded transport metadata)
        application_dict = self._assemble_application_section(
            application_message, application_protocol, transport_frame
        )

        # Propagate timestamp from deepest available level (application > transport > metadata)
        final_timestamp = metadata_dict.get("start_timestamp_unix_us", 0)

        # Check if application layer has a more specific timestamp
        if application_message and hasattr(application_message, "start_timestamp_unix_us"):
            if application_message.start_timestamp_unix_us:
                final_timestamp = application_message.start_timestamp_unix_us
        # Otherwise check transport layer
        elif transport_frame and hasattr(transport_frame, "start_timestamp_unix_us"):
            if transport_frame.start_timestamp_unix_us:
                final_timestamp = transport_frame.start_timestamp_unix_us

        # Determine payload - use application payload if available, otherwise transport payload
        payload_data = b""
        if application_message and hasattr(application_message, "payload"):
            payload_data = application_message.payload or b""
        elif transport_frame and hasattr(transport_frame, "payload"):
            payload_data = transport_frame.payload or b""
        else:
            raise ValueError("No payload data found")

        return {
            # Metadata fields flattened to top level (for better Spark table structure)
            **metadata_dict,
            # Propagated timestamp for efficient sorting
            "start_timestamp_unix_us": final_timestamp,
            # CAN arbitration ID (preserved from input)
            "id": arbitration_id,
            # Protocol payload data (transport or application level)
            "payload": bytes(payload_data) if payload_data else b"",
            "payload_length": len(payload_data) if payload_data else 0,
            # Top-level protocol identification for easy Spark querying
            "transport_id": transport_protocol.value,
            "application_id": application_protocol.value,
            # Consolidated application section (includes transport metadata)
            "application": application_dict,
        }

    def _assemble_application_section(
        self,
        application_message: Optional[ApplicationMessage],
        application_protocol: ApplicationProtocolType,
        transport_frame: Optional[TransportFrame] = None,
    ) -> Dict[str, Any]:
        """
        Assemble the consolidated application section with embedded transport metadata.

        Args:
            application_message: Optional application layer message
            application_protocol: Detected application protocol type
            transport_frame: Optional transport frame containing metadata to embed

        Returns:
            Consolidated application section dictionary with protocol-specific data and transport metadata
        """
        # Initialize all known application protocols to None
        application_section = {
            "j1939": None,
            "none": None,  # For raw unprocessed frames
            "uds": None,
        }

        if application_message is None:
            return application_section

        # Generate consolidated data via appropriate adapter
        if isinstance(application_message, UDSMessage):
            adapter = UDSMessageSparkAdapter(application_message, transport_frame)
            app_data = adapter.to_consolidated_dict()
        elif isinstance(application_message, J1939TransportMessage):
            adapter = J1939ApplicationSparkAdapter(application_message)
            app_data = adapter.to_consolidated_dict()
        else:
            raise ValueError("Unknown application message type")

        # Use protocol enumeration to determine which section to populate
        if application_protocol == ApplicationProtocolType.UDS:
            application_section["uds"] = app_data
        elif application_protocol == ApplicationProtocolType.J1939:
            application_section["j1939"] = app_data
        elif application_protocol == ApplicationProtocolType.NONE:
            application_section["none"] = app_data

        return application_section
