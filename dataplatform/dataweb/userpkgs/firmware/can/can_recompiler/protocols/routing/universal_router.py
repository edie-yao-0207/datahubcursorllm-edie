"""
Universal CAN Protocol Router

This router handles ALL CAN protocol routing decisions, from raw CAN frames to 
processed application messages. It eliminates the need for any routing logic 
in higher-level processors like SparkCANProtocolProcessor.

Architecture:
1. Raw CAN frame input
2. Transport protocol detection (ISO-TP vs J1939 vs None)
3. Transport processing (frame assembly, session management)
4. Application protocol routing (UDS vs within ISO-TP)
5. Application processing
6. Return final application message

This provides a single entry point for all CAN protocol processing.
"""

from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
import logging

from ...core.frame_types import CANTransportFrame
from ...core.enums import TransportProtocolType, ApplicationProtocolType
from ...protocols.j1939.detection import can_detect_j1939
from ...protocols.isotp.detection import can_detect_isotp
from .application_router import ApplicationProtocolRouter, create_application_router

logger = logging.getLogger("can_recompiler.protocols.routing.universal_router")


@dataclass
class RoutingResult:
    """Result of universal routing with complete context."""

    # Core results
    application_message: Optional[Any] = None
    transport_frame: Optional[Any] = None

    # Protocol classification
    transport_protocol: TransportProtocolType = TransportProtocolType.NONE
    application_protocol: ApplicationProtocolType = ApplicationProtocolType.NONE

    # Routing metadata
    routing_path: str = ""  # Description of routing path taken
    processing_time_us: Optional[int] = None

    # Success indicators
    success: bool = False
    error_message: Optional[str] = None


class UniversalCANProtocolRouter:
    """
    Universal router that handles ALL CAN protocol routing and processing.

    This router eliminates the need for any routing logic in calling code:
    - Input: Raw CAN frame
    - Output: Processed application message + metadata

    Handles:
    1. Transport protocol detection (ISO-TP, J1939, None)
    2. Transport frame processing
    3. Application protocol routing (UDS)
    4. Application message processing
    """

    def __init__(self):
        self._application_router = create_application_router()

        # Direct transport processors (no coupling)
        self._transport_processors = self._initialize_transport_processors()

    def route_and_process(self, can_frame: Optional[CANTransportFrame]) -> RoutingResult:
        """
        Universal routing and processing entry point.

        Args:
            can_frame: Raw CAN transport frame

        Returns:
            Complete routing result with application message and metadata
        """
        # Handle None frame edge case
        if can_frame is None:
            return RoutingResult(
                success=False,
                error_message="Empty CAN frame cannot be processed",
                routing_path="input_validation -> failed",
            )

        # Step 1: Detect transport protocol
        print(
            f"Universal router: processing frame with id=0x{can_frame.arbitration_id:X}, payload={can_frame.payload[:4]}"
        )
        transport_protocol = self._detect_transport_protocol(can_frame)
        print(f"Universal router: detected transport protocol: {transport_protocol}")

        if transport_protocol == TransportProtocolType.NONE:
            print("Universal router: no transport protocol detected")
            return RoutingResult(
                transport_protocol=transport_protocol,
                routing_path="transport_detection -> none",
                success=True,  # Successfully identified as unprocessable
            )

        # Step 2: Process at transport layer
        print(f"Universal router: processing at transport layer: {transport_protocol}")
        transport_result = self._process_transport_layer(can_frame, transport_protocol)
        print(f"Universal router: transport result: {transport_result}")

        if not transport_result[0]:  # transport_frame
            print("Universal router: transport processing failed")
            return RoutingResult(
                transport_protocol=transport_protocol,
                error_message="Transport processing failed",
                routing_path=f"transport_detection -> {transport_protocol.name.lower()} -> failed",
            )

        transport_frame = transport_result[0]

        # Step 3: Process at application layer
        if transport_protocol == TransportProtocolType.ISOTP:
            return self._process_isotp_application(transport_frame, transport_protocol)
        elif transport_protocol == TransportProtocolType.J1939:
            return self._process_j1939_application(transport_frame, transport_protocol)
        else:
            return RoutingResult(
                transport_frame=transport_frame,
                transport_protocol=transport_protocol,
                error_message=f"Unknown transport protocol: {transport_protocol}",
                routing_path=f"transport_detection -> {transport_protocol.name.lower()} -> unknown",
            )

    def _detect_transport_protocol(
        self, can_frame: Optional[CANTransportFrame]
    ) -> TransportProtocolType:
        """
        Detect transport protocol from CAN frame using existing protocol detection functions.

        First checks for pre-classified transport_id (from signal catalog overrides),
        then falls back to addressing-based detection if not provided.
        """
        # Step 1: Honor pre-classified transport_id if available (signal catalog override)
        if can_frame.transport_id is not None:
            try:
                # Convert int to TransportProtocolType enum
                return TransportProtocolType(can_frame.transport_id)
            except ValueError:
                # Invalid transport_id value, fall back to addressing detection
                logger.warning(
                    f"Invalid transport_id {can_frame.transport_id}, falling back to addressing detection"
                )

        # Step 2: Fall back to addressing-based detection using standalone function
        return detect_protocol(can_frame.arbitration_id)

    def _process_transport_layer(
        self, can_frame: CANTransportFrame, transport_protocol: TransportProtocolType
    ) -> Tuple:
        """Process CAN frame at transport layer."""
        processor = self._transport_processors.get(transport_protocol)

        if not processor:
            return None, transport_protocol, None, ApplicationProtocolType.NONE

        transport_frame = processor.process(can_frame)

        if transport_frame:
            return transport_frame, transport_protocol, None, ApplicationProtocolType.NONE
        else:
            return None, transport_protocol, None, ApplicationProtocolType.NONE

    def _process_isotp_application(
        self, transport_frame, transport_protocol: TransportProtocolType
    ) -> RoutingResult:
        """Process ISO-TP frame at application layer using decoupled routing."""

        application_message = self._application_router.route_frame(transport_frame)

        if application_message:
            # Determine application protocol from message type
            app_protocol = self._determine_application_protocol(application_message)

            return RoutingResult(
                application_message=application_message,
                transport_frame=transport_frame,
                transport_protocol=transport_protocol,
                application_protocol=app_protocol,
                routing_path=f"transport -> isotp -> application -> {app_protocol.name.lower()}",
                success=True,
            )
        else:
            # Transport succeeded but no application processing
            return RoutingResult(
                transport_frame=transport_frame,
                transport_protocol=transport_protocol,
                routing_path="transport -> isotp -> application -> none",
                success=True,
            )

    def _process_j1939_application(
        self, transport_frame, transport_protocol: TransportProtocolType
    ) -> RoutingResult:
        """Process J1939 frame (transport frame contains application data)."""

        # For J1939, the transport frame IS the application message
        return RoutingResult(
            application_message=transport_frame,
            transport_frame=transport_frame,
            transport_protocol=transport_protocol,
            application_protocol=ApplicationProtocolType.J1939,
            routing_path="transport -> j1939 -> application -> j1939",
            success=True,
        )

    def _determine_application_protocol(self, application_message) -> ApplicationProtocolType:
        """Determine application protocol from message type."""
        message_type = type(application_message).__name__

        if message_type == "UDSMessage":
            return ApplicationProtocolType.UDS
        elif message_type == "J1939TransportMessage":
            return ApplicationProtocolType.J1939
        else:
            return ApplicationProtocolType.NONE

    def _initialize_transport_processors(self) -> Dict[TransportProtocolType, Any]:
        """Initialize transport processors without coupling."""
        processors = {}

        try:
            from ...protocols.isotp.transport import ISOTPTransport

            processors[TransportProtocolType.ISOTP] = ISOTPTransport()
        except ImportError:
            logger.warning("ISO-TP transport not available")

        try:
            from ...protocols.j1939.transport import J1939Transport

            processors[TransportProtocolType.J1939] = J1939Transport()
        except ImportError:
            logger.warning("J1939 transport not available")

        logger.info(f"Initialized {len(processors)} transport processors")
        return processors

    def cleanup(self, start_timestamp_unix_us: int) -> None:
        """Clean up all processors."""
        # Clean up application router
        self._application_router.cleanup_processors(start_timestamp_unix_us)

        # Clean up transport processors
        for processor in self._transport_processors.values():
            if hasattr(processor, "cleanup"):
                processor.cleanup(start_timestamp_unix_us)

    def get_routing_info(self) -> Dict[str, Any]:
        """Get comprehensive routing configuration for debugging."""
        return {
            "transport_processors": list(self._transport_processors.keys()),
            "application_routing": self._application_router.get_routing_info(),
            "supported_protocols": {
                "transport": [tp.name for tp in self._transport_processors.keys()],
                "application": self._application_router.get_routing_info()["registered_processors"],
            },
        }

    def route_with_audit(self, can_frame: CANTransportFrame) -> Dict[str, Any]:
        """Route with detailed audit trail for debugging."""
        result = self.route_and_process(can_frame)

        # Get application routing audit if ISO-TP
        app_audit = None
        if result.transport_protocol == TransportProtocolType.ISOTP and result.transport_frame:
            app_audit = self._application_router.route_frame_with_audit(result.transport_frame)

        return {
            "routing_result": result,
            "application_audit": app_audit,
            "transport_protocol_detected": result.transport_protocol.name,
            "routing_path": result.routing_path,
            "success": result.success,
        }


def create_universal_router() -> UniversalCANProtocolRouter:
    """
    Create a universal CAN protocol router with all processors configured.

    Returns:
        Configured UniversalCANProtocolRouter instance
    """
    return UniversalCANProtocolRouter()


def detect_protocol(arbitration_id: int) -> TransportProtocolType:
    """
    Detect transport protocol based on CAN arbitration ID using addressing-based detection only.

    This function provides addressing-based detection logic for use in assets like
    fct_can_trace_recompiled as a fallback when signal catalog classification is not available.

    Note: This function only performs addressing-based detection. For full detection including
    signal catalog overrides, use UniversalCANProtocolRouter._detect_transport_protocol.

    Args:
        arbitration_id: CAN frame arbitration ID

    Returns:
        Detected transport protocol type (ISOTP, J1939, or NONE)
    """

    # Use the same detection order as UniversalCANProtocolRouter:
    # ISO-TP first (for specific extended ranges), then J1939
    # This ensures ISO-TP extended addressing takes precedence
    if can_detect_isotp(arbitration_id):
        return TransportProtocolType.ISOTP

    if can_detect_j1939(arbitration_id):
        return TransportProtocolType.J1939

    return TransportProtocolType.NONE
