"""
Protocol Processor Registry

Manages registration and lifecycle of application protocol processors.
This provides a clean separation between routing logic and processor management.
"""

from typing import Dict, Optional, Protocol, Any
import logging

logger = logging.getLogger("can_recompiler.protocols.routing.registry")


class ApplicationProcessor(Protocol):
    """
    Protocol interface for application processors.

    This defines the expected interface without requiring inheritance,
    using Python's structural typing for clean decoupling.
    """

    def process(self, transport_frame: Any) -> Optional[object]:
        """Process a transport frame and return application message or None."""
        ...

    def cleanup(self, start_timestamp_unix_us: int) -> None:
        """Clean up processor state."""
        ...


class ProtocolRegistry:
    """
    Registry for managing application protocol processors.

    This handles processor registration, retrieval, and lifecycle management
    independently of the routing decision logic.
    """

    def __init__(self):
        self.processors: Dict[str, ApplicationProcessor] = {}

    def register(self, key: str, processor: ApplicationProcessor) -> None:
        """
        Register an application processor.

        Args:
            key: Unique identifier for the processor
            processor: Processor instance implementing ApplicationProcessor protocol
        """
        if key in self.processors:
            logger.warning(f"Overwriting existing processor: {key}")

        self.processors[key] = processor
        logger.info(f"Registered application processor: {key}")

    def get(self, key: str) -> Optional[ApplicationProcessor]:
        """
        Get a registered processor by key.

        Args:
            key: Processor identifier

        Returns:
            Processor instance if registered, None otherwise
        """
        return self.processors.get(key)

    def unregister(self, key: str) -> bool:
        """
        Unregister a processor.

        Args:
            key: Processor identifier

        Returns:
            True if processor was found and removed, False otherwise
        """
        if key in self.processors:
            del self.processors[key]
            logger.info(f"Unregistered processor: {key}")
            return True
        else:
            logger.warning(f"Attempted to unregister unknown processor: {key}")
            return False

    def list_registered(self) -> list[str]:
        """
        Get list of registered processor keys.

        Returns:
            List of registered processor keys
        """
        return list(self.processors.keys())

    def cleanup_all(self, start_timestamp_unix_us: int) -> None:
        """
        Clean up all registered processors.

        Args:
            start_timestamp_unix_us: Timestamp for cleanup operations
        """
        for key, processor in self.processors.items():
            try:
                processor.cleanup(start_timestamp_unix_us)
                logger.debug(f"Cleaned up processor: {key}")
            except Exception as e:
                logger.warning(f"Cleanup failed for processor {key}: {e}")

    def get_registry_info(self) -> Dict[str, Any]:
        """
        Get registry information for debugging.

        Returns:
            Dictionary with registry status information
        """
        return {
            "registered_processors": list(self.processors.keys()),
            "processor_count": len(self.processors),
            "processor_types": [
                {
                    "key": key,
                    "type": type(processor).__name__,
                    "module": type(processor).__module__,
                }
                for key, processor in self.processors.items()
            ],
        }


def create_default_registry() -> ProtocolRegistry:
    """
    Create a registry with default automotive protocol processors.

    Returns:
        ProtocolRegistry with common processors registered
    """
    registry = ProtocolRegistry()

    try:
        # Import and register UDS message processor
        from ...protocols.uds.frames import UDSMessage

        class UDSProcessor:
            """Lightweight processor wrapper for UDSMessage."""

            def process(self, transport_frame: Any) -> Optional[object]:
                try:
                    uds_message = UDSMessage(transport_frame=transport_frame)
                    # Filter out requests - only return responses for data analysis
                    if not uds_message.is_response:
                        logger.debug(
                            f"UDS processor: filtering out request (service_id={uds_message.service_id})"
                        )
                        return None
                    # Filter out negative responses - only return positive responses for data analysis
                    if uds_message.is_negative_response:
                        logger.debug(
                            f"UDS processor: filtering out negative response (service_id={uds_message.service_id})"
                        )
                        return None
                    logger.debug(
                        f"UDS processor: returning UDS message (service_id={uds_message.service_id})"
                    )
                    return uds_message
                except Exception as e:
                    logger.warning(f"UDS processor: exception creating UDS message: {e}")
                    return None

            def cleanup(self, start_timestamp_unix_us: int) -> None:
                pass  # Stateless

        registry.register("uds", UDSProcessor())
    except ImportError as e:
        logger.warning(f"Failed to register UDS processor: {e}")

    logger.info(f"Created default registry with {len(registry.processors)} processors")
    return registry
