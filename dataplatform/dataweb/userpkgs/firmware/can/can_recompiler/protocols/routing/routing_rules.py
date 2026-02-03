"""
Protocol Routing Rules

Defines the rule engine for routing frames to appropriate application processors.
This is transport-agnostic and focuses on application-layer protocol identification.
"""

from typing import List, Tuple, Callable, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger("can_recompiler.protocols.routing.rules")

# Type alias for address ranges
AddressRange = Tuple[int, int]  # (min_address, max_address)


@dataclass
class RoutingRule:
    """
    Rule for routing frames to appropriate application processors.

    Rules are evaluated in priority order to determine which processor
    should handle a given frame.
    """

    name: str
    priority: int  # Lower numbers = higher priority
    address_ranges: List[AddressRange]  # Address ranges this rule applies to
    payload_validators: List[Callable[[bytes], bool]]  # Payload validation functions
    processor_key: str  # Key to identify the target processor
    description: str

    def matches_frame(self, source_address: int, payload: bytes) -> bool:
        """
        Check if this rule matches a given frame.

        Args:
            source_address: Source address of the frame
            payload: Frame payload to validate

        Returns:
            True if the frame matches this rule
        """
        # Check address ranges
        address_match = any(
            min_addr <= source_address <= max_addr for min_addr, max_addr in self.address_ranges
        )

        if not address_match:
            return False

        # Check payload validators
        for validator in self.payload_validators:
            try:
                if not validator(payload):
                    return False
            except Exception as e:
                logger.debug(f"Payload validator failed for rule {self.name}: {e}")
                return False

        return True


class RoutingRuleFactory:
    """Factory for creating common automotive protocol routing rules."""

    @staticmethod
    def _is_uds_service_id(payload: bytes) -> bool:
        """Check if payload starts with a valid UDS or OBD-II service ID."""
        if not payload:
            return False

        service_id = payload[0]

        # Valid UDS request modes (0x10-0x3E) or response modes (0x40-0x7F)
        is_uds_request_mode = 0x10 <= service_id <= 0x3E
        is_uds_response_mode = 0x40 <= service_id <= 0x7F

        # Valid OBD-II request modes (0x01-0x09) or response modes (0x40-0x49)
        is_obd_request_mode = 0x01 <= service_id <= 0x09
        is_obd_response_mode = 0x40 <= service_id <= 0x49

        return (
            is_uds_request_mode
            or is_uds_response_mode
            or is_obd_request_mode
            or is_obd_response_mode
        )

    @staticmethod
    def create_uds_rule() -> RoutingRule:
        """Create routing rule for UDS and OBD-II diagnostic protocols."""
        return RoutingRule(
            name="UDS_Diagnostic",
            priority=10,  # High priority
            address_ranges=[
                (0x7DF, 0x7DF),  # Functional addressing
                (0x7E0, 0x7E9),  # Physical addressing (tester to ECU, including ECU responses)
                (0x700, 0x700),  # Additional UDS addressing range
            ],
            payload_validators=[
                RoutingRuleFactory._is_uds_service_id,
            ],
            processor_key="uds",
            description="UDS and OBD-II diagnostic messages with valid service IDs in diagnostic address ranges",
        )

    @staticmethod
    def create_default_routing_rules() -> List[RoutingRule]:
        """
        Create default routing rules for common automotive protocols.

        Returns:
            List of routing rules sorted by priority
        """
        rules = [RoutingRuleFactory.create_uds_rule()]

        # Sort by priority (lower number = higher priority)
        rules.sort(key=lambda r: r.priority)

        logger.info(f"Created {len(rules)} default routing rules")
        return rules
