"""
Application Protocol Router

Main router that coordinates between routing rules and processor registry to
provide intelligent routing of transport frames to appropriate application processors.

This is transport-agnostic and focuses on application-layer protocol coordination.
"""

from typing import Optional, List, Any
import logging

from .routing_rules import RoutingRule, RoutingRuleFactory
from .protocol_registry import ProtocolRegistry, create_default_registry

logger = logging.getLogger("can_recompiler.protocols.routing.application_router")


class ApplicationProtocolRouter:
    """
    Main application protocol router.

    This router eliminates the need for processors to be aware of each other
    by centralizing routing decisions based on configurable rules and a
    processor registry.
    """

    def __init__(
        self,
        registry: Optional[ProtocolRegistry] = None,
        routing_rules: Optional[List[RoutingRule]] = None,
    ):
        """
        Initialize the application router.

        Args:
            registry: Protocol processor registry (creates default if None)
            routing_rules: List of routing rules (creates default if None)
        """
        self.registry = registry or create_default_registry()
        self.routing_rules = (
            routing_rules
            if routing_rules is not None
            else RoutingRuleFactory.create_default_routing_rules()
        )

        # Ensure rules are sorted by priority
        self.routing_rules.sort(key=lambda r: r.priority)

        logger.info(
            f"Initialized router with {len(self.routing_rules)} rules and "
            f"{len(self.registry.list_registered())} processors"
        )

    def route_frame(self, transport_frame: Any) -> Optional[object]:
        """
        Route a transport frame to the appropriate application processor.

        This method is transport-agnostic - it works with any transport frame
        that has source_address and payload attributes.

        Args:
            transport_frame: Transport frame with source_address and payload

        Returns:
            Application message if successfully processed, None otherwise
        """
        if (
            not transport_frame
            or not hasattr(transport_frame, "payload")
            or not transport_frame.payload
        ):
            logger.debug("Skipping frame with no payload")
            return None

        source_address = getattr(transport_frame, "source_address", 0)
        payload = transport_frame.payload

        # Find the first matching rule (rules are priority-ordered)
        for rule in self.routing_rules:
            if rule.matches_frame(source_address, payload):
                processor = self.registry.get(rule.processor_key)
                if processor:
                    logger.debug(
                        f"Routing frame (0x{source_address:X}) to {rule.processor_key} via rule: {rule.name}"
                    )
                    try:
                        return processor.process(transport_frame)
                    except Exception as e:
                        logger.warning(f"Processor {rule.processor_key} failed: {e}")
                        continue
                else:
                    logger.warning(
                        f"Processor {rule.processor_key} not registered for rule: {rule.name}"
                    )
                    continue

        logger.debug(f"No matching routing rule found for frame (0x{source_address:X})")
        return None

    def add_routing_rule(self, rule: RoutingRule) -> None:
        """
        Add a routing rule and maintain priority order.

        Args:
            rule: Routing rule to add
        """
        self.routing_rules.append(rule)
        # Keep rules sorted by priority (lower number = higher priority)
        self.routing_rules.sort(key=lambda r: r.priority)
        logger.info(f"Added routing rule: {rule.name} (priority {rule.priority})")

    def register_processor(self, key: str, processor: Any) -> None:
        """
        Register an application processor.

        Args:
            key: Unique identifier for the processor
            processor: Processor instance
        """
        self.registry.register(key, processor)

    def cleanup_processors(self, start_timestamp_unix_us: int) -> None:
        """Clean up all registered processors."""
        self.registry.cleanup_all(start_timestamp_unix_us)

    def get_routing_info(self) -> dict:
        """
        Get current routing configuration for debugging.

        Returns:
            Dictionary with routing rules and registered processors
        """
        return {
            "routing_rules": [
                {
                    "name": rule.name,
                    "priority": rule.priority,
                    "processor_key": rule.processor_key,
                    "description": rule.description,
                    "address_ranges": rule.address_ranges,
                }
                for rule in self.routing_rules
            ],
            **self.registry.get_registry_info(),
        }

    def route_frame_with_audit(self, transport_frame: Any) -> dict:
        """
        Route a frame and return detailed audit information.

        Useful for debugging routing decisions.

        Args:
            transport_frame: Transport frame to route

        Returns:
            Dictionary with result and audit trail
        """
        if (
            not transport_frame
            or not hasattr(transport_frame, "payload")
            or not transport_frame.payload
        ):
            return {
                "result": None,
                "reason": "No payload",
                "evaluated_rules": [],
                "matched_rule": None,
                "processor_used": None,
            }

        source_address = getattr(transport_frame, "source_address", 0)
        payload = transport_frame.payload
        evaluated_rules = []

        # Evaluate each rule and track decisions
        for rule in self.routing_rules:
            rule_evaluation = {
                "rule_name": rule.name,
                "priority": rule.priority,
                "matches": rule.matches_frame(source_address, payload),
                "processor_key": rule.processor_key,
            }
            evaluated_rules.append(rule_evaluation)

            if rule_evaluation["matches"]:
                processor = self.registry.get(rule.processor_key)
                if processor:
                    try:
                        result = processor.process(transport_frame)
                        return {
                            "result": result,
                            "reason": "Successfully processed",
                            "evaluated_rules": evaluated_rules,
                            "matched_rule": rule.name,
                            "processor_used": rule.processor_key,
                        }
                    except Exception as e:
                        rule_evaluation["error"] = str(e)
                        continue
                else:
                    rule_evaluation["error"] = "Processor not registered"
                    continue

        return {
            "result": None,
            "reason": "No matching rule found",
            "evaluated_rules": evaluated_rules,
            "matched_rule": None,
            "processor_used": None,
        }


def create_application_router() -> ApplicationProtocolRouter:
    """
    Create a pre-configured application router with default rules and processors.

    Returns:
        Configured ApplicationProtocolRouter instance
    """
    return ApplicationProtocolRouter()
