"""
Protocol Routing Package

This package provides intelligent routing and coordination between different
automotive protocol processors, eliminating coupling between protocol implementations.

Recommended API:
- UniversalCANProtocolRouter: Complete routing solution for all CAN protocols
- create_universal_router(): Factory for universal router

Lower-level APIs (used internally by UniversalCANProtocolRouter):
- ApplicationProtocolRouter: ISO-TP application-layer routing only
- RoutingRule, AddressRange, ProtocolRegistry: Routing infrastructure
"""

from .application_router import ApplicationProtocolRouter, create_application_router
from .routing_rules import RoutingRule, AddressRange
from .protocol_registry import ProtocolRegistry
from .universal_router import (
    UniversalCANProtocolRouter,
    create_universal_router,
    RoutingResult,
    detect_protocol,
)

__all__ = [
    # Recommended API
    "UniversalCANProtocolRouter",
    "create_universal_router",
    "RoutingResult",
    "detect_protocol",
    # Lower-level APIs (used internally)
    "ApplicationProtocolRouter",
    "create_application_router",
    "RoutingRule",
    "AddressRange",
    "ProtocolRegistry",
]
