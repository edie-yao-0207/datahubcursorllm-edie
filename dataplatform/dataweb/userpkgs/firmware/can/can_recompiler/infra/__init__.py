"""
Infrastructure layer for CAN protocol processing.

This package contains all infrastructure concerns (serialization, adapters,
external system integration) separated from the core domain logic.
"""

# Export main Spark adapters for easy access
from .spark.adapters import (
    ISOTPFrameSparkAdapter,
    J1939TransportSparkAdapter,
    UDSMessageSparkAdapter,
    J1939ApplicationSparkAdapter,
)

from .spark.schema import get_topological_schema

__all__ = [
    # Spark adapters
    "ISOTPFrameSparkAdapter",
    "J1939TransportSparkAdapter",
    "UDSMessageSparkAdapter",
    "J1939ApplicationSparkAdapter",
    # Schema services
    "get_topological_schema",
]
