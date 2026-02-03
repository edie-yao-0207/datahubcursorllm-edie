"""Shared pytest fixtures for protocol-specific architecture."""

import pytest
from ..infra.spark.processor import SparkCANProtocolProcessor
from ..protocols.isotp.transport import ISOTPTransport
from ..protocols.j1939.transport import J1939Transport


@pytest.fixture
def protocol_processor() -> SparkCANProtocolProcessor:
    """Create a fresh Spark-compatible CAN protocol processor for each test."""
    return SparkCANProtocolProcessor()


@pytest.fixture
def isotp_transport_processor() -> ISOTPTransport:
    """Create a fresh ISO-TP transport processor for each test."""
    return ISOTPTransport()


@pytest.fixture
def j1939_transport_processor() -> J1939Transport:
    """Create a fresh J1939 transport processor for each test."""
    return J1939Transport()


# since message classes are now self-parsing
