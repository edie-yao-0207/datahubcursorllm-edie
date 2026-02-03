"""
Spark adapters for CAN protocol domain objects.

These adapters handle all Spark-specific serialization, schema generation,
and data conversion logic, keeping the core domain objects clean.
Also includes SQL generation methods for efficient struct creation.
"""

from typing import Dict, Any, Optional
from dataclasses import fields

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    BinaryType,
    BooleanType,
)

from ...protocols.isotp.frames import ISOTPFrame
from ...protocols.j1939.frames import J1939TransportMessage
from ...protocols.j1939.enums import classify_j1939_pgn
from ...protocols.uds.frames import UDSMessage


def _struct_type_to_sql_string(struct_type: StructType) -> str:
    """
    Convert a Spark StructType to SQL type string representation.

    Example: StructType([StructField("id", IntegerType(), True)])
             -> "struct<id:int>"
    """
    field_strings = []
    for field in struct_type.fields:
        sql_type = _spark_type_to_sql_string(field.dataType)
        field_strings.append(f"{field.name}:{sql_type}")

    return f"struct<{','.join(field_strings)}>"


def _spark_type_to_sql_string(spark_type) -> str:
    """Convert individual Spark data type to SQL string."""
    type_name = str(spark_type)

    if type_name == "IntegerType()":
        return "int"
    elif type_name == "LongType()":
        return "bigint"
    elif type_name == "BooleanType()":
        return "boolean"
    elif type_name == "StringType()":
        return "string"
    elif type_name == "BinaryType()":
        return "binary"
    elif type_name.startswith("StructType"):
        return _struct_type_to_sql_string(spark_type)
    else:
        # Default fallback - this might need refinement
        return type_name.lower().replace("type()", "").replace("type", "")


class ISOTPFrameSparkAdapter:
    """Spark adapter for ISO-TP frames."""

    def __init__(self, isotp_frame: ISOTPFrame):
        self.frame = isotp_frame

    def to_spark_dict(self) -> Dict[str, Any]:
        """Convert ISO-TP frame to Spark-compatible dictionary."""
        return {
            "source_address": int(self.frame.source_address)
            if self.frame.source_address is not None
            else 0,
            "payload": bytes(self.frame.payload) if self.frame.payload else b"",
            "payload_length": int(len(self.frame.payload)) if self.frame.payload else 0,
            "frame_count": int(self.frame.frame_count),
            "expected_length": int(self.frame.expected_length),
            "start_timestamp_unix_us": int(self.frame.start_timestamp_unix_us),
        }

    @staticmethod
    def get_spark_schema() -> StructType:
        """Get Spark schema for ISO-TP transport metadata."""
        return StructType(
            [
                StructField("expected_length", IntegerType(), True),
                StructField("frame_count", IntegerType(), True),
                StructField("payload", BinaryType(), True),
                StructField("payload_length", IntegerType(), True),
                StructField("source_address", IntegerType(), True),
                StructField("start_timestamp_unix_us", LongType(), True),
            ]
        )

    @staticmethod
    def get_transport_metadata_fields() -> list[StructField]:
        """Get ISO-TP transport metadata fields for embedding in application schemas."""
        return [
            StructField("source_address", IntegerType(), True),
            StructField("frame_count", IntegerType(), True),
            StructField("expected_length", IntegerType(), True),
        ]


class J1939TransportSparkAdapter:
    """Spark adapter for J1939 transport messages."""

    def __init__(self, j1939_transport: J1939TransportMessage):
        self.transport = j1939_transport

    def to_spark_dict(self) -> Dict[str, Any]:
        """Convert J1939 transport message to Spark-compatible dictionary."""
        # Access fields directly from the domain object (no longer has to_dict)
        return {
            "pgn": int(self.transport.pgn),
            "source_address": int(self.transport.source_address),
            "destination_address": int(self.transport.destination_address),
            "priority": int(self.transport.priority),
            "is_broadcast": bool(self.transport.is_broadcast),
            "transport_protocol_used": int(self.transport.transport_protocol_used.value)
            if self.transport.transport_protocol_used
            else None,
            "payload": bytes(self.transport.payload) if self.transport.payload else b"",
            "payload_length": int(len(self.transport.payload)) if self.transport.payload else 0,
            "frame_count": int(self.transport.frame_count),
            "expected_length": int(self.transport.expected_length),
            "start_timestamp_unix_us": int(self.transport.start_timestamp_unix_us),
        }

    @staticmethod
    def get_transport_metadata_fields() -> list[StructField]:
        """Get J1939 transport metadata fields for embedding in application schemas."""
        return [
            StructField("source_address", IntegerType(), True),
            StructField("destination_address", IntegerType(), True),
            StructField("priority", IntegerType(), True),
            StructField("is_broadcast", BooleanType(), True),
            StructField("frame_count", IntegerType(), True),
            StructField("expected_length", IntegerType(), True),
            StructField("transport_protocol_used", IntegerType(), True),
        ]


class UDSMessageSparkAdapter:
    """Spark adapter for UDS application messages."""

    def __init__(self, uds_message: UDSMessage, transport_frame: Optional[ISOTPFrame] = None):
        self.message = uds_message
        self.transport_frame = transport_frame

    def to_consolidated_dict(self) -> Dict[str, Any]:
        """
        Convert UDS message to consolidated dictionary with ISO-TP transport metadata.

        This combines UDS application data with ISO-TP transport metadata
        to eliminate duplication between transport and application layers.

        CRITICAL: Field order must match get_consolidated_spark_schema() exactly!
        """
        # Get transport data first
        transport_data = {}
        if self.transport_frame is not None:
            isotp_adapter = ISOTPFrameSparkAdapter(self.transport_frame)
            transport_data = isotp_adapter.to_spark_dict()

        # Build result in EXACT schema field order to prevent PyArrow schema mismatch
        # Schema order: data_identifier, data_identifier_type, is_negative_response, is_response, service_id,
        #              composite_data_identifier, source_address, frame_count, expected_length

        # Calculate composite_data_identifier based on parameter size
        data_id = self.message.data_identifier
        service_id = self.message.service_id
        if data_id is None:
            # Service-only operations: just use service ID
            composite_data_id = service_id
        elif service_id in (34, 36, 42, 46, 47, 49):
            # 2-byte parameter services: service in bits 16-23
            composite_data_id = (service_id << 16) + data_id
        else:
            # 1-byte parameter services: service in bits 8-15
            composite_data_id = (service_id << 8) + data_id

        result = {
            # UDS application data fields in schema order
            "data_identifier": int(self.message.data_identifier)
            if self.message.data_identifier is not None
            else None,
            "data_identifier_type": int(self.message.data_identifier_type.value),
            "is_negative_response": bool(self.message.is_negative_response),
            "is_response": bool(self.message.is_response),
            "service_id": int(self.message.service_id),
            "composite_data_identifier": int(composite_data_id),
            # ISO-TP transport metadata fields in schema order
            "source_address": transport_data.get("source_address", 0),
            "frame_count": transport_data.get("frame_count", 1),
            "expected_length": transport_data.get("expected_length", 0),
        }

        return result

    @staticmethod
    def get_consolidated_spark_schema() -> StructType:
        """Get consolidated Spark schema for UDS + ISO-TP transport metadata."""
        return StructType(
            [
                # UDS application data
                StructField("data_identifier", IntegerType(), True),
                StructField("data_identifier_type", IntegerType(), True),
                StructField("is_negative_response", BooleanType(), True),
                StructField("is_response", BooleanType(), True),
                StructField("service_id", IntegerType(), True),
                StructField("composite_data_identifier", LongType(), True),
                # ISO-TP transport metadata
                *ISOTPFrameSparkAdapter.get_transport_metadata_fields(),
            ]
        )

    @classmethod
    def get_null_struct_sql(cls) -> str:
        """
        Generate SQL expression for a null UDS struct.

        Returns:
            SQL expression like: cast(null as struct<data_identifier:int,...>)
        """
        schema = cls.get_consolidated_spark_schema()
        struct_type_str = _struct_type_to_sql_string(schema)
        return f"cast(null as {struct_type_str})"

    @classmethod
    def get_populated_struct_sql(cls, **field_expressions) -> str:
        """
        Generate SQL expression for populated UDS struct.

        Args:
            **field_expressions: SQL expressions for each field

        Returns:
            SQL expression like: named_struct('data_identifier', expr1, ...)
        """
        schema = cls.get_consolidated_spark_schema()
        field_pairs = []

        for field in schema.fields:
            field_name = field.name
            if field_name in field_expressions:
                expr = field_expressions[field_name]
            else:
                # Default to null with proper casting
                sql_type = _spark_type_to_sql_string(field.dataType)
                expr = f"cast(null as {sql_type})"

            field_pairs.append(f"'{field_name}'")
            field_pairs.append(expr)

        return f"named_struct({', '.join(field_pairs)})"


class J1939ApplicationSparkAdapter:
    """Spark adapter for J1939 transport messages (used as application messages)."""

    def __init__(self, j1939_transport: J1939TransportMessage):
        # J1939 transport frame contains all the data we need
        self.transport_frame = j1939_transport

    def to_consolidated_dict(self) -> Dict[str, Any]:
        """Convert J1939 transport message to consolidated dictionary with transport metadata."""
        # Start with J1939 application data (match schema field ordering exactly)
        result = {
            "data_identifier_type": int(classify_j1939_pgn(self.transport_frame.pgn).value),
            "pgn": int(self.transport_frame.pgn),
        }

        # Add J1939 transport metadata if available
        if self.transport_frame is not None:
            transport_adapter = J1939TransportSparkAdapter(self.transport_frame)
            transport_data = transport_adapter.to_spark_dict()
            result.update(
                {
                    # J1939 transport metadata
                    "source_address": transport_data.get("source_address", 0),
                    "destination_address": transport_data.get("destination_address", 0),
                    "priority": transport_data.get("priority", 6),
                    "is_broadcast": transport_data.get("is_broadcast", False),
                    "frame_count": transport_data.get("frame_count", 1),
                    "expected_length": transport_data.get("expected_length", 0),
                    "transport_protocol_used": transport_data.get("transport_protocol_used", 0),
                }
            )
        else:
            # Default transport metadata
            result.update(
                {
                    "source_address": 0,
                    "destination_address": 0,
                    "priority": 6,
                    "is_broadcast": False,
                    "frame_count": 1,
                    "expected_length": 0,
                    "transport_protocol_used": 0,
                }
            )

        return result

    @staticmethod
    def get_consolidated_spark_schema() -> StructType:
        """Get consolidated Spark schema for J1939 + J1939 transport metadata."""
        return StructType(
            [
                # J1939 application data
                StructField("data_identifier_type", IntegerType(), True),
                StructField("pgn", IntegerType(), True),
                # J1939 transport metadata
                *J1939TransportSparkAdapter.get_transport_metadata_fields(),
            ]
        )

    @classmethod
    def get_null_struct_sql(cls) -> str:
        """
        Generate SQL expression for a null J1939 struct.

        Returns:
            SQL expression like: cast(null as struct<data_identifier:int,...>)
        """
        schema = cls.get_consolidated_spark_schema()
        struct_type_str = _struct_type_to_sql_string(schema)
        return f"cast(null as {struct_type_str})"

    @classmethod
    def get_populated_struct_sql(cls, **field_expressions) -> str:
        """
        Generate SQL expression for populated J1939 struct.

        Args:
            **field_expressions: SQL expressions for each field

        Returns:
            SQL expression like: named_struct('data_identifier', expr1, ...)
        """
        schema = cls.get_consolidated_spark_schema()
        field_pairs = []

        for field in schema.fields:
            field_name = field.name
            if field_name in field_expressions:
                expr = field_expressions[field_name]
            else:
                # Default to null with proper casting
                sql_type = _spark_type_to_sql_string(field.dataType)
                expr = f"cast(null as {sql_type})"

            field_pairs.append(f"'{field_name}'")
            field_pairs.append(expr)

        return f"named_struct({', '.join(field_pairs)})"
