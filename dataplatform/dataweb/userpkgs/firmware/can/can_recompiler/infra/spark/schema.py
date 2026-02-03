"""
Centralized Spark schema definitions using adapters.

This module contains all Spark SQL schema generation logic,
using the adapter pattern to keep domain objects clean.
"""

from pyspark.sql.types import StructType, StructField, IntegerType, LongType, BinaryType

from .adapters import UDSMessageSparkAdapter, J1939ApplicationSparkAdapter
from .frame_types import PartitionMetadata


def get_topological_schema() -> StructType:
    """
    Get unified topological schema using Spark adapters.

    This schema is generated entirely through the infrastructure layer,
    keeping the core domain objects free from Spark dependencies.

    Returns:
        StructType representing the consolidated topological schema
    """
    # Get partition metadata fields from the domain object
    metadata_fields = PartitionMetadata.get_spark_schema().fields

    return StructType(
        [
            # Metadata fields first (matches asset UDF_COLUMNS order)
            *metadata_fields,
            # Propagated timestamp for efficient sorting
            StructField("start_timestamp_unix_us", LongType(), False),
            # CAN arbitration ID from raw frame for debugging and stream_id correlation
            StructField("id", LongType(), False),
            # Protocol payload data (transport or application level)
            StructField("payload", BinaryType(), False),
            StructField("payload_length", IntegerType(), False),
            # Top-level protocol identification for easy querying
            StructField("transport_id", IntegerType(), False),
            StructField("application_id", IntegerType(), False),
            # Consolidated application layer (transport metadata embedded via adapters)
            StructField(
                "application",
                StructType(
                    [
                        # J1939 application with transport metadata (via adapter)
                        StructField(
                            "j1939",
                            J1939ApplicationSparkAdapter.get_consolidated_spark_schema(),
                            True,
                        ),
                        # None application - raw frame data for unclassified frames
                        StructField(
                            "none",
                            StructType(
                                [
                                    StructField("arbitration_id", LongType(), True),
                                    StructField("payload_length", IntegerType(), True),
                                ]
                            ),
                            True,
                        ),
                        # UDS application with ISO-TP transport metadata (via adapter)
                        StructField(
                            "uds", UDSMessageSparkAdapter.get_consolidated_spark_schema(), True
                        ),
                    ]
                ),
                False,
            ),
        ]
    )
