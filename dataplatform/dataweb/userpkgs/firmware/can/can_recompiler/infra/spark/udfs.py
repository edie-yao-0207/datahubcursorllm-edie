"""
Spark UDF utilities for CAN protocol processing.

This module provides standardized UDF registration functions that can be used
across multiple DataWeb assets for consistent protocol detection logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def register_detect_protocol_udf(spark: SparkSession) -> None:
    """
    Register the DETECT_PROTOCOL UDF for SQL usage in Spark.

    This function registers a UDF that wraps the can_recompiler detect_protocol
    function for addressing-based transport protocol detection. The UDF can then
    be used in SQL queries as DETECT_PROTOCOL(can_id).

    Args:
        spark: Spark session to register the UDF with

    Example:
        >>> from can_recompiler.infra.spark.udfs import register_detect_protocol_udf
        >>> spark = SparkSession.builder.getOrCreate()
        >>> register_detect_protocol_udf(spark)
        >>> # Now you can use DETECT_PROTOCOL(can_id) in SQL queries
    """

    def detect_protocol_udf(arbitration_id: int) -> int:
        """
        UDF wrapper for detect_protocol providing addressing-based detection fallback.

        This UDF is used when signal catalog-based classification is not available
        or as a fallback when diagnostic mappings don't have entries for specific CAN IDs.

        Args:
            arbitration_id: CAN frame arbitration ID

        Returns:
            Transport protocol type as integer (ISOTP=1, J1939=2, NONE=0)
        """
        # Import inside UDF to avoid Spark serialization issues
        from can_recompiler import detect_protocol

        # Perform addressing-based detection
        transport_type = detect_protocol(arbitration_id)
        return transport_type.value

    # Register the UDF for SQL usage
    spark.udf.register("DETECT_PROTOCOL", detect_protocol_udf, IntegerType())
