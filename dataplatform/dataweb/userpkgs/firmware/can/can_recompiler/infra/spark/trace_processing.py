"""
Spark-based CAN trace processing utilities.

This module contains DataFrame creation, distributed processing, and UDF factory 
functions for CAN trace recompilation workflows. These utilities handle the 
Spark-specific aspects of CAN frame processing.
"""

import pandas as pd
from typing import List, Callable, Any
from pyspark.sql import DataFrame

# Import adapters for SQL generation
from .adapters import UDSMessageSparkAdapter, J1939ApplicationSparkAdapter
from ...core.enums import TransportProtocolType, ApplicationProtocolType

# Type hint for Dagster context - use Any to avoid import resolution issues
DagsterContext = Any


def generate_application_struct_sql(active_protocol: str, **protocol_data) -> str:
    """
    Generate complete application struct SQL with one active protocol and others as null structs.

    This replaces hardcoded SQL by using adapter methods to generate proper struct expressions.
    Active protocol gets populated fields, inactive protocols become null structs.
    The 'none' field is always null regardless of active_protocol.

    Args:
        active_protocol: "none", "j1939", or "uds"
        **protocol_data: SQL expressions for the active protocol's fields (ignored for "none")

    Returns:
        Complete SQL for application struct

    Example:
        >>> generate_application_struct_sql("uds", service_id="34", data_identifier="cast(0x123 as int)")
        "named_struct('j1939', cast(null as struct<...>), 'none', cast(null as ...), ...)"
    """
    # Generate SQL for each protocol - either populated or null struct
    if active_protocol == "uds":
        uds_sql = UDSMessageSparkAdapter.get_populated_struct_sql(**protocol_data)
        j1939_sql = J1939ApplicationSparkAdapter.get_null_struct_sql()
    elif active_protocol == "j1939":
        uds_sql = UDSMessageSparkAdapter.get_null_struct_sql()
        j1939_sql = J1939ApplicationSparkAdapter.get_populated_struct_sql(**protocol_data)
    else:  # active_protocol == "none" or unknown
        uds_sql = UDSMessageSparkAdapter.get_null_struct_sql()
        j1939_sql = J1939ApplicationSparkAdapter.get_null_struct_sql()

    # Generate 'none' struct - always null (user preference: no struct with null fields)
    none_sql = "cast(null as struct<arbitration_id: bigint, payload_length: int>)"

    # Assemble complete application struct (matches schema field order)
    return f"""named_struct(
        'j1939', {j1939_sql},
        'none', {none_sql},
        'uds', {uds_sql}
    )"""


def create_passthrough_dataframe(none_df: DataFrame) -> DataFrame:
    """
    Create efficient passthrough DataFrame for None frames without using processor.

    This function handles CAN frames that don't match any known transport protocol
    by creating a basic representation with minimal processing (no pandas, no processor).

    Args:
        none_df: Spark DataFrame containing unclassified frames

    Returns:
        Spark DataFrame with passthrough frame data matching new schema
    """

    # Use enum values for None frames
    none_transport_id = TransportProtocolType.NONE.value
    none_application_id = ApplicationProtocolType.NONE.value

    return none_df.selectExpr(
        "trace_uuid",
        "date",
        "CAST(org_id AS bigint) AS org_id",
        "CAST(device_id AS bigint) AS device_id",
        "source_interface",
        "direction",
        "start_timestamp_unix_us",
        "end_timestamp_unix_us",
        "duplicate_count",
        "payload",
        "CAST(length(payload) AS int) AS payload_length",
        f"{none_transport_id} AS transport_id",
        f"{none_application_id} AS application_id",
        f"{generate_application_struct_sql('none')} AS application",
        "mmyef_id",
    )


def process_frames_distributed(non_none_df: DataFrame, context: DagsterContext) -> DataFrame:
    """
    Process CAN frames using distributed applyInPandas processing.

    Args:
        non_none_df: Spark DataFrame with non-None frames needing processor
        context: Dagster asset context

    Returns:
        Spark DataFrame with recompiled frames using topological schema
    """
    context.log.info("Recompiling non-NONE transport protocol frames using applyInPandas...")

    # Import topological schema for consistent structure
    from can_recompiler import get_topological_schema

    # Create the decode UDF with topological schema
    decode_udf = create_decode_traces_udf()

    # Normal distributed processing using applyInPandas for non-None frames only
    # Use the library's schema definition for consistency (includes deduplication columns)
    processor_schema = get_topological_schema()

    recompiled_df = non_none_df.groupBy(
        "trace_uuid", "transport_id", "source_interface", "mmyef_id"
    ).applyInPandas(decode_udf, schema=processor_schema)

    # Post-process type casting for schema alignment (much more efficient than in-UDF casting)
    # This handles primitive type mismatches between processor output and target schema
    context.log.info("Applying efficient post-processing type alignment...")

    recompiled_df_aligned = recompiled_df.selectExpr(
        "trace_uuid",
        "date",
        "CAST(org_id AS bigint) AS org_id",
        "CAST(device_id AS bigint) AS device_id",
        "source_interface",
        "CAST(direction AS int) AS direction",
        "start_timestamp_unix_us",
        "end_timestamp_unix_us",
        "duplicate_count",
        "CAST(id AS bigint) AS id",
        "payload",
        "CAST(payload_length AS int) AS payload_length",
        "CAST(transport_id AS int) AS transport_id",
        "CAST(application_id AS int) AS application_id",
        "application",
        "mmyef_id",
    )

    return recompiled_df_aligned


def create_decode_traces_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """
    Create a pandas UDF function for decoding CAN traces.

    This factory function creates a UDF that uses the topological schema structure
    from the new can_recompiler processor.

    Returns:
        Function suitable for use as a pandas UDF in Spark
    """

    def decode_traces_in_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Pandas UDF to decode CAN frames using can_recompiler library.

        This function processes a group of CAN frames (typically from the same trace)
        and decodes them using the can_recompiler library. It handles:
        - Frame sorting by timestamp
        - CAN frame processing via CANFrameProcessor
        - Data sanitization and schema enforcement
        - Partition column preservation

        Args:
            pdf: Pandas DataFrame containing CAN frames for processing

        Returns:
            Pandas DataFrame with decoded CAN frame data matching COLUMNS schema
        """
        # Import the new SparkCANProtocolProcessor
        from can_recompiler import SparkCANProtocolProcessor

        # Sort frames by timestamp for proper sequence processing
        pdf = pdf.sort_values("start_timestamp_unix_us")

        records = []
        processor = SparkCANProtocolProcessor()

        # Process each frame
        for _, row in pdf.iterrows():
            # Convert pandas row to dict for new processor
            row_dict = row.to_dict()

            parsed_dict = processor.process_frame_to_dict(row_dict)
            if parsed_dict is None:
                continue

            # Use processor output directly - it returns fresh dicts with nested struct data
            # No copy needed since processor creates new dict instances
            # No JSON conversion needed since schema expects struct types
            records.append(parsed_dict)

        # Create DataFrame from processor results
        if not records:
            # Return empty DataFrame with proper schema to prevent PyArrow mismatch
            # Create empty DataFrame with all required columns
            empty_data = {
                "date": pd.Series([], dtype="string"),
                "trace_uuid": pd.Series([], dtype="string"),
                "org_id": pd.Series([], dtype="Int64"),
                "device_id": pd.Series([], dtype="Int64"),
                "source_interface": pd.Series([], dtype="string"),
                "direction": pd.Series([], dtype="Int32"),
                "mmyef_id": pd.Series([], dtype="Int64"),
                "end_timestamp_unix_us": pd.Series([], dtype="Int64"),
                "duplicate_count": pd.Series([], dtype="Int64"),
                "start_timestamp_unix_us": pd.Series([], dtype="Int64"),
                "id": pd.Series([], dtype="Int64"),
                "payload": pd.Series([], dtype="object"),
                "payload_length": pd.Series([], dtype="Int32"),
                "transport_id": pd.Series([], dtype="Int32"),
                "application_id": pd.Series([], dtype="Int32"),
                "application": pd.Series([], dtype="object"),
            }
            return pd.DataFrame(empty_data)

        return pd.DataFrame(records)

    return decode_traces_in_pandas
