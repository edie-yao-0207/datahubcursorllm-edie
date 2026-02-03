"""
CAN Trace Recompiled

This table contains recompiled CAN frames processed using the can_recompiler library.
Raw CAN frames are classified by transport protocol, reassembled from data link PDUs
into protocol PDUs, and structured for downstream analysis. Each row represents a
recompiled CAN frame with its transport layer, application layer, and payload information.
"""

from dagster import AssetExecutionContext
from dataweb import table
from dataweb.userpkgs.constants import (
    AWSRegion,
    Database,
    InstanceType,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataclasses import replace
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    struct_with_comments,
)

# Define new column types for deduplication timestamps
START_TIMESTAMP_UNIX_US = Column(
    name="start_timestamp_unix_us",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(
        comment="First occurrence timestamp of this payload in microseconds"
    ),
)

END_TIMESTAMP_UNIX_US = Column(
    name="end_timestamp_unix_us",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(
        comment="Last occurrence timestamp of this payload in microseconds"
    ),
)

DUPLICATE_COUNT = Column(
    name="duplicate_count",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Number of duplicate frames with this payload"),
)

PAYLOAD_LENGTH = Column(
    name="payload_length",
    type=DataType.INTEGER,
    nullable=False,
    metadata=Metadata(comment="Length of protocol payload in bytes"),
)

STREAM_ID = Column(
    name="stream_id",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(
        comment="Data-type identifier for joining frames to signal definitions (excludes can_id/interface so same signal matches across ECUs/buses): J1939=hash(app_id,pgn), UDS=hash(app_id,service_id,data_identifier), other=hash(app_id,id) (xxhash64)"
    ),
)

SOURCE_STREAM_ID = Column(
    name="source_stream_id",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(
        comment="Data-source identifier for tracing frames to specific physical source (includes can_id/interface for debugging which ECU/bus): J1939=hash(app_id,can_id,interface), UDS=hash(app_id,can_id,request,interface), other=hash(app_id,id,interface) (xxhash64)"
    ),
)

from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query, format_query
from .dim_device_vehicle_properties import MMYEF_ID
from .fct_sampled_can_frames import CAN_ID


from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    element_at,
    when,
    expr,
)
import pyspark.sql.functions as F
from dataweb.userpkgs.firmware.can.can_recompiler import (
    TransportProtocolType,
    ApplicationProtocolType,
    process_frames_distributed,
)
from dataweb.userpkgs.firmware.can.can_recompiler.protocols.uds.enums import (
    UDSDataIdentifierType,
)
from dataweb.userpkgs.firmware.can.can_recompiler.infra.spark.adapters import (
    UDSMessageSparkAdapter,
    J1939ApplicationSparkAdapter,
)
from dataweb.userpkgs.firmware.can.can_recompiler.infra.spark.trace_processing import (
    generate_application_struct_sql,
)

# =============================================================================
# CONSTANTS
# =============================================================================

# J1939 Transport Protocol PGN constants for multiframe detection
J1939_TP_CM_PGN = 0xEC00  # Connection Management
J1939_TP_DT_PGN = 0xEB00  # Data Transfer
J1939_TP_ACK_PGN = 0xEA00  # Acknowledgment (can be multiframe or single)

# OBD-II Service ID ranges (now unified as UDS)
OBD_REQUEST_SERVICES = (1, 10)  # Services 01-0A
OBD_RESPONSE_SERVICES = (65, 74)  # Services 41-4A


def _configure_spark_session(context: AssetExecutionContext) -> SparkSession:
    """Configure Spark session with dynamic partition sizing based on actual data volume."""
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Only set configurations that are specific to this job or not set at cluster level
    # Most AQE and Delta settings are already optimally configured in cluster config

    # Dynamically calculate optimal shuffle partitions based on actual trace count
    optimal_partitions = _calculate_optimal_partitions(spark, context)
    spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))

    context.log.info(
        f"Set dynamic shuffle partitions to {optimal_partitions} based on data volume"
    )

    return spark


def _calculate_optimal_partitions(
    spark: SparkSession, context: AssetExecutionContext
) -> int:
    """Calculate optimal shuffle partitions based on actual trace count for the date range."""
    try:
        # Get date range for the current partition
        partition_keys = partition_key_ranges_from_context(context)[0]
        date_start = partition_keys[0]
        date_end = partition_keys[-1]

        # Query to get approximate trace count for the date range
        trace_count_query = f"""
        SELECT APPROX_COUNT_DISTINCT(trace_uuid) as trace_count
        FROM product_analytics_staging.fct_sampled_can_frames
        WHERE date BETWEEN '{date_start}' AND '{date_end}'
          AND source_interface != 'j1587'
        """

        result = spark.sql(trace_count_query).collect()
        trace_count = result[0]["trace_count"] if result else 1000  # Default fallback

        # Calculate optimal partitions based on trace count
        # Formula: Start with ~1.2 partitions per trace, but keep within reasonable bounds
        # Min: 200 (for small datasets), Max: 2048 (reasonable upper bound), Default cluster: 4096
        optimal_partitions = max(200, min(2048, int(trace_count * 1.2)))

        context.log.info(
            f"Detected ~{trace_count} traces for date range {date_start} to {date_end}"
        )

        return optimal_partitions

    except Exception as e:
        # Fallback to conservative estimate if query fails
        context.log.warning(
            f"Failed to calculate dynamic partitions: {e}. Using fallback value."
        )
        return 1000  # Conservative default


def _classify_frames(can_frames: DataFrame, classifications_df: DataFrame) -> DataFrame:
    """Classify CAN frames by transport protocol and frame type."""
    return (
        can_frames.alias("base")
        .join(
            classifications_df.alias("cls"),
            [
                col("base.date") == col("cls.date"),
                col("base.org_id") == col("cls.org_id"),
                col("base.device_id") == col("cls.device_id"),
                col("base.trace_uuid") == col("cls.trace_uuid"),
            ],
            "left_outer",
        )
        .select(
            col("base.*"),
            col("cls.mmyef_id"),
            element_at(col("cls.can_id_to_classification_map"), col("base.id")).alias(
                "classification"
            ),
        )
        # Extract fields from the struct instead of repeated map lookups
        .withColumn(
            "base_transport_id", coalesce(col("classification.transport_id"), lit(0))
        )
        .withColumn("pgn", coalesce(col("classification.pgn"), lit(None).cast("long")))
        .withColumn(
            "is_j1939_multiframe",
            coalesce(col("classification.is_j1939_multiframe"), lit(False)),
        )
        .withColumn(
            "is_valid_isotp", coalesce(col("classification.is_valid_isotp"), lit(False))
        )
        .drop("classification")
        # PGN is now extracted from classification map above
        # J1939 Transport Protocol Handling
        # NOTE: KNOWN SEMANTIC INCONSISTENCY - J1939 responses can be single-frame or multi-frame
        # depending on payload size, but we process them differently:
        # - Single-frame responses: Passthrough with full payload
        # - Multi-frame responses: Would need TP.CM/TP.DT/TP.ACK reassembly via UDF
        # For simplicity, we drop transport layer frames and only process complete messages.
        # This means multi-frame J1939 responses will appear as individual TP frames rather
        # than reassembled application messages. This is acceptable since TP frames contain
        # minimal useful application data - the real data is in the reassembled result.
        # Drop J1939 multiframe transport frames immediately after classification extraction
        # This prevents Catalyst from pushing this filter back and re-expanding map lookups
        .filter(
            ~(
                (col("base_transport_id") == TransportProtocolType.J1939.value)
                & (col("is_j1939_multiframe") == True)
            )
        )
        .select(
            "date",
            "trace_uuid",
            "org_id",
            "device_id",
            "source_interface",
            "direction",
            "start_timestamp_unix_us",
            "end_timestamp_unix_us",
            "duplicate_count",
            "payload",
            "id",
            "mmyef_id",
            "base_transport_id",
            "is_j1939_multiframe",
            "is_valid_isotp",
            # Calculate transport_id - simplified without -1 marker since we filtered already
            when(
                col("base_transport_id") == TransportProtocolType.J1939.value,
                lit(
                    TransportProtocolType.NONE.value
                ),  # Single-frame J1939 - passthrough as NONE
            )
            .otherwise(col("base_transport_id"))
            .alias("transport_id"),
        )
        # Calculate all derived columns in a second consolidated projection
        .select(
            "date",
            "trace_uuid",
            "org_id",
            "device_id",
            "source_interface",
            "direction",
            "start_timestamp_unix_us",
            "end_timestamp_unix_us",
            "duplicate_count",
            "payload",
            "id",
            "mmyef_id",
            "base_transport_id",
            "is_j1939_multiframe",
            "is_valid_isotp",
            "transport_id",
            # Calculate isotp_frame_type
            when(
                col("transport_id") == TransportProtocolType.ISOTP.value,
                expr("CAST(CONV(HEX(SUBSTRING(payload, 1, 1)), 16, 10) AS INT) >> 4"),
            )
            .otherwise(lit(-1))
            .alias("isotp_frame_type"),
            # Calculate final_transport_id (reclassify invalid ISOTP as NONE)
            when(
                (col("transport_id") == TransportProtocolType.ISOTP.value)
                & (col("is_valid_isotp") == False),
                lit(TransportProtocolType.NONE.value),
            )
            .otherwise(col("transport_id"))
            .alias("final_transport_id"),
            # Calculate needs_udf_processing
            when(
                # ISOTP multiframe messages (First Frame=1, Consecutive Frame=2, Flow Control=3)
                (col("transport_id") == TransportProtocolType.ISOTP.value)
                & (
                    expr(
                        "CAST(CONV(HEX(SUBSTRING(payload, 1, 1)), 16, 10) AS INT) >> 4"
                    )
                    != 0
                ),
                lit(True),
            )
            .when(
                # All other transport protocols that need UDF (excluding J1939 and ISOTP single-frame)
                (col("transport_id") != TransportProtocolType.NONE.value)
                & (col("transport_id") != TransportProtocolType.ISOTP.value),
                lit(True),
            )
            .otherwise(lit(False))
            .alias("needs_udf_processing"),
            # Calculate is_j1939_single_frame
            (col("base_transport_id") == TransportProtocolType.J1939.value).alias(
                "is_j1939_single_frame"
            ),
        )
        # Final projection to calculate is_isotp_single_frame and clean up
        .select(
            "date",
            "trace_uuid",
            "org_id",
            "device_id",
            "source_interface",
            "direction",
            "start_timestamp_unix_us",
            "end_timestamp_unix_us",
            "duplicate_count",
            "payload",
            "id",
            "mmyef_id",
            "needs_udf_processing",
            "is_j1939_single_frame",
            col("final_transport_id").alias("transport_id"),  # Rename to transport_id
            # Calculate is_isotp_single_frame
            (
                (col("final_transport_id") == TransportProtocolType.ISOTP.value)
                & (col("isotp_frame_type") == 0)
                & (col("is_valid_isotp") == True)
            ).alias("is_isotp_single_frame"),
        )
        # No filter needed here - J1939 multiframe frames already filtered at line 230
    )


def _load_and_classify_frames(
    spark: SparkSession, context: AssetExecutionContext
) -> DataFrame:
    """Load CAN frames and apply transport protocol classifications."""
    # Load base CAN frames
    formatted_query = format_date_partition_query(BASE_QUERY, context)
    context.log.info("Loading base CAN frames...")
    can_frames = spark.sql(formatted_query)

    # Load classifications with tight projection
    context.log.info("Loading classifications for efficient map lookup...")
    classifications_query = format_date_partition_query(CLASSIFICATIONS_QUERY, context)
    classifications_df = spark.sql(classifications_query)

    # Use broadcast hint only for small classification tables
    cls = classifications_df.alias("cls")

    return _classify_frames(can_frames, cls)


def _process_complex_frames(
    classified_frames: DataFrame, context: AssetExecutionContext
) -> DataFrame:
    """Process complex frames that require UDF processing."""
    # Filter frames needing UDF processing
    complex_frames = classified_frames.filter(col("needs_udf_processing") == True).drop(
        "needs_udf_processing", "is_j1939_single_frame", "is_isotp_single_frame"
    )

    # Repartition and sort for temporal ordering (critical for CAN protocol reconstruction)
    # Let AQE determine optimal partitioning, just ensure correct grouping for UDF
    complex_frames_partitioned = (
        complex_frames.repartition(
            "trace_uuid", "transport_id", "source_interface", "mmyef_id"
        )
        .sortWithinPartitions(
            "trace_uuid",
            "transport_id",
            "source_interface",
            "mmyef_id",
            "start_timestamp_unix_us",
        )
        .cache()  # Use simple cache() instead of specific storage level
    )

    context.log.info("Processing complex frames via UDF with optimized partitioning...")

    # Apply distributed processing for frame recompilation
    recompiled_df = process_frames_distributed(complex_frames_partitioned, context)

    # Add processed_by_udf flag to UDF-processed frames
    # stream_id and source_stream_id will be calculated at the end for all frames
    recompiled_df_with_flag = recompiled_df.select(
        "date",
        "trace_uuid",
        "org_id",
        "device_id",
        "source_interface",
        "direction",
        "start_timestamp_unix_us",
        "end_timestamp_unix_us",
        "duplicate_count",
        "id",
        "payload",
        "payload_length",
        "transport_id",
        "application_id",
        "application",
        "mmyef_id",
    ).withColumn(
        "processed_by_udf",
        lit(True),  # All frames processed through this function went through UDF
    )

    complex_frames_partitioned.unpersist()  # unpersist() works for both cache() and persist()
    return recompiled_df_with_flag


def _generate_passthrough_sql(context: AssetExecutionContext) -> str:
    """Generate SQL for combining all passthrough results."""
    # Generate application struct SQL for J1939 single frames
    j1939_application_sql = generate_application_struct_sql(
        "j1939",
        data_identifier_type=f"CAST({ApplicationProtocolType.J1939.value} AS INT)",
        pgn="CAST(CASE WHEN ((id >> 16) & 255) >= 240 THEN (((id >> 16) & 255) << 8) | ((id >> 8) & 255) ELSE ((id >> 16) & 255) << 8 END AS INT)",
        source_address="CAST((id & 255) AS INT)",
        destination_address="CASE WHEN ((id >> 16) & 255) >= 240 THEN 255 ELSE CAST(((id >> 8) & 255) AS INT) END",
        priority="CAST(((id >> 26) & 7) AS INT)",
        is_broadcast="((id >> 16) & 255) >= 240",
        frame_count="1",
        expected_length="LENGTH(payload)",
        transport_protocol_used="0",
    )

    # Get schema for consistent struct generation - must match UDF output schemas
    j1939_schema = J1939ApplicationSparkAdapter.get_consolidated_spark_schema()
    uds_schema = UDSMessageSparkAdapter.get_consolidated_spark_schema()

    # Helper function to generate null application struct
    def _null_application_struct():
        return f"""NAMED_STRUCT(
          'j1939', CAST(NULL AS {j1939_schema.simpleString()}),
          'none', CAST(NULL AS STRUCT<arbitration_id: BIGINT, payload_length: INT>),
          'uds', CAST(NULL AS {uds_schema.simpleString()})
        )"""

    return f"""
    WITH
    -- Pre-calculate ISOTP frame information to eliminate duplication
    isotp_with_parsed_header AS (
      SELECT
        *,
        -- Parse diagnostic payload after stripping ISOTP PCI byte
        -- Single-frame ISOTP format: [PCI byte][service_id][parameters...]
        -- PCI byte format: 0x0X where X is the number of data bytes
        -- BINARY-SAFE: Use CONV+HEX for binary data, not ASCII() which corrupts non-UTF8 bytes
        CAST(CONV(HEX(SUBSTRING(payload, 2, 1)), 16, 10) AS INT) AS service_id,
        -- Calculate actual payload length from PCI byte (0x0X = X data bytes)
        (CAST(CONV(HEX(SUBSTRING(payload, 1, 1)), 16, 10) AS INT) & 15) AS isotp_payload_length,
        -- For single-frame ISOTP, strip the PCI byte to get application data
        -- The actual data stripping will be done later based on service type
        SUBSTRING(payload, 2) AS stripped_payload
      FROM isotp_single_frames_base
      WHERE LENGTH(payload) > 1
    ),

    isotp_with_request_sid AS (
      SELECT
        *,
        -- Pre-calculate request service ID to avoid repeated calculations
        CASE WHEN service_id >= 64 THEN service_id - 64 ELSE service_id END AS request_sid
      FROM isotp_with_parsed_header
    ),

    isotp_with_parsed_bytes AS (
      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction,
        start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
        id, payload, service_id, isotp_payload_length, stripped_payload,
        request_sid, mmyef_id,
        -- Consolidate all service-specific parsing logic into a single STRUCT
        -- This makes the code easier to follow and maintain
        CASE
          -- ========================================================================
          -- OBD-II Service-Only Operations (0x03, 0x04, 0x07, 0x0A)
          -- No parameters, just service ID
          -- ========================================================================
          WHEN request_sid IN (3, 4, 7, 10) THEN NAMED_STRUCT(
            'data_identifier', CAST(NULL AS INT),
            'data_identifier_type', {UDSDataIdentifierType.SERVICE_ONLY.value},
            'final_payload', SUBSTRING(stripped_payload, 2),
            'bytes_to_strip', 1
          )

          -- ========================================================================
          -- OBD-II PID-Based Operations (0x01, 0x02, 0x05, 0x06, 0x08, 0x09)
          -- 1-byte PID parameter
          -- ========================================================================
          WHEN request_sid IN (1, 2, 5, 6, 8, 9) THEN NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 2
                THEN CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type', {UDSDataIdentifierType.OBD_PID.value},
            'final_payload', SUBSTRING(stripped_payload, 3),
            'bytes_to_strip', 2
          )

          -- ========================================================================
          -- UDS 2-Byte DID Operations (0x22, 0x24, 0x2E, 0x2F)
          -- ReadDataByIdentifier, ReadScalingData, WriteData, IOControl
          -- ========================================================================
          WHEN request_sid IN (34, 36, 46, 47) THEN NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 3
                THEN (CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT) << 8) |
                      CAST(CONV(HEX(SUBSTRING(stripped_payload, 3, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type', {UDSDataIdentifierType.DID.value},
            'final_payload', SUBSTRING(stripped_payload, 4),
            'bytes_to_strip', 3
          )

          -- ========================================================================
          -- UDS Local Identifier (0x21)
          -- 1-byte identifier parameter
          -- ========================================================================
          WHEN request_sid = 33 THEN NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 2
                THEN CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type', {UDSDataIdentifierType.UDS_LOCAL_IDENTIFIER.value},
            'final_payload', SUBSTRING(stripped_payload, 3),
            'bytes_to_strip', 2
          )

          -- ========================================================================
          -- UDS Routine Control (0x31)
          -- 2-byte routine identifier parameter
          -- ========================================================================
          WHEN request_sid = 49 THEN NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 3
                THEN (CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT) << 8) |
                      CAST(CONV(HEX(SUBSTRING(stripped_payload, 3, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type', {UDSDataIdentifierType.ROUTINE.value},
            'final_payload', SUBSTRING(stripped_payload, 4),
            'bytes_to_strip', 3
          )

          -- ========================================================================
          -- UDS DTC Operations (0x19)
          -- 1-byte subfunction parameter
          -- ========================================================================
          WHEN request_sid = 25 THEN NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 2
                THEN CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type', {UDSDataIdentifierType.DTC_SUB.value},
            'final_payload', SUBSTRING(stripped_payload, 3),
            'bytes_to_strip', 2
          )

          -- ========================================================================
          -- UDS Periodic Identifier (0x2A)
          -- 2-byte composite parameter (mode + identifier)
          -- ========================================================================
          WHEN request_sid = 42 THEN NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 3
                THEN (CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT) << 8) |
                      CAST(CONV(HEX(SUBSTRING(stripped_payload, 3, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type', {UDSDataIdentifierType.DID.value},
            'final_payload', SUBSTRING(stripped_payload, 4),
            'bytes_to_strip', 3
          )

          -- ========================================================================
          -- UDS Service-Only Operations (0x10, 0x11, 0x14, 0x27, 0x28, 0x3E, 0x85)
          -- No parameters, just service ID
          -- ========================================================================
          WHEN request_sid IN (16, 17, 20, 39, 40, 62, 133) THEN NAMED_STRUCT(
            'data_identifier', CAST(NULL AS INT),
            'data_identifier_type', {UDSDataIdentifierType.SERVICE_ONLY.value},
            'final_payload', SUBSTRING(stripped_payload, 2),
            'bytes_to_strip', 1
          )

          -- ========================================================================
          -- Default: Generic UDS or unknown service
          -- Try to extract second byte if available
          -- ========================================================================
          ELSE NAMED_STRUCT(
            'data_identifier',
              CASE WHEN LENGTH(stripped_payload) >= 2
                THEN CAST(CONV(HEX(SUBSTRING(stripped_payload, 2, 1)), 16, 10) AS INT)
                ELSE CAST(NULL AS INT) END,
            'data_identifier_type',
              CASE
                WHEN service_id >= 16 AND service_id <= 127 THEN {UDSDataIdentifierType.UDS_GENERIC.value}
                ELSE {UDSDataIdentifierType.NONE.value}
              END,
            'final_payload', SUBSTRING(stripped_payload, 2),
            'bytes_to_strip', 1
          )
        END AS parsed_service_data
      FROM isotp_with_request_sid
    ),

    isotp_with_extracted_fields AS (
      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction,
        start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
        id, payload, service_id, isotp_payload_length,
        request_sid, mmyef_id,
        -- Extract fields from the parsed service data struct
        parsed_service_data.data_identifier AS data_identifier,
        parsed_service_data.data_identifier_type AS data_identifier_type,
        parsed_service_data.final_payload AS stripped_payload,
        parsed_service_data.bytes_to_strip AS bytes_to_strip,
        -- Pre-calculate response flag and final service_id
        CAST(service_id >= 64 AND service_id <= 127 AS BOOLEAN) AS is_response,
        CAST(request_sid AS INT) AS final_service_id,
        -- Calculate composite data_identifier based on parameter size
        CASE
          -- Service-only operations (no data_identifier): just use service ID
          WHEN parsed_service_data.data_identifier IS NULL THEN
            CAST(request_sid AS BIGINT)
          -- 2-byte parameter services: service in bits 16-23
          WHEN request_sid IN (34, 36, 42, 46, 47, 49) THEN
            CAST((request_sid << 16) + COALESCE(parsed_service_data.data_identifier, 0) AS BIGINT)
          -- 1-byte parameter services: service in bits 8-15
          ELSE
            CAST((request_sid << 8) + COALESCE(parsed_service_data.data_identifier, 0) AS BIGINT)
        END AS composite_data_identifier,
        -- Unified diagnostic protocol approach: all ISOTP diagnostic traffic is UDS
        -- This simplifies signal catalog management by treating OBD-II and UDS uniformly
        {ApplicationProtocolType.UDS.value} AS detected_application_id
      FROM isotp_with_parsed_bytes
    ),

    regular_passthrough_formatted AS (
      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction,
        start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
        id, payload,
        LENGTH(payload) AS payload_length,
        {TransportProtocolType.NONE.value} AS transport_id,
        {ApplicationProtocolType.NONE.value} AS application_id,
        -- For NONE frames, populate the none struct with arbitration_id and payload_length
        NAMED_STRUCT(
          'j1939', CAST(NULL AS {j1939_schema.simpleString()}),
          'none', NAMED_STRUCT(
            'arbitration_id', id,
            'payload_length', LENGTH(payload)
          ),
          'uds', CAST(NULL AS {uds_schema.simpleString()})
        ) AS application,
        mmyef_id,
        FALSE AS processed_by_udf
      FROM regular_passthrough_base
    ),

    j1939_single_formatted AS (
      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction,
        start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
        id, payload,
        LENGTH(payload) AS payload_length,
        {TransportProtocolType.J1939.value} AS transport_id,
        {ApplicationProtocolType.J1939.value} AS application_id,
        {j1939_application_sql} AS application,
        mmyef_id,
        FALSE AS processed_by_udf
      FROM j1939_single_frames_base
    ),

    isotp_single_formatted AS (
      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction,
        start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
        id, stripped_payload AS payload,
        -- Correct payload_length after stripping service + PID/DID headers
        isotp_payload_length - bytes_to_strip AS payload_length,
        {TransportProtocolType.ISOTP.value} AS transport_id,
        detected_application_id AS application_id,
        -- Unified diagnostic protocol: all diagnostic traffic uses UDS structure
        NAMED_STRUCT(
          'j1939', CAST(NULL AS {j1939_schema.simpleString()}),
          'none', CAST(NULL AS STRUCT<arbitration_id: BIGINT, payload_length: INT>),
          'uds', NAMED_STRUCT(
            'data_identifier', data_identifier,
            'data_identifier_type', data_identifier_type,
            'is_negative_response', FALSE,
            'is_response', is_response,
            'service_id', final_service_id,
            'composite_data_identifier', composite_data_identifier,
            'source_address', CAST(id AS INT),  -- Must cast: id is BIGINT but schema expects INT
            'frame_count', 1,
            'expected_length', isotp_payload_length - bytes_to_strip  -- Length of stripped application data
          )
        ) AS application,
        mmyef_id,
        FALSE AS processed_by_udf
      FROM isotp_with_extracted_fields
    ),

    -- Combine all frames from different sources
    combined_frames AS (
      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction, start_timestamp_unix_us,
        end_timestamp_unix_us, duplicate_count,
        id, payload, payload_length, transport_id, application_id, application, mmyef_id, processed_by_udf
      FROM recompiled_frames

      UNION ALL

      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction, start_timestamp_unix_us,
        end_timestamp_unix_us, duplicate_count,
        id, payload, payload_length, transport_id, application_id, application, mmyef_id, processed_by_udf
      FROM regular_passthrough_formatted

      UNION ALL

      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction, start_timestamp_unix_us,
        end_timestamp_unix_us, duplicate_count,
        id, payload, payload_length, transport_id, application_id, application, mmyef_id, processed_by_udf
      FROM j1939_single_formatted

      UNION ALL

      SELECT
        trace_uuid, date, org_id, device_id, source_interface, direction,
        start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
        id, payload, payload_length, transport_id, application_id, application, mmyef_id, processed_by_udf
      FROM isotp_single_formatted
    )

    -- =====================================================================================
    -- Calculate stream_id and source_stream_id
    -- =====================================================================================
    SELECT
      date, trace_uuid, org_id, device_id, source_interface, direction,
      start_timestamp_unix_us, end_timestamp_unix_us, duplicate_count,
      id, 
      -- Trim all payloads to declared length to ensure consistent buffer sizes for decoding
      SUBSTRING(payload, 1, payload_length) AS payload,
      payload_length, transport_id, application_id, application,
      -- stream_id: Protocol-specific data type identifier
      CASE
        WHEN application_id = {ApplicationProtocolType.J1939.value} THEN
          XXHASH64(
            CAST({ApplicationProtocolType.J1939.value} AS BIGINT),
            CAST(application.j1939.pgn AS BIGINT)
          )
        WHEN application_id = {ApplicationProtocolType.UDS.value} THEN
          XXHASH64(
            CAST({ApplicationProtocolType.UDS.value} AS BIGINT),
            CAST(COALESCE(application.uds.composite_data_identifier, -1) AS BIGINT)
          )
        ELSE
          -- For NONE/unclassified frames, use CAN ID as the data identifier
          XXHASH64(
            CAST(COALESCE(application_id, {ApplicationProtocolType.NONE.value}) AS BIGINT),
            CAST(id AS BIGINT)
          )
      END AS stream_id,
      -- source_stream_id: Protocol-specific unique source identifier including interface
      CASE
        WHEN application_id = {ApplicationProtocolType.J1939.value} THEN
          XXHASH64(
            CAST({ApplicationProtocolType.J1939.value} AS BIGINT),
            CAST(id AS BIGINT),
            source_interface
          )
        WHEN application_id = {ApplicationProtocolType.UDS.value} THEN
          XXHASH64(
            CAST({ApplicationProtocolType.UDS.value} AS BIGINT),
            CAST(id AS BIGINT),
            CAST(COALESCE(application.uds.composite_data_identifier, -1) AS BIGINT),
            source_interface
          )
        ELSE
          XXHASH64(
            CAST(COALESCE(application_id, {ApplicationProtocolType.NONE.value}) AS BIGINT),
            CAST(id AS BIGINT),
            source_interface
          )
      END AS source_stream_id,
      mmyef_id,
      processed_by_udf
    FROM combined_frames
    """


def _process_passthrough_frames(
    classified_frames: DataFrame, context: AssetExecutionContext
):
    """Process passthrough frames that don't need UDF processing."""
    # Split passthrough frames into three categories
    regular_passthrough = classified_frames.filter(
        (col("needs_udf_processing") == False)
        & (col("is_j1939_single_frame") == False)
        & (col("is_isotp_single_frame") == False)
    ).drop("needs_udf_processing", "is_j1939_single_frame", "is_isotp_single_frame")

    j1939_single_frames = classified_frames.filter(
        col("is_j1939_single_frame") == True
    ).drop("needs_udf_processing", "is_j1939_single_frame", "is_isotp_single_frame")

    isotp_single_frames = classified_frames.filter(
        col("is_isotp_single_frame") == True
    ).drop("needs_udf_processing", "is_j1939_single_frame", "is_isotp_single_frame")

    # Register temporary views for SQL processing
    regular_passthrough.createOrReplaceTempView("regular_passthrough_base")
    j1939_single_frames.createOrReplaceTempView("j1939_single_frames_base")
    isotp_single_frames.createOrReplaceTempView("isotp_single_frames_base")


APPLICATION_STRUCT = struct_with_comments(
    (
        "j1939",
        struct_with_comments(
            # Application layer data
            (
                "pgn",
                DataType.INTEGER,
                "Parameter Group Number (serves as data identifier)",
            ),
            ("data_identifier_type", DataType.INTEGER, "J1939 data identifier type"),
            # Transport layer metadata
            ("source_address", DataType.INTEGER, "J1939 source address"),
            ("destination_address", DataType.INTEGER, "J1939 destination address"),
            ("priority", DataType.INTEGER, "Message priority (0-7)"),
            ("is_broadcast", DataType.BOOLEAN, "Broadcast message flag"),
            ("frame_count", DataType.INTEGER, "Number of CAN frames in message"),
            ("expected_length", DataType.INTEGER, "Expected J1939 message length"),
            (
                "transport_protocol_used",
                DataType.INTEGER,
                "Transport protocol type used",
            ),
        ),
        "J1939 application data with transport metadata",
    ),
    (
        "none",
        struct_with_comments(
            ("arbitration_id", DataType.LONG, "CAN arbitration ID"),
            ("payload_length", DataType.INTEGER, "Length of payload"),
        ),
        "Unclassified frame data",
    ),
    (
        "uds",
        struct_with_comments(
            # Application layer data
            ("data_identifier", DataType.INTEGER, "UDS data identifier"),
            (
                "data_identifier_type",
                DataType.INTEGER,
                f"Data identifier type ({UDSDataIdentifierType.NONE.value}=NONE, {UDSDataIdentifierType.DID.value}=DID, {UDSDataIdentifierType.NRC.value}=NRC, {UDSDataIdentifierType.DTC_SUB.value}=DTC_SUB, {UDSDataIdentifierType.UDS_GENERIC.value}=UDS_GENERIC, {UDSDataIdentifierType.UDS_LOCAL_IDENTIFIER.value}=UDS_LOCAL_IDENTIFIER, {UDSDataIdentifierType.SERVICE_ONLY.value}=SERVICE_ONLY, {UDSDataIdentifierType.ROUTINE.value}=ROUTINE, {UDSDataIdentifierType.OBD_PID.value}=OBD_PID)",
            ),
            ("is_negative_response", DataType.BOOLEAN, "Negative response flag"),
            ("is_response", DataType.BOOLEAN, "Response message flag"),
            ("service_id", DataType.INTEGER, "UDS service identifier"),
            (
                "composite_data_identifier",
                DataType.LONG,
                "Composite data identifier: service-only=service, 1-byte param=(service<<8)+param, 2-byte param=(service<<16)+param",
            ),
            # ISO-TP transport metadata
            ("source_address", DataType.INTEGER, "ISO-TP source address"),
            ("frame_count", DataType.INTEGER, "Number of CAN frames in message"),
            ("expected_length", DataType.INTEGER, "Expected ISO-TP message length"),
        ),
        "UDS application data with ISO-TP transport metadata",
    ),
)

APPLICATION = Column(
    name="application",
    type=APPLICATION_STRUCT,
    nullable=False,
    metadata=Metadata(
        comment="Application layer protocol data with embedded transport metadata"
    ),
)

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TRACE_UUID,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(ColumnType.SOURCE_INTERFACE.value, nullable=False),
    replace(ColumnType.DIRECTION.value, nullable=False),
    START_TIMESTAMP_UNIX_US,
    END_TIMESTAMP_UNIX_US,
    DUPLICATE_COUNT,
    CAN_ID,
    ColumnType.PAYLOAD,
    PAYLOAD_LENGTH,
    ColumnType.TRANSPORT_ID,
    ColumnType.APPLICATION_ID,
    APPLICATION,
    STREAM_ID,
    SOURCE_STREAM_ID,
    MMYEF_ID,
    Column(
        name="processed_by_udf",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="True if frame was processed through UDF (multi-frame), False if processed through SQL (single-frame)"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

BASE_QUERY = """
WITH deduplicated_frames AS (
    SELECT
        can.date,
        can.trace_uuid,
        can.org_id,
        can.device_id,
        can.source_interface,
        can.direction,
        can.payload,
        can.id,
        MIN(can.timestamp_unix_us) AS start_timestamp_unix_us,
        MAX(can.timestamp_unix_us) AS end_timestamp_unix_us,
        COUNT(*) AS duplicate_count
    FROM product_analytics_staging.fct_sampled_can_frames can
    WHERE can.date BETWEEN "{date_start}" AND "{date_end}"
      AND can.payload IS NOT NULL
      AND length(can.payload) > 0
      AND can.source_interface != 'j1587'
    GROUP BY
        can.date,
        can.trace_uuid,
        can.org_id,
        can.device_id,
        can.source_interface,
        can.direction,
        can.payload,
        can.id
)
SELECT
    date,
    trace_uuid,
    org_id,
    device_id,
    source_interface,
    direction,
    start_timestamp_unix_us,
    end_timestamp_unix_us,
    duplicate_count,
    payload,
    id
FROM deduplicated_frames
"""

CLASSIFICATIONS_QUERY = """
SELECT
    date,
    org_id,
    device_id,
    trace_uuid,
    mmyef_id,
    can_id_to_classification_map
FROM {product_analytics_staging}.map_can_classifications
WHERE date BETWEEN "{date_start}" AND "{date_end}"
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table contains recompiled CAN frames processed using the can_recompiler library with enhanced signal-catalog-informed protocol detection. "
        "Raw CAN frames are classified by transport protocol (J1939, UDS, etc.) using both addressing-based detection and vehicle-specific signal catalog lookup. "
        "Frames are reassembled from data link PDUs into protocol PDUs and enriched with application layer information. "
        "Enhanced detection leverages trace-level optimization to efficiently identify OEM-specific diagnostic traffic by pre-computing diagnostic CAN IDs "
        "for each vehicle (make/model/year/engine) and reusing this context across all frames within a trace, eliminating millions of redundant lookups. "
        "Each row represents a recompiled CAN frame with transport/application classification and structured payload data.",
        row_meaning="Each row represents a recompiled CAN frame with enhanced transport protocol detection and application layer information.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    # CAN recording only enabled in the US currently
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SAMPLED_CAN_FRAMES),
        AnyUpstream(ProductAnalyticsStaging.MAP_CAN_CLASSIFICATIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_can_trace_recompiled(context: AssetExecutionContext) -> DataFrame:
    """
    Recompile raw CAN frames using pre-computed protocol classifications with multi-protocol optimization and payload deduplication.

    This asset processes raw CAN frames from fct_sampled_can_frames and recompiles them
    using the can_recompiler library with enhanced protocol detection that leverages
    pre-computed classifications from map_can_classifications. Frames are classified
    by transport protocol and reassembled from data link PDUs into protocol PDUs with
    application layer information.

    **Payload Deduplication Optimization:**
    - Groups duplicate frames by payload and creates time ranges (start_timestamp_unix_us, end_timestamp_unix_us)
    - Reduces processing volume by 20-30% by eliminating duplicate payload processing
    - Preserves duplicate_count for downstream analysis

    **Multi-Protocol Optimizations:**

    **J1939 Optimization:**
    - Single-frame J1939: Optimized passthrough (no UDF overhead)
    - Multiframe J1939: Transport protocol frames (TP.CM/TP.DT/TP.ACK) processed via UDF

    **ISOTP Optimization (UDS):**
    - Single-frame ISOTP: Optimized passthrough with header stripping and service-ID-based protocol detection
    - Protocol Classification: UDS (services 01-0A, 41-4A for OBD-II, 21, 22, etc. for UDS)
    - Multiframe ISOTP: First/Consecutive frames processed via UDF

    Processing Strategy:
    1. Load base CAN frames with pre-computed protocol classifications
    2. Detect frame types: J1939 (PGN-based), ISOTP (header-based)
    3. Split into four paths:
       - Complex/multiframe (UDF processing)
       - J1939 single-frame (passthrough with PGN extraction)
       - ISOTP single-frame (passthrough with header stripping and service-ID classification)
       - Other (standard passthrough)
    4. Process only complex frames with Python pandas UDF
    5. Union results with proper application structures

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Spark DataFrame containing recompiled CAN frames
    """
    # Initialize optimized Spark session with dynamic partitioning
    spark = _configure_spark_session(context)

    # Load and classify CAN frames with optimized processing
    classified_frames = _load_and_classify_frames(spark, context)

    # CRITICAL: Persist classified frames to prevent 4x table scans in UNION
    # This ensures all processing paths reuse the same classified data
    classified_frames.cache()

    # Trigger materialization to ensure cache is populated
    frame_count = classified_frames.count()
    total_duplicates = classified_frames.select(F.sum("duplicate_count")).collect()[0][
        0
    ]
    original_frame_count = (
        frame_count + (total_duplicates - frame_count)
        if total_duplicates
        else frame_count
    )
    deduplication_ratio = (
        (1 - frame_count / original_frame_count) * 100
        if original_frame_count > 0
        else 0
    )

    context.log.info(
        f"Classified and cached {frame_count:,} unique CAN frames for processing"
    )
    context.log.info(
        f"Deduplication reduced {original_frame_count:,} frames to {frame_count:,} unique payloads ({deduplication_ratio:.1f}% reduction)"
    )

    # Process complex frames requiring UDF
    recompiled_df = _process_complex_frames(classified_frames, context)
    recompiled_df.createOrReplaceTempView("recompiled_frames")

    # Process passthrough frames with optimizations
    _process_passthrough_frames(classified_frames, context)

    # Generate and execute final SQL to combine all results
    final_query = _generate_passthrough_sql(context)
    result = spark.sql(final_query)

    # CRITICAL: Cache and materialize result while classified_frames cache is still available
    # This ensures the query executes with the cached data before we clean up
    result.cache()
    # Trigger materialization efficiently - only reads one row to force cache population
    result.take(1)  # Much faster than count() for large datasets
    context.log.info("Materialized and cached final recompiled frames result")

    # Now safe to clean up intermediate cached data
    classified_frames.unpersist()

    return result
