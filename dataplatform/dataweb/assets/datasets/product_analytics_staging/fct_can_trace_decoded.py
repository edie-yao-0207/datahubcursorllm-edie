"""
fct_can_trace_decoded

Fact table for decoded CAN signals with efficient broadcast join approach:
1. Broadcast small signal definition tables to all workers
2. Build per-frame rules_array via in-memory joins (no row explosion)  
3. Apply transform decoder over rules_array

This eliminates intermediate table shuffles while preserving all DBC decoding logic.
"""

from dagster import AssetExecutionContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    broadcast,
    expr,
)
from pyspark.sql import functions as F
from dataweb import table
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
    InstanceType,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    struct_with_comments,
    array_of,
)
from dataweb.userpkgs.firmware.can.can_recompiler import (
    ApplicationProtocolType,
    EndiannessType,
    SignType,
    DecodeReasonType,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.query import (
    format_query,
    create_run_config_overrides,
    format_date_partition_query,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE

from .fct_can_trace_recompiled import (
    APPLICATION,
    START_TIMESTAMP_UNIX_US,
    END_TIMESTAMP_UNIX_US,
    DUPLICATE_COUNT,
    PAYLOAD_LENGTH,
    STREAM_ID,
    SOURCE_STREAM_ID,
)
from .fct_sampled_can_frames import CAN_ID
from .dim_device_vehicle_properties import MMYEF_ID


DECODED_SIGNALS_ARRAY = array_of(
    struct_with_comments(
        (
            "signal_catalog_id",
            DataType.LONG,
            "Unique identifier for the signal definition in catalog",
        ),
        (
            "data_identifier",
            DataType.LONG,
            "Compound data identifier for signal matching",
        ),
        ("obd_value", DataType.LONG, "OBD-specific value for legacy compatibility"),
        ("bit_start", DataType.INTEGER, "Starting bit position in the payload"),
        ("bit_length", DataType.INTEGER, "Length of the signal in bits"),
        ("endian", DataType.LONG, "Endianness type enum value (1=LITTLE, 2=BIG)"),
        ("sign", DataType.LONG, "Sign type enum value (1=SIGNED, 2=UNSIGNED)"),
        ("scale", DataType.DOUBLE, "Scaling factor for the decoded value"),
        ("offset", DataType.DOUBLE, "Offset applied to the decoded value"),
        ("minimum", DataType.DOUBLE, "Minimum valid value for the signal"),
        ("maximum", DataType.DOUBLE, "Maximum valid value for the signal"),
        ("unit", DataType.STRING, "Unit of measurement for the signal"),
        ("source_id", DataType.INTEGER, "Source system identifier"),
        ("decoded_value", DataType.DOUBLE, "The decoded numeric value of the signal"),
        (
            "can_apply_rule",
            DataType.BOOLEAN,
            "Whether the decoding rule could be applied successfully",
        ),
        ("decode_reason", DataType.INTEGER, "Reason code for decode success/failure"),
        (
            "is_out_of_range",
            DataType.BOOLEAN,
            "Whether the decoded value is out of valid range",
        ),
        (
            "is_standard",
            DataType.BOOLEAN,
            "Whether this is a standard signal definition",
        ),
        (
            "is_broadcast",
            DataType.BOOLEAN,
            "Whether this signal uses broadcast addressing",
        ),
    ),
    contains_null=False,
)

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TRACE_UUID,
    START_TIMESTAMP_UNIX_US,
    END_TIMESTAMP_UNIX_US,
    DUPLICATE_COUNT,
    CAN_ID,
    ColumnType.TRANSPORT_ID,
    ColumnType.APPLICATION_ID,
    ColumnType.SOURCE_INTERFACE,
    ColumnType.PAYLOAD,
    PAYLOAD_LENGTH,
    STREAM_ID,
    SOURCE_STREAM_ID,
    APPLICATION,
    MMYEF_ID,
    Column(
        name="decoded_signals",
        type=DECODED_SIGNALS_ARRAY,
        nullable=False,
        metadata=Metadata(
            comment="Array of decoded signal values from the CAN frame payload."
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

# Queries
RECOMPILED_BASE_QUERY = """
SELECT
    date,
    org_id,
    device_id,
    trace_uuid,
    start_timestamp_unix_us,
    end_timestamp_unix_us,
    duplicate_count,
    id,
    transport_id,
    application_id,
    source_interface,
    payload,
    payload_length,
    stream_id,
    source_stream_id,
    application,
    mmyef_id
FROM {product_analytics_staging}.fct_can_trace_recompiled
WHERE date BETWEEN '{date_start}' AND '{date_end}'
"""

SIGNAL_DEFINITIONS_QUERY = """
SELECT
    signal_catalog_id,
    mmyef_id,
    stream_id,
    application_id,
    data_identifier,
    obd_value,
    bit_start,
    bit_length,
    endian,
    sign,
    scale,
    offset,
    minimum,
    maximum,
    unit,
    source_id,
    passive_response_mask,
    is_standard,
    is_broadcast
FROM {product_analytics_staging}.dim_combined_signal_definitions
"""

# Frame keys for consistent selection across functions
FRAME_KEYS = [
    "date",
    "org_id",
    "device_id",
    "trace_uuid",
    "start_timestamp_unix_us",
    "end_timestamp_unix_us",
    "duplicate_count",
    "id",
    "transport_id",
    "application_id",
    "source_interface",
    "payload",
    "payload_length",
    "stream_id",
    "source_stream_id",
    "application",
    "mmyef_id",
]


def _build_rules_array_with_direct_broadcast(
    context, recompiled_frames: DataFrame, signal_definitions: DataFrame
) -> DataFrame:
    """
    Build rules_array per frame using direct broadcast of signal definitions.

    This approach:
    1. Broadcasts the entire signal definitions table once (no intermediate shuffles)
    2. Performs a single join with complex conditions for both vehicle-specific and universal signals
    3. Groups by frame to collect all matching rules into rules_array

    Much more efficient than creating intermediate lookup tables.
    """

    # Reduce columns early for better performance
    rule_cols = [
        "signal_catalog_id",
        "data_identifier",
        "obd_value",
        "bit_start",
        "bit_length",
        "endian",
        "sign",
        "scale",
        "offset",
        "minimum",
        "maximum",
        "unit",
        "source_id",
        "passive_response_mask",
        "is_standard",
        "is_broadcast",
    ]

    # Broadcast the entire signal definitions table (no shuffles needed)
    signal_defs_broadcast = broadcast(
        signal_definitions.select(*rule_cols, "mmyef_id", "stream_id", "application_id")
    )

    context.log.info("Broadcasting signal definitions directly")

    # Filter out UDS requests - only decode responses which contain actual signal data
    # UDS requests are commands (e.g., "read DID 0x92F6") with no signal values to decode
    frames_to_decode = recompiled_frames.select(*FRAME_KEYS).filter(
        (col("application_id") != ApplicationProtocolType.UDS.value)
        | (col("application.uds.is_response") == True)
    )

    # LEFT OUTER join - preserve all frames, even those without signal matches
    frames_with_all_signals = (
        frames_to_decode.alias("frames")
        .join(
            signal_defs_broadcast.alias("signals"),
            # Vehicle-specific match
            (
                (col("frames.mmyef_id") == col("signals.mmyef_id"))
                & (col("frames.stream_id") == col("signals.stream_id"))
                & (col("frames.application_id") == col("signals.application_id"))
            )
            |
            # OR Universal match
            (
                (col("frames.stream_id") == col("signals.stream_id"))
                & (col("frames.application_id") == col("signals.application_id"))
                & (col("signals.is_standard") == True)
                & col("signals.mmyef_id").isNull()
            ),
            how="left",  # LEFT join preserves all frames
        )
        .select(
            *[col(f"frames.{c}").alias(c) for c in FRAME_KEYS],
            *[col(f"signals.{c}") for c in rule_cols],
        )
    )

    # Group by frame and collect all matching rules into rules_array
    # Filter out NULL signal_catalog_id from LEFT JOIN mismatches using filter() on the array
    # This approach avoids double table scan while preserving all frames
    frames_with_rules = (
        frames_with_all_signals.groupBy(*FRAME_KEYS)
        .agg(F.collect_list(F.struct(*rule_cols)).alias("rules_array"))
        .withColumn(
            "rules_array",
            F.filter(
                F.coalesce(F.col("rules_array"), F.array()),
                lambda r: r.signal_catalog_id.isNotNull(),
            ),
        )
    )

    return frames_with_rules


def _decode_frames_with_signal_arrays(frames_with_rules: DataFrame) -> DataFrame:
    """
    Apply transform decoder over rules_array with full DBC bit extraction logic.
    """

    # Apply transform decoder over rules_array with enum values for readability
    decoded_arr = expr(
        f"""
    transform(
        rules_array,
        r -> named_struct(
          'signal_catalog_id', r.signal_catalog_id,
          'data_identifier',   r.data_identifier,
          'obd_value',         r.obd_value,
          'bit_start',         r.bit_start,
          'bit_length',        r.bit_length,
          'endian',            r.endian,
          'sign',              r.sign,
          'scale',             coalesce(r.scale, 1.0),
          'offset',            coalesce(r.offset, 0.0),
          'minimum',           r.minimum,
          'maximum',           r.maximum,
          'unit',              r.unit,
          'source_id',         r.source_id,
          'decoded_value',
            CASE 
              WHEN payload IS NULL OR r.bit_start IS NULL OR r.bit_length IS NULL
                   OR r.bit_length <= 0 OR r.bit_length > 64
              THEN CAST(NULL AS DOUBLE)
              ELSE
              -- PRESERVED: Full LSB0 normalization and validity check
                CASE
                  WHEN (CASE WHEN r.endian = {EndiannessType.BIG.value}
                             THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                             ELSE r.bit_start END) < 0
                       OR cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                      THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                      ELSE r.bit_start END) + r.bit_length - 1) >> 3 as int) >= payload_length
                  THEN CAST(NULL AS DOUBLE)
                  ELSE
                  -- PRESERVED: Full bit extraction with proper BIGINT sign extension
                    CAST(
                      CASE
                        WHEN r.sign = {SignType.SIGNED.value} AND r.bit_length < 64
                             AND (
                               (SHIFTRIGHT(
                                 CAST(CONV(
                                   CONCAT_WS('', 
                                     CASE WHEN r.endian = {EndiannessType.LITTLE.value}
                                          THEN REVERSE(SPLIT(TRIM(REGEXP_REPLACE(
                                                 HEX(SUBSTRING(payload, 
                                                   cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                               THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                               ELSE r.bit_start END) >> 3) as int) + 1,
                                                   cast((((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                                THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                                ELSE r.bit_start END) + r.bit_length - 1) >> 3) as int) - 
                                                   cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                               THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                               ELSE r.bit_start END) >> 3) as int) + 1)),
                                                 '(.{{2}})', '$1 ')), ' '))
                                          ELSE SPLIT(TRIM(REGEXP_REPLACE(
                                                 HEX(SUBSTRING(payload,
                                                   cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                               THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                               ELSE r.bit_start END) >> 3) as int) + 1,
                                                   cast((((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                                THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                                ELSE r.bit_start END) + r.bit_length - 1) >> 3) as int) - 
                                                   cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                               THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                               ELSE r.bit_start END) >> 3) as int) + 1)),
                                                 '(.{{2}})', '$1 ')), ' ')
                                     END
                                   ),
                                   16, 10) AS BIGINT),
                                 (CASE WHEN r.endian = {EndiannessType.BIG.value}
                                       THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                       ELSE r.bit_start END) & 7
                               ) & (CASE WHEN r.bit_length = 64 THEN ~CAST(0 AS BIGINT)
                                         ELSE (SHIFTLEFT(CAST(1 AS BIGINT), r.bit_length) - 1) END)
                             ) & SHIFTLEFT(1, r.bit_length - 1)
                           ) <> 0
                        THEN ( 
                          (SHIFTRIGHT(
                            CAST(CONV(
                              CONCAT_WS('', 
                                CASE WHEN r.endian = {EndiannessType.LITTLE.value}
                                     THEN REVERSE(SPLIT(TRIM(REGEXP_REPLACE(
                                            HEX(SUBSTRING(payload, 
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1,
                                              cast((((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                           THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                           ELSE r.bit_start END) + r.bit_length - 1) >> 3) as int) - 
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1)),
                                            '(.{{2}})', '$1 ')), ' '))
                                     ELSE SPLIT(TRIM(REGEXP_REPLACE(
                                            HEX(SUBSTRING(payload,
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1,
                                              cast((((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                           THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                           ELSE r.bit_start END) + r.bit_length - 1) >> 3) as int) - 
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1)),
                                            '(.{{2}})', '$1 ')), ' ')
                                END
                              ),
                              16, 10) AS BIGINT),
                            (CASE WHEN r.endian = {EndiannessType.BIG.value}
                                  THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                  ELSE r.bit_start END) & 7
                          ) & (CASE WHEN r.bit_length = 64 THEN ~CAST(0 AS BIGINT)
                                    ELSE (SHIFTLEFT(CAST(1 AS BIGINT), r.bit_length) - 1) END)
                          ) - SHIFTLEFT(1, r.bit_length)
                        )  
                        ELSE (
                          SHIFTRIGHT(
                            CAST(CONV(
                              CONCAT_WS('', 
                                CASE WHEN r.endian = {EndiannessType.LITTLE.value}
                                     THEN REVERSE(SPLIT(TRIM(REGEXP_REPLACE(
                                            HEX(SUBSTRING(payload, 
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1,
                                              cast((((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                           THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                           ELSE r.bit_start END) + r.bit_length - 1) >> 3) as int) - 
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1)),
                                            '(.{{2}})', '$1 ')), ' '))
                                     ELSE SPLIT(TRIM(REGEXP_REPLACE(
                                            HEX(SUBSTRING(payload,
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1,
                                              cast((((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                           THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                           ELSE r.bit_start END) + r.bit_length - 1) >> 3) as int) - 
                                              cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                                          THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                                          ELSE r.bit_start END) >> 3) as int) + 1)),
                                            '(.{{2}})', '$1 ')), ' ')
                                END
                              ),
                              16, 10) AS BIGINT),
                            (CASE WHEN r.endian = {EndiannessType.BIG.value}
                                  THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                  ELSE r.bit_start END) & 7
                          ) & (CASE WHEN r.bit_length = 64 THEN ~CAST(0 AS BIGINT)
                                    ELSE (SHIFTLEFT(CAST(1 AS BIGINT), r.bit_length) - 1) END)
                        )
                      END
                    AS DOUBLE) * coalesce(r.scale, 1.0) + coalesce(r.offset, 0.0)
                END
            END,
          'can_apply_rule',
            payload IS NOT NULL AND r.bit_start IS NOT NULL AND r.bit_length IS NOT NULL
            AND r.bit_length BETWEEN 1 AND 64
            AND (CASE WHEN r.endian = {EndiannessType.BIG.value}
                      THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                      ELSE r.bit_start END) >= 0
            AND cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                             THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                             ELSE r.bit_start END) + r.bit_length - 1) >> 3 as int) < payload_length,
          'decode_reason',
            CASE 
              WHEN payload IS NULL OR r.bit_start IS NULL OR r.bit_length IS NULL
                   OR r.bit_length <= 0 OR r.bit_length > 64 THEN {DecodeReasonType.INVALID_BIT_LENGTH.value}
              WHEN (CASE WHEN r.endian = {EndiannessType.BIG.value}
                         THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                         ELSE r.bit_start END) < 0
                   OR cast(((CASE WHEN r.endian = {EndiannessType.BIG.value}
                                  THEN (cast(r.bit_start/8 as int)*8 + (7 - (r.bit_start % 8)))
                                  ELSE r.bit_start END) + r.bit_length - 1) >> 3 as int) >= payload_length
              THEN {DecodeReasonType.PAYLOAD_TOO_SMALL.value}
              ELSE {DecodeReasonType.OK.value}
            END,
       'is_out_of_range', false,
       'is_standard', r.is_standard,
       'is_broadcast', r.is_broadcast
      )
    )
    """
    )

    return frames_with_rules.withColumn("decoded_signals", decoded_arr).select(
        *FRAME_KEYS, "decoded_signals"
    )


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Fact table for decoded CAN signals using efficient broadcast join approach. "
        "Broadcasts small signal definition tables to workers for in-memory rule resolution, "
        "then applies transform decoder with full DBC bit extraction logic. "
        "Eliminates intermediate table shuffles while preserving correctness.",
        row_meaning="Each row represents a CAN frame with an array of decoded signals.",
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
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_TRACE_RECOMPILED),
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
        spark_conf_overrides={
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "50MB",  # Enable auto-broadcast
        },
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_can_trace_decoded(context: AssetExecutionContext) -> DataFrame:
    """
    Generate decoded CAN signals using direct broadcast of signal definitions.

    This approach:
    1. Broadcasts the entire signal definitions table to all workers (no intermediate shuffles)
    2. Performs a single join to match both vehicle-specific and universal signals
    3. Groups by frame to build rules_array, then applies transform decoder

    Benefits:
    - No intermediate table creation or shuffles for groupBy operations
    - Single broadcast of raw signal definitions table
    - All rule resolution happens in worker memory with optimal data locality
    - Preserves additive approach: vehicle-specific + universal rules
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Enable auto-broadcast for tables up to 50MB
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")

    # Load recompiled frames
    recompiled_query = format_date_partition_query(RECOMPILED_BASE_QUERY, context)
    recompiled_frames = spark.sql(recompiled_query)

    # Load signal definitions (will be broadcast directly)
    context.log.info("Loading signal definitions for direct broadcast")
    signal_definitions_query = format_query(SIGNAL_DEFINITIONS_QUERY)
    signal_definitions = spark.sql(signal_definitions_query)

    # Build rules array using direct broadcast (no intermediate shuffles)
    context.log.info("Building rules arrays with direct broadcast")
    frames_with_rules = _build_rules_array_with_direct_broadcast(
        context, recompiled_frames, signal_definitions
    )

    # Apply decoding transform
    context.log.info("Applying transform decoder over rules arrays")
    result_df = _decode_frames_with_signal_arrays(frames_with_rules)

    return result_df
