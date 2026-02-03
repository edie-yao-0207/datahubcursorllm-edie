"""
Daily Signal Sources by OBD Value with Statistics

This asset creates a daily device-level aggregation showing all unique sources (bus_id, ecu_id,
request_id, data_id combinations) that provide each OBD signal value, along with statistical
distributions of signal values (min, max, P25, P50, P75, mean) and timing information (first/last
seen times, observation counts). This enables answering questions like "are there multiple sources
for the same signal?" and "what is the distribution of values for this signal?" while helping
understand signal source diversity and value patterns across the fleet.

Each row represents one device per day with a map from OBD value to a struct containing:
(1) sources array, (2) value_stats with distribution metrics, (3) timing information.
This structure prevents row explosion and provides efficient lookup of all information for any signal.
Use MAP_KEYS() to get all OBD values, or obd_value_to_sources_map[obd_value] to get the full struct
for a specific signal. This provides comprehensive signal-to-source relationships with value context
for analysis of signal redundancy, source diversity, value distributions, and signal availability.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    array_of,
    columns_to_schema,
    map_of,
    struct_with_comments,
)
from dataweb.userpkgs.firmware.table import KinesisStats, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    create_run_config_overrides,
    format_date_partition_query,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="obd_value_to_sources_map",
        type=map_of(
            DataType.LONG,  # obd_value as key
            struct_with_comments(
                ("sources", array_of(struct_with_comments(
                    ("bus_id", DataType.LONG, "Bus identifier for this source"),
                    ("ecu_id", DataType.LONG, "ECU identifier for this source"),
                    ("request_id", DataType.LONG, "Request identifier for this source"),
                    ("data_id", DataType.LONG, "Data identifier for this source"),
                    ("source_stats", struct_with_comments(
                        ("mean_value", DataType.DOUBLE, "Mean/average signal value for this source"),
                        ("median_value", DataType.DOUBLE, "Median signal value for this source"),
                        ("min_value", DataType.DOUBLE, "Minimum signal value for this source"),
                        ("max_value", DataType.DOUBLE, "Maximum signal value for this source"),
                        ("p25_value", DataType.DOUBLE, "25th percentile (P25) signal value for this source"),
                        ("p75_value", DataType.DOUBLE, "75th percentile (P75) signal value for this source"),
                    ), "Per-source statistical distribution of signal values"),
                )), "Array of all unique sources that provide this OBD signal, each with per-source statistics"),
                ("value_stats", struct_with_comments(
                    ("min_value", DataType.DOUBLE, "Minimum signal value observed for this OBD value"),
                    ("max_value", DataType.DOUBLE, "Maximum signal value observed for this OBD value"),
                    ("p25_value", DataType.DOUBLE, "25th percentile (P25) of signal values"),
                    ("p50_value", DataType.DOUBLE, "50th percentile (P50/median) of signal values"),
                    ("p75_value", DataType.DOUBLE, "75th percentile (P75) of signal values"),
                    ("mean_value", DataType.DOUBLE, "Mean/average of signal values"),
                    ("value_count", DataType.LONG, "Total number of signal value observations"),
                ), "Statistical distribution of signal values observed"),
                ("timing", struct_with_comments(
                    ("first_seen_time", DataType.LONG, "First time this OBD signal was observed (Unix timestamp in milliseconds)"),
                    ("last_seen_time", DataType.LONG, "Last time this OBD signal was observed (Unix timestamp in milliseconds)"),
                    ("observation_count", DataType.LONG, "Number of distinct time observations"),
                ), "Timing information for signal observations"),
            ),  # struct containing sources, stats, and timing as value
            value_contains_null=True,
        ),
        nullable=False,
        metadata=Metadata(
            comment="Map from OBD signal value (obd_value) to struct containing: (1) sources array of all unique (bus_id, ecu_id, request_id, data_id) combinations with per-source statistics (mean, median, min, max, p25, p75), (2) value_stats with min/max/P25/P50/P75/mean/count of signal values aggregated across all sources, (3) timing with first/last seen times and observation count. Use MAP_KEYS() to get all OBD values, or obd_value_to_sources_map[obd_value] to get the struct for a specific signal.",
        ),
    ),
    Column(
        name="total_obd_values",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique OBD signal values (keys in obd_value_to_sources_map) for the device on this day.",
        ),
    ),
    Column(
        name="total_unique_sources",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique sources across all OBD values for the device on this day.",
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
WITH flattened_entries AS (
    SELECT
        date,
        time,
        org_id,
        object_id AS device_id,
        inline(value.proto_value.vdp_signal_cache_snapshot.entries)
    FROM kinesisstats_history.osdvdpsignalcachesnapshot
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
        AND NOT value.is_end
        AND NOT value.is_databreak
    DISTRIBUTE BY date, org_id, device_id
),

entry_details AS (
    SELECT
        date,
        time,
        org_id,
        device_id,
        BIGINT(source.bus) AS bus_id,
        COALESCE(source.ecu_id, 0) AS ecu_id,
        source.request_id,
        source.data_id,
        signals
    FROM flattened_entries
),

signal_source_pairs AS (
    SELECT
        date,
        time,
        org_id,
        device_id,
        bus_id,
        ecu_id,
        request_id,
        data_id,
        BIGINT(signal.signal.obd_value) AS obd_value,
        COALESCE(signal.signal.double_value, CAST(signal.signal.int_value AS DOUBLE)) AS signal_value,
        signal.count AS signal_count
    FROM entry_details
    LATERAL VIEW explode(signals) AS signal
    WHERE signal.signal.obd_value IS NOT NULL
        AND (signal.signal.double_value IS NOT NULL OR signal.signal.int_value IS NOT NULL)
),

-- Calculate per-source statistics by aggregating signals from the same source
per_source_stats AS (
    SELECT
        date,
        org_id,
        device_id,
        obd_value,
        bus_id,
        ecu_id,
        request_id,
        data_id,
        -- Aggregate per-source statistics (weighted by signal count if available)
        CASE 
            WHEN SUM(signal_count) > 0 THEN
                SUM(signal_value * COALESCE(signal_count, 1)) / SUM(COALESCE(signal_count, 1))
            ELSE AVG(signal_value)
        END AS source_mean,
        PERCENTILE_APPROX(signal_value, 0.50) AS source_median,
        MIN(signal_value) AS source_min,
        MAX(signal_value) AS source_max,
        PERCENTILE_APPROX(signal_value, 0.25) AS source_p25,
        PERCENTILE_APPROX(signal_value, 0.75) AS source_p75
    FROM signal_source_pairs
    GROUP BY date, org_id, device_id, obd_value, bus_id, ecu_id, request_id, data_id
),

-- Calculate timing information per OBD value
obd_value_timing AS (
    SELECT
        date,
        org_id,
        device_id,
        obd_value,
        MIN(time) AS first_seen_time,
        MAX(time) AS last_seen_time,
        CAST(COUNT(DISTINCT time) AS BIGINT) AS observation_count,
        CAST(SUM(signal_count) AS BIGINT) AS value_count
    FROM signal_source_pairs
    GROUP BY date, org_id, device_id, obd_value
),

-- Combined aggregation to avoid double scan of signal_source_pairs
obd_value_aggregation AS (
    SELECT
        pss.date,
        pss.org_id,
        pss.device_id,
        pss.obd_value,
        -- Collect unique sources with their statistics (deduplicated)
        COLLECT_SET(
            STRUCT(
                pss.bus_id,
                pss.ecu_id,
                pss.request_id,
                pss.data_id,
                STRUCT(
                    pss.source_mean AS mean_value,
                    pss.source_median AS median_value,
                    pss.source_min AS min_value,
                    pss.source_max AS max_value,
                    pss.source_p25 AS p25_value,
                    pss.source_p75 AS p75_value
                ) AS source_stats
            )
        ) AS sources,
        -- Value statistics aggregated across all sources
        MIN(pss.source_min) AS min_value,
        MAX(pss.source_max) AS max_value,
        PERCENTILE_APPROX(pss.source_p25, 0.50) AS p25_value,
        PERCENTILE_APPROX(pss.source_median, 0.50) AS p50_value,
        PERCENTILE_APPROX(pss.source_p75, 0.50) AS p75_value,
        AVG(pss.source_mean) AS mean_value
    FROM per_source_stats pss
    GROUP BY pss.date, pss.org_id, pss.device_id, pss.obd_value
),

-- Calculate total_unique_sources from original data (more efficient than exploding aggregated sources)
device_unique_sources AS (
    SELECT
        date,
        org_id,
        device_id,
        CAST(COUNT(DISTINCT STRUCT(bus_id, ecu_id, request_id, data_id)) AS BIGINT) AS total_unique_sources
    FROM signal_source_pairs
    GROUP BY date, org_id, device_id
),

device_daily_aggregation AS (
    SELECT
        ova.date,
        ova.org_id,
        ova.device_id,
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    ova.obd_value AS key,
                    STRUCT(
                        ova.sources,
                        STRUCT(
                            ova.min_value,
                            ova.max_value,
                            ova.p25_value,
                            ova.p50_value,
                            ova.p75_value,
                            ova.mean_value,
                            ovt.value_count
                        ) AS value_stats,
                        STRUCT(
                            ovt.first_seen_time,
                            ovt.last_seen_time,
                            ovt.observation_count
                        ) AS timing
                    ) AS value
                )
            )
        ) AS obd_value_to_sources_map,
        CAST(COUNT(DISTINCT ova.obd_value) AS BIGINT) AS total_obd_values
    FROM obd_value_aggregation ova
    LEFT JOIN obd_value_timing ovt
        ON ova.date = ovt.date
        AND ova.org_id = ovt.org_id
        AND ova.device_id = ovt.device_id
        AND ova.obd_value = ovt.obd_value
    GROUP BY ova.date, ova.org_id, ova.device_id
)

SELECT
    dda.date,
    dda.org_id,
    dda.device_id,
    dda.obd_value_to_sources_map,
    dda.total_obd_values,
    COALESCE(dus.total_unique_sources, CAST(0 AS BIGINT)) AS total_unique_sources
FROM device_daily_aggregation dda
LEFT JOIN device_unique_sources dus
    ON dda.date = dus.date
    AND dda.org_id = dus.org_id
    AND dda.device_id = dus.device_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily device-level aggregation of all unique sources (bus_id, ecu_id, request_id, data_id) "
        "that provide each OBD signal value, along with statistical distributions (min, max, P25, P50, P75, mean) "
        "and timing information (first/last seen, observation counts). Stored as a map from obd_value to struct "
        "containing sources array (each with per-source statistics), value_stats struct (aggregated across all sources), "
        "and timing struct. Enables analysis of signal source diversity, redundancy, and value distributions. "
        "Use MAP_KEYS(obd_value_to_sources_map) to get all OBD values, or obd_value_to_sources_map[obd_value] to get "
        "the full struct for a specific signal. Use SIZE(obd_value_to_sources_map[obd_value].sources) > 1 to identify "
        "signals with multiple sources, or obd_value_to_sources_map[obd_value].value_stats to analyze value distributions. "
        "Each source in the sources array includes per-source statistics (mean, median, min, max, p25, p75) enabling "
        "cross-source comparison analysis. Each row represents one device per day with all OBD value to source mappings "
        "with statistical context.",
        row_meaning="Each row represents all OBD signal values and their unique sources with statistical context for a device on a given day. "
        "The obd_value_to_sources_map is a map where keys are OBD values and values are structs containing: "
        "(1) sources array of unique (bus_id, ecu_id, request_id, data_id) combinations, each with per-source statistics "
        "(source_stats: mean, median, min, max, p25, p75), "
        "(2) value_stats struct with min/max/P25/P50/P75/mean/count of signal values aggregated across all sources, "
        "(3) timing struct with first/last seen times and observation count. "
        "This structure prevents row explosion and enables efficient queries across all signals with full statistical context, "
        "including per-source statistics for cross-source comparison.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_VDP_SIGNAL_CACHE_SNAPSHOT),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        min_workers=4,
        max_workers=32,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_SIGNAL_CACHE_SOURCES_BY_OBD_VALUE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_signal_cache_sources_by_obd_value(context: AssetExecutionContext) -> str:
    """
    Generate daily device-level aggregation of signal sources by OBD value with statistics.

    This asset transforms granular VDP signal cache events into a structured format
    that groups by device, creating a map from OBD value to struct containing sources
    (each with per-source statistics: mean, median, min, max, p25, p75), value statistics
    aggregated across all sources (min, max, percentiles, mean), and timing information.
    This prevents row explosion (one row per device per day instead of one row per device
    per OBD value) and enables efficient analysis of signal source diversity, redundancy,
    and value distributions. The per-source statistics enable downstream assets to perform
    cross-source comparison analysis without re-reading the expensive Kinesis source.

    Args:
        context: Dagster asset execution context providing partition and run information

    Returns:
        str: Formatted SQL query string for the current partition date range
    """
    return format_date_partition_query(QUERY, context)

