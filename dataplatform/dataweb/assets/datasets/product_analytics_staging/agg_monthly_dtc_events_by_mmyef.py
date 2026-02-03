"""
agg_monthly_dtc_events_by_mmyef

30-day rolling aggregation of DTC (Diagnostic Trouble Code) events by vehicle characteristics (MMYEF ID) with fault protocol source tracking.
Provides population-level DTC analysis by grouping passenger and J1939 DTCs separately by vehicle 
make/model/year/engine/fuel combinations over a 30-day lookback window. Features separate arrays for 
passenger (OBD-II) and J1939 (SPN/FMI) DTCs, with counts of distinct organizations and devices 
experiencing each DTC, plus arrays of distinct fault protocol sources used to report each DTC, 
enabling population health analysis, protocol source diversity tracking, and identification of vehicle-specific 
diagnostic patterns across different vehicle types.
"""

from dataclasses import replace
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
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
    get_non_null_columns,
    get_primary_keys,
    struct_with_comments,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
)
from dataweb.assets.datasets.product_analytics_staging.dim_device_vehicle_properties import (
    MMYEF_ID,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

COLUMNS = [
    ColumnType.DATE,
    replace(MMYEF_ID, primary_key=True),
    Column(
        name="passenger_dtcs",
        type=array_of(
            struct_with_comments(
                ("decimal", DataType.INTEGER, "Raw decimal value of the passenger DTC"),
                (
                    "code",
                    DataType.STRING,
                    "Human-readable DTC code in P0XXX/C0XXX/B0XXX/U0XXX format",
                ),
                (
                    "protocol_sources",
                    array_of(
                        struct_with_comments(
                            (
                                "protocol_id",
                                DataType.INTEGER,
                                "Fault protocol source ID",
                            ),
                            (
                                "occurrence_count",
                                DataType.LONG,
                                "Number of times this protocol reported this DTC",
                            ),
                        )
                    ),
                    "Array of protocol sources with their respective occurrence counts for this DTC",
                ),
                (
                    "distinct_org_count",
                    DataType.LONG,
                    "Number of distinct organizations experiencing this DTC",
                ),
                (
                    "distinct_device_count",
                    DataType.LONG,
                    "Number of distinct devices experiencing this DTC",
                ),
                (
                    "total_occurrence_count",
                    DataType.LONG,
                    "Total occurrences of this DTC across all devices and protocols",
                ),
                (
                    "first_seen_date",
                    DataType.STRING,
                    "First date this DTC was observed in the 30-day window",
                ),
                (
                    "last_seen_date",
                    DataType.STRING,
                    "Most recent date this DTC was observed in the 30-day window",
                ),
            )
        ),
        nullable=True,
        metadata=Metadata(
            comment="Array of passenger vehicle (OBD-II) DTC structs with aggregated statistics for this vehicle type over the past 30 days."
        ),
    ),
    Column(
        name="j1939_dtcs",
        type=array_of(
            struct_with_comments(
                ("spn", DataType.INTEGER, "Suspect Parameter Number for the J1939 DTC"),
                ("fmi", DataType.INTEGER, "Failure Mode Identifier for the J1939 DTC"),
                (
                    "protocol_sources",
                    array_of(
                        struct_with_comments(
                            (
                                "protocol_id",
                                DataType.INTEGER,
                                "Fault protocol source ID",
                            ),
                            (
                                "occurrence_count",
                                DataType.LONG,
                                "Number of times this protocol reported this DTC",
                            ),
                        )
                    ),
                    "Array of protocol sources with their respective occurrence counts for this DTC",
                ),
                (
                    "distinct_org_count",
                    DataType.LONG,
                    "Number of distinct organizations experiencing this DTC",
                ),
                (
                    "distinct_device_count",
                    DataType.LONG,
                    "Number of distinct devices experiencing this DTC",
                ),
                (
                    "total_occurrence_count",
                    DataType.LONG,
                    "Total occurrences of this DTC across all devices and protocols",
                ),
                (
                    "first_seen_date",
                    DataType.STRING,
                    "First date this DTC was observed in the 30-day window",
                ),
                (
                    "last_seen_date",
                    DataType.STRING,
                    "Most recent date this DTC was observed in the 30-day window",
                ),
            )
        ),
        nullable=True,
        metadata=Metadata(
            comment="Array of heavy-duty J1939 DTC structs with aggregated statistics for this vehicle type over the past 30 days."
        ),
    ),
    Column(
        name="distinct_org_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of distinct organizations that experienced any DTC for this vehicle type in the past 30 days."
        ),
    ),
    Column(
        name="distinct_device_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of distinct devices that experienced any DTC for this vehicle type in the past 30 days."
        ),
    ),
    Column(
        name="total_dtc_types",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique DTC types observed for this vehicle type in the past 30 days."
        ),
    ),
    Column(
        name="total_occurrences",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of DTC occurrences across all DTCs for this vehicle type in the past 30 days."
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
-- First, get all DTC data with mmyef_id joins and flatten into daily aggregates
-- The daily table has nested protocol_sources, so we need to explode twice
WITH dtc_daily_base AS (
    SELECT
        dtc_data.date,
        vprops.mmyef_id,
        dtc.decimal AS dtc_decimal,
        dtc.code AS dtc_code,
        NULL AS spn,
        NULL AS fmi,
        protocol.protocol_id AS fault_protocol_source,
        dtc_data.org_id,
        dtc_data.device_id,
        protocol.count AS dtc_count,
        'passenger' AS dtc_type
    FROM {product_analytics_staging}.fct_dtc_events_daily AS dtc_data
    INNER JOIN product_analytics_staging.dim_device_vehicle_properties AS vprops
        ON dtc_data.date = vprops.date
        AND dtc_data.org_id = vprops.org_id
        AND dtc_data.device_id = vprops.device_id
    LATERAL VIEW EXPLODE(dtc_data.passenger_dtcs) AS dtc
    LATERAL VIEW EXPLODE(dtc.protocol_sources) AS protocol
    WHERE dtc_data.date BETWEEN DATE_SUB(DATE("{date_start}"), 29) AND DATE("{date_end}")
        AND vprops.mmyef_id IS NOT NULL
        AND dtc_data.passenger_dtcs IS NOT NULL

    UNION ALL

    SELECT
        dtc_data.date,
        vprops.mmyef_id,
        NULL AS dtc_decimal,
        NULL AS dtc_code,
        dtc.spn,
        dtc.fmi,
        protocol.protocol_id AS fault_protocol_source,
        dtc_data.org_id,
        dtc_data.device_id,
        protocol.count AS dtc_count,
        'j1939' AS dtc_type
    FROM {product_analytics_staging}.fct_dtc_events_daily AS dtc_data
    INNER JOIN product_analytics_staging.dim_device_vehicle_properties AS vprops
        ON dtc_data.date = vprops.date
        AND dtc_data.org_id = vprops.org_id
        AND dtc_data.device_id = vprops.device_id
    LATERAL VIEW EXPLODE(dtc_data.j1939_dtcs) AS dtc
    LATERAL VIEW EXPLODE(dtc.protocol_sources) AS protocol
    WHERE dtc_data.date BETWEEN DATE_SUB(DATE("{date_start}"), 29) AND DATE("{date_end}")
        AND vprops.mmyef_id IS NOT NULL
        AND dtc_data.j1939_dtcs IS NOT NULL
),

-- Aggregate by date, mmyef_id, DTC, and protocol to get per-protocol counts
dtc_daily_by_protocol AS (
    SELECT
        date,
        mmyef_id,
        dtc_decimal,
        dtc_code,
        spn,
        fmi,
        dtc_type,
        fault_protocol_source,
        COUNT(DISTINCT org_id) AS daily_org_count,
        COUNT(DISTINCT device_id) AS daily_device_count,
        SUM(dtc_count) AS daily_dtc_count
    FROM dtc_daily_base
    GROUP BY date, mmyef_id, dtc_decimal, dtc_code, spn, fmi, dtc_type, fault_protocol_source
),

-- Use window functions to calculate 30-day rolling sums per protocol
dtc_rolling_30d_by_protocol AS (
    SELECT
        date,
        mmyef_id,
        dtc_decimal,
        dtc_code,
        spn,
        fmi,
        dtc_type,
        fault_protocol_source,
        -- 30-day rolling window sums per protocol using window functions
        SUM(daily_dtc_count) OVER (
            PARTITION BY mmyef_id, dtc_decimal, dtc_code, spn, fmi, dtc_type, fault_protocol_source
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_protocol_dtc_count
    FROM dtc_daily_by_protocol
),

-- Aggregate across protocols for each DTC to get overall counts and first/last dates
dtc_rolling_30d_overall AS (
    SELECT
        date,
        mmyef_id,
        dtc_decimal,
        dtc_code,
        spn,
        fmi,
        dtc_type,
        -- Use the protocol-level data from the base to get overall metrics
        SUM(daily_org_count) OVER (
            PARTITION BY mmyef_id, dtc_decimal, dtc_code, spn, fmi, dtc_type
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_org_count,
        SUM(daily_device_count) OVER (
            PARTITION BY mmyef_id, dtc_decimal, dtc_code, spn, fmi, dtc_type
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_device_count,
        MIN(date) OVER (
            PARTITION BY mmyef_id, dtc_decimal, dtc_code, spn, fmi, dtc_type
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS first_seen_date,
        MAX(date) OVER (
            PARTITION BY mmyef_id, dtc_decimal, dtc_code, spn, fmi, dtc_type
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS last_seen_date
    FROM dtc_daily_by_protocol
),

-- Filter to target date range and join protocol-level and overall metrics
target_date_protocol_metrics AS (
    SELECT
        p.date,
        p.mmyef_id,
        p.dtc_decimal,
        p.dtc_code,
        p.spn,
        p.fmi,
        p.dtc_type,
        p.fault_protocol_source,
        p.rolling_protocol_dtc_count,
        o.rolling_org_count,
        o.rolling_device_count,
        o.first_seen_date,
        o.last_seen_date
    FROM dtc_rolling_30d_by_protocol p
    INNER JOIN dtc_rolling_30d_overall o
        ON p.date = o.date
        AND p.mmyef_id = o.mmyef_id
        AND p.dtc_decimal <=> o.dtc_decimal
        AND p.dtc_code <=> o.dtc_code
        AND p.spn <=> o.spn
        AND p.fmi <=> o.fmi
        AND p.dtc_type = o.dtc_type
    WHERE p.date BETWEEN DATE("{date_start}") AND DATE("{date_end}")
),

-- Deduplicate passenger DTCs: one entry per DTC per date/mmyef_id with protocol sources as array of structs
passenger_dtcs_deduped AS (
    SELECT
        date,
        mmyef_id,
        dtc_decimal,
        dtc_code,
        COLLECT_LIST(STRUCT(
            fault_protocol_source AS protocol_id,
            rolling_protocol_dtc_count AS occurrence_count
        )) AS protocol_sources,
        MAX(rolling_org_count) AS distinct_org_count,
        MAX(rolling_device_count) AS distinct_device_count,
        SUM(rolling_protocol_dtc_count) AS total_occurrence_count,
        MIN(first_seen_date) AS first_seen_date,
        MAX(last_seen_date) AS last_seen_date
    FROM target_date_protocol_metrics
    WHERE dtc_type = 'passenger' AND dtc_decimal IS NOT NULL
    GROUP BY date, mmyef_id, dtc_decimal, dtc_code
),

-- Separate passenger DTCs with rolling aggregations
passenger_dtcs_final AS (
    SELECT
        date,
        mmyef_id,
        COLLECT_LIST(STRUCT(
            dtc_decimal AS decimal,
            dtc_code AS code,
            protocol_sources,
            distinct_org_count,
            distinct_device_count,
            total_occurrence_count,
            first_seen_date,
            last_seen_date
        )) AS passenger_dtcs,
        SUM(total_occurrence_count) AS passenger_total_occurrences,
        COUNT(DISTINCT dtc_decimal) AS passenger_dtc_types,
        MAX(distinct_org_count) AS passenger_max_org_count,
        MAX(distinct_device_count) AS passenger_max_device_count
    FROM passenger_dtcs_deduped
    GROUP BY date, mmyef_id
),

-- Deduplicate J1939 DTCs: one entry per DTC per date/mmyef_id with protocol sources as array of structs
j1939_dtcs_deduped AS (
    SELECT
        date,
        mmyef_id,
        spn,
        fmi,
        COLLECT_LIST(STRUCT(
            fault_protocol_source AS protocol_id,
            rolling_protocol_dtc_count AS occurrence_count
        )) AS protocol_sources,
        MAX(rolling_org_count) AS distinct_org_count,
        MAX(rolling_device_count) AS distinct_device_count,
        SUM(rolling_protocol_dtc_count) AS total_occurrence_count,
        MIN(first_seen_date) AS first_seen_date,
        MAX(last_seen_date) AS last_seen_date
    FROM target_date_protocol_metrics
    WHERE dtc_type = 'j1939' AND spn IS NOT NULL AND fmi IS NOT NULL
    GROUP BY date, mmyef_id, spn, fmi
),

-- Separate J1939 DTCs with rolling aggregations
j1939_dtcs_final AS (
    SELECT
        date,
        mmyef_id,
        COLLECT_LIST(STRUCT(
            spn,
            fmi,
            protocol_sources,
            distinct_org_count,
            distinct_device_count,
            total_occurrence_count,
            first_seen_date,
            last_seen_date
        )) AS j1939_dtcs,
        SUM(total_occurrence_count) AS j1939_total_occurrences,
        COUNT(DISTINCT STRUCT(spn, fmi)) AS j1939_dtc_types,
        MAX(distinct_org_count) AS j1939_max_org_count,
        MAX(distinct_device_count) AS j1939_max_device_count
    FROM j1939_dtcs_deduped
    GROUP BY date, mmyef_id
)

-- Final result: Combine passenger and J1939 aggregations
SELECT
    COALESCE(p.date, j.date) AS date,
    COALESCE(p.mmyef_id, j.mmyef_id) AS mmyef_id,
    COALESCE(p.passenger_dtcs, ARRAY()) AS passenger_dtcs,
    COALESCE(j.j1939_dtcs, ARRAY()) AS j1939_dtcs,
    GREATEST(
        COALESCE(p.passenger_max_org_count, 0),
        COALESCE(j.j1939_max_org_count, 0)
    ) AS distinct_org_count,
    GREATEST(
        COALESCE(p.passenger_max_device_count, 0),
        COALESCE(j.j1939_max_device_count, 0)
    ) AS distinct_device_count,
    COALESCE(p.passenger_dtc_types, 0) + COALESCE(j.j1939_dtc_types, 0) AS total_dtc_types,
    COALESCE(p.passenger_total_occurrences, 0) + COALESCE(j.j1939_total_occurrences, 0) AS total_occurrences
FROM passenger_dtcs_final p
FULL OUTER JOIN j1939_dtcs_final j
    ON p.date = j.date AND p.mmyef_id = j.mmyef_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="High-performance 30-day rolling aggregation of DTC events (both passenger and J1939) by vehicle characteristics (MMYEF ID) with fault protocol source tracking. "
        "Uses Spark window functions to provide population-level DTC analysis by grouping DTCs by vehicle make/model/year/engine/fuel combinations "
        "over a 30-day lookback window without expensive cross joins. Each row represents one vehicle type with separate arrays for passenger (OBD-II) and J1939 DTCs, "
        "including arrays of distinct protocol sources used to report each DTC, counts of unique organizations/devices affected, and summary statistics calculated via efficient window operations, "
        "enabling comprehensive population health analysis and identification of vehicle-specific diagnostic patterns across the fleet with optimal performance.",
        row_meaning="Each row represents one vehicle type (MMYEF ID) for a given date with complete DTC profile "
        "including separate arrays for passenger DTCs (OBD-II format) and J1939 DTCs (SPN/FMI format), protocol sources for each DTC, counts of affected organizations/devices, and summary statistics over the past 30 days.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_DTC_EVENTS_DAILY),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_MONTHLY_DTC_EVENTS_BY_MMYEF.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_monthly_dtc_events_by_mmyef(context: AssetExecutionContext) -> str:
    """
    Create 30-day rolling aggregation of DTC events (both passenger and J1939) by vehicle characteristics with protocol source tracking.

    This completely redesigned asset uses window functions to eliminate expensive cross joins:
    1. Joins DTC events with vehicle properties and flattens into daily base data, extracting fault_protocol_source
    2. Aggregates by date/mmyef_id/DTC to get daily totals with unique protocol sources (eliminates duplicate counting)
    3. Uses Spark window functions with ROWS BETWEEN for true 30-day rolling calculations
    4. Collects distinct protocol sources used to report each DTC over the 30-day window
    5. Separates passenger DTCs (with decimal values) and J1939 DTCs (with SPN/FMI values)
    6. Applies rolling aggregations per DTC type using efficient window operations
    7. Flattens and de-duplicates protocol source arrays for each DTC
    8. Filters to target date range and structures final arrays with rolling statistics
    9. Provides comprehensive DTC profiles with org/device counts, protocol sources, and occurrence totals

    The result enables comprehensive fleet-wide DTC analysis by vehicle characteristics with rolling 30-day trends,
    supporting population health monitoring, vehicle-specific issue identification, protocol source analysis,
    and comparative diagnostics analysis across both passenger and heavy-duty vehicle types over time.

    Each partition date shows the DTC profile for that vehicle type over the 30 days ending on that date,
    with separate arrays for passenger (OBD-II) and J1939 DTC types, including protocol source diversity.
    """
    return format_date_partition_query(
        QUERY,
        context,
    )
