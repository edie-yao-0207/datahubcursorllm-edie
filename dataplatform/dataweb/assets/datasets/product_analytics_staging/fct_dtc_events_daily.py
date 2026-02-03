"""
fct_dtc_events_daily

Daily processing of DTC (Diagnostic Trouble Code) events with per-protocol occurrence counts.
Extracts DTCs from engine fault Kinesis events, decodes passenger DTCs to standardized format (P0XXX, C0XXX, etc.),
and includes J1939 DTCs with SPN/FMI values. Each row represents a device's complete DTC activity 
for one day with structured arrays. Each unique DTC includes an array of protocol sources with their 
respective occurrence counts, providing visibility into which communication protocols reported each fault.
"""

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
    Definitions,
    KinesisStatsHistory,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
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
                                "count",
                                DataType.LONG,
                                "Number of times this protocol reported this DTC on this device on this day",
                            ),
                        )
                    ),
                    "Array of protocol sources with their respective occurrence counts for this DTC",
                ),
                (
                    "total_count",
                    DataType.LONG,
                    "Total number of times this DTC occurred on this device on this day across all protocols",
                ),
            )
        ),
        nullable=True,
        metadata=Metadata(
            comment="Array of passenger vehicle (OBD-II) DTC structs with decimal values, decoded standard format codes, protocol sources with counts, and total occurrence counts. "
            "Each struct represents a unique passenger DTC detected on this device on this day."
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
                                "count",
                                DataType.LONG,
                                "Number of times this protocol reported this DTC on this device on this day",
                            ),
                        )
                    ),
                    "Array of protocol sources with their respective occurrence counts for this DTC",
                ),
                (
                    "total_count",
                    DataType.LONG,
                    "Total number of times this DTC occurred on this device on this day across all protocols",
                ),
            )
        ),
        nullable=True,
        metadata=Metadata(
            comment="Array of heavy-duty J1939 DTC structs with SPN/FMI values, protocol sources with counts, and total occurrence counts. "
            "Each struct represents a unique J1939 DTC detected on this device on this day."
        ),
    ),
    Column(
        name="unique_passenger_dtcs",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of unique passenger (OBD-II) DTCs detected on this device on this day"
        ),
    ),
    Column(
        name="unique_j1939_dtcs",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of unique J1939 DTCs (unique SPN/FMI combinations) detected on this device on this day"
        ),
    ),
    Column(
        name="total_unique_dtcs",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique DTCs across both passenger and J1939 fault types on this device on this day"
        ),
    ),
    Column(
        name="unique_fault_protocol_sources",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="Number of distinct fault protocol sources used across all DTCs on this device on this day"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
WITH passenger_dtc_events AS (
    SELECT
        f.date,
        f.object_id AS device_id,
        f.object_id,
        f.org_id,
        pass_fault.fault_protocol_source,
        EXPLODE(
            CASE WHEN pass_fault.dtcs IS NULL THEN ARRAY(NULL) ELSE pass_fault.dtcs END
        ) as dtc_decimal,
        NULL as spn,
        NULL as fmi,
        1 as occurrence_count, -- Passenger DTCs need to be counted per event
        'passenger' as fault_type
    FROM kinesisstats_history.osdenginefault f 
    LATERAL VIEW EXPLODE(value.proto_value.vehicle_fault_event.passenger_faults) AS pass_fault
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
        AND value.proto_value.vehicle_fault_event.passenger_faults IS NOT NULL
        AND NOT value.is_databreak
        AND NOT value.is_end
),

j1939_dtc_events AS (
    SELECT
        f.date,
        f.object_id AS device_id,
        f.object_id,
        f.org_id,
        j1939_fault.fault_protocol_source,
        NULL as dtc_decimal,
        j1939_fault.spn,
        j1939_fault.fmi,
        j1939_fault.occurance_count as occurrence_count,
        'j1939' as fault_type
    FROM kinesisstats_history.osdenginefault f 
    LATERAL VIEW EXPLODE(value.proto_value.vehicle_fault_event.j1939_faults) AS j1939_fault
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
        AND value.proto_value.vehicle_fault_event.j1939_faults IS NOT NULL
        AND NOT value.is_databreak
        AND NOT value.is_end
),

all_dtc_events AS (
    SELECT * FROM passenger_dtc_events
    UNION ALL
    SELECT * FROM j1939_dtc_events
),

dtc_decoded AS (
    SELECT 
        *,
        -- Only decode passenger DTCs; J1939 doesn't need a code field
        CASE 
            WHEN fault_type = 'passenger' AND dtc_decimal IS NOT NULL THEN 
                CONCAT(
                    CASE 
                        WHEN (SHIFTRIGHT(dtc_decimal, 14) & 3) = 0 THEN 'P' 
                        WHEN (SHIFTRIGHT(dtc_decimal, 14) & 3) = 1 THEN 'C' 
                        WHEN (SHIFTRIGHT(dtc_decimal, 14) & 3) = 2 THEN 'B' 
                        ELSE 'U' 
                    END, 
                    (SHIFTRIGHT(dtc_decimal, 12) & 3), 
                    LPAD(HEX(dtc_decimal & 4095), 3, '0')
                ) 
            ELSE NULL
        END as dtc_code
    FROM all_dtc_events
    WHERE (dtc_decimal IS NOT NULL OR (spn IS NOT NULL AND fmi IS NOT NULL))
),

-- First aggregate by DTC and protocol to get per-protocol counts
dtc_per_protocol AS (
    SELECT 
        date,
        org_id,
        device_id,
        object_id,
        dtc_decimal,
        spn,
        fmi,
        dtc_code,
        fault_type,
        fault_protocol_source,
        -- For passenger DTCs, count events; for J1939, sum the occurrence counts
        CASE 
            WHEN fault_type = 'passenger' THEN COUNT(*)
            WHEN fault_type = 'j1939' THEN SUM(occurrence_count)
            ELSE COUNT(*)
        END AS protocol_dtc_count
    FROM dtc_decoded
    GROUP BY date, org_id, device_id, object_id, dtc_decimal, spn, fmi, dtc_code, fault_type, fault_protocol_source
),

-- Then aggregate by DTC (without protocol) to collect protocol sources
dtc_counts AS (
    SELECT 
        date,
        org_id,
        device_id,
        object_id,
        dtc_decimal,
        spn,
        fmi,
        dtc_code,
        fault_type,
        COLLECT_LIST(STRUCT(
            fault_protocol_source AS protocol_id,
            protocol_dtc_count AS count
        )) AS protocol_sources,
        SUM(protocol_dtc_count) AS total_dtc_count
    FROM dtc_per_protocol
    GROUP BY date, org_id, device_id, object_id, dtc_decimal, spn, fmi, dtc_code, fault_type
),

dtc_aggregated AS (
    SELECT 
        date,
        org_id,
        device_id,
        COLLECT_LIST(
            CASE WHEN fault_type = 'passenger' THEN 
                STRUCT(
                    dtc_decimal AS decimal, 
                    dtc_code AS code,
                    protocol_sources,
                    total_dtc_count AS total_count
                )
            END
        ) AS passenger_dtcs,
        COLLECT_LIST(
            CASE WHEN fault_type = 'j1939' THEN 
                STRUCT(
                    spn, 
                    fmi,
                    protocol_sources,
                    total_dtc_count AS total_count
                )
            END
        ) AS j1939_dtcs,
        COUNT(DISTINCT CASE WHEN fault_type = 'passenger' THEN dtc_decimal END) AS unique_passenger_dtcs,
        COUNT(DISTINCT CASE WHEN fault_type = 'j1939' THEN STRUCT(spn, fmi) END) AS unique_j1939_dtcs,
        -- Count unique protocols by flattening all protocol_sources arrays
        SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(
            TRANSFORM(protocol_sources, ps -> ps.protocol_id)
        )))) AS unique_fault_protocol_sources
    FROM dtc_counts
    GROUP BY ALL
)

SELECT 
    date,
    org_id,
    device_id,
    FILTER(passenger_dtcs, x -> x IS NOT NULL) AS passenger_dtcs,
    FILTER(j1939_dtcs, x -> x IS NOT NULL) AS j1939_dtcs,
    unique_passenger_dtcs,
    unique_j1939_dtcs,
    unique_passenger_dtcs + unique_j1939_dtcs AS total_unique_dtcs,
    unique_fault_protocol_sources
FROM dtc_aggregated
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily aggregation of DTC (Diagnostic Trouble Code) events per device from both passenger vehicle "
        "and heavy-duty J1939 sources with per-protocol occurrence counts and summary statistics. Extracts passenger DTCs and decodes them to standardized format "
        "(P0XXX, C0XXX, etc.), and extracts J1939 DTCs with SPN/FMI values. Each row represents all DTCs detected on a device "
        "on a given day with separate arrays for passenger and J1939 fault types. Each unique DTC includes an array of protocol sources "
        "with their respective occurrence counts, providing complete visibility into which communication protocols reported each fault and how often.",
        row_meaning="Each row represents a device's complete DTC activity for one day, containing separate arrays for "
        "passenger DTCs (OBD-II format with decoded codes) and J1939 DTCs (SPN/FMI values). Each unique DTC includes nested protocol sources "
        "with per-protocol counts, plus summary statistics for unique fault counts and protocol source diversity.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_FAULT),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_DTC_EVENTS_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_dtc_events_daily(context: AssetExecutionContext) -> str:
    """
    Process and aggregate DTC events per device per day from both passenger and J1939 sources with per-protocol occurrence counts and summary statistics.

    This asset:
    1. Extracts passenger DTCs from osdenginefault Kinesis events and decodes them to P0XXX/C0XXX/B0XXX/U0XXX format
    2. Extracts J1939 DTCs with SPN/FMI values
    3. Aggregates each unique DTC with nested protocol_sources array showing which protocols reported it and how often
    4. Counts occurrences per protocol per DTC (events for passenger, sum occurrence_count for J1939)
    5. Separates fault types into dedicated passenger_dtcs and j1939_dtcs arrays for easy analysis
    6. Calculates summary statistics: unique counts for passenger DTCs, J1939 DTCs, total DTCs, and protocol sources

    Schema notes:
    - passenger_dtcs: Contains decimal, code (P0XXX/C0XXX/etc), protocol_sources array (protocol_id + count), and total_count
    - j1939_dtcs: Contains spn, fmi, protocol_sources array (protocol_id + count), and total_count
    - Each unique DTC appears once with all protocol sources nested inside
    - unique_passenger_dtcs: Count of distinct passenger DTCs on this device/day
    - unique_j1939_dtcs: Count of distinct J1939 SPN/FMI combinations on this device/day
    - total_unique_dtcs: Sum of unique passenger + unique J1939 DTCs
    - unique_fault_protocol_sources: Count of distinct protocol sources across all DTCs

    The nested protocol structure provides complete visibility into which protocols detected each fault and their
    respective occurrence counts, while maintaining one entry per unique DTC for efficient querying. The summary
    statistics enable quick filtering (e.g., devices with > 5 unique DTCs) without needing to explode arrays.
    """
    return format_date_partition_query(QUERY, context)
