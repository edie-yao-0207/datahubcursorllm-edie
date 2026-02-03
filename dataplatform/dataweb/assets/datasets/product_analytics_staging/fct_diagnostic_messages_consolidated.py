"""
Consolidated Diagnostic Messages

This table consolidates CAN diagnostic messages directly from Kinesis source data into
a single efficient row per device per day. Raw diagnostic message arrays are exploded,
classified by transport protocol (ISOTP/J1939) and application protocol (UDS/J1979/J1939)
using enum values aligned with the can_recompiler library, then compressed back into
structured arrays. Enhanced protocol summary includes source diversity (unique ECUs) and
message diversity (unique PGNs/services) metrics critical for reverse engineering analysis.
This eliminates intermediate staging tables and provides 99% row count reduction while
maintaining full protocol classification and diversity tracking capability.
"""

from dagster import AssetExecutionContext
from dataweb import SQLDQCheck, build_general_dq_checks, table
from dataweb._core.dq_utils import Operator
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    Database,
    DQCheckMode,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.can.can_recompiler.core.enums import (
    ApplicationProtocolType,
    TransportProtocolType,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.enums import CanBusType
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    array_of,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    map_of,
    struct_with_comments,
)
from dataweb.userpkgs.firmware.table import KinesisStatsHistory, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import (
    create_run_config_overrides,
    format_date_partition_query,
)
from dataweb.userpkgs.utils import build_table_description

QUERY = """
WITH raw_messages AS (
    SELECT
        date,
        org_id,
        object_id AS device_id,
        time,
        -- Explode diagnostic_messages_seen array directly from Kinesis
        diagnostic_messages_seen.bus_type,
        diagnostic_messages_seen.txid,
        diagnostic_messages_seen.msg_id,
        diagnostic_messages_seen.last_reported_ago_ms,
        diagnostic_messages_seen.total_count
    FROM kinesisstats_history.osDDiagnosticMessagesSeen
    LATERAL VIEW EXPLODE(value.proto_value.diagnostic_messages_seen) AS diagnostic_messages_seen
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
        AND NOT value.is_databreak
        AND NOT value.is_end
        AND value.proto_value.diagnostic_messages_seen IS NOT NULL
),

-- Field extraction and cleanup: normalize fields without classification
cleaned_messages AS (
    SELECT
        date, org_id, device_id,
        COALESCE(bus_type, 0) AS bus_id,
        COALESCE(txid, 0) AS tx_id,  -- Keep raw tx_id for format detection

        -- Extract message_id: use provided msg_id if available, otherwise extract PGN for J1939 only
        CASE
            WHEN COALESCE(msg_id, 0) > 0 THEN COALESCE(msg_id, 0)  -- Use parsed msg_id (service ID for ISO-TP, PGN for J1939)
            -- Only extract PGN for J1939 29-bit CAN IDs, not for ISO-TP
            WHEN COALESCE(txid, 0) > 268435456  -- 29-bit CAN ID
                 AND FLOOR(COALESCE(txid, 0) / 256) % 262144 NOT BETWEEN 55808 AND 56319  -- NOT ISO-TP (0xDA00-0xDBFF)
                 THEN FLOOR(COALESCE(txid, 0) / 256) % 262144  -- Extract PGN for J1939: (tx_id >> 8) & 0x3FFFF
            ELSE 0  -- Keep as 0 for unparsed ISO-TP (service ID unknown)
        END AS message_id,

        last_reported_ago_ms,
        total_count

    FROM raw_messages
),

-- Protocol classification using clean fields
protocol_classified_messages AS (
    SELECT
        date, org_id, device_id,
        bus_id,
        tx_id,  -- Keep raw tx_id for format detection

        -- Normalize tx_id to 8-bit source address for J1939, keep as-is for others
        CASE
            WHEN tx_id > 268435456  -- 29-bit CAN ID
                 AND FLOOR(tx_id / 256) % 262144 NOT BETWEEN 55808 AND 56319  -- J1939 (not ISO-TP)
                 THEN tx_id % 256  -- Extract 8-bit source address from 29-bit J1939 CAN ID
            ELSE tx_id  -- Keep original for 8-bit J1939, ISO-TP, and NONE protocols
        END AS normalized_tx_id,

        message_id,

        -- TRANSPORT PROTOCOL CLASSIFICATION (enum values only)
        CASE
            -- J1939: 8-bit source addresses with valid PGNs
            WHEN tx_id BETWEEN 0 AND 253
                 AND message_id BETWEEN 0 AND 65535 THEN {transport_j1939}

            -- ISO-TP: 11-bit CAN IDs
            WHEN tx_id <= 2047 THEN {transport_isotp}

            -- 29-bit CAN IDs: distinguish ISO-TP vs J1939 by PGN pattern
            WHEN tx_id > 268435456 THEN  -- 29-bit range
                CASE
                    -- ISO-TP: 0x18DA#### pattern (PGN 0xDA00-0xDAFF range)
                    WHEN FLOOR(tx_id / 256) % 262144 BETWEEN 55808 AND 56063 THEN {transport_isotp}  -- 0xDA00-0xDAFF
                    -- ISO-TP: 0x18DB#### pattern (PGN 0xDB00-0xDBFF range)
                    WHEN FLOOR(tx_id / 256) % 262144 BETWEEN 56064 AND 56319 THEN {transport_isotp}  -- 0xDB00-0xDBFF
                    -- Otherwise assume J1939
                    ELSE {transport_j1939}
                END

            ELSE {transport_none}
        END AS transport_id,

        -- APPLICATION PROTOCOL CLASSIFICATION (enum values only)
        CASE
            -- J1939 context: 8-bit tx_id (0-255) with extracted message_id (PGN)
            WHEN tx_id <= 255 AND message_id > 0 THEN {app_j1939}

            -- 29-bit CAN IDs: use extracted or provided message_id for classification
            WHEN tx_id > 268435456 THEN  -- 29-bit range
                CASE
                    -- ISO-TP patterns → classify by message content (service IDs)
                    WHEN FLOOR(tx_id / 256) % 262144 BETWEEN 55808 AND 56319 THEN  -- 0xDA00-0xDBFF (ISO-TP)
                        CASE
                            -- Use processed message_id (service ID from payload) for UDS classification
                            WHEN message_id BETWEEN 1 AND 10 THEN {app_uds}        -- OBD-II request modes (unified as UDS)
                            WHEN message_id BETWEEN 65 AND 74 THEN {app_uds}       -- OBD-II response modes (unified as UDS)
                            WHEN message_id BETWEEN 16 AND 127 THEN {app_uds}      -- UDS service IDs
                            WHEN message_id > 127 THEN {app_uds}                    -- Extended UDS
                            -- If no payload service ID available, classify as UDS by default (most common ISO-TP usage)
                            ELSE {app_uds}  -- Default ISO-TP to UDS when service ID unknown
                        END
                    -- J1939 patterns
                    ELSE {app_j1939}
                END

            -- ISO-TP context: classify all diagnostic traffic as UDS based on processed message_id (service/mode)
            WHEN message_id BETWEEN 1 AND 10 THEN {app_uds}        -- OBD-II request modes (unified as UDS)
            WHEN message_id BETWEEN 65 AND 74 THEN {app_uds}       -- OBD-II response modes (unified as UDS)
            WHEN message_id BETWEEN 16 AND 127 THEN {app_uds}      -- UDS service IDs
            WHEN message_id > 127 THEN {app_uds}                    -- Extended UDS

            ELSE {app_none}  -- ApplicationProtocolType.NONE
        END AS application_id,

        AVG(COALESCE(last_reported_ago_ms, 0)) AS avg_last_reported_ago_ms,
        SUM(total_count) AS total_count

    FROM cleaned_messages
    GROUP BY date, org_id, device_id, bus_id, tx_id, normalized_tx_id, message_id, transport_id, application_id
),

-- Proprietary signal classification: uses protocol classifications for clarity
classified_messages AS (
    SELECT *,
        -- PROPRIETARY CLASSIFICATION for reverse engineering prioritization
        CASE
            -- Assume signals on J1708 bus are standard
            WHEN bus_id IN (
                {bus_j1708_j1587},
                {bus_j1708_9pin_pins_f_g},
                {bus_j1708_6pin_pins_a_b},
                {bus_j1708_rp1226_pins_6_13},
                {bus_j1708_j1587_multiple_cable_ids},
                {bus_j1708_cat_9pin_pins_h_j},
                {bus_j1708_cat_14pin_pins_f_g},
                {bus_j1708_copen_flying_leads}) THEN FALSE
            -- NONE protocol (either transport or application) is always proprietary (unknown signals)
            WHEN transport_id = {transport_none} OR application_id = {app_none} THEN TRUE

            -- J1939 proprietary PGN ranges (manufacturer-specific)
            -- Note: only when message_id > 0 (successfully parsed), not for unparsed 29-bit CAN IDs
            WHEN application_id = {app_j1939} THEN
                CASE
                    WHEN message_id BETWEEN 65280 AND 65535 THEN TRUE  -- FF00-FFFF: Proprietary B range
                    WHEN message_id BETWEEN 65024 AND 65279 THEN TRUE  -- FE00-FEFF: Proprietary A range
                    WHEN message_id BETWEEN 61440 AND 65023 THEN TRUE  -- F000-FDFF: Network management/proprietary range
                    WHEN message_id BETWEEN 256 AND 511 THEN TRUE      -- 0100-01FF: Proprietary range
                    ELSE FALSE  -- Standard J1939 PGNs
                END

            -- ISO-TP (UDS/J1979) are generally standard protocols
            ELSE FALSE
        END AS is_proprietary_signal

    FROM protocol_classified_messages
),

-- Aggregate protocol summary stats by application type with source/message diversity
protocol_summary_stats AS (
    SELECT
        date,
        org_id,
        device_id,
        application_id,
        SUM(total_count) AS protocol_total_count,
        COUNT(*) AS protocol_unique_combinations,                    -- Unique (bus_id, normalized_tx_id, message_id, transport_id) combinations
        COUNT(DISTINCT normalized_tx_id) AS protocol_unique_sources,            -- Unique ECU/source addresses (key for coverage diversity)
        COUNT(DISTINCT message_id) AS protocol_unique_message_ids,   -- Unique PGNs/service IDs (key for signal diversity)
        COUNT(DISTINCT CASE WHEN is_proprietary_signal THEN normalized_tx_id END) AS protocol_proprietary_sources,  -- Unique sources with proprietary signals

        -- Collect actual signal identifiers for debugging and analysis
        COLLECT_SET(normalized_tx_id) AS protocol_tx_ids,                       -- Set of all normalized tx_ids seen for this protocol
        COLLECT_SET(message_id) AS protocol_message_ids,             -- Set of all message_ids (PGNs/service IDs) seen for this protocol
        COLLECT_SET(
            CASE
                WHEN application_id = {app_j1939} THEN
                    -- Reconstruct 29-bit J1939 arbitration ID: 0x18000000 + (PGN << 8) + source_address
                    402653184 + (message_id * 256) + normalized_tx_id  -- 0x18000000 = 402653184
                ELSE tx_id  -- Use raw tx_id for NONE, ISO-TP protocols
            END
        ) AS protocol_arbitration_ids,
        COLLECT_SET(CASE WHEN is_proprietary_signal THEN normalized_tx_id END) AS protocol_proprietary_tx_ids,
        COLLECT_SET(CASE WHEN is_proprietary_signal THEN message_id END) AS protocol_proprietary_message_ids,
        COLLECT_SET(
            CASE WHEN is_proprietary_signal THEN
                CASE
                    WHEN application_id = {app_j1939} THEN
                        -- Reconstruct 29-bit J1939 arbitration ID for proprietary signals
                        -- 0x18000000 = 402653184
                        402653184 + (message_id * 256) + normalized_tx_id
                    -- Use raw tx_id for NONE, ISO-TP protocols
                    ELSE tx_id
                END
            END
        ) AS protocol_proprietary_arbitration_ids
    FROM classified_messages
    GROUP BY ALL
),

-- Create tx_id to message_ids mapping for signal source analysis
protocol_tx_id_mappings AS (
    SELECT
        date,
        org_id,
        device_id,
        application_id,
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    CAST(normalized_tx_id AS STRING) AS key,
                    message_ids AS value
                )
            )
        ) AS tx_id_to_message_ids
    FROM (
        SELECT
            date,
            org_id,
            device_id,
            application_id,
            normalized_tx_id,
            COLLECT_SET(message_id) AS message_ids
        FROM classified_messages
        GROUP BY date, org_id, device_id, application_id, normalized_tx_id
    )
    GROUP BY date, org_id, device_id, application_id
),

-- Aggregate proprietary signal identifiers across all protocols for trace prioritization
proprietary_signal_summary AS (
    SELECT
        date,
        org_id,
        device_id,
        -- Count unique signal identifiers with protocol prefix to ensure same values across different transport
        -- or applications are counted separately.
        COUNT(DISTINCT CASE
            WHEN transport_id = {transport_none} AND is_proprietary_signal
                THEN XXHASH64(0, normalized_tx_id)
            WHEN application_id = {app_none} AND is_proprietary_signal
                THEN XXHASH64(1, message_id)
            WHEN application_id = {app_uds} AND is_proprietary_signal
                THEN XXHASH64(2, message_id)
            WHEN application_id = {app_j1939} AND is_proprietary_signal
                THEN XXHASH64(3, message_id)
        END) AS total_proprietary_signal_ids,
        SUM(CASE WHEN is_proprietary_signal THEN total_count ELSE 0 END) AS total_proprietary_messages
    FROM classified_messages
    GROUP BY ALL
),

-- Collect diagnostic messages per device
device_messages AS (
    SELECT
        date,
        org_id,
        device_id,

        -- Structured messages array (enum values only, no names)
        COLLECT_LIST(STRUCT(
            bus_id,
            normalized_tx_id AS tx_id,
            message_id,
            -- Reconstruct arbitration ID for debugging/display
            CASE
                WHEN application_id = {app_j1939} THEN
                    -- Reconstruct 29-bit J1939 arbitration ID: 0x18000000 + (PGN << 8) + source_address
                    -- 0x18000000 = 402653184
                    402653184 + (message_id * 256) + normalized_tx_id
                -- Use raw tx_id for NONE, ISO-TP protocols
                ELSE tx_id
            END AS arbitration_id,
            transport_id,
            application_id,
            total_count,
            avg_last_reported_ago_ms
        )) AS diagnostic_messages

    FROM classified_messages
    GROUP BY ALL
),

-- Collect protocol summary per device
device_protocol_summary AS (
    SELECT
        date,
        org_id,
        device_id,

        -- Protocol summary for analytics (enum-based map)
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    pss.application_id AS key,
                    STRUCT(
                        pss.application_id,
                        pss.protocol_total_count AS total_count,
                        pss.protocol_unique_combinations AS unique_combinations,
                        pss.protocol_unique_sources AS unique_sources,
                        pss.protocol_unique_message_ids AS unique_message_ids,
                        pss.protocol_proprietary_sources AS proprietary_sources,
                        pss.protocol_tx_ids,
                        pss.protocol_message_ids,
                        pss.protocol_arbitration_ids,
                        pss.protocol_proprietary_tx_ids,
                        pss.protocol_proprietary_message_ids,
                        pss.protocol_proprietary_arbitration_ids,
                        ptm.tx_id_to_message_ids
                    ) AS value
                )
            )
        ) AS protocol_summary

    FROM protocol_summary_stats pss
    LEFT JOIN protocol_tx_id_mappings ptm USING (date, org_id, device_id, application_id)
    GROUP BY ALL
),

-- COMPRESS: Single row per device with structured diagnostic data
consolidated AS (
    SELECT
        dm.date,
        dm.org_id,
        dm.device_id,
        dm.diagnostic_messages,
        dps.protocol_summary,

        -- Proprietary signal summary for trace prioritization
        COALESCE(pss.total_proprietary_signal_ids, 0) AS total_proprietary_signal_ids,
        COALESCE(pss.total_proprietary_messages, 0) AS total_proprietary_messages

    FROM device_messages dm
    LEFT JOIN device_protocol_summary dps USING (date, org_id, device_id)
    LEFT JOIN proprietary_signal_summary pss USING (date, org_id, device_id)
)

SELECT * FROM consolidated
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="diagnostic_messages",
        type=array_of(
            struct_with_comments(
                (
                    "application_id",
                    DataType.INTEGER,
                    "ApplicationProtocolType enum: 0=NONE, 1=UDS, 2=J1979, 3=J1939. See can_recompiler.core.enums for mapping.",
                ),
                (
                    "avg_last_reported_ago_ms",
                    DataType.DOUBLE,
                    "Average last reported timestamp",
                ),
                ("bus_id", DataType.LONG, "CAN bus identifier"),
                (
                    "message_id",
                    DataType.LONG,
                    "PGN for J1939, service ID for ISO-TP, or 0 for NONE protocol",
                ),
                ("total_count", DataType.LONG, "Total message count"),
                (
                    "transport_id",
                    DataType.INTEGER,
                    "TransportProtocolType enum: 0=NONE, 1=ISOTP, 2=J1939. See can_recompiler.core.enums for mapping.",
                ),
                (
                    "tx_id",
                    DataType.LONG,
                    "ECU/source address (normalized to 8-bit for J1939)",
                ),
                (
                    "arbitration_id",
                    DataType.LONG,
                    "Full CAN arbitration ID: reconstructed 29-bit for J1939 (0x18PPPPSS), raw tx_id for others",
                ),
            )
        ),
        nullable=False,
        metadata=Metadata(
            comment="Array of classified diagnostic messages with enum-based protocol identification aligned with can_recompiler library"
        ),
    ),
    Column(
        name="protocol_summary",
        type=map_of(
            key_type=DataType.INTEGER,
            value_type=struct_with_comments(
                (
                    "application_id",
                    DataType.INTEGER,
                    "ApplicationProtocolType enum value: 0=NONE, 1=UDS, 2=J1979, 3=J1939",
                ),
                (
                    "total_count",
                    DataType.LONG,
                    "Total message count for this protocol type",
                ),
                (
                    "unique_combinations",
                    DataType.LONG,
                    "Count of unique (bus_id, tx_id, message_id, transport_id) combinations",
                ),
                (
                    "unique_sources",
                    DataType.LONG,
                    "Count of unique source addresses (tx_id) - indicates ECU diversity for reverse engineering",
                ),
                (
                    "unique_message_ids",
                    DataType.LONG,
                    "Count of unique message IDs (PGNs/services) - indicates signal type diversity for reverse engineering",
                ),
                (
                    "proprietary_sources",
                    DataType.LONG,
                    "Count of unique sources broadcasting proprietary signals within this protocol (NONE protocol or J1939 proprietary PGNs)",
                ),
                (
                    "protocol_tx_ids",
                    array_of(DataType.LONG),
                    "Array of all tx_ids (source addresses/CAN IDs) seen for this protocol - useful for debugging classification",
                ),
                (
                    "protocol_message_ids",
                    array_of(DataType.LONG),
                    "Array of all message_ids (PGNs/service IDs) seen for this protocol - useful for debugging classification",
                ),
                (
                    "protocol_arbitration_ids",
                    array_of(DataType.LONG),
                    "Array of all arbitration IDs seen for this protocol - reconstructed 29-bit for J1939, raw for others",
                ),
                (
                    "protocol_proprietary_tx_ids",
                    array_of(DataType.LONG),
                    "Array of tx_ids that broadcast proprietary signals within this protocol - key sources for reverse engineering",
                ),
                (
                    "protocol_proprietary_message_ids",
                    array_of(DataType.LONG),
                    "Array of proprietary message_ids (PGNs/service IDs) within this protocol - key signals for reverse engineering",
                ),
                (
                    "protocol_proprietary_arbitration_ids",
                    array_of(DataType.LONG),
                    "Array of arbitration IDs broadcasting proprietary signals - reconstructed 29-bit for J1939, raw for others",
                ),
                (
                    "tx_id_to_message_ids",
                    map_of(
                        key_type=DataType.STRING,
                        value_type=array_of(DataType.LONG),
                        value_contains_null=True,
                    ),
                    "Map from tx_id (source address/ECU) to array of message_ids (PGNs/service IDs) that tx_id broadcasts - shows which ECU sends which signals",
                ),
            ),
            value_contains_null=True,
        ),
        nullable=True,
        metadata=Metadata(
            comment="Enhanced diagnostic activity summary by ApplicationProtocolType with source/message diversity and proprietary signal metrics. Map: application_id_integer -> {application_id, total_count, unique_combinations, unique_sources, unique_message_ids, proprietary_sources, protocol_tx_ids[], protocol_message_ids[], protocol_arbitration_ids[], protocol_proprietary_tx_ids[], protocol_proprietary_message_ids[], protocol_proprietary_arbitration_ids[], tx_id_to_message_ids{}}. Arrays show actual signal identifiers for debugging classification. arbitration_ids arrays show reconstructed 29-bit CAN IDs for J1939, raw for others. tx_id_to_message_ids map shows which ECU (tx_id) broadcasts which signals (message_ids) - critical for reverse engineering signal source analysis."
        ),
    ),
    Column(
        name="total_proprietary_signal_ids",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total count of unique proprietary signal identifiers (TX IDs for NONE protocol, PGNs for J1939 proprietary) - key metric for trace prioritization representing signal diversity"
        ),
    ),
    Column(
        name="total_proprietary_messages",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total count of proprietary diagnostic messages (NONE protocol + J1939 proprietary PGNs) - indicates reverse engineering potential"
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Consolidates CAN diagnostic messages from Kinesis into single row per device per day. "
        "Messages are classified by transport/application protocols using can_recompiler enum values and "
        "compressed into structured arrays. Enhanced protocol summary tracks source diversity (unique ECUs) "
        "and message diversity (unique PGNs/services) - critical metrics for reverse engineering analysis. "
        "Eliminates intermediate staging tables with 99% row count reduction while maintaining full classification.",
        row_meaning="Each row represents a single device diagnostic activity for one day with all messages classified and structured into arrays.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    backfill_batch_size=10,
    upstreams=[
        AnyUpstream(KinesisStatsHistory.OSD_DIAGNOSTIC_MESSAGES_SEEN),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=16,
        min_workers=4,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_DIAGNOSTIC_MESSAGES_CONSOLIDATED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ) + [
        SQLDQCheck(
            name="dq_proprietary_signals_consistent",
            sql_query="""
                SELECT COUNT(*) AS observed_value FROM df
                WHERE (total_proprietary_signal_ids = 0) != (total_proprietary_messages = 0)
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
    ],
)
def fct_diagnostic_messages_consolidated(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        # Transport Protocol enum values
        transport_none=TransportProtocolType.NONE.value,
        transport_isotp=TransportProtocolType.ISOTP.value,
        transport_j1939=TransportProtocolType.J1939.value,
        # Application Protocol enum values
        app_none=ApplicationProtocolType.NONE.value,
        app_uds=ApplicationProtocolType.UDS.value,
        app_j1939=ApplicationProtocolType.J1939.value,
        # CAN Bus enum values
        bus_j1708_j1587=CanBusType.J1708_J1587.value,
        bus_j1708_9pin_pins_f_g=CanBusType.J1708_9PIN_PINS_F_G.value,
        bus_j1708_6pin_pins_a_b=CanBusType.J1708_6PIN_PINS_A_B.value,
        bus_j1708_rp1226_pins_6_13=CanBusType.J1708_RP1226_PINS_6_13.value,
        bus_j1708_j1587_multiple_cable_ids=CanBusType.J1708_J1587_MULTIPLE_CABLE_IDS.value,
        bus_j1708_cat_9pin_pins_h_j=CanBusType.J1708_CAT_9PIN_PINS_H_J.value,
        bus_j1708_cat_14pin_pins_f_g=CanBusType.J1708_CAT_14PIN_PINS_F_G.value,
        bus_j1708_copen_flying_leads=CanBusType.J1708_COPEN_FLYING_LEADS.value,
    )
