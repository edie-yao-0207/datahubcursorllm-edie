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
    DQCheckMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    Metadata,
    DataType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    Definitions,
)
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

QUERY = """
WITH data AS (
  SELECT
    STRING(date) AS date,
    org_id,
    device_id,
    bus_id,
    COALESCE(tx_id, 0) AS tx_id,
    COALESCE(message_id, 0) AS message_id,
    sum_total_count AS total_count
  FROM product_analytics_staging.fct_osddiagnosticmessagesseen
  WHERE date BETWEEN "{date_start}" and "{date_end}"
),

pgns AS (
  SELECT DISTINCT pgn, pg_label
  FROM definitions.j1939_da_2023
  WHERE pgn IS NOT NULL
    AND pg_label NOT LIKE "% to %"
    AND pg_label NOT LIKE "%bit-mapped%"
    AND pgn = 0 OR pgn >= 256
),

classified_messages AS (
  SELECT
    date,
    org_id,
    device_id,
    bus_id,
    bus.name AS bus_name,
    tx_id,
    HEX(tx_id) AS hex_tx_id,
    message_id,
    HEX(message_id) AS hex_message_id,

    CASE
      WHEN tx_id <= 2047 AND message_id <= 4095 THEN 'UDS (11-bit)'
      WHEN tx_id > 2047 AND FLOOR(tx_id / 65536) % 256 = 218 THEN 'UDS (29-bit)'
      WHEN tx_id BETWEEN 0 AND 253 AND message_id BETWEEN 0 AND 65535 THEN 'J1939 (29-bit)'
      ELSE 'Unknown'
    END AS protocol,

    CASE 
      WHEN message_id BETWEEN 61440 AND 64999 THEN 'J1939 Proprietary A'
      WHEN message_id BETWEEN 65000 AND 65535 THEN 'J1939 Proprietary B'
      WHEN message_id BETWEEN 55808 AND 56063 THEN 'UDS PGN-like (0xDAxx)'
      ELSE 'Standard / Defined'
    END AS pgn_type,

    CASE
      WHEN tx_id = 2015 THEN 'Broadcast (UDS Functional)'
      WHEN message_id % 256 = 0 THEN 'Broadcast (J1939 PDU2)'
      WHEN message_id BETWEEN 55808 AND 56063 AND FLOOR(tx_id / 256) % 256 = 241 THEN 'Broadcast (UDS 29-bit)'
      ELSE 'Directed / Peer-to-Peer'
    END AS broadcast_type,

    COALESCE(
      CASE
        WHEN protocol LIKE "%J1939%" THEN pgns.pg_label
      END,
      "UNKNOWN"
    ) AS message_name,

    COALESCE(
      CASE
        WHEN protocol LIKE '%J1939%' THEN mapping.name
        WHEN tx_id = 2015 THEN 'UDS Functional Request (0x7DF)'
        WHEN tx_id BETWEEN 2016 AND 2023 THEN CONCAT('UDS Tester → ECU ', CAST(tx_id - 2016 AS STRING), ' (0x', HEX(tx_id), ')')
        WHEN tx_id BETWEEN 2024 AND 2031 THEN CONCAT('UDS ECU ', CAST(tx_id - 2024 AS STRING), ' → Tester (0x', HEX(tx_id), ')')
      END,
      'UNKNOWN'
    ) AS tx_id_name,

    total_count

  FROM data
  LEFT JOIN definitions.j1939_txid_to_source AS mapping ON data.tx_id = mapping.id
  LEFT JOIN definitions.can_bus_types AS bus ON data.bus_id = bus.id
  LEFT JOIN pgns ON data.message_id = pgns.pgn
)

SELECT * FROM classified_messages
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(ColumnType.BUS_ID.value, primary_key=True),
    replace(ColumnType.BUS_NAME.value, primary_key=True),
    Column(
        name="tx_id",
        type=DataType.LONG,
        metadata=Metadata(
            comment="Transaction ID",
        ),
        primary_key=True,
    ),
    Column(
        name="hex_tx_id",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Hexadecimal representation of the transaction ID",
        ),
        primary_key=True,
    ),
    Column(
        name="message_id",
        type=DataType.LONG,
        metadata=Metadata(
            comment="Message ID",
        ),
        primary_key=True,
    ),
    Column(
        name="hex_message_id",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Hexadecimal representation of the message ID",
        ),
        primary_key=True,
    ),
    Column(
        name="protocol",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Protocol",
        ),
        nullable=True,
    ),
    Column(
        name="pgn_type",
        type=DataType.STRING,
        metadata=Metadata(
            comment="PGN type",
        ),
        nullable=True,
    ),
    Column(
        name="broadcast_type",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Broadcast type",
        ),
        nullable=True,
    ),
    Column(
        name="message_name",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Message name",
        ),
        nullable=True,
    ),
    Column(
        name="tx_id_name",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Transaction ID name",
        ),
        nullable=True,
    ),
    Column(
        name="total_count",
        type=DataType.LONG,
        metadata=Metadata(
            comment="Total count",
        ),
        nullable=True,
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Fingerprint and classify diagnostic messages based on PGN, broadcast type, and protocol",
        row_meaning="Each row is a protocol fingerprinted message by bus/device/date",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Definitions.J1939_DA_2023),
        AnyUpstream(Definitions.J1939_TXID_TO_SOURCE),
        AnyUpstream(Definitions.CAN_BUS_TYPES),
        AnyUpstream(ProductAnalyticsStaging.FCT_OSD_DIAGNOSTIC_MESSAGES_SEEN),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_DIAGNOSTIC_MESSAGES_SEEN_CLASSIFIED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_osddiagnosticmessagesseen_classified(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)