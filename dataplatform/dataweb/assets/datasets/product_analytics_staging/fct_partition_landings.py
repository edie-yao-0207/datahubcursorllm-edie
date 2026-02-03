"""
Daily partition landing events with SLA metrics

Tracks when partitions land and calculates landing latency (days between partition date
and actual landing timestamp). Enables SLA monitoring for data freshness.

Join to dim_tables on (date, table_id) for region/database/table names.

Grain: one row per partition landing event (table_id + partition_date)
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    AWSRegion,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, Auditlog
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.schema import (
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.assets.datasets.product_analytics_staging.dim_tables import (
    TABLE_ID_COLUMN,
    TABLE_ID_SQL,
)

# Schema definition - table_id references dim_tables
COLUMNS = [
    ColumnType.DATE,
    TABLE_ID_COLUMN,
    Column(
        name="partition_date",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="The data partition date that landed"),
    ),
    Column(
        name="landed_timestamp",
        type=DataType.TIMESTAMP,
        nullable=True,
        metadata=Metadata(comment="Timestamp when the partition actually landed"),
    ),
    Column(
        name="landing_latency_days",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="Days between partition_date and landing_date"),
    ),
    Column(
        name="meets_1d_sla",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="True if partition landed within 1 day of partition_date"
        ),
    ),
    Column(
        name="meets_2d_sla",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="True if partition landed within 2 days of partition_date"
        ),
    ),
    Column(
        name="landing_hour",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="Hour of day when partition landed (0-23)"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Transform landed_partitions into a fact table with SLA metrics
-- Join to dim_tables for region/database/table names
-- landed_timestamp is stored as epoch seconds
SELECT
  CAST(DATE(FROM_UNIXTIME(landed_timestamp)) AS STRING) AS date,
  {TABLE_ID_SQL} AS table_id,
  CAST(DATE(landed_partition) AS STRING) AS partition_date,
  CAST(FROM_UNIXTIME(landed_timestamp) AS TIMESTAMP) AS landed_timestamp,
  DATEDIFF(DATE(FROM_UNIXTIME(landed_timestamp)), DATE(landed_partition)) AS landing_latency_days,
  DATEDIFF(DATE(FROM_UNIXTIME(landed_timestamp)), DATE(landed_partition)) <= 1 AS meets_1d_sla,
  DATEDIFF(DATE(FROM_UNIXTIME(landed_timestamp)), DATE(landed_partition)) <= 2 AS meets_2d_sla,
  HOUR(FROM_UNIXTIME(landed_timestamp)) AS landing_hour
FROM auditlog.landed_partitions
WHERE DATE(FROM_UNIXTIME(landed_timestamp)) BETWEEN DATE("{date_start}") AND DATE("{date_end}")
  AND landed_partition IS NOT NULL
  AND landed_timestamp IS NOT NULL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Partition landing events with SLA metrics. Join to dim_tables for table names.",
        row_meaning="Each row represents a single partition landing event with landing latency and SLA compliance flags.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Auditlog.LANDED_PARTITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_PARTITION_LANDINGS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_partition_landings(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context, TABLE_ID_SQL=TABLE_ID_SQL)
