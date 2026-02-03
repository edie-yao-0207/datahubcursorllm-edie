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
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.metric import Metric, MetricEnum
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import (
    build_table_description,
)
from pyspark.sql import DataFrame, SparkSession


COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TIME,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    Column(
        name="total_duration_different",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Total duration in milliseconds where ESR and legacy engine states differ"
        ),
    ),
    Column(
        name="total_duration_ms",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Total duration in milliseconds for all engine state periods"
        ),
    ),
    Column(
        name="pct_engine_state_disagreement",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of time where ESR and legacy engine states disagree (0.0 to 1.0)"
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_KEYS = get_non_null_columns(COLUMNS)


class TableMetric(MetricEnum):
    PCT_ENGINE_STATE_DISAGREEMENT = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_COMPARISON_STATS,
        field="pct_engine_state_disagreement",
        label="pct_engine_state_disagreement"
    )

QUERY = """
WITH stateComparison AS (

SELECT
date
, org_id
, device_id
, time
, end_time - time AS duration_ms
, prod_state != log_only_state AS is_state_different
FROM {product_analytics_staging}.fct_enginestate_comparison
WHERE date BETWEEN "{date_start}" AND "{date_end}"

), stats AS (

SELECT
  date
  , org_id
  , device_id
  , MAX(time) AS time
  , SUM(CASE WHEN is_state_different THEN duration_ms ELSE 0 END) AS total_duration_different
  , SUM(duration_ms) AS total_duration_ms
FROM stateComparison
GROUP BY
  date
  , org_id
  , device_id

)

SELECT
  *,
  CAST(NULL AS LONG) AS bus_id,
  CAST(NULL AS LONG) AS request_id,
  CAST(NULL AS LONG) AS response_id,
  CAST(NULL AS LONG) AS obd_value,
  total_duration_different / total_duration_ms AS pct_engine_state_disagreement
FROM stats
WHERE total_duration_ms > 30 * 60 * 1000 -- 30 minutes drive time minimum
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Statistics comparing legacy and ESR engine state implementations by device",
        row_meaning="Daily aggregated statistics showing percentage of time engine states differ between legacy and ESR implementations",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_ENGINE_STATE_COMPARISON),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ENGINE_STATE_COMPARISON_STATS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
)
def fct_enginestate_comparison_stats(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
