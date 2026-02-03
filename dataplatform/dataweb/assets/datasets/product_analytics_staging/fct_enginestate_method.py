from dataclasses import replace
from dagster import AssetExecutionContext
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.metric import StrEnum
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    KinesisStatsHistory,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.firmware.metric import Metric, MetricEnum, StrEnum
from dataweb.userpkgs.query import format_date_partition_query

QUERY = """
WITH logOnly AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    MODE(coalesce(value.proto_value.engine_state.engine_activity_internal_feature_used, false)) AS is_log_only_esr
  FROM kinesisstats_history.osdenginestatelogonly
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
  AND NOT value.is_end
  AND NOT value.is_databreak
  GROUP BY date, org_id, object_id
), prod AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    MODE(coalesce(value.proto_value.engine_state.engine_activity_internal_feature_used, false)) AS is_prod_esr
  FROM kinesisstats_history.osdenginestate
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
  AND NOT value.is_end
  AND NOT value.is_databreak
  GROUP BY date, org_id, object_id
)
SELECT
  logOnly.date,
  logOnly.org_id,
  logOnly.device_id,
  CAST(NULL AS LONG) AS time,
  CAST(NULL AS LONG) AS bus_id,
  CAST(NULL AS LONG) AS request_id,
  CAST(NULL AS LONG) AS response_id,
  CAST(NULL AS LONG) AS obd_value,
  logOnly.is_log_only_esr,
  prod.is_prod_esr
FROM logonly
JOIN prod
USING (date, org_id, device_id)
"""

class TableDimension(StrEnum):
    IS_LOG_ONLY_ESR = "is_log_only_esr"
    IS_PROD_ESR = "is_prod_esr"

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
        name=TableDimension.IS_LOG_ONLY_ESR,
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether the log-only engine state method uses ESR (Engine State Refresh) based on engine activity internal feature.",
        )
    ),
    Column(
        name=TableDimension.IS_PROD_ESR,
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether the production engine state method uses ESR (Engine State Refresh) based on engine activity internal feature.",
        )
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


class TableMetric(MetricEnum):
    IS_LOG_ONLY_ESR = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_METHOD,
        field=TableDimension.IS_LOG_ONLY_ESR,
        label="engine_state_method_is_log_only_esr"
    )
    IS_PROD_ESR = Metric(
        type=ProductAnalyticsStaging.FCT_ENGINE_STATE_METHOD,
        field=TableDimension.IS_PROD_ESR,
        label="engine_state_method_is_prod_esr"
    )


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""
        This table shows whether the engine state refresh method was used in log-only or production engine state implementations.
        """.strip(),
        row_meaning="""
        Each row represents a device on a specific date, showing the engine state refresh method used in both log-only and production implementations.
        """.strip(),
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
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_STATE),
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_STATE_LOG_ONLY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=2,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ENGINE_STATE_METHOD.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_enginestate_method(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
