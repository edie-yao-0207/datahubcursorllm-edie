"""
Rolling cost aggregations at multiple granularities

Provides rolling window cost aggregations (1d, 7d, 30d, 90d, 365d) at various grouping 
levels (region, team, product_group, dataplatform_feature, service) with point-to-point deltas.

Uses consistent cost_hierarchy_id from dim_cost_hierarchies for joining with usage tables.
Dimension details can be retrieved by joining to dim_cost_hierarchies on cost_hierarchy_id.
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
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.schema import (
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_with_comments,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.userpkgs.firmware.cost_hierarchy import HIERARCHY_ID_SQL
from dataweb.assets.datasets.product_analytics_staging.dim_cost_hierarchies import (
    COST_HIERARCHY_ID_COLUMN,
)

# Define cost metrics struct type
COST_METRICS_STRUCT = struct_with_comments(
    ("cost", DataType.DOUBLE, "Cost in USD"),
    ("delta", DataType.DOUBLE, "Day-over-day change in cost"),
)

# Schema definition - just date and cost_cost_hierarchy_id for joining
COLUMNS = [
    ColumnType.DATE,
    COST_HIERARCHY_ID_COLUMN,
    Column(
        name="daily",
        type=COST_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily cost and delta metrics"),
    ),
    Column(
        name="weekly",
        type=COST_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="7-day rolling window cost and delta metrics"),
    ),
    Column(
        name="monthly",
        type=COST_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="30-day rolling window cost and delta metrics"),
    ),
    Column(
        name="quarterly",
        type=COST_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="90-day rolling window cost and delta metrics"),
    ),
    Column(
        name="yearly",
        type=COST_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="365-day rolling window cost and delta metrics"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Join fct_table_cost_daily to dim_billing_services via billing_service_id
WITH source_data AS (
  SELECT 
    c.date,
    s.region,
    s.team,
    s.product_group,
    s.dataplatform_feature,
    s.service,
    c.total_cost
  FROM {product_analytics_staging}.fct_table_cost_daily c
  JOIN {product_analytics_staging}.dim_billing_services s
    USING (date, billing_service_id)
  WHERE c.date BETWEEN DATE_SUB(DATE('{date_start}'), 364) AND DATE('{date_end}')
),

agg AS (
  SELECT
    date,
    -- Use shared HIERARCHY_ID_SQL for consistent joining
    {HIERARCHY_ID_SQL} AS cost_hierarchy_id,
    SUM(total_cost) AS total_cost
  FROM source_data
  GROUP BY
    GROUPING SETS (
      -- Overall
      (date),
      -- Region level
      (date, region),
      -- Team level
      (date, region, team),
      -- Product group level
      (date, region, team, product_group),
      -- DataPlatform feature level
      (date, region, team, product_group, dataplatform_feature),
      -- Service level (most granular)
      (date, region, team, product_group, dataplatform_feature, service),
      -- Common groupings for dashboard filters
      (date, team),
      (date, team, product_group),
      (date, team, product_group, dataplatform_feature)
    )
),

rolled AS (
  SELECT
    date,
    cost_hierarchy_id,

    total_cost AS daily_cost,

    SUM(total_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_cost,

    SUM(total_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_cost,

    SUM(total_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS rolling_90d_cost,

    SUM(total_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS rolling_365d_cost

  FROM agg
),

with_deltas AS (
  SELECT
    date,
    cost_hierarchy_id,
    daily_cost AS rolling_1d_cost,
    rolling_7d_cost,
    rolling_30d_cost,
    rolling_90d_cost,
    rolling_365d_cost,

    (daily_cost
     - LAG(daily_cost, 1) OVER (
           PARTITION BY cost_hierarchy_id
           ORDER BY date
       )
    ) AS delta_1d,

    (rolling_7d_cost
     - LAG(rolling_7d_cost, 1) OVER (
           PARTITION BY cost_hierarchy_id
           ORDER BY date
       )
    ) AS delta_7d,

    (rolling_30d_cost
     - LAG(rolling_30d_cost, 1) OVER (
           PARTITION BY cost_hierarchy_id
           ORDER BY date
       )
    ) AS delta_30d,

    (rolling_90d_cost
     - LAG(rolling_90d_cost, 1) OVER (
           PARTITION BY cost_hierarchy_id
           ORDER BY date
       )
    ) AS delta_90d,

    (rolling_365d_cost
     - LAG(rolling_365d_cost, 1) OVER (
           PARTITION BY cost_hierarchy_id
           ORDER BY date
       )
    ) AS delta_365d
  FROM rolled
)

SELECT
  date,
  cost_hierarchy_id,
  NAMED_STRUCT('cost', rolling_1d_cost, 'delta', delta_1d) AS daily,
  NAMED_STRUCT('cost', rolling_7d_cost, 'delta', delta_7d) AS weekly,
  NAMED_STRUCT('cost', rolling_30d_cost, 'delta', delta_30d) AS monthly,
  NAMED_STRUCT('cost', rolling_90d_cost, 'delta', delta_90d) AS quarterly,
  NAMED_STRUCT('cost', rolling_365d_cost, 'delta', delta_365d) AS yearly
FROM with_deltas
WHERE date BETWEEN "{date_start}" AND "{date_end}"
ORDER BY date, cost_hierarchy_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Rolling window cost aggregations with cost_hierarchy_id for joining to usage and dim_cost_hierarchies.",
        row_meaning="Each row represents aggregated costs over multiple time horizons (1d, 7d, 30d, 90d, 365d) for a specific hierarchy and date.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TABLE_COST_DAILY),
        AnyUpstream(ProductAnalyticsStaging.DIM_BILLING_SERVICES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_COSTS_ROLLING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_costs_rolling(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
    )
