"""
Daily table-level cost tracking

Aggregates billing records by service. Join to dim_billing_services on 
date + billing_service_id to get team, product_group, dataplatform_feature, etc.
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
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, Billing
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
from dataweb.userpkgs.firmware.cost_hierarchy import BILLING_SERVICE_ID_SQL
from dataweb.assets.datasets.product_analytics_staging.dim_billing_services import (
    BILLING_SERVICE_ID_COLUMN,
)

# Schema definition - billing_service_id is FK to dim_billing_services
# Since billing_service_id hash includes all dimension columns (including databricks_*),
# we only need date + billing_service_id as primary keys
COLUMNS = [
    ColumnType.DATE,
    BILLING_SERVICE_ID_COLUMN,
    # Cost metrics
    Column(
        name="dbx_cost",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Databricks compute cost in USD"),
    ),
    Column(
        name="aws_cost",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="AWS infrastructure cost in USD"),
    ),
    Column(
        name="total_cost",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Total cost (DBX + AWS) in USD"),
    ),
    Column(
        name="total_dbu",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Total Databricks Units consumed"),
    ),
    Column(
        name="cost_per_dbu",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Cost per DBU in USD"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Aggregate billing costs by billing_service_id (hash includes all dimension columns)
-- Join to dim_billing_services on date + billing_service_id for dimensions
SELECT
  CAST(date AS STRING) AS date,
  {BILLING_SERVICE_ID_SQL} AS billing_service_id,
  SUM(COALESCE(dbx_cost, 0)) AS dbx_cost,
  SUM(COALESCE(aws_cost, 0)) AS aws_cost,
  SUM(COALESCE(dbx_cost, 0) + COALESCE(aws_cost, 0)) AS total_cost,
  SUM(COALESCE(dbus, 0)) AS total_dbu,
  CASE
    WHEN SUM(COALESCE(dbus, 0)) > 0 
    THEN SUM(COALESCE(dbx_cost, 0)) / SUM(COALESCE(dbus, 0))
    ELSE 0 
  END AS cost_per_dbu
FROM billing.dataplatform_costs
WHERE date BETWEEN "{date_start}" AND "{date_end}"
GROUP BY
  date,
  service,
  region,
  costAllocation,
  team,
  productgroup,
  dataplatformfeature,
  dataplatformjobtype,
  databricks_sku,
  databricks_workspace_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily costs by service. Join to dim_billing_services on date + billing_service_id for dimensions.",
        row_meaning="Each row represents the daily cost for a specific billing_service_id (unique per service/region/team/sku/workspace combination).",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Billing.DATAPLATFORM_COSTS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TABLE_COST_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_table_cost_daily(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        BILLING_SERVICE_ID_SQL=BILLING_SERVICE_ID_SQL,
    )
