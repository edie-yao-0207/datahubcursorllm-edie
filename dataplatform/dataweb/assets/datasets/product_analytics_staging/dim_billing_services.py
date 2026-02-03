"""
Billing services dimension table (date-partitioned)

Maps unique service combinations from billing data per date. Uses billing columns directly
(team, region, product_group, dataplatform_feature, dataplatform_job_type, cost_allocation).

Includes billing_service_id hash for efficient joining from fact tables.
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
    struct_with_comments,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.userpkgs.firmware.cost_hierarchy import BILLING_SERVICE_ID_SQL
from dataweb.assets.datasets.product_analytics_staging.dim_tables import (
    DAGSTER_TABLE_ID_SQL,
)

# Primary key - hash ID for efficient joining
BILLING_SERVICE_ID_COLUMN = Column(
    name="billing_service_id",
    type=DataType.LONG,
    nullable=False,
    primary_key=True,
    metadata=Metadata(
        comment="xxhash64(date|service|region|cost_allocation) - PK for joining from fct_table_cost_daily"
    ),
)

# Dimension columns from billing
SERVICE_COLUMN = Column(
    name="service",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Full service name from billing"),
)

REGION_COLUMN = Column(
    name="region",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="AWS region where the service was run (us-west-2, eu-west-1)"),
)

TEAM_COLUMN = Column(
    name="team",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Team from billing - source of truth for ownership"),
)

PRODUCT_GROUP_COLUMN = Column(
    name="product_group",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Product group from billing (aianddata, telematics, etc.)"),
)

DATAPLATFORM_FEATURE_COLUMN = Column(
    name="dataplatform_feature",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(
        comment="DataPlatform feature/tool that delivered the data (dagster, spark, kinesis, etc.)"
    ),
)

DATAPLATFORM_JOB_TYPE_COLUMN = Column(
    name="dataplatform_job_type",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Job type from billing (databricksjob, data-pipeline, etc.)"),
)

COST_ALLOCATION_COLUMN = Column(
    name="cost_allocation",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Cost allocation category (COGS, R&D)"),
)

DATABRICKS_SKU_COLUMN = Column(
    name="databricks_sku",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Databricks SKU type (JOBS_COMPUTE, SQL_COMPUTE, etc.)"),
)

DATABRICKS_WORKSPACE_ID_COLUMN = Column(
    name="databricks_workspace_id",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Databricks workspace ID"),
)

# Struct for parsed service components (for services matching known patterns)
DAGSTER_JOB_STRUCT = struct_with_comments(
    ("database", DataType.STRING, "Database name parsed from service (databricksjob-dagster-{database}-{table})"),
    ("table", DataType.STRING, "Table name parsed from service"),
)

DAGSTER_JOB_COLUMN = Column(
    name="dagster_job",
    type=DAGSTER_JOB_STRUCT,
    nullable=True,
    metadata=Metadata(
        comment="Parsed components for dagster job services (databricksjob-dagster-{database}-{table}). NULL for non-dagster services."
    ),
)

# Table ID for joining with reliability tables (fct_partition_landings, agg_landing_reliability_rolling)
TABLE_ID_COLUMN = Column(
    name="table_id",
    type=DataType.LONG,
    nullable=True,
    metadata=Metadata(
        comment="xxhash64(region|database|table) - join key for reliability tables. NULL for non-dagster services."
    ),
)

# Schema definition - billing_service_id is the primary key
COLUMNS = [
    ColumnType.DATE,
    BILLING_SERVICE_ID_COLUMN,
    SERVICE_COLUMN,
    REGION_COLUMN,
    COST_ALLOCATION_COLUMN,
    TEAM_COLUMN,
    PRODUCT_GROUP_COLUMN,
    DATAPLATFORM_FEATURE_COLUMN,
    DATAPLATFORM_JOB_TYPE_COLUMN,
    DATABRICKS_SKU_COLUMN,
    DATABRICKS_WORKSPACE_ID_COLUMN,
    DAGSTER_JOB_COLUMN,
    TABLE_ID_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Extract distinct service combinations from billing per date
-- billing_service_id is hash of join keys for efficient FK lookups
WITH parsed AS (
  SELECT DISTINCT
    CAST(date AS STRING) AS date,
    {BILLING_SERVICE_ID_SQL} AS billing_service_id,
    service,
    region,
    costAllocation AS cost_allocation,
    team,
    productgroup AS product_group,
    dataplatformfeature AS dataplatform_feature,
    dataplatformjobtype AS dataplatform_job_type,
    databricks_sku,
    databricks_workspace_id,
    -- Parse dagster job services: databricksjob-dagster-<database>-<table>
    CASE
      WHEN service LIKE 'databricksjob-dagster-%' THEN
        NAMED_STRUCT(
          'database', SPLIT(service, '-')[2],
          'table', SPLIT(service, '-')[3]
        )
      ELSE NULL
    END AS dagster_job
  FROM billing.dataplatform_costs
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
)
SELECT
  date,
  billing_service_id,
  service,
  region,
  cost_allocation,
  team,
  product_group,
  dataplatform_feature,
  dataplatform_job_type,
  databricks_sku,
  databricks_workspace_id,
  dagster_job,
  -- table_id: hash of (region, database, table) for joining reliability tables
  CASE
    WHEN dagster_job IS NOT NULL THEN {DAGSTER_TABLE_ID_SQL}
    ELSE NULL
  END AS table_id
FROM parsed
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Date-partitioned billing service dimension with billing_service_id for efficient joining from fact tables.",
        row_meaning="Each row represents a unique service/region/cost_allocation combination for a specific date.",
        table_type=TableType.DAILY_DIMENSION,
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
        asset_name=ProductAnalyticsStaging.DIM_BILLING_SERVICES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_billing_services(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        BILLING_SERVICE_ID_SQL=BILLING_SERVICE_ID_SQL,
        DAGSTER_TABLE_ID_SQL=DAGSTER_TABLE_ID_SQL,
    )
