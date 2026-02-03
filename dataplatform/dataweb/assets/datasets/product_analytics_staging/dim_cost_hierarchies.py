"""
Cost hierarchies dimension table (date-partitioned)

Defines aggregation populations for cost analysis by creating consistent hierarchy
identifiers. Uses billing columns directly (team, region, product_group, dataplatform_feature).

Date-partitioned to support backfilling and historical lookups. Join on date + cost_hierarchy_id.

Two ways to select populations:

1. `grouping_columns` - Which dimension COLUMNS are in the grouping:
   - 'team' = team-only rollup (region aggregated)
   - 'region.team' = region + team
   - 'region.team.product_group.dataplatform_feature.service' = full service grain

2. `hierarchy_label` - The actual VALUES for those columns:
   - 'firmwarevdp' = team-only rollup with team='firmwarevdp'
   - 'us-west-2.firmwarevdp' = region + team rollup
   - 'us-west-2.firmwarevdp.signal_decoding.decoding.dim_signal_definitions' = full service

Example queries:
  -- All service-level groupings for firmwarevdp team
  WHERE grouping_columns = 'region.team.product_group.dataplatform_feature.service'
    AND team = 'firmwarevdp'

  -- Team-level rollup (no region breakdown)
  WHERE hierarchy_label = 'firmwarevdp'
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
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.userpkgs.firmware.cost_hierarchy import (
    HIERARCHY_ID_SQL,
    HIERARCHY_LABEL_SQL,
    GROUPING_COLUMNS_SQL,
)
from dataclasses import replace
from dataweb.userpkgs.firmware.hash_utils import (
    HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
)

# Cost-specific hierarchy ID - rename from generic HIERARCHY_ID_COLUMN
COST_HIERARCHY_ID_COLUMN = replace(
    HIERARCHY_ID_COLUMN,
    name="cost_hierarchy_id",
    metadata=Metadata(comment="xxhash64 of cost dimension values - FK to dim_cost_hierarchies"),
)

# Cost-specific column definitions
REGION_COLUMN = Column(
    name="region",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="AWS region (NULL if not in this hierarchy level)"),
)

TEAM_COLUMN = Column(
    name="team",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Team (NULL if not in this hierarchy level)"),
)

PRODUCT_GROUP_COLUMN = Column(
    name="product_group",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Product group (NULL if not in this hierarchy level)"),
)

DATAPLATFORM_FEATURE_COLUMN = Column(
    name="dataplatform_feature",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="DataPlatform feature (NULL if not in this hierarchy level)"),
)

SERVICE_COLUMN = Column(
    name="service",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Service name (NULL if not in this hierarchy level)"),
)

# Population count columns - help understand hierarchy cardinality
SERVICE_COUNT_COLUMN = Column(
    name="service_count",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of services in this hierarchy grouping"),
)

DISTINCT_REGIONS_COLUMN = Column(
    name="distinct_regions",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct regions in this grouping"),
)

DISTINCT_TEAMS_COLUMN = Column(
    name="distinct_teams",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct teams in this grouping"),
)

DISTINCT_PRODUCT_GROUPS_COLUMN = Column(
    name="distinct_product_groups",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct product groups in this grouping"),
)

DISTINCT_FEATURES_COLUMN = Column(
    name="distinct_features",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct dataplatform features in this grouping"),
)


# Schema definition - date-partitioned for backfill support
COLUMNS = [
    ColumnType.DATE,
    COST_HIERARCHY_ID_COLUMN,
    REGION_COLUMN,
    TEAM_COLUMN,
    PRODUCT_GROUP_COLUMN,
    DATAPLATFORM_FEATURE_COLUMN,
    SERVICE_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
    # Population counts
    SERVICE_COUNT_COLUMN,
    DISTINCT_REGIONS_COLUMN,
    DISTINCT_TEAMS_COLUMN,
    DISTINCT_PRODUCT_GROUPS_COLUMN,
    DISTINCT_FEATURES_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

# Use shared hierarchy SQL from cost_hierarchy module
QUERY = """
-- Get distinct dimension combinations from billing services per date
-- Includes population counts for understanding hierarchy cardinality
WITH service_dims AS (
  SELECT DISTINCT
    date,
    region,
    team,
    product_group,
    dataplatform_feature,
    service
  FROM {product_analytics_staging}.dim_billing_services
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
)

-- Generate all hierarchy levels using GROUPING SETS
SELECT
  date,
  -- Hash for consistent joining (uses shared HIERARCHY_ID_SQL)
  {HIERARCHY_ID_SQL} AS cost_hierarchy_id,
  region,
  team,
  product_group,
  dataplatform_feature,
  service,
  -- Which dimension columns are in this grouping (e.g., 'region.team.service')
  {GROUPING_COLUMNS_SQL} AS grouping_columns,
  -- Values for this grouping (e.g., 'us-west-2.firmwarevdp.dim_signal_definitions')
  {HIERARCHY_LABEL_SQL} AS hierarchy_label,
  -- Population counts help understand hierarchy cardinality
  COUNT(DISTINCT service) AS service_count,
  COUNT(DISTINCT region) AS distinct_regions,
  COUNT(DISTINCT team) AS distinct_teams,
  COUNT(DISTINCT product_group) AS distinct_product_groups,
  COUNT(DISTINCT dataplatform_feature) AS distinct_features
FROM service_dims
GROUP BY
  GROUPING SETS (
    -- Overall (no dimensions)
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
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Date-partitioned cost hierarchy dimension with consistent hash IDs for joining cost and usage tables.",
        row_meaning="Each row represents a unique hierarchy entry (dimension combination) for a specific date that can be used to look up dimension values.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_BILLING_SERVICES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_COST_HIERARCHIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_cost_hierarchies(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
        GROUPING_COLUMNS_SQL=GROUPING_COLUMNS_SQL,
        HIERARCHY_LABEL_SQL=HIERARCHY_LABEL_SQL,
    )
