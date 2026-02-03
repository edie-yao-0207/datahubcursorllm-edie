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
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
)
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.query import build_population_schema_header, format_agg_date_partition_query, generate_distinct_count_columns
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)
from .agg_telematics_populations import DIMENSIONS, GROUPINGS

QUERY = """
WITH devices AS (
  SELECT
    dim.*,
    dim.device_type,
    classifier.most_frequent_top_issue AS issue
  FROM {product_analytics_staging}.fct_telematics_issue_classifier_smoothed AS classifier
  JOIN {product_analytics_staging}.dim_telematics_coverage_full AS dim USING (date, org_id, device_id)
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
  date,
  issue,
  {grouping_hash},
  {count_distinct_columns},
  {count_distinct_device_id}

FROM devices
GROUP BY
  date,
  issue,
  {grouping_sets}
"""

COLUMNS =  build_population_schema_header(
    partition_columns=[
        ColumnType.DATE,
        ColumnType.ISSUE,
    ],
    aggregate_columns=generate_distinct_count_columns(DIMENSIONS + [ColumnType.DEVICE_ID]),
)

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics population issues.",
        row_meaning="Telematics issues of populations of devices by type and equipment type.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_ISSUE_CLASSIFIER_SMOOTHED),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_POPULATION_ISSUES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_population_issues(context: AssetExecutionContext) -> str:
    return format_agg_date_partition_query(
        context,
        QUERY,
        DIMENSIONS,
        GROUPINGS,
    )