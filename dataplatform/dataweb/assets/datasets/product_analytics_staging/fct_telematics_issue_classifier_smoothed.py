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
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
WITH base AS (
  SELECT
    date,
    org_id,
    device_id,
    top_issue
  FROM {product_analytics_staging}.fct_telematics_issue_classifier
  WHERE date BETWEEN DATE_SUB("{date_start}", 6) AND "{date_end}"
),

-- Expand issue history using a 7-day rolling window per device
windowed_counts AS (
  SELECT
    date,
    org_id,
    device_id,
    top_issue,
    COUNT(*) OVER (
      PARTITION BY org_id, device_id, top_issue
      ORDER BY CAST(date AS TIMESTAMP)
      RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    ) AS rolling_count
  FROM base
),

-- Rank the most frequent issue for the 7-day window
ranked_issues AS (
  SELECT *,
    RANK() OVER (
      PARTITION BY date, org_id, device_id
      ORDER BY rolling_count DESC, top_issue ASC
    ) AS rank
  FROM windowed_counts
)

-- Select the top-ranked issue for that device on that day
SELECT
  date,
  org_id,
  device_id,
  top_issue AS most_frequent_top_issue
FROM ranked_issues
WHERE rank = 1
  AND date BETWEEN "{date_start}" AND "{date_end}";
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="most_frequent_top_issue",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The most frequent top issue."),
    )
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics issue classifier smoothed.",
        row_meaning="Telematics issue classifier smoothed for a device over a given time period. Gives the most frequent top issue over the past 7 days.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_ISSUE_CLASSIFIER),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_ISSUE_CLASSIFIER_SMOOTHED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_issue_classifier_smoothed(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)