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
    array_of,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, Definitions
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """

WITH data AS (
    SELECT
        dim.date,
        dim.org_id,
        dim.device_id,
        priority.type,
        COALESCE(coverage.percent_month_days_covered, 0) as percent_month_days_covered

    FROM {product_analytics_staging}.dim_telematics_coverage_full AS dim
    JOIN definitions.telematics_market_priority AS priority USING (market, engine_type)
    LEFT JOIN {product_analytics_staging}.fct_telematics_coverage_rollup_full AS coverage USING (date, type, org_id, device_id)
    WHERE dim.date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
    date,
    org_id,
    device_id,
    COLLECT_SET(CASE WHEN percent_month_days_covered > 0 THEN type ELSE NULL END) as covered_signal_types,
    COLLECT_SET(CASE WHEN percent_month_days_covered = 0 THEN type ELSE NULL END) as not_covered_signal_types
FROM data
GROUP BY ALL
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="covered_signal_types",
        type=array_of(DataType.STRING),
        nullable=True,
        metadata=Metadata(comment="The most common missing signal set."),
    ),
    Column(
        name="not_covered_signal_types",
        type=array_of(DataType.STRING),
        nullable=True,
        metadata=Metadata(comment="The most common missing signal set."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics MMYEF gaps.",
        row_meaning="Telematics MMYEF gaps for a device over a given time period. Gives the most common missing signal set and the number of devices affected by it.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Definitions.TELEMATICS_MARKET_PRIORITY),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_SUMMARY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_coverage_summary(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)