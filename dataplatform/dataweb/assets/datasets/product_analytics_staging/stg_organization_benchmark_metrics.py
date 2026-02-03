from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "metric_type", "lookback_window"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "metric_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of metric for inclusion in benchmarking. One of IDLING_PERCENT, CRASH_PER_MILLION_MI, SEVERE_SPEEDING, MOBILE_USAGE_EVENTS_PER_1000MI, MPGE, DROWSINESS_PER_1000MI, HOS_COMPLIANCE_TIME_PERCENT, IA_DRIVING_COMBINED_EVENTS_PER_1000MI, and VEHICLE_UTILIZATION.",
        },
    },
    {
        "name": "lookback_window",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Lookback window for metric",
        },
    },
    {
        "name": "is_metric_populated",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE for all cases. This value is incorporated downstream to support sizing the number of organizations for which there is a valid benchmark value.",
        },
    },
    {
        "name": "metric_value",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Calculation of value of metric for organization_id for previous 90, 181, or 363 days as of partition date.",
        },
    },
]

# Lookback windows: (label, days_back)
LOOKBACK_WINDOWS = [
    ("364d", 363),
    ("182d", 181),
    ("91d", 90),
]

# Metrics available in stg_organization_benchmark_metrics_daily with numerator/denominator
# All metrics except MPGE are sourced from the daily table

# Metrics that need 100x multiplier (percentage metrics)
PERCENTAGE_METRICS = {
    "IDLING_PERCENT",
    "SEVERE_SPEEDING",
    "VEHICLE_UTILIZATION",
}

# HOS uses inverted formula: 100 * (1 - num/denom) instead of 100 * (num/denom)
HOS_METRIC = "HOS_COMPLIANCE_TIME_PERCENT"

# Metrics that use simple sum(num)/sum(denom) with no scaling
RATIO_METRICS = [
    "CRASH_PER_MILLION_MI",
    "MOBILE_USAGE_EVENTS_PER_1000MI",
    "DROWSINESS_PER_1000MI",
    "IA_DRIVING_COMBINED_EVENTS_PER_1000MI",
]

# All metrics from the daily table
DAILY_METRICS = (
    list(PERCENTAGE_METRICS)
    + [HOS_METRIC]
    + RATIO_METRICS
)


def _build_daily_metric_query(metric_type: str, interval_label: str, days_back: int) -> str:
    """
    Build query for a metric from stg_organization_benchmark_metrics_daily.

    Aggregates daily numerators and denominators over the lookback window,
    then calculates the final metric value.

    Data is lagged by 14 days, so for partition date X, we look at data from
    (X - 14 - days_back) to (X - 14).

    Handles three types of metrics:
    1. Percentage metrics (IDLING_PERCENT, SEVERE_SPEEDING, VEHICLE_UTILIZATION):
       100 * (SUM(num) / SUM(denom))
    2. HOS metric: 100 * (1 - SUM(num) / SUM(denom)) with HAVING clause for valid range
    3. Ratio metrics (crashes, mobile usage, etc.): SUM(num) / SUM(denom)
    """
    # Determine the metric calculation formula
    if metric_type == HOS_METRIC:
        metric_formula = "100.0 * (1 - SUM(metric_numerator) / SUM(metric_denominator))"
        having_clause = "\n    HAVING 100.0 * (1 - SUM(metric_numerator) / SUM(metric_denominator)) BETWEEN 0 AND 100"
    elif metric_type in PERCENTAGE_METRICS:
        metric_formula = "100 * (SUM(metric_numerator) / SUM(metric_denominator))"
        having_clause = ""
    else:
        metric_formula = "SUM(metric_numerator) / SUM(metric_denominator)"
        having_clause = ""

    return f"""
    (SELECT '{{PARTITION_START}}' AS date,
        org_id,
        '{metric_type}' AS metric_type,
        '{interval_label}' AS lookback_window,
        TRUE AS is_metric_populated,
        {metric_formula} AS metric_value
    FROM product_analytics_staging.stg_organization_benchmark_metrics_daily
    WHERE metric_type = '{metric_type}'
    AND date BETWEEN DATE_SUB('{{PARTITION_START}}', {14 + days_back}) AND DATE_SUB('{{PARTITION_START}}', 14)
    GROUP BY org_id{having_clause})
    """


def _build_mpge_query(interval_label: str) -> str:
    """
    Build MPGE query (source table already has lookback_window column).
    No aggregation needed - just filter and select.

    Data is lagged by 14 days.
    """
    return f"""
    (SELECT '{{PARTITION_START}}' AS date,
        org_id,
        'MPGE' AS metric_type,
        '{interval_label}' AS lookback_window,
        TRUE AS is_metric_populated,
        mpge AS metric_value
    FROM metrics_repo.vehicle_mpg_by_org_avg
    WHERE date = DATE_SUB('{{PARTITION_START}}', 14)
    AND lookback_window = '{interval_label}'
    AND mpge IS NOT NULL)
    """


def _build_query() -> str:
    """
    Build the complete query by generating explicit subqueries for each
    metric × lookback window combination.

    Key design decisions:
    1. Source most metrics from stg_organization_benchmark_metrics_daily
       - Uses numerator/denominator aggregation for accurate multi-day calculations
    2. 14-day lag applied to all metrics for data stability
    3. Explicit UNION ALL instead of joins - allows Spark to parallelize independent scans
    4. MPGE sourced directly from vehicle_mpg_by_org_avg (not in daily table)
    """
    subqueries = []

    # Metrics from daily table (8 metrics × 3 lookbacks = 24 subqueries)
    for metric_type in DAILY_METRICS:
        for interval_label, days_back in LOOKBACK_WINDOWS:
            subqueries.append(_build_daily_metric_query(metric_type, interval_label, days_back))

    # MPGE - not in daily table (3 subqueries)
    for interval_label, _ in LOOKBACK_WINDOWS:
        subqueries.append(_build_mpge_query(interval_label))

    header = """--sql
-- Each metric is calculated for three lookback windows (91d, 182d, 364d).
-- Instead of joining with a lookback_windows table, we explicitly UNION ALL
-- each metric × lookback combination. This allows Spark to parallelize the
-- independent scans and simplifies the query plan.
"""
    # Strip leading/trailing whitespace from each subquery before joining
    cleaned_subqueries = [sq.strip() for sq in subqueries]
    return header + "\nUNION ALL\n".join(cleaned_subqueries) + "\n--endsql\n"


QUERY = _build_query()


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Calculation of benchmark metric values per organization_id lagged 2 weeks to ensure complete safety event countx.""",
        row_meaning="""Each row represents the calculation of a metric_type per organization_id and date.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_benchmark_metrics"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_benchmark_metrics",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_benchmark_metrics",
            non_null_columns=["date", "org_id", "metric_type", "lookback_window", "is_metric_populated"],
            block_before_write=True,
        ),
    ],
    upstreams=[
        "product_analytics_staging.stg_organization_benchmark_metrics_daily",
        "metrics_repo.vehicle_mpg_by_org_avg",
    ],
)
def stg_organization_benchmark_metrics(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_organization_benchmark_metrics")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    context.log.info(f"{query}")
    return query
