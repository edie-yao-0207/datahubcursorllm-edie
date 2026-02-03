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
            "comment": "Name of metric for inclusion in benchmarking. One of IDLING_PERCENT, CRASH_PER_MILLION_MI, SEVERE_SPEEDING, MOBILE_USAGE_EVENTS_PER_1000MI, DROWSINESS_PER_1000MI, IA_DRIVING_COMBINED_EVENTS_PER_1000MI, and VEHICLE_UTILIZATION.",
        },
    },
    {
        "name": "lookback_window",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Lookback window for metric, equal to 1 day for stg_organization_benchmark_metrics_daily.",
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
            "comment": "Calculation of value of metric for organization_id for partition date. The metrics IDLING_PERCENT, SEVERE_SPEEDING,, VEHICLE_UTILIZATION, HOS_COMPLIANCE_TIME_PERCENT require converting the value to a percentage to reflect the format used on the Fleet Benchmarks report.",
        },
    },
    {
        "name": "metric_numerator",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Numerator of metric value, can be summed over multiple days and used alongside denominator to produce long range benchmark values.",
        },
    },
    {
        "name": "metric_denominator",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Denominator of metric value, can be summed over multiple days and used alongside numerator to produce long range benchmark values.",
        },
    },
]

QUERY = """

(SELECT date,
        org_id,
       'IDLING_PERCENT' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       100 * (SUM(idle_duration_ms) / SUM(on_duration_ms)) AS metric_value,
       SUM(idle_duration_ms) AS metric_numerator,
       SUM(on_duration_ms) AS metric_denominator
FROM metrics_repo.vehicle_idle_duration_percent
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
UNION ALL
(SELECT date,
       org_id,
       'CRASH_PER_MILLION_MI' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       SUM(crash_count) / (SUM(distance_miles) / 1000000) AS metric_value,
       SUM(crash_count) AS metric_numerator,
       SUM(distance_miles) / 1000000 AS metric_denominator
FROM metrics_repo.vehicle_crashes_avg
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
UNION ALL
(SELECT date,
       org_id,
       'MOBILE_USAGE_EVENTS_PER_1000MI' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       SUM(mobile_usage_count) / (SUM(distance_miles) / 1000) AS metric_value,
       SUM(mobile_usage_count) AS metric_numerator,
       SUM(distance_miles) / 1000 AS metric_denominator
FROM metrics_repo.vehicle_mobile_usage_avg
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
UNION ALL
(SELECT date,
        org_id,
       'SEVERE_SPEEDING' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       100 * (SUM(severe_speeding_ms) / SUM(driving_time_ms)) AS metric_value,
       SUM(severe_speeding_ms) AS metric_numerator,
       SUM(driving_time_ms) AS metric_denominator
FROM metrics_repo.vehicle_severe_speeding_percent
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
UNION ALL
(SELECT date,
       org_id,
       'DROWSINESS_PER_1000MI' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       SUM(drowsy_count) / (SUM(distance_miles) / 1000) AS metric_value,
       SUM(drowsy_count) AS metric_numerator,
       SUM(distance_miles) / 1000 AS metric_denominator
FROM metrics_repo.vehicle_drowsiness_avg
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
UNION ALL
(SELECT date,
       org_id,
       'HOS_COMPLIANCE_TIME_PERCENT' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       100.0 * (1 - SUM(violation_duration_ms) / SUM(possible_driver_ms)) AS metric_value,
       SUM(violation_duration_ms) AS metric_numerator,
       SUM(possible_driver_ms) AS metric_denominator
FROM metrics_repo.org_hos_violation_ratio
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id
HAVING 100.0 * (1 - SUM(violation_duration_ms) / SUM(possible_driver_ms)) BETWEEN 0 AND 100)
UNION ALL
(SELECT date,
       org_id,
       'IA_DRIVING_COMBINED_EVENTS_PER_1000MI' AS metric_type,
       '1d' AS lookback_window,
       TRUE AS is_metric_populated,
       SUM(COALESCE(total_distraction_count, 0)) / (SUM(COALESCE(distance_miles, 0)) / 1000) AS metric_value,
       SUM(COALESCE(total_distraction_count, 0)) AS metric_numerator,
       SUM(COALESCE(distance_miles, 0)) / 1000 AS metric_denominator
FROM metrics_repo.vehicle_inattentive_driving_avg
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
UNION ALL
(SELECT date,
        org_id,
        'VEHICLE_UTILIZATION' AS metric_type,
        '1d' aS lookback_window,
        TRUE AS is_metric_populated,
        100.0 * (SUM(total_utilized_hours) / SUM(total_available_hours)) AS utilization,
        SUM(total_utilized_hours) AS metric_numerator,
        SUM(total_available_hours) AS metric_denominator
FROM metrics_repo.vehicle_utilization_by_org_percent
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY date, org_id)
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Calculation of benchmark metric values per organization_id.""",
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
        max_workers=32,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_benchmark_metrics_daily"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_benchmark_metrics_daily",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_benchmark_metrics_daily",
            non_null_columns=["date", "org_id", "metric_type", "lookback_window", "is_metric_populated"],
            block_before_write=True,
        ),
    ],
    backfill_batch_size=5,
    upstreams=[
        "metrics_repo.vehicle_idle_duration_percent",
        "metrics_repo.vehicle_severe_speeding_percent",
        "metrics_repo.vehicle_mobile_usage_avg",
        "metrics_repo.vehicle_crashes_avg",
        "metrics_repo.vehicle_drowsiness_avg",
        "metrics_repo.org_hos_violation_ratio",
        "metrics_repo.vehicle_inattentive_driving_avg",
        "metrics_repo.vehicle_utilization_by_org_percent",
    ],
)
def stg_organization_benchmark_metrics_daily(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_organization_benchmark_metrics_daily")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
