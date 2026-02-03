from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    ValueRangeDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    get_trailing_end_of_quarter_dates,
    partition_key_ranges_from_context,
)
from datetime import date, datetime
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = ["date", "metric_type", "lookback_window", "metric_config_type", "cohort_id"]

METRIC_TYPE_FILE_MAPPING = {
    "benchmarkmetricssafetycrashes": ["CRASH_PER_MILLION_MI"],
    "benchmarkmetricsidling": ["IDLING_PERCENT"],
    "benchmarkmetricsmpge": ["MPGE"],
    "benchmarkmetricssafetyevents": [
        "SEVERE_SPEEDING",
        "MOBILE_USAGE_EVENTS_PER_1000MI",
        "DROWSINESS_PER_1000MI",
        "IA_DRIVING_COMBINED_EVENTS_PER_1000MI",
    ],
    "benchmarkmetricscompliance": ["HOS_COMPLIANCE_TIME_PERCENT"],
    "benchmarkmetricsefficiency": ["VEHICLE_UTILIZATION"],
}

METRICS_WHERE_HIGHER_VALUES_ARE_BETTER = ["MPGE", "HOS_COMPLIANCE_TIME_PERCENT", "VEHICLE_UTILIZATION"]

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
        "name": "cohort_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Id of cohort for corresponding category_id and category_value.",
        },
    },
    {
        "name": "metric_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of metric used",
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
        "name": "metric_config_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Configurable metric setting type, 'ALL' is used as the default value and for nonconfigurable metrics.",
        },
    },
    {
        "name": "mean",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Mean metric value across all organizations for which metric is populated.",
        },
    },
    {
        "name": "median",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Median metric value across all organizations for which metric is populated.",
        },
    },
    {
        "name": "top10_percentile",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "0.10 percentile value across all organizations.",
        },
    },
    {
        "name": "top25_percentile",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "0.25 percentile value across all organizations.",
        },
    },
    {
        "name": "bottom25_percentile",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "0.75 percentile value across all organizations.",
        },
    },
    {
        "name": "bottom10_percentile",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "0.90 percentile value across all organizations.",
        },
    },
    {
        "name": "cohort_size",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Size of cohort by number of valid benchmarking member organizations not inclusive of whether metric value is available for all organizations within cohort.",
        },
    },
]

QUERY = """
WITH
cohort_metric_values AS (
SELECT org_id,
       cohort_id,
       cohort_size,
       is_valid_benchmark_org,
       metric_value,
       metric_type,
       lookback_window,
       metric_config_type
FROM {SOURCE}.stg_organization_metric_configs
WHERE date = '{PARTITION_START}'),
cohort_metrics AS (
SELECT metric_values.cohort_id,
       metric_values.metric_type,
       metric_values.lookback_window,
       metric_values.metric_config_type,
       metric_values.cohort_size,
       AVG(metric_values.metric_value) AS mean,
       MEDIAN(metric_values.metric_value) AS median,
       CASE
           WHEN metric_values.metric_type IN ({HIGHER_BETTER_METRICS}) THEN PERCENTILE_APPROX(metric_values.metric_value, 0.9)
           ELSE PERCENTILE_APPROX(metric_values.metric_value, 0.1)
       END AS top10_percentile,
       CASE
           WHEN metric_values.metric_type IN ({HIGHER_BETTER_METRICS}) THEN PERCENTILE_APPROX(metric_values.metric_value, 0.75)
           ELSE PERCENTILE_APPROX(metric_values.metric_value, 0.25)
       END AS top25_percentile,
       CASE
           WHEN metric_values.metric_type IN ({HIGHER_BETTER_METRICS}) THEN PERCENTILE_APPROX(metric_values.metric_value, 0.25)
           ELSE PERCENTILE_APPROX(metric_values.metric_value, 0.75)
       END AS bottom25_percentile,
       CASE
           WHEN metric_values.metric_type IN ({HIGHER_BETTER_METRICS}) THEN PERCENTILE_APPROX(metric_values.metric_value, 0.1)
           ELSE PERCENTILE_APPROX(metric_values.metric_value, 0.9)
       END AS bottom10_percentile
FROM cohort_metric_values metric_values
GROUP BY 1,2,3,4,5)

SELECT '{PARTITION_START}' AS date,
       cohort_metrics.cohort_id,
       cohort_metrics.metric_type,
       cohort_metrics.lookback_window,
       cohort_metrics.metric_config_type,
       cohort_metrics.mean,
       cohort_metrics.median,
       cohort_metrics.top10_percentile,
       cohort_metrics.top25_percentile,
       cohort_metrics.bottom25_percentile,
       cohort_metrics.bottom10_percentile,
       cohort_metrics.cohort_size
FROM cohort_metrics
"""


def is_eoq(d: date) -> bool:
    """
    Check if a given date is an end-of-quarter (EoQ) date.
    """
    return (d.month, d.day) in {(3, 31), (6, 30), (9, 30), (12, 31)}


def generate_backfill_query(columns: list[str], dates_cte: str, base_cte: str) -> str:
    """
    Generate a backfill query for loading the benchmark CSVs.
    """

    backfill_query = """
        {dates_cte},
        {base_cte},
        most_recent_date AS (
            -- Get all relevant cohorts from most recent quarter end
            SELECT *
            FROM base
            WHERE date = (SELECT MAX(date) FROM dates)
        ),
        all_combinations AS (
            -- Cross join to get all combinations
            SELECT DISTINCT
                fd.cohort_id,
                fd.metric_type,
                fd.metric_config_type,
                fd.lookback_window,
                dv.date
            FROM most_recent_date fd
            CROSS JOIN dates dv
        ),
        joined AS (
            -- Take original values where possible
            SELECT
                ac.cohort_id,
                ac.metric_type,
                ac.metric_config_type,
                ac.lookback_window,
                ac.date,
                fd.mean AS original_mean,
                fd.median AS original_median,
                fd.top10_percentile AS original_top10_percentile,
                fd.top25_percentile AS original_top25_percentile,
                fd.bottom25_percentile AS original_bottom25_percentile,
                fd.bottom10_percentile AS original_bottom10_percentile,
                fd.cohort_size AS original_cohort_size
            FROM all_combinations ac
            LEFT JOIN base fd
                ON ac.cohort_id = fd.cohort_id
                AND ac.metric_type = fd.metric_type
                AND ac.metric_config_type = fd.metric_config_type
                AND ac.lookback_window = fd.lookback_window
                AND ac.date = fd.date
        ),
        backfilled AS (
            -- Impute from future
            SELECT
                cohort_id,
                metric_type,
                metric_config_type,
                lookback_window,
                date,
                FIRST_VALUE(original_mean) IGNORE NULLS OVER w AS mean,
                FIRST_VALUE(original_median) IGNORE NULLS OVER w AS median,
                FIRST_VALUE(original_top10_percentile) IGNORE NULLS OVER w AS top10_percentile,
                FIRST_VALUE(original_top25_percentile) IGNORE NULLS OVER w AS top25_percentile,
                FIRST_VALUE(original_bottom25_percentile) IGNORE NULLS OVER w AS bottom25_percentile,
                FIRST_VALUE(original_bottom10_percentile) IGNORE NULLS OVER w AS bottom10_percentile,
                FIRST_VALUE(original_cohort_size) IGNORE NULLS OVER w AS cohort_size,
                CASE
                    WHEN original_mean IS NULL AND FIRST_VALUE(original_mean) IGNORE NULLS OVER w IS NOT NULL THEN TRUE
                    ELSE FALSE
                END AS is_backfilled
            FROM joined
            WINDOW w AS (
                PARTITION BY cohort_id, metric_type, metric_config_type, lookback_window
                ORDER BY date ASC
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        )
        SELECT {COLUMNS}
        FROM backfilled
        """

    return backfill_query.format(
        COLUMNS=", ".join(columns),
        dates_cte=dates_cte,
        base_cte=base_cte,
    )


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Calculation of benchmark value and associated statistics for each metric_type and cohort_id.""",
        row_meaning="""Each row represents the benchmark value for a single metric_type for every cohort_id per date.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_cohort_benchmarks"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_cohort_benchmarks",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_cohort_benchmarks",
            non_null_columns=[
                "date",
                "cohort_id",
                "metric_type",
                "lookback_window",
                "metric_config_type",
                "mean",
                "median",
                "top10_percentile",
                "top25_percentile",
                "bottom25_percentile",
                "bottom10_percentile",
                "cohort_size",
            ],
            block_before_write=False,
        ),
        ValueRangeDQCheck(
            name="dq_value_range_stg_cohort_benchmarks",
            column_range_map={"cohort_size": (9, 50000)},
            block_before_write=False,
        ),
    ],
    upstreams=[
        "us-west-2:product_analytics_staging.stg_organization_metric_configs",
    ],
)
def stg_cohort_benchmarks(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_cohort_benchmarks")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    if get_run_env() == "dev":
        path = "/Volumes/s3/benchmarking-metrics/root/staging/"
        source = "datamodel_dev"
    else:
        # TODO change to prod when backend is ready to read from prod path
        # "/Volumes/s3/benchmarking-metrics/root/prod/" + metric_type_file_mapping
        path = "/Volumes/s3/benchmarking-metrics/root/staging/"
        source = "product_analytics_staging"
    folder_names = METRIC_TYPE_FILE_MAPPING.keys()
    metric_types = []
    for folder in folder_names:
        metric_types = metric_types + METRIC_TYPE_FILE_MAPPING[folder]
    METRIC_TYPE_LIST = (
        "ARRAY(" + ", ".join("'" + metric + "'" for metric in metric_types) + ")"
    )
    HIGHER_BETTER_METRICS = ", ".join(
        f"'{metric}'" for metric in METRICS_WHERE_HIGHER_VALUES_ARE_BETTER
    )
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        METRIC_TYPE_LIST=METRIC_TYPE_LIST,
        HIGHER_BETTER_METRICS=HIGHER_BETTER_METRICS,
        SOURCE=source,
    )
    df = spark.sql(query)
    df.createOrReplaceTempView("df")
    context.log.info(f"{query}")
    run_date = datetime.strptime(PARTITION_START, "%Y-%m-%d")
    quarter_ends = get_trailing_end_of_quarter_dates(PARTITION_START)
    if is_eoq(run_date):
        quarter_ends.remove(run_date.strftime("%Y-%m-%d")) # remove current date since it's coming from the df instead
    for folder_name in folder_names:
        full_path = path + folder_name
        metrics = METRIC_TYPE_FILE_MAPPING[folder_name]
        columns = [
            "cohort_id",
            "metric_type",
            "mean",
            "median",
            "top10_percentile",
            "top25_percentile",
            "bottom25_percentile",
            "bottom10_percentile",
            "cohort_size",
            "metric_config_type",
            "lookback_window",
            "date",
        ]
        # get benchmarks at each quarterly milestone
        if is_eoq(run_date):
            # union as it's a completed quarter
            date_cte = "WITH dates AS (\n" + \
                        "\nUNION ALL\n".join([f"SELECT DATE('{d}') AS date" for d in quarter_ends]) + "\n" + \
                        "UNION ALL SELECT DATE('{PARTITION_START}') AS date\n)".format(PARTITION_START=PARTITION_START)

            base_cte = """
            base AS (
                SELECT
                {COLUMNS}
                FROM {SOURCE}.stg_cohort_benchmarks
                WHERE metric_type IN ({metrics})
                AND date IN (
                    {quarter_ends}
                )

                UNION ALL

                SELECT
                {COLUMNS}
                FROM df
                WHERE metric_type IN ({metrics})
            )
            """.format(
                COLUMNS=", ".join(columns),
                metrics=", ".join(f"'{metric}'" for metric in metrics),
                SOURCE=source,
                quarter_ends=", ".join(f"'{quarter_end}'" for quarter_end in quarter_ends),
            )

            quarterly_query = generate_backfill_query(
                columns=columns,
                dates_cte=date_cte,
                base_cte=base_cte,
            )
        else:
            # take previous 4 completed quarters
            date_cte = "WITH dates AS (\n" + \
                        "\nUNION ALL\n".join([f"SELECT DATE('{d}') AS date" for d in quarter_ends]) + "\n)"

            base_cte = """
            base AS (
                SELECT
                {COLUMNS}
                FROM {SOURCE}.stg_cohort_benchmarks
                WHERE metric_type IN ({metrics})
                AND date IN (
                    {quarter_ends}
                )
            )
            """.format(
                COLUMNS=", ".join(columns),
                metrics=", ".join(f"'{metric}'" for metric in metrics),
                SOURCE=source,
                quarter_ends=", ".join(f"'{quarter_end}'" for quarter_end in quarter_ends),
            )

            quarterly_query = generate_backfill_query(
                columns=columns,
                dates_cte=date_cte,
                base_cte=base_cte,
            )

        context.log.info(f"Quarterly query: {quarterly_query}")
        combined_df = spark.sql(
            quarterly_query
        )
        combined_df.coalesce(1).write.mode("overwrite").option("header", "true").option(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        ).csv(full_path)
        metrics_str = ", ".join(metrics)
        context.log.info(f"Updated csv for benchmark metric: {metrics_str}")
    return df
