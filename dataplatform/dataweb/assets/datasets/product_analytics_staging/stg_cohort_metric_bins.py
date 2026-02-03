from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    table,
)
from dataweb._core.dq_utils import Operator
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
    get_run_env,
    partition_key_ranges_from_context,
    verify_data_equality,
)
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, lit

PRIMARY_KEYS = [
    "date",
    "cohort_id",
    "metric_type",
    "lookback_window",
    "metric_config_type",
    "bin_min",
    "bin_max",
]

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
            "comment": "Id of cohort for corresponding metric_type",
        },
    },
    {
        "name": "metric_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of metric for which bins are calculated to support histogram rendering.",
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
            "comment": "Name of metric configuration for which bins are calculated, 'ALL' is used as the default value and for nonconfigurable metrics.",
        },
    },
    {
        "name": "bin_min",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Start of range for current bin, wherein the number of decimals for rounding is determined by the bin width.",
        },
    },
    {
        "name": "bin_max",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "End of range for current bin (can be equal to bin_min), wherein the number of decimals for rounding is determined by the bin width.",
        },
    },
    {
        "name": "count_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of orgs within bin.",
        },
    },
    {
        "name": "distinct_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of valid benchmarking organizations with metric values.",
        },
    },
    {
        "name": "percentage",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Percentage of benchmark organizations within cohort_id where the metric type is available that fall between bin_min and bin_max.",
        },
    },
]

QUERY = """
WITH
metric_values AS (
SELECT org_id,
       cohort_id,
       cohort_size,
       is_metric_populated,
       CASE WHEN metric_type = 'IDLING_PERCENT' AND metric_value > 100 THEN 100 ELSE metric_value END AS metric_value,
       metric_type,
       lookback_window,
       metric_config_type
FROM {SOURCE}.stg_organization_metric_configs
WHERE date = '{PARTITION_START}'
AND metric_value >= 0
AND metric_type IN {ALL_METRICS}),
cohort_metric_values AS (
SELECT metric_values.cohort_id,
       metric_values.metric_type,
       metric_values.lookback_window,
       metric_values.metric_config_type,
       metric_values.cohort_size,
       metric_values.metric_value,
       metric_values.org_id
FROM metric_values),
capped_values AS (
select cohort_id,
       metric_type,
       lookback_window,
       metric_config_type,
       MIN(metric_value) as min_metric_value,
       MAX(metric_value) as max_metric_value,
       CASE WHEN PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY metric_value) > 0
            THEN PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY metric_value)
            WHEN MAX(metric_value) = 0 THEN 0
            ELSE MIN(CASE WHEN metric_value > 0 THEN metric_value END) END as cap_threshold,
       COUNT(DISTINCT metric_value) AS distinct_metric_values,
       COUNT(DISTINCT org_id) AS distinct_orgs
from cohort_metric_values
group by 1,2,3,4),
cohort_nbins AS (
(SELECT cmv.cohort_id,
       cmv.metric_type,
       cmv.lookback_window,
       cmv.metric_config_type,
       MIN(cmv.metric_value) AS min_metric_value,
       MAX(cmv.metric_value) AS max_metric_value,
       COUNT(DISTINCT cmv.metric_value) AS distinct_metric_values,
       COUNT(DISTINCT cmv.org_id) AS distinct_orgs,
       --if more bins are proposed by Sturges formula than there are distinct values, reduce to number of distinct values
       CEILING(1 + LOG(2,COUNT(DISTINCT cmv.org_id))) AS n_bins,
       (MAX(cmv.metric_value) -  MIN(cmv.metric_value)) /
        CEILING(1 + LOG(2,COUNT(DISTINCT cmv.org_id))) AS bin_width
FROM cohort_metric_values cmv
WHERE metric_type IN {STANDARD_BIN_METRICS}
GROUP BY 1,2,3,4)
UNION ALL
(SELECT cmv.cohort_id,
       cmv.metric_type,
       cmv.lookback_window,
       cmv.metric_config_type,
       MIN(cmv.metric_value) AS min_metric_value,
       MAX(CASE WHEN cmv.metric_value <= capped_values.cap_threshold THEN cmv.metric_value END) AS max_metric_value,
       COUNT(DISTINCT CASE WHEN cmv.metric_value <= capped_values.cap_threshold THEN cmv.metric_value END) AS distinct_metric_values,
       COUNT(DISTINCT CASE WHEN cmv.metric_value <= capped_values.cap_threshold THEN cmv.org_id END) AS distinct_orgs,
       --if more bins are proposed by Sturges formula than there are distinct values, reduce to number of distinct values
       CEILING(1 + LOG(2,COUNT(DISTINCT CASE WHEN cmv.metric_value <= capped_values.cap_threshold THEN cmv.org_id END)))n_bins,
       (MAX(CASE WHEN cmv.metric_value <= capped_values.cap_threshold THEN cmv.metric_value END) -  MIN(cmv.metric_value)) /
       CEILING(1 + LOG(2,COUNT(DISTINCT CASE WHEN cmv.metric_value <= capped_values.cap_threshold THEN cmv.org_id END))) AS bin_width
FROM cohort_metric_values cmv
INNER JOIN capped_values
     ON capped_values.cohort_id = cmv.cohort_id
     and capped_values.metric_type = cmv.metric_type
     and capped_values.lookback_window = cmv.lookback_window
     and capped_values.metric_config_type = cmv.metric_config_type
     and cmv.metric_value <= capped_values.cap_threshold
WHERE cmv.metric_type IN {CAPPED_BIN_METRICS}
GROUP BY 1,2,3,4)),
histogram_bins_pre AS (
  SELECT cohort_id,
         metric_type,
         lookback_window,
         metric_config_type,
         n_bins,
         bin_index,
         position,
         bin_width,
         min_metric_value + (bin_width * bin_index) AS bin_min_raw,
         min_metric_value + (bin_width * (bin_index + 1)) AS bin_max_raw,
         case when bin_index = 0 then true else false end as is_first_bin,
         case when bin_index = n_bins - 1 then true else false end as is_last_bin,
         CASE WHEN bin_width < 0.01 THEN ROUND(min_metric_value + (bin_width * bin_index), 3)
             WHEN bin_width >= .01 and bin_width < 0.1 THEN ROUND(min_metric_value + (bin_width * bin_index), 2)
             WHEN bin_width >= .1 and bin_width < 1 THEN ROUND(min_metric_value + (bin_width * bin_index), 1)
             WHEN bin_width >= 1 and bin_width < 10 THEN ROUND(min_metric_value + (bin_width * bin_index))
             WHEN bin_width >= 10 THEN ROUND(min_metric_value + (bin_width * bin_index), -1) END as bin_min,
        CASE WHEN bin_width < 0.01 THEN ROUND(min_metric_value + (bin_width * (bin_index + 1)),3)
             WHEN bin_width >= .01 and bin_width < 0.1 THEN ROUND(min_metric_value + (bin_width * (bin_index + 1)), 2)
             WHEN bin_width >= .1 and bin_width < 1 THEN ROUND(min_metric_value + (bin_width * (bin_index + 1)), 1)
             WHEN bin_width >= 1 and bin_width < 10 THEN ROUND(min_metric_value + (bin_width * (bin_index + 1)))
             WHEN bin_width >= 10 THEN ROUND(min_metric_value + (bin_width * (bin_index + 1)), -1) END as bin_max
  FROM cohort_nbins
  LATERAL VIEW POSEXPLODE(SEQUENCE(0, n_bins - 1)) AS position, bin_index),
    histogram_bins_dedup_stg AS (
  --eliminate cases where rounding is not sufficient to change the bin min or max
  SELECT * EXCEPT (position, is_last_bin, n_bins),
         LEAD(bin_min) OVER  (PARTITION BY cohort_id, metric_type, lookback_window, metric_config_type ORDER BY bin_min ASC) AS next_bin_min
  FROM histogram_bins_pre),
  histogram_bins_dedup AS (
  SELECT * EXCEPT(next_bin_min),
        RANK() OVER (PARTITION BY cohort_id, metric_type, lookback_window, metric_config_type ORDER BY bin_min ASC) AS position
  FROM histogram_bins_dedup_stg
  WHERE bin_min != bin_max OR
       (bin_min = bin_max AND (next_bin_min IS NULL OR next_bin_min != bin_max))),
  histogram_nbins_dedup AS (
   SELECT cohort_id, metric_type, lookback_window, metric_config_type, COUNT(*) AS n_bins
   FROM histogram_bins_dedup
   GROUP BY 1,2,3,4),
  histogram_bins AS (
  SELECT dedup.*,
         histogram_nbins_dedup.n_bins,
        CASE WHEN dedup.position = histogram_nbins_dedup.n_bins THEN TRUE ELSE FALSE END AS is_last_bin
  FROM histogram_bins_dedup dedup
  LEFT JOIN histogram_nbins_dedup
          ON histogram_nbins_dedup.cohort_id = dedup.cohort_id
          AND histogram_nbins_dedup.metric_type = dedup.metric_type
          AND histogram_nbins_dedup.lookback_window = dedup.lookback_window
          AND histogram_nbins_dedup.metric_config_type = dedup.metric_config_type),
  capped_last_bin AS (
    SELECT hb.cohort_id,
           hb.metric_type,
           hb.lookback_window,
           hb.metric_config_type,
           hb.n_bins,
           hb.bin_max AS bin_min,
           CASE WHEN bin_width < 0.01 THEN ROUND(capped.max_metric_value, 3)
             WHEN bin_width >= .01 and bin_width < 0.1 THEN ROUND(capped.max_metric_value, 2)
             WHEN bin_width >= .1 and bin_width < 1 THEN ROUND(capped.max_metric_value, 1)
             WHEN bin_width >= 1 and bin_width < 10 THEN ROUND(capped.max_metric_value)
             WHEN bin_width >= 10 THEN ROUND(capped.max_metric_value, -1) END AS bin_max,
           hb.position + 1 as position,
           TRUE as is_last_bin,
           hb.bin_width
    FROM histogram_bins hb
    LEFT JOIN capped_values capped
    ON capped.cohort_id = hb.cohort_id
    AND capped.metric_type = hb.metric_type
    AND capped.lookback_window = hb.lookback_window
    AND capped.metric_config_type = hb.metric_config_type
    WHERE hb.is_last_bin = TRUE
    AND hb.metric_type IN {CAPPED_BIN_METRICS}
    GROUP BY ALL),
  bins_unioned_pre AS (
   (SELECT cohort_id,
           metric_type,
           lookback_window,
           metric_config_type,
           n_bins,
           bin_min,
           bin_max,
           position,
          CASE WHEN metric_type IN {STANDARD_BIN_METRICS} AND is_last_bin = TRUE THEN TRUE ELSE FALSE END AS is_last_bin,
          bin_width
   FROM histogram_bins)
   UNION ALL
   (SELECT * FROM capped_last_bin)),
    bins_unioned AS (
    SELECT *
    FROM bins_unioned_pre
    WHERE bin_min != bin_max OR
       (bin_min = bin_max AND (position = 0 OR is_last_bin = TRUE))),
  cohort_metric_values_rounded AS (
   SELECT cmv.cohort_id,
          cmv.metric_type,
          cmv.lookback_window,
          cmv.metric_config_type,
          cmv.cohort_size,
          CASE WHEN bins.bin_width < 0.01 THEN ROUND(cmv.metric_value,3)
             WHEN bins.bin_width >= .01 and bins.bin_width < 0.1 THEN ROUND(cmv.metric_value, 2)
             WHEN bins.bin_width >= .1 and bins.bin_width < 1 THEN ROUND(cmv.metric_value, 1)
             WHEN bins.bin_width >= 1 and bins.bin_width < 10 THEN ROUND(cmv.metric_value)
             WHEN bins.bin_width >= 10 THEN ROUND(cmv.metric_value,-1) END AS metric_value,
          cmv.org_id
   FROM cohort_metric_values cmv
   LEFT JOIN (SELECT DISTINCT cohort_id, metric_type, lookback_window, metric_config_type, bin_width FROM histogram_bins) bins
    ON bins.cohort_id = cmv.cohort_id AND bins.metric_type = cmv.metric_type AND bins.lookback_window = cmv.lookback_window AND bins.metric_config_type = cmv.metric_config_type)

SELECT '{PARTITION_START}' AS date,
       histogram_bins.cohort_id,
       histogram_bins.metric_type,
       histogram_bins.lookback_window,
       histogram_bins.metric_config_type,
       histogram_bins.bin_min,
       histogram_bins.bin_max,
       COUNT(DISTINCT cohort_metric_values.org_id) AS count_orgs,
       MAX(capped.distinct_orgs) AS distinct_orgs,
       CAST(ROUND(100.0 * COUNT(DISTINCT cohort_metric_values.org_id)/ MAX(capped.distinct_orgs), 2) AS DOUBLE) AS percentage
FROM bins_unioned histogram_bins
LEFT OUTER JOIN capped_values capped
ON capped.cohort_id = histogram_bins.cohort_id
AND capped.metric_type = histogram_bins.metric_type
AND capped.lookback_window = histogram_bins.lookback_window
AND capped.metric_config_type = histogram_bins.metric_config_type
LEFT OUTER JOIN cohort_metric_values_rounded cohort_metric_values
  ON cohort_metric_values.cohort_id = histogram_bins.cohort_id
  AND cohort_metric_values.metric_type = histogram_bins.metric_type
  AND cohort_metric_values.lookback_window = histogram_bins.lookback_window
  AND cohort_metric_values.metric_config_type = histogram_bins.metric_config_type
  --for first bin to second to last bin, value must be >= bin min and < bin max
  AND ( (histogram_bins.is_last_bin = FALSE
  AND cohort_metric_values.metric_value >= histogram_bins.bin_min
  AND (cohort_metric_values.metric_value < histogram_bins.bin_max or histogram_bins.bin_max = 0))
  --for last bin, value must be >= bin min and <= bin max
       OR (histogram_bins.is_last_bin = TRUE AND
       (cohort_metric_values.metric_value >= histogram_bins.bin_min or histogram_bins.bin_max = 0)
       AND cohort_metric_values.metric_value <= histogram_bins.bin_max ))
GROUP BY ALL
order by metric_type asc, cohort_id desc
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Calculation of histogram bin ranges and percentage of peers within each bin for each cohort_id and metric_type to support the fleet benchmarks report.""",
        row_meaning="""Each row represents the binning for a cohort_id and metric_type per date.""",
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
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        min_workers=8,
        max_workers=8,
        spark_conf_overrides={
            "spark.sql.shuffle.partitions": "4096",
            "spark.executor.memory": "10g",
        },
    ),
    dq_checks=[
        SQLDQCheck(
            name="dq_sql_min_percentage_sum_stg_cohort_metric_bins",
            sql_query="""
                with t as (select metric_type, lookback_window, metric_config_type, cohort_id, sum(percentage) as total_perc FROM df group by 1,2,3,4)
                select min(total_perc) as observed_value from t
            """,
            expected_value=(99.0, 100.5),
            operator=Operator.between,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_max_percentage_sum_stg_cohort_metric_bins",
            sql_query="""
                with t as (select metric_type, lookback_window, metric_config_type, cohort_id, sum(percentage) as total_perc FROM df group by 1,2,3,4)
                select max(total_perc) as observed_value from t
            """,
            expected_value=(99.0, 100.5),
            operator=Operator.between,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_lt_4_bins_stg_cohort_metric_bins",
            sql_query="""
            select count(1) as observed_value from
                (
                select cohort_id, metric_type, lookback_window, metric_config_type, count(1) from df
                group by all
                having count(1) < 4
                )
                """,
            expected_value=20000,
            operator=Operator.lte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_lt_3_bins_stg_cohort_metric_bins",
            sql_query="""
            select count(1) as observed_value from
                (
                select cohort_id, metric_type, lookback_window, metric_config_type, count(1) from df
                group by all
                having count(1) < 3
                )
                """,
            expected_value=10000,
            operator=Operator.lte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_missing_cohorts",
            sql_query="""
            select
                count(distinct cohort_id) as observed_value
                from
                product_analytics_staging.map_benchmark_cohorts
                join product_analytics_staging.stg_organization_cohorts using (date, cohort_id)
                left join df using (date, cohort_id)
                where
                product_analytics_staging.stg_organization_cohorts.is_valid = true
                and df.cohort_id is null
                and product_analytics_staging.stg_organization_cohorts.date in (select distinct date from df)
                """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        NonEmptyDQCheck(name="dq_non_empty_stg_cohort_metric_bins"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_cohort_metric_bins",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_row_count_stg_cohort_metric_bins",
            sql_query="select count(1) as observed_value from df",
            expected_value=(4000000, 10000000),
            operator=Operator.between,
        ),
        SQLDQCheck(
            name="dq_sql_overlapping_bins_stg_cohort_metric_bins",
            sql_query="""WITH ranked_bins AS (
                            SELECT *,
                                    LAG(bin_max) OVER (PARTITION BY date, metric_type, lookback_window, metric_config_type, cohort_id ORDER BY bin_min) AS prev_bin_max
                            FROM df
                            )
                            SELECT count(1) as observed_value
                            FROM ranked_bins
                            WHERE prev_bin_max > bin_min
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_idling_percent_max_bin_max_stg_cohort_metric_bins",
            sql_query="""
                select max(bin_max) as observed_value from df where metric_type = 'IDLING_PERCENT'
            """,
            expected_value=100,
            operator=Operator.eq,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_bin_width_variation_stg_cohort_metric_bins",
            sql_query="""
                WITH df2 as (
                    select
                        *
                    from
                        df qualify row_number() over (
                        partition by cohort_id,
                        metric_type,
                        lookback_window,
                        metric_config_type
                        order by
                            bin_min desc
                        ) > 1
                    ),
                    df3 as (
                    select
                        *
                    from
                        df2 qualify row_number() over (
                        partition by cohort_id,
                        metric_type,
                        lookback_window,
                        metric_config_type
                        order by
                            bin_min asc
                        ) > 1
                    ),
                    t as (
                    select
                        cohort_id,
                        metric_type,
                        lookback_window,
                        metric_config_type,
                        min(bin_max - bin_min) as min_difference,
                        max(bin_max - bin_min) as max_difference
                    from
                        df3
                    group by
                        cohort_id,
                        metric_type,
                        lookback_window,
                        metric_config_type
                    )
                    select
                    round(max(max_difference / min_difference)) as observed_value
                    from
                    t
            """,
            expected_value=4,
            operator=Operator.lte,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:product_analytics_staging.stg_organization_metric_configs",
    ],
)
def stg_cohort_metric_bins(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_cohort_metric_bins")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    # for IDLING_PERCENT, values are truncated to 100% in the query pending future fix to underlying data
    # for STANDARD_BIN_METRICS, bins are created from entire metric distribution with no modifications
    STANDARD_BIN_METRICS = [
        "IDLING_PERCENT",
        "DROWSINESS_PER_1000MI",
        "HOS_COMPLIANCE_TIME_PERCENT",
    ]
    # for CAPPED_BIN_METRICS, if the 80th percentile is greater than 0, bins are created from the bottom 80%
    # one final bin is added with the highest 20% of values to the set of bins generated from the first 80%
    # in cases where the 80th percentile is 0 (most often in CRASH_PER_MILLION_MI)
    # the lowest non zero value is used instead of the true 80th percentile for the binning procedure
    CAPPED_BIN_METRICS = [
        "CRASH_PER_MILLION_MI",
        "MOBILE_USAGE_EVENTS_PER_1000MI",
        "SEVERE_SPEEDING",
        "MPGE",
        "IA_DRIVING_COMBINED_EVENTS_PER_1000MI",
        "VEHICLE_UTILIZATION",
    ]
    ALL_METRICS = ("(" + ", ".join("'" + metric + "'" for metric in (STANDARD_BIN_METRICS + CAPPED_BIN_METRICS)) + ")")
    CAPPED_BIN_METRICS = ("(" + ", ".join("'" + metric + "'" for metric in CAPPED_BIN_METRICS) + ")")
    STANDARD_BIN_METRICS = ("(" + ", ".join("'" + metric + "'" for metric in STANDARD_BIN_METRICS) + ")")
    if get_run_env() == "dev":
        source = "datamodel_dev"
    else:
        source = "product_analytics_staging"
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        ALL_METRICS=ALL_METRICS,
        STANDARD_BIN_METRICS=STANDARD_BIN_METRICS,
        CAPPED_BIN_METRICS=CAPPED_BIN_METRICS,
        SOURCE=source,
    )
    df = spark.sql(query)
    context.log.info(query)
    path = "/Volumes/s3/benchmarking-metrics/root/staging/benchmarkmetricshistograms"

    SKIP_EQUALITY_CHECK = False  # set to true in a dev run to skip equality check
    EQUALITY_CHECK_THRESHOLD = 1.00  # set in local run if expecting some differences
    if get_run_env() == "dev" and not SKIP_EQUALITY_CHECK:
        match_rate = verify_data_equality(
            spark=spark,
            context=context,
            table1=df,
            table2="product_analytics_staging.stg_cohort_metric_bins",
            date=PARTITION_START,
        )
        if match_rate < EQUALITY_CHECK_THRESHOLD:
            raise ValueError(
                f"""Data mismatch between dev and prod: {match_rate}.
                If this is expected, set SKIP_EQUALITY_CHECK to True.
                If you want to check the data and expect some differences,
                set EQUALITY_CHECK_THRESHOLD in local run
                """
            )
        df_csv = df.select(
            "cohort_id", "metric_type", "bin_min", "bin_max", "percentage", "metric_config_type", "lookback_window"
        )
        df_csv.coalesce(1).write.mode("overwrite").option("header", "true").option(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        ).csv(path)
    elif get_run_env() == "prod":
        df_csv = df.select(
            "cohort_id",
            "metric_type",
            "bin_min",
            "bin_max",
            "percentage",
            "metric_config_type",
            "lookback_window",
        )
        df_csv.coalesce(1).write.mode("overwrite").option("header", "true").option(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        ).csv(path)
    return df
