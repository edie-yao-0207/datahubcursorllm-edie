import datetime

import numpy as np
from pyspark.sql import Window as w
import pyspark.sql.functions as f
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import ArrayType, FloatType


# Get percentiles 0 - 99 so we can return relative benchmark rankings instead of raw values
@pandas_udf(ArrayType(FloatType()), PandasUDFType.GROUPED_AGG)
def percentile_udf(v):
    return v.quantile(q=np.arange(0, 1, 0.01)).tolist()


@pandas_udf("float", PandasUDFType.GROUPED_AGG)
def mean_udf(v):
    return v.mean()


class Benchmark:
    # table_name: table or view containing the metric used to calculate benchmarks.
    #   Each row of the table must contain the columns org_id, date, and a metric value for that date and org.
    # metric_cols: column names of the metrics for each org on each day
    #   Used to calculate org benchmarks.
    # denominator_col: divides the metric column.
    #   For example, harsh event metrics are per 1000 miles. Therefore, the denominator column units are 1000's of miles or (total_miles/1000)
    #   Speeding benchmarks are a percentage of total drive time. Therefore, the denominator column units are total ms of trips for a given org and date.
    #   Both the denominator and the metric column are summed over 8 weeks, then the ratio is calculated to get the benchmark value.
    # output_cols: list of metric names to output
    #   Each output col corresponds to a metric col. The output col is the result of summing the metric and the denominator over
    #   8 weeks then taking the ratio. The output col name is what will be surfaced to the frontend as the metric type.
    #   For reference look at the metricType field here https://cloud.samsara.com/debug/graphql?query=cXVlcnkgRmxlZXRCZW5jaG1hcmtzUmVwb3J0Q29tcG9uZW50KCRvcmdJZDogaW50NjQhKSB7CiAgb3JnYW5pemF0aW9uKGlkOiAkb3JnSWQpIHsKICAgIGNvaG9ydElkCiAgICBzYW1zYXJhQmVuY2htYXJrQXZlcmFnZXMgewogICAgICAuLi5CZW5jaG1hcmtTY29yZXNGb3JNZXRyaWMKICAgIH0KICAgIGJlbmNobWFya1Njb3JlcyB7CiAgICAgIC4uLkJlbmNobWFya1Njb3Jlc0Zvck1ldHJpYwogICAgfQogIH0KfQoKZnJhZ21lbnQgQmVuY2htYXJrU2NvcmVzRm9yTWV0cmljIG9uIEJlbmNobWFya1Njb3JlcyB7CiAgY29ob3J0SWQKICBtZXRyaWNUeXBlCiAgbWVhbgogIG1lZGlhbgogIHRvcDEwCn0K&variables=eyJvcmdJZCI6NDI5Njl9&trace=true
    #   If no output cols are given, the column name is used. In both cases, the final output will be all caps.
    # is_percentage: does the final output metric represent a percentage
    #   If this is true, the metric will be divided by the numerator, then multiplied by 100
    # lower_is_better:
    #   For harsh events, it's obviously better to have a lower value which means if we sort the
    #   harsh events / 1000 mi in ascending order, the best orgs would be at the start.
    #   Alternatively, for vehicle utilization higher numbers are better.
    #   Vehicle utilization represents the percentage of hours driving to total hours.
    #   If we sort this list of orgs in descending order, the best orgs will be at the start.
    # output_bucket: the s3 location where benchmark metrics are appended to

    def __init__(
        self,
        table_name: str,
        metric_cols: [str],
        denominator_col: str,
        output_cols=None,
        is_percentage=True,
        lower_is_better=True,
        output_bucket="s3://samsara-benchmarking-metrics/benchmark_metrics_v3",
    ):
        self.table_name = table_name
        self.metric_cols = metric_cols
        self.denominator_col = denominator_col
        self.is_percentage = is_percentage
        self.lower_is_better = lower_is_better
        self.output_bucket = output_bucket

        t = spark.table(self.table_name)

        assert "org_id" in t.columns and "date" in t.columns
        assert self.denominator_col in t.columns

        for col in self.metric_cols:
            if col not in t.columns:
                raise ValueError(f"column {col} not in table {table_name}")

        self.metric_to_output_name = None
        if output_cols is not None:
            assert len(output_cols) == len(metric_cols)
            self.metric_to_output_name = {}
            for i, o in enumerate(output_cols):
                self.metric_to_output_name[self.metric_cols[i]] = o

    def output_name(self, col):
        if self.metric_to_output_name is None:
            return col.upper()
        return self.metric_to_output_name[col].upper()

    def append_to_s3(self):
        t = spark.table(self.table_name)

        # Grouping by org_id and week, take the sum of all metrics and the denominator
        agg_cols = [f.sum(metric).alias(metric) for metric in self.metric_cols]
        agg_cols.append(f.sum(self.denominator_col).alias(self.denominator_col))

        # Handle the edge case where the month is Jan, but the week is 53 by
        # setting the 53rd week to belong to the previous year.
        org_by_week = (
            t.withColumn(
                "week",
                f.concat_ws(
                    "-",
                    f.when(
                        (f.month(t.date) == 1) & (f.weekofyear(t.date) == 53),
                        f.year(t.date) - 1,
                    ).otherwise(f.year(t.date)),
                    f.lpad(f.weekofyear(t.date), 2, "0"),
                ),
            )
            .groupBy("org_id", "week")
            .agg(*agg_cols)
        )

        # Sum all metrics and the denominator over the past 8 weeks for each org
        # w is an alias for the pyspark.sql.Window class
        prev_8_weeks = w.partitionBy("org_id").orderBy("week").rowsBetween(-8, -1)
        for col in [*self.metric_cols, self.denominator_col]:
            org_by_week = org_by_week.withColumn(col, f.sum(col).over(prev_8_weeks))

        # Divide each of the metrics by the denominator and setup the cohort aggregate functions
        cohort_agg_cols = []
        for col in self.metric_cols:
            multiple = 100 if self.is_percentage else 1
            org_by_week = org_by_week.withColumn(
                col, multiple * f.col(col) / f.col(self.denominator_col)
            )
            cohort_agg_cols += [
                percentile_udf(col).alias(f"perc_{col}"),
                mean_udf(col).alias(f"mean_{col}"),
            ]

        # Group by cohort_id and week to get the mean and percentiles across all orgs for each week
        org_segments = spark.table("dataproducts.org_segments_v3")
        cohort_metrics_by_week = (
            org_by_week.join(org_segments, org_by_week.org_id == org_segments.org_id)
            .groupBy(org_segments.cohort_id, org_by_week.week)
            .agg(*cohort_agg_cols)
        )

        each_cohort = w.partitionBy("cohort_id")
        # Get the mean and percentile values for the most recent week for each cohort
        cohort_benchmarks = (
            cohort_metrics_by_week.withColumn(
                "max_week", f.max("week").over(each_cohort)
            )
            .where(f.col("max_week") == f.col("week"))
            .drop("max_week")
        )

        # Is it better to have a lower or higher value for this metric?
        top10 = 10 if self.lower_is_better else 90

        col = self.metric_cols[0]
        percentiles_as_columns = [
            f.col(f"perc_{col}").getItem(i).alias(f"p{f'{i}'.rjust(2, '0')}")
            for i in range(100)
        ]
        final = cohort_benchmarks.select(
            f.lit(self.output_name(col)).alias("metric_type"),
            "cohort_id",
            "week",
            f.col(f"mean_{col}").alias("mean"),
            f.col(f"perc_{col}").getItem(50).alias("median"),
            f.col(f"perc_{col}").getItem(top10).alias("top10_percentile"),
            *percentiles_as_columns,
        )

        for col in self.metric_cols[1:]:
            percentiles_as_columns = [
                f.col(f"perc_{col}").getItem(i).alias(f"p{f'{i}'.rjust(2, '0')}")
                for i in range(100)
            ]
            final = final.union(
                cohort_benchmarks.select(
                    f.lit(self.output_name(col)).alias("metric_type"),
                    "cohort_id",
                    "week",
                    f.col(f"mean_{col}").alias("mean"),
                    f.col(f"perc_{col}").getItem(50).alias("median"),
                    f.col(f"perc_{col}").getItem(top10).alias("top10_percentile"),
                    *percentiles_as_columns,
                )
            )

        final.coalesce(1).write.format("csv").mode("append").option(
            "header", True
        ).save(self.output_bucket)


def get_optional_bool_env(key):
    val = dbutils.widgets.get(key)
    if not bool(val):
        return False

    if val.lower() not in ["true", "false"]:
        raise ValueError(f"{key} is not boolean")

    return val.lower() == "true"


def get_string_env(key):
    val = dbutils.widgets.get(key)
    if not bool(val):
        raise ValueError(f"{key} not provided")
    return val


def get_optional_list_env(key):
    val = dbutils.widgets.get(key)
    if not bool(val):
        return None
    return val.split(",")


def get_list_env(key):
    l = get_optional_list_env(key)
    if not l or len(l) == 0:
        raise ValueError(f"{key} not provided")
    return l


table_name = get_string_env("TABLE_NAME")
metric_cols = get_list_env("METRIC_COLS")
denominator_col = get_string_env("DENOMINATOR_COL")
output_cols = get_optional_list_env("OUTPUT_COLS")
is_percentage = get_optional_bool_env("IS_PERCENTAGE")
lower_is_better = get_optional_bool_env("LOWER_IS_BETTER")
output_bucket = get_string_env("OUTPUT_BUCKET")

benchmark = Benchmark(
    table_name,
    metric_cols,
    denominator_col,
    output_cols=output_cols,
    is_percentage=is_percentage,
    lower_is_better=lower_is_better,
    output_bucket=output_bucket,
)

benchmark.append_to_s3()
