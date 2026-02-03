import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
import datetime
import time


"""
This file is copied from dataplatform/notebooks/firmware/helpers.py,
and the logic is modified to include columns from right hand table and
simplify the interval overlap logic. It was copied so it can be referenced
in Dagster.
"""

# Creates a DataFrame containing daily intervals where the value of a user-specified column (column_name) remained constant,
# along with the value saved in column "state_value". The state value should not be null as we use isNull call to check
# for the first value.
def create_intervals_with_state_value(
    objstat_df, column_name, query_start_unix_ms, query_end_unix_ms, spark,
):
    # Rename some columns for convenience.
    raw_data_df = (
        objstat_df.withColumnRenamed("object_id", "device_id")
        .withColumn("state_value", F.col(column_name))
        .select(
            "org_id", "device_id", "value.is_databreak", "date", "time", "state_value"
        )
    )

    # Get the previous and following values.
    windowSpec = Window.partitionBy("device_id").orderBy("time")

    intervals_df = (
        raw_data_df.withColumn(
            "prev_is_databreak", F.lag("is_databreak", 1).over(windowSpec)
        )
        .withColumn("prev_value", F.lag("state_value", 1).over(windowSpec))
        .withColumn("next_is_databreak", F.lead("is_databreak", 1).over(windowSpec))
        .withColumn("next_value", F.lead("state_value", 1).over(windowSpec))
        .withColumn(
            "is_start",
            ~(F.col("is_databreak") | F.col("next_is_databreak"))
            & (
                F.col("prev_value").isNull()  # first value
                | (F.col("state_value") != F.col("prev_value"))  # change of state
                | F.col("prev_is_databreak")  # previous is databreak
            ),
        )
        .withColumn(
            "is_end",
            ~(F.col("is_databreak") | F.col("prev_is_databreak"))
            & (
                F.col("next_value").isNull()  # last value
                | (F.col("state_value") != F.col("prev_value"))  # change of state
                | F.col("next_is_databreak")  # next is databreak
            ),
        )
        .filter(F.col("is_start") | F.col("is_end"))
        .withColumn(
            "end_ms",
            F.lead(F.when(F.col("is_end"), F.col("time")), default=None).over(
                windowSpec
            ),
        )
        .filter(F.col("is_start"))
        .withColumnRenamed("time", "start_ms")
        .select(
            "date",
            "org_id",
            "device_id",
            "start_ms",
            "end_ms",
            "state_value",
        )
        .filter(
            (F.col("end_ms") > query_start_unix_ms)
            & (F.col("start_ms") < query_end_unix_ms)
        )
        .withColumn(
            "start_ms", F.greatest(F.col("start_ms"), F.lit(query_start_unix_ms))
        )
        .withColumn("end_ms", F.least(F.col("end_ms"), F.lit(query_end_unix_ms)))
    )

    # Generate dates sequence with everyday between query start and end time and
    # add columns with the unix start and end time for each day.
    date_intervals_df = (
        spark.createDataFrame(
            [(int(query_start_unix_ms), int(query_end_unix_ms))],
            schema=T.StructType(
                [
                    T.StructField("start_unix", T.LongType(), True),
                    T.StructField("end_unix", T.LongType(), True),
                ]
            ),
        )
        .withColumn(
            "start_date", F.to_date(F.from_unixtime(F.col("start_unix") / 1000))
        )
        .withColumn("end_date", F.to_date(F.from_unixtime(F.col("end_unix") / 1000)))
        .withColumn(
            "date", F.explode(F.expr("sequence(start_date, end_date, interval 1 day)"))
        )
        .select("date")
        .withColumn("day_start_ms", 1000 * F.unix_timestamp(F.col("date")))
        .withColumn(
            "day_end_ms", (86400e3 - 1) + (1000 * F.unix_timestamp(F.col("date")))
        )
    )

    # Join the daily intervals table on the intervals table, splitting intervals on the day
    # that overlap multiple days.
    # Use a broadcast join -- for some reason databricks spins up too many tasks when join
    # a small table with a large table using the default join mechanism.
    daily_intervals_df = (
        date_intervals_df.hint("broadcast")
        .join(
            intervals_df,
            (
                (intervals_df.start_ms >= date_intervals_df.day_start_ms)
                & (intervals_df.start_ms < date_intervals_df.day_end_ms)
            )
            | (
                (intervals_df.end_ms > date_intervals_df.day_start_ms)
                & (intervals_df.end_ms <= date_intervals_df.day_end_ms)
            )
            | (
                (intervals_df.start_ms <= date_intervals_df.day_start_ms)
                & (intervals_df.end_ms >= date_intervals_df.day_end_ms)
            ),
            how="inner",
        )
        .select(
            date_intervals_df.date,
            intervals_df.org_id,
            intervals_df.device_id,
            intervals_df.state_value,
            F.greatest(intervals_df.start_ms, date_intervals_df.day_start_ms).alias(
                "start_ms"
            ),
            F.least(intervals_df.end_ms, date_intervals_df.day_end_ms).alias("end_ms"),
        )
    )

    return daily_intervals_df

# Copied from helpers.py.
# TODO: Merge change into helpers.py in repo.
def intersect_intervals(
    intervals_1_df,
    intervals_2_df,
    right_hand_columns_to_keep=None,
    join_type="inner",
    range_join_hint_interval_length_ms=10000,
):
    right_hand_columns_to_keep = right_hand_columns_to_keep or []

    # Rename some columns for avoid namespace conflicts in the join.
    intervals_1_df = intervals_1_df.withColumnRenamed(
        "start_ms", "start_ms_1"
    ).withColumnRenamed("end_ms", "end_ms_1")
    intervals_2_df = intervals_2_df.withColumnRenamed(
        "start_ms", "start_ms_2"
    ).withColumnRenamed("end_ms", "end_ms_2")

    # If two intervals overlap take the max of the start of the two intervals and min of the end of the two intervals.
    result_table_df = (
        intervals_1_df.hint("range_join", range_join_hint_interval_length_ms).join(
            intervals_2_df,
            (intervals_1_df.org_id == intervals_2_df.org_id)
            & (intervals_1_df.device_id == intervals_2_df.device_id)
            & (intervals_1_df.start_ms_1.between(intervals_2_df.start_ms_2, intervals_2_df.end_ms_2)),
            how=join_type,
        )
        .withColumn(
            "start_ms", F.greatest(intervals_1_df.start_ms_1, intervals_2_df.start_ms_2)
        )
        .withColumn("end_ms", F.least(intervals_1_df.end_ms_1, intervals_2_df.end_ms_2))
        .select(
            intervals_1_df["*"],
            F.col("start_ms"),
            F.col("end_ms"),
            *right_hand_columns_to_keep
        )
        .drop(intervals_1_df.start_ms_1)
        .drop(intervals_1_df.end_ms_1)
    )

    return result_table_df


def to_ms(d):
    return time.mktime(d.timetuple()) * 1000



def date_sub_now(days_ago):
    return datetime.date.today() + datetime.timedelta(days=-days_ago)


def date_sub(from_date, days_ago):
    return from_date + datetime.timedelta(days=-days_ago)


def string_to_date(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d")


def day_ms():
    return 86400000
