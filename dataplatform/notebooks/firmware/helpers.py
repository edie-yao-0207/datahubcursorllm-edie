# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


# DEPRECATED
# objstat_df must be a kinesisstats.osXXX table (or have the same schema).
def create_intervals(objstat_df, interval_func, query_end_unix_ms):
    # Rename some columns for convenience.
    raw_data_df = objstat_df.withColumnRenamed("object_id", "device_id").select(
        "org_id",
        "device_id",
        "value.is_databreak",
        "time",
        "value.int_value",
        "value.proto_value",
    )

    # Define a UDF that is just the interval_func (this function converts the int_value to a boolean state).
    stateUdf = F.udf(interval_func, T.BooleanType()).asNondeterministic()

    # Convert the int_value to a boolean state.
    states_df = raw_data_df.withColumn(
        "state", stateUdf(F.col("int_value")) & ~F.col("is_databreak")
    ).drop("int_value")

    # Get the previous relevant values.
    windowSpec = Window.partitionBy("device_id", "org_id").orderBy("time")
    lag_df = (
        states_df.withColumn(
            "prev_is_databreak", F.lag("is_databreak", 1).over(windowSpec)
        )
        .withColumn("prev_time", F.lag("time", 1).over(windowSpec))
        .withColumn("prev_state", F.lag("state", 1).over(windowSpec))
        .withColumn("next_is_databreak", F.lead("is_databreak", 1).over(windowSpec))
        .select(
            "org_id",
            "device_id",
            "prev_is_databreak",
            "prev_time",
            "prev_state",
            "is_databreak",
            "time",
            "state",
            "proto_value",
            "next_is_databreak",
        )
    )

    # Get just the values where a state transition occured or the prev state is null (indicating this is the first value).
    transitions_df = lag_df.filter(
        (lag_df.prev_state.isNull() & lag_df.state.isNotNull())
        | (lag_df.state.isNotNull() & lag_df.prev_state != lag_df.state)
        # databreak
        | ((lag_df.prev_is_databreak == True) & (lag_df.is_databreak == False))
        | (
            lag_df.next_is_databreak.isNotNull()
            & (lag_df.is_databreak == False)
            & (lag_df.next_is_databreak == True)
        )
    )

    # Create the intervals by looking at the next transition and filter for when the state is true.
    windowSpec = Window.partitionBy("device_id", "org_id").orderBy("start_ms")
    intervals_df = (
        transitions_df.select(
            "org_id", "device_id", "is_databreak", "time", "state", "proto_value"
        )
        .withColumnRenamed("time", "start_ms")
        .withColumn("end_ms_or_null", F.lead("start_ms", 1).over(windowSpec))
        .withColumn("is_databreak_lead", F.lead("is_databreak", 1).over(windowSpec))
        .withColumn(
            "end_ms", F.coalesce(F.col("end_ms_or_null"), F.lit(query_end_unix_ms))
        )
        .filter(
            (F.col("state") == True)
            & (F.col("is_databreak") == False)
            & (F.coalesce(F.col("is_databreak_lead"), F.lit(False)) == False)
        )
        .drop("end_ms_or_null")
        .drop("state")
        .drop("is_databreak")
        .drop("is_databreak_lead")
    )

    return intervals_df


# objstat_df must be a kinesisstats.osXXX table (or have the same schema).
def create_intervals_v2(
    objstat_df, interval_func, query_start_unix_ms, query_end_unix_ms
):
    # Rename some columns for convenience.
    raw_data_df = objstat_df.withColumnRenamed("object_id", "device_id").select(
        "org_id",
        "device_id",
        "value.is_databreak",
        "time",
        "value.int_value",
        "value.proto_value",
    )

    # Define a UDF that is just the interval_func (this function converts the int_value to a boolean state).
    stateUdf = F.udf(interval_func, T.BooleanType()).asNondeterministic()

    # Convert the int_value to a boolean state.
    states_df = raw_data_df.withColumn(
        "state", stateUdf(F.col("int_value")) & ~F.col("is_databreak")
    ).drop("int_value")

    # Get the previous relevant values.
    windowSpec = Window.partitionBy("device_id").orderBy("time")
    lag_df = (
        states_df.withColumn(
            "prev_is_databreak", F.lag("is_databreak", 1).over(windowSpec)
        )
        .withColumn("prev_time", F.lag("time", 1).over(windowSpec))
        .withColumn("prev_state", F.lag("state", 1).over(windowSpec))
        .withColumn("next_is_databreak", F.lead("is_databreak", 1).over(windowSpec))
        .select(
            "org_id",
            "device_id",
            "prev_is_databreak",
            "prev_time",
            "prev_state",
            "is_databreak",
            "time",
            "state",
            "proto_value",
            "next_is_databreak",
        )
    )

    # lag_df.show()

    # Get just the values where a state transition occured or the prev state is null (indicating this is the first value).
    transitions_df = lag_df.filter(
        (lag_df.prev_state.isNull() & lag_df.state.isNotNull())
        | (lag_df.state.isNotNull() & lag_df.prev_state != lag_df.state)
        # databreak
        | ((lag_df.prev_is_databreak == True) & (lag_df.is_databreak == False))
        | (
            lag_df.next_is_databreak.isNotNull()
            & (lag_df.is_databreak == False)
            & (lag_df.next_is_databreak == True)
        )
    )

    # transitions_df.show()

    # Create the intervals by looking at the next transition and filter for when the state is true.
    windowSpec = Window.partitionBy("device_id").orderBy("start_ms")
    intervals_df = (
        transitions_df.select(
            "org_id", "device_id", "is_databreak", "time", "state", "proto_value"
        )
        .withColumnRenamed("time", "start_ms")
        .withColumn("end_ms_or_null", F.lead("start_ms", 1).over(windowSpec))
        .withColumn("is_databreak_lead", F.lead("is_databreak", 1).over(windowSpec))
        .withColumn(
            "end_ms", F.coalesce(F.col("end_ms_or_null"), F.lit(query_end_unix_ms))
        )
        .filter(
            (F.col("state") == True)
            & (F.col("is_databreak") == False)
            & (F.coalesce(F.col("is_databreak_lead"), F.lit(False)) == False)
        )
        .drop("end_ms_or_null")
        .drop("state")
        .drop("is_databreak")
        .drop("is_databreak_lead")
    )

    intervals_filtered_df = intervals_df.filter(
        (F.col("end_ms") > query_start_unix_ms)
        & (F.col("start_ms") < query_end_unix_ms)
    )
    intervals_truncated_df = intervals_filtered_df.withColumn(
        "start_ms", F.greatest(F.col("start_ms"), F.lit(query_start_unix_ms))
    ).withColumn("end_ms", F.least(F.col("end_ms"), F.lit(query_end_unix_ms)))

    return intervals_truncated_df


# Intervals_table_df must have a the columns org_id, device_id, start_ms, end_ms.
def create_intervals_daily(intervals_df, utc_start_date, utc_end_date):

    # Create a df with every day since the start and end query date.
    date_range_df = spark.createDataFrame(
        data=[(utc_start_date, utc_end_date)],
        schema=T.StructType(
            [
                T.StructField("start_date", T.DateType(), True),
                T.StructField("end_date", T.DateType(), True),
            ]
        ),
    )
    dates_df = (
        date_range_df.withColumn(
            "date", F.explode(F.expr("sequence(start_date, end_date, interval 1 day)"))
        )
        .drop("start_date")
        .drop("end_date")
    )

    # Add columns with the unix start and end ms time for each day.
    date_intervals_df = dates_df.withColumn(
        "day_start_ms", 1000 * F.unix_timestamp(F.col("date"))
    ).withColumn("day_end_ms", (86400e3 - 1) + (1000 * F.unix_timestamp(F.col("date"))))

    # Join the daily intervals table on the intervals table, splitting intervals on the day that overlap multiple days
    # and ending the last interval at utc_end_date if interval ends more recently.
    # Use a broadcast join -- for some reason databcricks spins up too many tasks when join a small table with a large table using the default join mechanism.
    daily_intervals_df = (
        date_intervals_df.hint("broadcast")
        .join(
            intervals_df,
            (
                (intervals_df.start_ms >= date_intervals_df.day_start_ms)
                & (intervals_df.start_ms <= date_intervals_df.day_end_ms)
            )
            | (
                (intervals_df.end_ms >= date_intervals_df.day_start_ms)
                & (intervals_df.end_ms <= date_intervals_df.day_end_ms)
            )
            | (
                (intervals_df.start_ms < date_intervals_df.day_start_ms)
                & (intervals_df.end_ms > date_intervals_df.day_end_ms)
            ),
            how="inner",
        )
        .select(
            date_intervals_df.date,
            intervals_df.org_id,
            intervals_df.device_id,
            F.greatest(intervals_df.start_ms, date_intervals_df.day_start_ms).alias(
                "start_ms"
            ),
            F.least(intervals_df.end_ms, date_intervals_df.day_end_ms).alias("end_ms"),
        )
    )

    return daily_intervals_df


# Intervals_table_df must have a the columns org_id, device_id, start_ms, end_ms.
def create_intervals_hourly(intervals_df, utc_start_date, utc_end_date):
    # Create a df with every day since the start and end query date.
    date_range_df = spark.createDataFrame(
        data=[(utc_start_date, utc_end_date)],
        schema=T.StructType(
            [
                T.StructField("start_date", T.DateType(), True),
                T.StructField("end_date", T.DateType(), True),
            ]
        ),
    )
    dates_df = (
        date_range_df.withColumn(
            "date",
            F.explode(
                F.expr(
                    "sequence(timestamp(start_date), timestamp(end_date), interval 1 hour)"
                )
            ),
        )
        .drop("start_date")
        .drop("end_date")
    )

    # Add columns with the unix start and end ms time for each hour.
    hour_intervals_df = dates_df.withColumn(
        "hour_start_ms", 1000 * F.unix_timestamp(F.col("date"))
    ).withColumn("hour_end_ms", (3600e3 - 1) + (1000 * F.unix_timestamp(F.col("date"))))

    # Join the daily intervals table on the intervals table, splitting intervals on the day that overlap multiple days
    # and ending the last interval at utc_end_date if interval ends more recently.
    # Use a broadcast join -- for some reason databcricks spins up too many tasks when join a small table with a large table using the default join mechanism.
    hourly_intervals_df = (
        hour_intervals_df.hint("broadcast")
        .join(
            intervals_df,
            (
                (intervals_df.start_ms >= hour_intervals_df.hour_start_ms)
                & (intervals_df.start_ms <= hour_intervals_df.hour_end_ms)
            )
            | (
                (intervals_df.end_ms >= hour_intervals_df.hour_start_ms)
                & (intervals_df.end_ms <= hour_intervals_df.hour_end_ms)
            )
            | (
                (intervals_df.start_ms < hour_intervals_df.hour_start_ms)
                & (intervals_df.end_ms > hour_intervals_df.hour_end_ms)
            ),
            how="inner",
        )
        .select(
            hour_intervals_df.date,
            intervals_df.org_id,
            intervals_df.device_id,
            F.greatest(intervals_df.start_ms, hour_intervals_df.hour_start_ms).alias(
                "start_ms"
            ),
            F.least(intervals_df.end_ms, hour_intervals_df.hour_end_ms).alias("end_ms"),
        )
    )

    return hourly_intervals_df


# Creates a DataFrame containing daily intervals where the value of a user-specified column (column_name) remained constant,
# along with the value saved in column "state_value". The state value should not be null as we use isNull call to check
# for the first value.
def create_intervals_with_state_value(
    objstat_df, column_name, query_start_unix_ms, query_end_unix_ms
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


# Intersect the intervals from two tables.
# Noth tables must have a the columns org_id, device_id, start_ms, end_ms and each table cannot have intervals that overlap other intervals in the table.
# Resulting table also contains all other columns from the first table.
def intersect_intervals(intervals_1_df, intervals_2_df):
    # Rename some columns for avoid namespace conflicts in the join.
    intervals_1_df = intervals_1_df.withColumnRenamed(
        "start_ms", "start_ms_1"
    ).withColumnRenamed("end_ms", "end_ms_1")
    intervals_2_df = intervals_2_df.withColumnRenamed(
        "start_ms", "start_ms_2"
    ).withColumnRenamed("end_ms", "end_ms_2")

    # If two intervals overlap take the max of the start of the two intervals and min of the end of the two intervals.
    result_table_df = (
        intervals_1_df.join(
            intervals_2_df,
            (intervals_1_df.org_id == intervals_2_df.org_id)
            & (intervals_1_df.device_id == intervals_2_df.device_id)
            & (
                (
                    (intervals_2_df.start_ms_2 >= intervals_1_df.start_ms_1)
                    & (intervals_2_df.start_ms_2 < intervals_1_df.end_ms_1)
                )
                | (
                    (intervals_2_df.end_ms_2 > intervals_1_df.start_ms_1)
                    & (intervals_2_df.end_ms_2 <= intervals_1_df.end_ms_1)
                )
                | (
                    (intervals_2_df.start_ms_2 <= intervals_1_df.start_ms_1)
                    & (intervals_2_df.end_ms_2 >= intervals_1_df.end_ms_1)
                )
            ),
            how="inner",
        )
        .withColumn(
            "start_ms", F.greatest(intervals_1_df.start_ms_1, intervals_2_df.start_ms_2)
        )
        .withColumn("end_ms", F.least(intervals_1_df.end_ms_1, intervals_2_df.end_ms_2))
        .select(intervals_1_df["*"], F.col("start_ms"), F.col("end_ms"))
        .drop(intervals_1_df.start_ms_1)
        .drop(intervals_1_df.end_ms_1)
    )

    return result_table_df


def intersect_intervals_cm_vg(intervals_1_df, intervals_2_df):
    """
    Intersects two interval tables by joining on org_id, vg_device_id, and cm_device_id.
    This is necessary for cm_uptime calculations.

    Both tables must have the columns: org_id, vg_device_id, cm_device_id, start_ms, end_ms.
    Each table cannot have intervals that overlap other intervals in the same table.
    The resulting DataFrame contains all other columns from the first table.
    """

    # Rename some columns to avoid namespace conflicts in the join.
    intervals_1_df = intervals_1_df.withColumnRenamed(
        "start_ms", "start_ms_1"
    ).withColumnRenamed("end_ms", "end_ms_1")
    intervals_2_df = intervals_2_df.withColumnRenamed(
        "start_ms", "start_ms_2"
    ).withColumnRenamed("end_ms", "end_ms_2")

    # If two intervals overlap take the max of the start of the two intervals and min of the end of the two intervals.
    result_table_df = (
        intervals_1_df.join(
            intervals_2_df,
            (intervals_1_df.org_id == intervals_2_df.org_id)
            & (intervals_1_df.vg_device_id == intervals_2_df.vg_device_id)
            & (intervals_1_df.cm_device_id == intervals_2_df.cm_device_id)
            & (
                (
                    (intervals_2_df.start_ms_2 >= intervals_1_df.start_ms_1)
                    & (intervals_2_df.start_ms_2 < intervals_1_df.end_ms_1)
                )
                | (
                    (intervals_2_df.end_ms_2 > intervals_1_df.start_ms_1)
                    & (intervals_2_df.end_ms_2 <= intervals_1_df.end_ms_1)
                )
                | (
                    (intervals_2_df.start_ms_2 <= intervals_1_df.start_ms_1)
                    & (intervals_2_df.end_ms_2 >= intervals_1_df.end_ms_1)
                )
            ),
            how="inner",
        )
        .withColumn(
            "start_ms", F.greatest(intervals_1_df.start_ms_1, intervals_2_df.start_ms_2)
        )
        .withColumn("end_ms", F.least(intervals_1_df.end_ms_1, intervals_2_df.end_ms_2))
        .select(intervals_1_df["*"], F.col("start_ms"), F.col("end_ms"))
        .drop(intervals_1_df.start_ms_1)
        .drop(intervals_1_df.end_ms_1)
    )

    return result_table_df


# device_last_heartbeats_df must have columns org_id, device_id, and last_heartbeat_ms
def terminate_intervals_at_last_hb(intervals_df, device_last_heartbeats_df):
    interval_and_hb_df = (
        intervals_df.alias("a")
        .join(
            device_last_heartbeats_df.alias("b"),
            (F.col("a.org_id") == F.col("b.org_id"))
            & (F.col("a.device_id") == F.col("b.device_id"))
            & (F.col("a.start_ms") <= F.col("b.last_heartbeat_ms")),
        )
        .select(intervals_df["*"], "b.last_heartbeat_ms")
        .withColumn("new_end_ms", F.least("last_heartbeat_ms", "end_ms"))
        .drop("end_ms")
        .drop("last_heartbeat_ms")
        .withColumnRenamed("new_end_ms", "end_ms")
    )

    return interval_and_hb_df


# COMMAND ----------

import datetime
import time


def to_ms(d):
    return time.mktime(d.timetuple()) * 1000


def yesterday_ms():
    yesterday = datetime.date.today() + datetime.timedelta(days=-1)
    return time.mktime(yesterday.timetuple()) * 1000


def date_sub_now(days_ago):
    return datetime.date.today() + datetime.timedelta(days=-days_ago)


def date_sub(from_date, days_ago):
    return from_date + datetime.timedelta(days=-days_ago)


def string_to_date(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d")


def day_ms():
    return 86400000


# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


def create_or_update_table(target_table, update_df, partition_col, merge_on_cols):
    update_df.createOrReplaceTempView("update")

    spark.sql(
        """
    create table if not exists {}
    using delta
    partitioned by ({})
    as
    select * from update
    """.format(
            target_table, partition_col
        )
    )

    merge_logic = "on " + " and ".join(
        ["target.{} = updates.{}".format(v, v) for v in merge_on_cols]
    )

    spark.sql(
        """
    merge into {} as target
    using update as updates
    {}
    when matched then update set *
    when not matched then insert *
    """.format(
            target_table, merge_logic
        )
    )


# COMMAND ----------


def create_or_replace_by_date(target_table, update_df, start_date, end_date):
    """
    Creates a table if it doesn't exist, or overwrites partitions for the given date range.

    This is more efficient than MERGE when the output is deterministic per date partition,
    as it avoids expensive shuffle/join operations. Uses Delta Lake's replaceWhere to
    atomically overwrite only the affected date partitions.

    Args:
        target_table: Fully qualified table name (e.g., "dataprep_firmware.my_table")
        update_df: DataFrame to write (must have a "date" column)
        start_date: Start date of the range to overwrite (inclusive, datetime.date or datetime.datetime)
        end_date: End date of the range to overwrite (inclusive, datetime.date or datetime.datetime)
    """
    # Ensure dates are formatted as YYYY-MM-DD regardless of datetime vs date type.
    # string_to_date() returns datetime.datetime which formats as "2025-01-05 00:00:00",
    # but date_sub_now() returns datetime.date which formats as "2025-01-05".
    # We need consistent YYYY-MM-DD format for the replaceWhere predicate.
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    if not spark.catalog.tableExists(target_table):
        # First run: create table partitioned by date
        (
            update_df.write.format("delta")
            .partitionBy("date")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
    else:
        # Subsequent runs: overwrite only the date partitions being updated
        (
            update_df.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"date >= '{start_date_str}' AND date <= '{end_date_str}'",
            )
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )


# COMMAND ----------


def delete_entries_by_date(target_table, utc_start_date, utc_end_date):
    """
    Deletes all entries in the given table where `date` is between utc_start_date and utc_end_date (inclusive).
    Assumes `date` is a string/date column in the format 'YYYY-MM-DD'.
    Uses spark.catalog.tableExists, which is supported in all Databricks cluster access modes.
    """
    if spark.catalog.tableExists(target_table):
        spark.sql(
            """--sql
            DELETE FROM {}
            WHERE date >= DATE('{}') AND date <= DATE('{}')
            --endsql
            """.format(
                target_table, utc_start_date, utc_end_date
            )
        )
    else:
        print(f"Table '{target_table}' does not exist. Skipping DELETE.")


# COMMAND ----------


def get_start_end_date(default_start_days_ago, default_end_days_ago):
    start_date = getArgument("start_date")
    if start_date == "":
        start_date = date_sub_now(default_start_days_ago)
    else:
        start_date = string_to_date(start_date)

    end_date = getArgument("end_date")
    if end_date == "":
        end_date = date_sub_now(default_end_days_ago)
    else:
        end_date = string_to_date(end_date)

    return start_date, end_date


# COMMAND ----------
# from pyspark.sql import Row

# Create intervals unit testing

# org_id = 123
# object_id = 1234

# value_schema = T.StructType([
#     T.StructField("int_value", T.IntegerType(), True),
#     T.StructField("proto_value", T.IntegerType(), True),
#     T.StructField("is_databreak", T.BooleanType(), True)
# ])

# schema = T.StructType([
#     T.StructField("org_id", T.IntegerType(), True),
#     T.StructField("object_id", T.IntegerType(), True),
#     T.StructField("time", T.IntegerType(), True),
#     T.StructField("value", value_schema, True)
# ])

# interval_schema = T.StructType([
#     T.StructField("org_id", T.IntegerType(), True),
#     T.StructField("object_id", T.IntegerType(), True),
#     T.StructField("start_ms", T.IntegerType(), True),
#     T.StructField("proto_value", T.IntegerType(), True),
#     T.StructField("end_ms", T.IntegerType(), True),
# ])


# normal_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=0, proto_value=None, is_databreak=False)),\
# ]

# data_df = spark.createDataFrame(normal_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 2, None, 3)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# int_value_zero_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=0, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=1, proto_value=None, is_databreak=False)),\
# ]

# data_df = spark.createDataFrame(int_value_zero_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 0, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 2, None, 3)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# int_value_zero_databreak_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=0, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=0, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 4, Row(int_value=0, proto_value=None, is_databreak=True)),\
# ]

# data_df = spark.createDataFrame(int_value_zero_databreak_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 0, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 2, None, 3)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# no_end_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=1, proto_value=None, is_databreak=False)),\
# ]

# data_df = spark.createDataFrame(no_end_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 2, None, 99)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# repeated_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 4, Row(int_value=0, proto_value=None, is_databreak=False)),\
# ]

# data_df = spark.createDataFrame(repeated_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 2, None, 4)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# databreak_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=0, proto_value=None, is_databreak=True)),\
# ]

# data_df = spark.createDataFrame(databreak_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# assert actual_interval_df.count() == 0, "test failed"


# databreak_repeated_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 4, Row(int_value=0, proto_value=None, is_databreak=True)),\
# ]

# data_df = spark.createDataFrame(databreak_repeated_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 2, None, 3)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# interval_starts_immediately_data = [\
#   (org_id, object_id, 1, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 2, Row(int_value=0, proto_value=None, is_databreak=False)),\
# ]

# data_df = spark.createDataFrame(interval_starts_immediately_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 1, None, 2)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"


# multiple_intervals_data = [\
#   (org_id, object_id, 1, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 2, Row(int_value=0, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 3, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 4, Row(int_value=0, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 5, Row(int_value=0, proto_value=None, is_databreak=True)),\
#   (org_id, object_id, 6, Row(int_value=1, proto_value=None, is_databreak=False)),\
#   (org_id, object_id, 7, Row(int_value=0, proto_value=None, is_databreak=False)),\
# ]

# data_df = spark.createDataFrame(multiple_intervals_data, schema)
# actual_interval_df = create_intervals(data_df, lambda x: x == 1, 99)
# expected_interval_df = spark.createDataFrame([(org_id, object_id, 3, None, 4), (org_id, object_id, 6, None, 7)], interval_schema)
# assert actual_interval_df.exceptAll(expected_interval_df).count() == 0 and expected_interval_df.exceptAll(actual_interval_df).count() == 0, "test failed"
