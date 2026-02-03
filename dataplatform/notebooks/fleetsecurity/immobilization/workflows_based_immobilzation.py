# MAGIC %sql
# MAGIC --- If this fails it means that the function has already been created, so just turn on the main function, unfortunately CREATE TEMPORARY FUNCTION with IF NOT EXISTS is not allowed in databricks.
# MAGIC CREATE TEMPORARY FUNCTION map_operation_type(value INTEGER) RETURNS STRING RETURN CASE value
# MAGIC     WHEN 0 THEN 'INVALID'
# MAGIC     WHEN 1 THEN 'IMMOBILIZE'
# MAGIC     WHEN 2 THEN 'REMOBILIZE'
# MAGIC     ELSE CAST(value AS STRING)
# MAGIC END;
# MAGIC
# MAGIC CREATE TEMPORARY FUNCTION map_status(value INTEGER) RETURNS STRING RETURN CASE value
# MAGIC     WHEN 0 THEN 'INVALID'
# MAGIC     WHEN 1 THEN 'created'
# MAGIC     WHEN 2 THEN 'sent'
# MAGIC     WHEN 3 THEN 'received'
# MAGIC     WHEN 4 THEN 'applying'
# MAGIC     WHEN 5 THEN 'done'
# MAGIC     WHEN 6 THEN 'failed'
# MAGIC     WHEN 7 THEN 'ignored'
# MAGIC     WHEN 99 THEN 'not_applicable'
# MAGIC     ELSE CAST(value AS STRING)
# MAGIC END;
# MAGIC
# MAGIC CREATE TEMPORARY FUNCTION map_source(value INTEGER) RETURNS STRING RETURN CASE value
# MAGIC     WHEN 0 THEN 'INVALID'
# MAGIC     WHEN 1 THEN 'dashboard'
# MAGIC     WHEN 2 THEN 'api'
# MAGIC     WHEN 3 THEN 'local'
# MAGIC     WHEN 4 THEN 'workflows'
# MAGIC     WHEN 99 THEN 'not_applicable'
# MAGIC     ELSE CAST(value AS STRING)
# MAGIC END;
# MAGIC
# MAGIC CREATE TEMPORARY FUNCTION map_change_reason(value INTEGER) RETURNS STRING RETURN CASE value
# MAGIC     WHEN 0 THEN 'INVALID'
# MAGIC     WHEN 1 THEN 'no_change'
# MAGIC     WHEN 2 THEN 'tamper'
# MAGIC     WHEN 3 THEN 'jamming'
# MAGIC     WHEN 4 THEN 'config'
# MAGIC     WHEN 5 THEN 'auto_remob'
# MAGIC     WHEN 6 THEN 'hub_request'
# MAGIC     WHEN 99 THEN 'not_applicable'
# MAGIC     ELSE CAST(value AS STRING)
# MAGIC END;
# MAGIC
# MAGIC CREATE TEMPORARY FUNCTION map_immobilizer_type(value INTEGER) RETURNS STRING RETURN CASE value
# MAGIC     WHEN 0 THEN 'INVALID'
# MAGIC     WHEN 1 THEN 'SOZE'
# MAGIC     WHEN 2 THEN 'MORPHEUS'
# MAGIC     ELSE CAST(value AS STRING)
# MAGIC END;

# COMMAND ----------

# MAGIC %run /backend/platformops/datadog

# COMMAND ----------


import boto3
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, MapType, StringType
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("WorkflowsImmobilizationDetection").getOrCreate()

S3_PREFIX = (
    "/Volumes/s3/databricks-workspace/fleetops/fleetsecurity/soze_immobilizer_count"
)
REGION = boto3.session.Session().region_name
DAYS_LIMIT = 7  # Past 7 days, feel free to change

# Define a function to fetch data from the kinesisstats.osdimmobilizerevent
def fetch_data():
    query = f"""
    SELECT
        from_unixtime(CAST(time/1000 as BIGINT)) as datetime,
        time,
        org_id,
        object_id,
        value.proto_value.immobilizer_event.relays_events[0].relay_id as relay_id,
        map_operation_type(value.proto_value.immobilizer_event.relays_events[0].operation_type) as operation_type,
        map_status(value.proto_value.immobilizer_event.relays_events[0].status) as status,
        map_source(value.proto_value.immobilizer_event.relays_events[0].source) as source,
        map_change_reason(value.proto_value.immobilizer_event.relays_events[0].change_reason) as change_reason,
        value.proto_value.immobilizer_event.relays_events[0].command_uuid as command_uuid,
        map_immobilizer_type(value.proto_value.immobilizer_event.context.immobilizer_type) as immobilizer_type
    FROM kinesisstats.osdimmobilizerevent
    WHERE date >= current_date() - {DAYS_LIMIT} AND value.proto_value.immobilizer_event.relays_events[0].command_uuid IS NOT NULL

    UNION ALL

    SELECT
        from_unixtime(CAST(time/1000 as BIGINT)) as datetime,
        time,
        org_id,
        object_id,
        value.proto_value.immobilizer_event.relays_events[1].relay_id as relay_id,
        map_operation_type(value.proto_value.immobilizer_event.relays_events[1].operation_type) as operation_type,
        map_status(value.proto_value.immobilizer_event.relays_events[1].status) as status,
        map_source(value.proto_value.immobilizer_event.relays_events[1].source) as source,
        map_change_reason(value.proto_value.immobilizer_event.relays_events[1].change_reason) as change_reason,
        value.proto_value.immobilizer_event.relays_events[1].command_uuid as command_uuid,
        map_immobilizer_type(value.proto_value.immobilizer_event.context.immobilizer_type) as immobilizer_type
    FROM kinesisstats.osdimmobilizerevent
    WHERE date >= current_date() - {DAYS_LIMIT} AND value.proto_value.immobilizer_event.relays_events[1].command_uuid IS NOT NULL
    ORDER BY time DESC
    """
    df = spark.sql(query)
    return df


def fill_0000_uuid(df: DataFrame) -> DataFrame:
    # Define a window partitioned by object_id and ordered by time
    window_spec = Window.partitionBy("object_id").orderBy("time")

    # Define a window to get the first non-zero command_uuid within each partition
    data_with_previous_uuid = df.withColumn(
        "prev_uuid", F.lag("command_uuid").over(window_spec)
    )

    # Flag rows where command_uuid is all-zero
    data_with_previous_uuid = data_with_previous_uuid.withColumn(
        "command_uuid",
        F.when(
            F.col("command_uuid") == "00000000-0000-0000-0000-000000000000",
            F.col("prev_uuid"),
        ).otherwise(F.col("command_uuid")),
    )

    # Drop the intermediate columns used for processing
    data_with_previous_uuid = data_with_previous_uuid.drop("prev_uuid")

    return data_with_previous_uuid


def command_lifecycle(df: DataFrame) -> DataFrame:
    # Group by command_uuid to process each command lifecycle individually
    lifecycle_df = df.groupBy("command_uuid").agg(
        F.min("time").alias("earliest_time"),
        F.first("org_id").alias("org_id"),
        F.first("object_id").alias("object_id"),
        F.first("immobilizer_type").alias("immobilizer_type"),
        F.first("relay_id").alias("relay_id"),
        F.first("operation_type").alias("operation_type"),
        F.expr("FILTER(collect_list(source), source -> source != 'not_applicable')")[
            0
        ].alias("source"),
        F.first("change_reason").alias("change_reason"),
        F.collect_list(F.struct("status", "time")).alias("statuses_times"),
        F.last("status").alias("last_status"),
    )

    # Define a function to generate the lifecycle JSON structure
    def build_lifecycle_json(status_times):
        lifecycle = {}
        for entry in status_times:
            status = entry["status"]
            time = entry["time"]
            if status in [
                "created",
                "sent",
                "received",
                "applying",
                "done",
                "failed",
                "ignored",
            ]:
                lifecycle[status] = time
        return lifecycle

    # Register the function as a Spark UDF
    build_lifecycle_json_udf = F.udf(
        build_lifecycle_json, MapType(StringType(), LongType())
    )

    # Apply the UDF to generate the lifecycle JSON
    lifecycle_df = lifecycle_df.withColumn(
        "lifecycle", build_lifecycle_json_udf("statuses_times")
    )

    # Select and reorder columns as required
    result_df = lifecycle_df.select(
        "earliest_time",
        "org_id",
        "object_id",
        "immobilizer_type",
        "relay_id",
        "operation_type",
        "source",
        "change_reason",
        "command_uuid",
        "last_status",
        "lifecycle",
    )

    result_df = result_df.withColumn("lifecycle", F.to_json(F.col("lifecycle")))

    return result_df


def apply_ignored_status(df: DataFrame) -> DataFrame:
    # Flag rows where last_status is not 'done'
    df = df.withColumn(
        "not_done", F.when(F.col("last_status") != "done", 1).otherwise(0)
    )

    # Find the maximum `time` for each device_id
    max_time_per_device = df.groupBy("object_id").agg(
        F.max("earliest_time").alias("max_time")
    )

    # Join with the original DataFrame to get the maximum time for each device_id
    df = df.join(max_time_per_device, on="object_id")

    # Check if any row with `not_done = 1` has a later `time` within each device_id
    df = df.withColumn(
        "last_status",
        F.when(
            (F.col("not_done") == 1)
            & (F.col("earliest_time") < F.col("max_time"))
            & (F.col("immobilizer_type") == "SOZE"),
            "ignored",
        ).otherwise(F.col("last_status")),
    )

    # Drop the intermediate columns used for processing
    df = df.drop("not_done", "max_time")

    return df


def filter_out_workflows(df: DataFrame) -> DataFrame:
    # Filter out rows where source is not 'workflows'
    df = df.filter(
        F.col("source") == "workflows"
    )  # Change to dashboard to check if it's working

    # Choose the columns
    df = df.select(
        "org_id",
        "object_id",
        F.col("earliest_time").alias("creation_time"),
        "immobilizer_type",
        "relay_id",
        "operation_type",
        "last_status",
        "lifecycle",
        "command_uuid",
    )

    return df


# Save data to S3 function
def save_to_s3(df, path):
    df.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(
        path
    )


# Save data to Delta function
def save_to_delta(df, name):
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"fleetops_dev.`{name}_{REGION}`"
    )


# Fetch the data
data = fetch_data()
exclude_0000_1 = fill_0000_uuid(data)  # detected
exclude_0000_2 = fill_0000_uuid(exclude_0000_1)  # applying
exclude_0000_3 = fill_0000_uuid(exclude_0000_2)  # done
lifecycle_data = command_lifecycle(exclude_0000_3)
failed_status_data = apply_ignored_status(lifecycle_data)

workflows_incidents = filter_out_workflows(failed_status_data)
# If you want to save this table to S3, uncomment the line below
# save_to_s3(workflows_incidents, S3_PREFIX)

# If you want to save this table to Delta, uncomment the line below
# save_to_delta(workflows_incidents, "workflows_immobilization")

# Get the total event count
events_count = workflows_incidents.count()

# Get counts for specific statuses
done_count = workflows_incidents.filter(F.col("last_status") == "done").count()
ignored_count = workflows_incidents.filter(F.col("last_status") == "ignored").count()
failed_count = workflows_incidents.filter(F.col("last_status") == "failed").count()

# Display the counts
log_datadog_metrics(
    [
        {
            "metric": "databricks.fleetsecurity.workflows.count",
            "points": events_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.workflows.done",
            "points": done_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.workflows.ignored",
            "points": ignored_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.workflows.failed",
            "points": failed_count,
            "tags": [f"region:{REGION}"],
        },
    ]
)
