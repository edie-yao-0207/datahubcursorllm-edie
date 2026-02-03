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

# MAGIC %run /backend/fleetsecurity/immobilization/common

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType, LongType
import boto3


# Initialize Spark session
spark = SparkSession.builder.appName("WorkflowsImmobilizationDetection").getOrCreate()

DAYS_LIMIT = 7  # Past 7 days, feel free to change

# Define a function to fetch data from the kinesisstats.osdimmobilizerevent
def fetch_data():
    query = f"""
    SELECT
        from_unixtime(CAST(d.time/1000 as BIGINT)) as datetime,
        d.time,
        d.org_id,
        o.name,
        o.locale,
        d.object_id,
        d.value.proto_value.immobilizer_event.relays_events[0].relay_id as relay_id,
        map_operation_type(d.value.proto_value.immobilizer_event.relays_events[0].operation_type) as operation_type,
        map_status(d.value.proto_value.immobilizer_event.relays_events[0].status) as status,
        map_source(d.value.proto_value.immobilizer_event.relays_events[0].source) as source,
        map_change_reason(d.value.proto_value.immobilizer_event.relays_events[0].change_reason) as change_reason,
        d.value.proto_value.immobilizer_event.relays_events[0].command_uuid as command_uuid,
        map_immobilizer_type(d.value.proto_value.immobilizer_event.context.immobilizer_type) as immobilizer_type,
        d.value.proto_value.immobilizer_event.context.location.latitude as latitude,
        d.value.proto_value.immobilizer_event.context.location.longitude as longitude
    FROM
        kinesisstats.osdimmobilizerevent d INNER JOIN
        clouddb.organizations o ON d.org_id = o.id
    WHERE
        o.internal_type != 1 AND
        d.date >= current_date() - {DAYS_LIMIT} AND
        d.value.proto_value.immobilizer_event.relays_events[0].command_uuid IS NOT NULL AND
        d.value.proto_value.immobilizer_event.context.immobilizer_type = 1

    UNION ALL

    SELECT
        from_unixtime(CAST(d.time/1000 as BIGINT)) as datetime,
        d.time,
        d.org_id,
        o.name,
        o.locale,
        d.object_id,
        d.value.proto_value.immobilizer_event.relays_events[1].relay_id as relay_id,
        map_operation_type(d.value.proto_value.immobilizer_event.relays_events[1].operation_type) as operation_type,
        map_status(d.value.proto_value.immobilizer_event.relays_events[1].status) as status,
        map_source(d.value.proto_value.immobilizer_event.relays_events[1].source) as source,
        map_change_reason(d.value.proto_value.immobilizer_event.relays_events[1].change_reason) as change_reason,
        d.value.proto_value.immobilizer_event.relays_events[1].command_uuid as command_uuid,
        map_immobilizer_type(d.value.proto_value.immobilizer_event.context.immobilizer_type) as immobilizer_type,
        d.value.proto_value.immobilizer_event.context.location.latitude as latitude,
        d.value.proto_value.immobilizer_event.context.location.longitude as longitude
    FROM
        kinesisstats.osdimmobilizerevent d INNER JOIN
        clouddb.organizations o ON d.org_id = o.id
    WHERE
        o.internal_type != 1 AND
        d.date >= current_date() - {DAYS_LIMIT} AND
        d.value.proto_value.immobilizer_event.relays_events[1].command_uuid IS NOT NULL AND
        d.value.proto_value.immobilizer_event.context.immobilizer_type = 1
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
        F.min("datetime").alias("earliest_date"),
        F.first("org_id").alias("org_id"),
        F.first("name").alias("name"),
        F.first("locale").alias("locale"),
        F.first("object_id").alias("object_id"),
        F.first("immobilizer_type").alias("immobilizer_type"),
        F.first("relay_id").alias("relay_id"),
        F.first("operation_type").alias("operation_type"),
        F.expr("FILTER(collect_list(source), source -> source != 'not_applicable')")[
            0
        ].alias("source"),
        F.last("change_reason").alias("change_reason"),
        F.collect_list(F.struct("status", "time")).alias("statuses_times"),
        F.last("status").alias("last_status"),
        F.first("latitude").alias("latitude"),
        F.first("longitude").alias("longitude"),
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
        "earliest_date",
        "org_id",
        "name",
        "locale",
        "object_id",
        "immobilizer_type",
        "relay_id",
        "operation_type",
        "source",
        "change_reason",
        "command_uuid",
        "last_status",
        "lifecycle",
        "latitude",
        "longitude",
    )

    return result_df


def apply_ignored_status(df: DataFrame) -> DataFrame:
    # Flag rows where last_status is not 'created' or 'sent'
    df = df.withColumn(
        "not_done",
        F.when(
            (F.col("last_status") == "created") | (F.col("last_status") == "sent"), 1
        ).otherwise(0),
    )

    # Find the maximum `time` for each device_id
    max_time_per_device = df.groupBy("object_id").agg(
        F.max("earliest_time").alias("max_time")
    )

    # Join with the original DataFrame to get the maximum time for each device_id
    df = df.join(max_time_per_device, on="object_id")

    # Update last_status to 'ignored' only for SOZE immobilizer type, where not_done = 1,
    # and earliest_time < max_time, and explicitly excluding rows with 'failed' last_status
    df = df.withColumn(
        "last_status",
        F.when(
            (F.col("not_done") == 1)
            & (F.col("earliest_time") < F.col("max_time"))
            & (F.col("immobilizer_type") == "SOZE"),
            "fx_ignored",
        ).otherwise(F.col("last_status")),
    )

    # Drop the intermediate columns used for processing
    df = df.drop("not_done", "max_time")

    return df


def check_time_btwn_clicks(df: DataFrame) -> DataFrame:
    # Define a window partitioned by device_id and ordered by time
    window_spec = Window.partitionBy("object_id").orderBy("earliest_time")

    # Define a window to get the first non-zero command_uuid within each partition
    df = df.withColumn("prev_click_time", F.lag("earliest_time").over(window_spec))

    df = df.withColumn(
        "time_btwn_clicks_in_s",
        F.when(
            F.col("prev_click_time").isNotNull(),
            ((F.col("earliest_time") - F.col("prev_click_time")) / 1000),
        ).otherwise(0),
    )

    df = df.drop("prev_click_time")

    return df


def create_event_links(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "link",
        F.concat(
            F.lit("https://cloud.samsara.com/o/"),
            F.col("org_id"),
            F.lit("/devices/"),
            F.col("object_id"),
            F.lit("/show?end_ms="),
            F.expr("earliest_time + 1800000"),
            F.lit(
                '&duration=3600&diagnosticsPresetId="undefined"&device-show-graphs-vg=%5B"sozeEngineImmobilizerStatus"%2C"sozeEngineImmobilizerTamperStatus"%2C"gpsJammingMetrics"%2C"gpsJammingStatus"%5D&customGraphPresets=%5B%5D'
            ),
        ),
    )

    return df


# Fetch the data
data = fetch_data()
exclude_0000_1 = fill_0000_uuid(data)
exclude_0000_2 = fill_0000_uuid(exclude_0000_1)
exclude_0000_3 = fill_0000_uuid(exclude_0000_2)  # Fix uuid 0000 for the processed data
data = command_lifecycle(exclude_0000_3)  # Process the data to the lifecycle format
data = apply_ignored_status(data)  # Apply backend ignored data
data = check_time_btwn_clicks(data)  # Check time between clicks
data = create_event_links(data)
save_to_delta(data, "soze_immobilizations_processed")

# Get the total event count
events_count = data.count()

# Get counts for specific statuses
done_count = data.filter(F.col("last_status") == "done").count()
ignored_count = data.filter(F.col("last_status") == "ignored").count()
fx_ignored_count = data.filter(F.col("last_status") == "fx_ignored").count()
failed_count = data.filter(F.col("last_status") == "failed").count()

# Display the counts
log_datadog_metrics(
    [
        {
            "metric": "databricks.fleetsecurity.soze_immobilizations.count",
            "points": events_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.soze_immobilizations.done",
            "points": done_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.soze_immobilizations.ignored",
            "points": ignored_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.soze_immobilizations.fx_ignored",
            "points": fx_ignored_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.soze_immobilizations.failed",
            "points": failed_count,
            "tags": [f"region:{REGION}"],
        },
    ]
)
