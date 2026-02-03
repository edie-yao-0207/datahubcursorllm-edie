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
        map_immobilizer_type(d.value.proto_value.immobilizer_event.context.immobilizer_type) as immobilizer_type
    FROM
        kinesisstats.osdimmobilizerevent d INNER JOIN
        clouddb.organizations o ON d.org_id = o.id
    WHERE
        o.internal_type != 1 AND
        d.date >= current_date() - {DAYS_LIMIT} AND
        d.value.proto_value.immobilizer_event.relays_events[0].command_uuid IS NOT NULL AND
        d.value.proto_value.immobilizer_event.context.immobilizer_type = 2

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
        map_immobilizer_type(d.value.proto_value.immobilizer_event.context.immobilizer_type) as immobilizer_type
    FROM
        kinesisstats.osdimmobilizerevent d INNER JOIN
        clouddb.organizations o ON d.org_id = o.id
    WHERE
        o.internal_type != 1 AND
        d.date >= current_date() - {DAYS_LIMIT} AND
        d.value.proto_value.immobilizer_event.relays_events[1].command_uuid IS NOT NULL AND
        d.value.proto_value.immobilizer_event.context.immobilizer_type = 2
    ORDER BY time DESC
    """
    df = spark.sql(query)
    return df


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
    )

    return result_df


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


# Fetch the data
data = fetch_data()
lifecycle_data = command_lifecycle(data)
processed_data = check_time_btwn_clicks(lifecycle_data)
save_to_delta(processed_data, "morpheus_requested_immobilizations")

# Get the total event count
events_count = lifecycle_data.count()

# Get counts for specific statuses
immobilziations_count = lifecycle_data.filter(
    F.col("operation_type") == "IMMOBILIZE"
).count()
remobilziations_count = lifecycle_data.filter(
    F.col("operation_type") == "REMOBILIZE"
).count()

# Display the counts
log_datadog_metrics(
    [
        {
            "metric": "databricks.fleetsecurity.morpheus_requested_immobilizations.total",
            "points": events_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.morpheus_requested_immobilizations.immobilizations",
            "points": immobilziations_count,
            "tags": [f"region:{REGION}"],
        },
        {
            "metric": "databricks.fleetsecurity.morpheus_requested_immobilizations.remobilizations",
            "points": remobilziations_count,
            "tags": [f"region:{REGION}"],
        },
    ]
)
