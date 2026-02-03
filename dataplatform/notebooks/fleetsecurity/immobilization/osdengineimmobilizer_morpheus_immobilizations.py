# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/fleetsecurity/immobilization/common

# COMMAND ----------

# Manual Immobilizations Events Per Org for morpheus based on kinesisstats.osdengineimmobilizer. It show actual immobilizations events, not the requests.

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName(
    "MorpheusOsdengineimmobilizerImmobilizationDetection"
).getOrCreate()

DAYS_BACK = 7

# Fetch data from last 7 days
def fetch_data():
    query = f"""
      SELECT
        d.org_id,
        o.name as org_name,
        o.locale,
        d.time as timestamp,
        d.object_id as device_id,
        d.value.proto_value.engine_immobilizer_status as morpheus_state
      FROM
        kinesisstats.osdengineimmobilizer d
        INNER JOIN clouddb.organizations o ON d.org_id = o.id
      WHERE
        d.value.proto_value.engine_immobilizer_status.usb_relay_controller_connected
        AND o.internal_type != 1
        AND date > current_date() - {DAYS_BACK}
      ORDER BY timestamp ASC
    """
    return spark.sql(query)


# Filter events for state transitions
def filter_events(data, current_state, previous_state):
    window_spec = Window.partitionBy("device_id").orderBy("timestamp")
    data_with_prev = data.withColumn(
        "prev_morpheus_state", F.lag("morpheus_state").over(window_spec)
    )
    return data_with_prev.filter(
        (F.col("morpheus_state.configured_state") == current_state)
        & (F.col("prev_morpheus_state.configured_state") == previous_state)
    )


# Summarize events by org and device
def summarize_events(data, event_col_name):
    data_summary = data.count()

    log_datadog_metrics(
        [
            {
                "metric": f"databricks.fleetsecurity.morpheus_weekly_{event_col_name}.count",
                "points": data_summary,
                "tags": [f"region:{REGION}"],
            }
        ]
    )

    org_summary = (
        data.groupBy("org_id", "locale")
        .agg(F.count("*").alias(event_col_name))
        .join(data.select("org_id", "org_name").distinct(), on="org_id", how="inner")
        .orderBy(F.col(event_col_name).desc())
    )
    device_summary = (
        data.groupBy("org_id", "locale", "device_id")
        .agg(F.count("*").alias(event_col_name))
        .join(data.select("org_id", "org_name").distinct(), on="org_id", how="inner")
        .orderBy(F.col(event_col_name).desc())
    )
    return org_summary, device_summary


# Main processing function
def process_events(data):
    immobilizations = filter_events(
        data, 1, 2
    )  # State 1 -> immobilized, 2 -> remobilized
    remobilizations = filter_events(data, 2, 1)

    immobilizations_per_org, immobilizations_per_device = summarize_events(
        immobilizations, "immobilizations"
    )
    remobilizations_per_org, remobilizations_per_device = summarize_events(
        remobilizations, "remobilizations"
    )

    save_to_delta(
        immobilizations_per_org, "morpheus_osdengineimmobilizer_immobilizations_per_org"
    )
    save_to_delta(
        remobilizations_per_org, "morpheus_osdengineimmobilizer_remobilizations_per_org"
    )
    save_to_delta(
        immobilizations_per_device,
        "morpheus_osdengineimmobilizer_immobilizations_per_device",
    )
    save_to_delta(
        remobilizations_per_device,
        "morpheus_osdengineimmobilizer_remobilizations_per_device",
    )


# Run the processing function
data = fetch_data()
process_events(data)
