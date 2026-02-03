# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/fleetsecurity/immobilization/common

# COMMAND ----------

#  Morpheus Immobilizers Count grouped by org

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("MorpheusImmobilizerCount").getOrCreate()

DAYS_LIMIT = 30

# Fetch data function
def fetch_data():
    query = f"""
        SELECT
            d.org_id,
            o.name AS org_name,
            o.locale,
            COUNT(DISTINCT d.object_id) AS immobilizer_count
        FROM
            kinesisstats.osdengineimmobilizer d
        INNER JOIN
            clouddb.organizations o ON d.org_id = o.id
        WHERE
            d.value.proto_value.engine_immobilizer_status.usb_relay_controller_connected
            AND o.internal_type != 1
            AND date > current_date() - {DAYS_LIMIT}
        GROUP BY
            d.org_id, o.name, o.locale
        ORDER BY immobilizer_count DESC
    """
    return spark.sql(query)


# Log total immobilizer count to Datadog
def log_total_count_to_datadog(total_count):
    log_datadog_metrics(
        [
            {
                "metric": "databricks.fleetsecurity.morpheus_immobilizer.count",
                "points": total_count,
                "tags": [f"region:{REGION}"],
            },
        ]
    )


# Main function to process data
def process_data():
    # Fetch data
    org_segments_sdf = fetch_data()
    save_to_delta(org_segments_sdf, "morpheus_immobilizer_count")

    # Log the total count to Datadog
    total_count = org_segments_sdf.agg({"immobilizer_count": "sum"}).collect()[0][0]
    log_total_count_to_datadog(total_count)


# Run the processing function
process_data()
