# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/fleetsecurity/immobilization/common

# COMMAND ----------

# Soze immobilizer read_state NULL count grouped by org - this may indicate wrong installation

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SozeWrongInstallation").getOrCreate()

# Constants
DAYS_BACK = 7

# Fetch data function
def fetch_data():
    query = f"""
        SELECT
            o.id AS org_id,
            o.name AS org_name,
            d.object_id AS device_id,
            COUNT(1) AS read_state_null_count
        FROM
            kinesisstats.osdsozeengineimmobilizerstatus d
        INNER JOIN
            clouddb.organizations o ON d.org_id = o.id
        WHERE
            o.internal_type != 1
            AND d.date > current_date() - {DAYS_BACK}
            AND (
                d.value.proto_value.soze_engine_immobilizer_status.relay_1.read_state IS NULL OR
                d.value.proto_value.soze_engine_immobilizer_status.relay_2.read_state IS NULL
            )
        GROUP BY
            o.id, o.name, d.object_id
        HAVING
            COUNT(1) >= 10
        ORDER BY
            read_state_null_count DESC
    """
    return spark.sql(query)


# Log total read_state NULL count to Datadog
def log_total_count_to_datadog(total_count):
    log_datadog_metrics(
        [
            {
                "metric": "databricks.fleetsecurity.soze_wrong_installation_devices.count",
                "points": total_count,
                "tags": [f"region:{REGION}"],
            },
        ]
    )


# Main function to process data
def process_data():
    # Fetch data
    org_segments_sdf = fetch_data()

    # Save results to S3 and Delta
    save_to_delta(org_segments_sdf, "soze_power_source_wrong_installation")

    # Log the total count to Datadog
    total_count = org_segments_sdf.agg({"device_id": "count"}).collect()[0][0]
    log_total_count_to_datadog(total_count)


# Run the processing function
process_data()
