# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/fleetsecurity/immobilization/common

# COMMAND ----------

# Permissions Rollout Count

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("PermissionsCount").getOrCreate()

# Fetch data function
def fetch_data():
    query = """
        SELECT DISTINCT
            uo.organization_id,
            o.name AS org_name,
            COUNT(DISTINCT uo.user_id) AS num_of_users
        FROM
            clouddb.users_organizations uo
        JOIN
            clouddb.organizations o ON uo.organization_id = o.id
        WHERE
            uo.custom_role_uuid IN (
                SELECT DISTINCT uuid
                FROM clouddb.custom_roles
                LATERAL VIEW EXPLODE(permissions.permissions) AS permission
                WHERE (
                    permission.id = 'security.immobilization.administration'
                    OR
                    permission.id = 'security.immobilization.configuration'
                ) AND permission.edit = TRUE
            )
            AND uo.role_id != 1  -- Exclude full admin role
            AND uo.organization_id NOT IN (
                SELECT id
                FROM clouddb.organizations
                WHERE internal_type = 1
            )
        GROUP BY
            uo.organization_id, o.name
        ORDER BY
            num_of_users DESC
    """
    return spark.sql(query)


# Log total user count to Datadog
def log_total_user_count_to_datadog(total_count):
    log_datadog_metrics(
        [
            {
                "metric": "databricks.fleetsecurity.permission_rollout.user_count",
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
    save_to_delta(org_segments_sdf, "permission_rollout")

    # Log the total count to Datadog
    total_count = org_segments_sdf.agg({"num_of_users": "sum"}).collect()[0][0]
    log_total_user_count_to_datadog(total_count)


# Run the processing function
process_data()
