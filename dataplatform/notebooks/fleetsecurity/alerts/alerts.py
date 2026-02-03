from pyspark.sql import SparkSession
import boto3

# Initialize Spark session
spark = SparkSession.builder.appName("FleetsecurityAlertsData").getOrCreate()

# Constants
S3_PREFIX = "/Volumes/s3/databricks-workspace/fleetsec/product_dashboard/alerts_data"
REGION = boto3.session.Session().region_name

# Save data to Delta function
def save_to_delta(df, name):
    df.write.format("delta").mode("overwrite").saveAsTable(f"fleetsec_dev.`{name}`")


# Save data to S3 function
def save_to_s3(df, path):
    df.coalesce(1).write.format("csv").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("header", True).save(path)


# Fetch data function
def fetch_incident_data():
    query = f"""
        SELECT
            o.locale,
            CASE t.type
                WHEN 1047 THEN 'Jamming Alert'
                WHEN 1045 THEN 'Tampering Alert'
                WHEN 1034 THEN 'Panic Button Alert'
                WHEN 1032 THEN 'GPS Signal Loss Alert'
                WHEN 1033 THEN 'LTE Signal Loss Alert'
                WHEN 1067 THEN 'Assigned Driver On Duty Alert'
                WHEN 1068 THEN 'Assigned Driver Off Duty Alert'
                WHEN 5034 THEN 'Sudden Fuel Rise Alert'
                WHEN 5035 THEN 'Sudden Fuel Drop Alert'
                WHEN 5040 THEN 'Immobilizer State Change Alert'
                ELSE 'Unknown'
            END AS trigger_name,
            COUNT(*) as incident_count,
            current_date() as date
        FROM workflowsdb_shards.workflow_incidents wi
        JOIN workflowsdb_shards.triggers t ON wi.workflow_id = t.workflow_uuid
        JOIN clouddb.organizations o ON o.id = wi.org_id
        WHERE
            o.internal_type != 1
            AND t.deleted_at IS NULL AND
            t.type IN (1047, 1045, 1034, 1032, 1033, 1067, 1068, 5034, 5035, 5040) AND
            wi.occurred_at_ms / 1000 >= unix_timestamp(current_date() - 7)
        GROUP BY t.type, o.locale
        ORDER BY trigger_name ASC
    """
    return spark.sql(query)


def fetch_incidents_historic_data():
    table_name = f"default.fleetsec_dev.`alerts_incidents_per_locale_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_incidents_historic_data():
    # Fetch the data
    historic_data = fetch_incidents_historic_data()
    current_data = fetch_incident_data()

    combined_data = historic_data.union(current_data) if historic_data else current_data
    save_to_delta(
        combined_data,
        "alerts_incidents_per_locale_historic",
    )
    save_to_s3(
        combined_data,
        f"{S3_PREFIX}/alerts_incidents_per_locale_historic",
    )


process_incidents_historic_data()

# Fetch data function
def fetch_triggers_data():
    query = f"""
        SELECT
            o.locale,
            CASE t.type
                WHEN 1047 THEN 'Jamming Alert'
                WHEN 1045 THEN 'Tampering Alert'
                WHEN 1034 THEN 'Panic Button Alert'
                WHEN 1032 THEN 'GPS Signal Loss Alert'
                WHEN 1033 THEN 'LTE Signal Loss Alert'
                WHEN 1067 THEN 'Assigned Driver On Duty Alert'
                WHEN 1068 THEN 'Assigned Driver Off Duty Alert'
                WHEN 5034 THEN 'Sudden Fuel Rise Alert'
                WHEN 5035 THEN 'Sudden Fuel Drop Alert'
                WHEN 5040 THEN 'Immobilizer State Change Alert'
                ELSE 'Unknown'
            END AS trigger_name,
            COUNT(DISTINCT t.uuid) as alerts_count,
            current_date() as date
        FROM workflowsdb_shards.triggers t
        JOIN clouddb.organizations o ON o.id = t.org_id
        WHERE
            o.internal_type != 1  AND
            t.type IN (1047, 1045, 1034, 1032, 1033, 1067, 1068, 5034, 5035, 5040) AND
            t.deleted_at IS NULL
        GROUP BY all
        ORDER BY alerts_count DESC
    """
    return spark.sql(query)


def fetch_triggers_historic_data():
    table_name = f"default.fleetsec_dev.`alerts_triggers_per_locale_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_triggers_historic_data():
    # Fetch the data
    historic_data = fetch_triggers_historic_data()
    current_data = fetch_triggers_data()

    combined_data = historic_data.union(current_data) if historic_data else current_data
    save_to_delta(
        combined_data,
        "alerts_triggers_per_locale_historic",
    )
    save_to_s3(
        combined_data,
        f"{S3_PREFIX}/alerts_triggers_per_locale_historic",
    )


process_triggers_historic_data()

# Fetch data function - unique means that we only count the events once per object_id and occurred_at_ms, the number of configured alerts doesn't matter
def fetch_unique_incident_data():
    query = f"""
        SELECT
        o.locale,
        CASE t.type
            WHEN 1047 THEN 'Jamming Alert'
            WHEN 1045 THEN 'Tampering Alert'
            WHEN 1034 THEN 'Panic Button Alert'
            WHEN 1032 THEN 'GPS Signal Loss Alert'
            WHEN 1033 THEN 'LTE Signal Loss Alert'
            WHEN 1067 THEN 'Assigned Driver On Duty Alert'
            WHEN 1068 THEN 'Assigned Driver Off Duty Alert'
            WHEN 5034 THEN 'Sudden Fuel Rise Alert'
            WHEN 5035 THEN 'Sudden Fuel Drop Alert'
            WHEN 5040 THEN 'Immobilizer State Change Alert'
            ELSE 'Unknown'
        END AS trigger_name,
        COUNT(DISTINCT proto.trigger_matches.object_id, proto.trigger_matches.occurred_at_ms) AS unique_incident_count,
        CURRENT_DATE() AS date
    FROM workflowsdb_shards.workflow_incidents wi
    JOIN workflowsdb_shards.triggers t ON wi.workflow_id = t.workflow_uuid
    JOIN clouddb.organizations o ON o.id = wi.org_id
    WHERE
        o.internal_type != 1
        AND t.deleted_at IS NULL
        AND t.type IN (1047, 1045, 1034, 1032, 1033, 1067, 1068, 5034, 5035, 5040)
        AND wi.occurred_at_ms / 1000 >= UNIX_TIMESTAMP(CURRENT_DATE() - 7)
    GROUP BY t.type, o.locale
    ORDER BY trigger_name ASC;
    """
    return spark.sql(query)


def fetch_unique_incidents_historic_data():
    table_name = f"default.fleetsec_dev.`alerts_unique_incidents_per_locale_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_unique_incidents_historic_data():
    # Fetch the data
    historic_data = fetch_unique_incidents_historic_data()
    current_data = fetch_unique_incident_data()

    combined_data = historic_data.union(current_data) if historic_data else current_data
    save_to_delta(
        combined_data,
        "alerts_unique_incidents_per_locale_historic",
    )
    save_to_s3(
        combined_data,
        f"{S3_PREFIX}/alerts_unique_incidents_per_locale_historic",
    )


process_unique_incidents_historic_data()
