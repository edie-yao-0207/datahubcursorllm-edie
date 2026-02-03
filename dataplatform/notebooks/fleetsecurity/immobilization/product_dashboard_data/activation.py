# MAGIC %run /backend/fleetsecurity/immobilization/product_dashboard_data/common

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FleetsecurityActivationData").getOrCreate()

# Constants
S3_PREFIX = (
    "/Volumes/s3/databricks-workspace/fleetsec/product_dashboard/activation_data"
)


# Fetch data function
def fetch_morpheus_data():
    query = f"""
        SELECT
            locale,
            sum(immobilizations) as  total_immobilizations,
            current_date() as date
        FROM default.fleetsec_dev.`morpheus_osdengineimmobilizer_immobilizations_per_org`
        GROUP BY locale
    """
    return spark.sql(query)


def fetch_morpheus_historic_data():
    table_name = f"default.fleetsec_dev.`morpheus_osdengineimmobilizer_immobilizations_per_locale_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_morpheus_historic_data():
    # Fetch the data
    historic_data = fetch_morpheus_historic_data()
    current_data = fetch_morpheus_data()

    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(
            combined_data,
            "morpheus_osdengineimmobilizer_immobilizations_per_locale_historic",
        )
        save_to_s3(
            combined_data,
            f"{S3_PREFIX}/morpheus_osdengineimmobilizer_immobilizations_per_locale_historic",
        )
    else:
        save_to_delta(
            current_data,
            "morpheus_osdengineimmobilizer_immobilizations_per_locale_historic",
        )
        save_to_s3(
            current_data,
            f"{S3_PREFIX}/morpheus_osdengineimmobilizer_immobilizations_per_locale_historic",
        )


process_morpheus_historic_data()

# Fetch data function
def fetch_soze_data():
    query = f"""
        SELECT
            locale,
            count(1) as  total_immobilizations,
            current_date() as date
        FROM default.fleetsec_dev.`soze_immobilizations_processed`
        WHERE
            last_status = 'done'
            AND (change_reason = 'config' OR change_reason = 'hub_request')
            AND operation_type = 'IMMOBILIZE'
        GROUP BY locale
    """
    return spark.sql(query)


def fetch_soze_historic_data():
    table_name = f"default.fleetsec_dev.`soze_immobilizer_count_per_locale_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_soze_historic_data():
    # Fetch the data
    historic_data = fetch_soze_historic_data()
    current_data = fetch_soze_data()

    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(combined_data, "soze_immobilizer_count_per_locale_historic")
        save_to_s3(
            combined_data, f"{S3_PREFIX}/soze_immobilizer_count_per_locale_historic"
        )
    else:
        save_to_delta(current_data, "soze_immobilizer_count_per_locale_historic")
        save_to_s3(
            current_data, f"{S3_PREFIX}/soze_immobilizer_count_per_locale_historic"
        )


process_soze_historic_data()


def count_jamming_and_tampering():
    query = f"""
        SELECT
            COUNT(DISTINCT s.OrgId) AS total_mx_orgs,
            COUNT(DISTINCT CASE WHEN get_json_object(s.Relay1Config, '$.ImmobilizeOnJamming') = 'true' OR  get_json_object(s.Relay2Config, '$.ImmobilizeOnJamming') = 'true' THEN s.OrgId END) AS jamming_on_count,
            COUNT(DISTINCT CASE WHEN get_json_object(s.Relay1Config, '$.ImmobilizeOnTamper') = 'true' OR  get_json_object(s.Relay2Config, '$.ImmobilizeOnTamper') = 'true' THEN s.OrgId END) AS tampering_on_count,
            current_date() as date
         FROM
            dynamodb.soze_engine_immobilizer_org_configs s INNER JOIN
            clouddb.organizations o
            ON s.OrgId = o.id
        WHERE
            o.locale = 'mx';
    """
    return spark.sql(query)


def fetch_jamming_tampering_historic_data():
    table_name = f"default.fleetsec_dev.`jamming_tampering_on_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_jamming_tampering_historic_data():
    # Fetch the data
    historic_data = fetch_jamming_tampering_historic_data()
    current_data = count_jamming_and_tampering()

    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(combined_data, "jamming_tampering_on_historic")
        save_to_s3(combined_data, f"{S3_PREFIX}/jamming_tampering_on_historic")
    else:
        save_to_delta(current_data, "jamming_tampering_on_historic")
        save_to_s3(current_data, f"{S3_PREFIX}/jamming_tampering_on_historic")


process_jamming_tampering_historic_data()
