# MAGIC %run /backend/fleetsecurity/immobilization/product_dashboard_data/common

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FleetsecurityImpactData").getOrCreate()

# Constants
S3_PREFIX = "/Volumes/s3/databricks-workspace/fleetsec/product_dashboard/impact_data"


# Fetch data function
def fetch_licenses_data():
    query = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN sku = "LIC-EI-A" THEN org_id END) AS LIC_EI_A,
            COUNT(DISTINCT CASE WHEN sku = "LIC-EI-SEC" THEN org_id END) AS LIC_EI_SEC,
            COUNT(DISTINCT CASE WHEN sku = "LIC-EI-EU" THEN org_id END) AS LIC_EI_EU,
            COUNT(DISTINCT CASE WHEN sku = "ACC-EI" THEN org_id END) AS ACC_EI,
            COUNT(DISTINCT CASE WHEN sku = "HW-EI21" THEN org_id END) AS HW_EI21,
            current_date() as date
        FROM datamodel_platform.dim_licenses;
    """
    return spark.sql(query)


def fetch_licenses_historic_data():
    table_name = f"default.fleetsec_dev.`licenses_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_licenses_historic_data():
    # Fetch the data
    historic_data = fetch_licenses_historic_data()
    current_data = fetch_licenses_data()

    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(combined_data, "licenses_historic")
        save_to_s3(combined_data, f"{S3_PREFIX}/licenses_historic")
    else:
        save_to_delta(current_data, "licenses_historic")
        save_to_s3(current_data, f"{S3_PREFIX}/licenses_historic")


process_licenses_historic_data()

# Fetch data function
def fetch_jamming_and_tampering_data():
    query = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN change_reason = 'jamming' THEN earliest_time END) AS jamming,
            COUNT(DISTINCT CASE WHEN change_reason = 'tamper' THEN earliest_time END) AS tampering,
            current_date() as date
        FROM default.fleetsec_dev.`soze_immobilizations_processed`
        WHERE
            last_status = 'done'
            AND locale = 'mx'
    """
    return spark.sql(query)


def fetch_jamming_and_tampering_historic_data():
    table_name = f"default.fleetsec_dev.`jamming_and_tampering_count_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_jamming_and_tampering_historic_data():
    # Fetch the data
    historic_data = fetch_jamming_and_tampering_historic_data()
    current_data = fetch_jamming_and_tampering_data()

    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(combined_data, "jamming_and_tampering_count_historic")
        save_to_s3(combined_data, f"{S3_PREFIX}/jamming_and_tampering_count_historic")
    else:
        save_to_delta(current_data, "jamming_and_tampering_count_historic")
        save_to_s3(current_data, f"{S3_PREFIX}/jamming_and_tampering_count_historic")


process_jamming_and_tampering_historic_data()
