# MAGIC %run /backend/fleetsecurity/immobilization/product_dashboard_data/common

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FleetsecurityOnboardingData").getOrCreate()

# Constants
S3_PREFIX = (
    "/Volumes/s3/databricks-workspace/fleetsec/product_dashboard/onboarding_data"
)


# Fetch data function
def fetch_morpheus_data():
    query = f"""
        SELECT
            locale,
            sum(immobilizer_count) as  immobilizer_count,
            current_date() as date
        FROM default.fleetsec_dev.`morpheus_immobilizer_count`
        GROUP BY locale
    """
    return spark.sql(query)


def fetch_morpheus_historic_data():
    table_name = f"default.fleetsec_dev.`morpheus_immobilizer_count_historic`"
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

    # Combine data and save
    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(combined_data, "morpheus_immobilizer_count_historic")
        save_to_s3(combined_data, f"{S3_PREFIX}/morpheus_immobilizer_count_historic")
    else:
        save_to_delta(current_data, "morpheus_immobilizer_count_historic")
        save_to_s3(current_data, f"{S3_PREFIX}/morpheus_immobilizer_count_historic")


process_morpheus_historic_data()

# Fetch data function
def fetch_soze_data():
    query = f"""
         SELECT
            locale,
            sum(immobilizer_count) as  immobilizer_count,
            current_date() as date
        FROM default.fleetsec_dev.`soze_immobilizer_count`
        GROUP BY locale
    """
    return spark.sql(query)


def fetch_soze_historic_data():
    table_name = f"default.fleetsec_dev.`soze_immobilizer_count_historic`"
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
        save_to_delta(combined_data, "soze_immobilizer_count_historic")
        save_to_s3(combined_data, f"{S3_PREFIX}/soze_immobilizer_count_historic")
    else:
        save_to_delta(current_data, "soze_immobilizer_count_historic")
        save_to_s3(current_data, f"{S3_PREFIX}/soze_immobilizer_count_historic")


process_soze_historic_data()


def count_ei_customers():
    query = f"""
        SELECT
            locale,
            COUNT(DISTINCT org_id) AS unique_customer_count,
            current_date() AS date
        FROM (
            SELECT org_id, locale FROM default.fleetsec_dev.`morpheus_immobilizer_count`
        UNION
            SELECT org_id, locale FROM default.fleetsec_dev.`soze_immobilizer_count`
        ) combined
        GROUP BY locale;
    """
    return spark.sql(query)


def fetch_ei_customer_count_historic_data():
    table_name = f"default.fleetsec_dev.`ei_customer_count_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_ei_customer_count_historic_data():
    # Fetch the data
    historic_data = fetch_ei_customer_count_historic_data()
    current_data = count_ei_customers()

    if historic_data:
        combined_data = historic_data.union(current_data)
        save_to_delta(combined_data, "ei_customer_count_historic")
        save_to_s3(combined_data, f"{S3_PREFIX}/ei_customer_count_historic")
    else:
        save_to_delta(current_data, "ei_customer_count_historic")
        save_to_s3(current_data, f"{S3_PREFIX}/ei_customer_count_historic")


process_ei_customer_count_historic_data()
