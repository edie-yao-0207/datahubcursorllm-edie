from pyspark.sql import SparkSession
import boto3

# Initialize Spark session
spark = SparkSession.builder.appName("FleetsecurityPanicButtonData").getOrCreate()

# Constants
S3_PREFIX = "/Volumes/s3/databricks-workspace/fleetsec/panic_button"

REGION = boto3.session.Session().region_name

# Save data to Delta function
def save_to_delta(df, name):
    df.write.format("delta").mode("overwrite").saveAsTable(f"fleetsec_dev.`{name}`")


# Save data to S3 function
def save_to_s3(df, path):
    df.coalesce(1).write.format("csv").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("header", True).save(path)


# Fetch data function - number 14 is the panic button identifier
def fetch_panic_button_data():
    query = f"""
        SELECT
            o.locale,
            count(1) as panic_button,
            current_date() as date
        FROM
            clouddb.devices d
            JOIN clouddb.organizations o ON o.id = d.org_id
        WHERE
            digi1_type_id = 14
        GROUP BY all
    """
    return spark.sql(query)


def fetch_panic_button_historic_data():
    table_name = f"default.fleetsec_dev.`panic_button_per_locale_historic`"
    try:
        query = f"SELECT * FROM {table_name}"
        return spark.sql(query)
    except Exception as e:
        print(f"Failed to query {table_name}. Error: {e}")
        return None


def process_panic_button_historic_data():
    # Fetch the data
    historic_data = fetch_panic_button_historic_data()
    current_data = fetch_panic_button_data()

    combined_data = historic_data.union(current_data) if historic_data else current_data
    save_to_delta(
        combined_data,
        "panic_button_per_locale_historic",
    )
    save_to_s3(
        combined_data,
        f"{S3_PREFIX}/panic_button_per_locale_historic",
    )


process_panic_button_historic_data()
