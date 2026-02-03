# Databricks notebook source
# MAGIC %md
# MAGIC # Speed Limit Sign Metadata Ingestion
# MAGIC
# MAGIC This notebook ingests speed limit sign metadata from the dojo database and processes it for downstream use.
# MAGIC
# MAGIC - Surface and soft-delete any prod records older than their retention window,
# MAGIC - only introduce the valid & (complately new | have same value in primary key but higher image quality)
# MAGIC Note: the primary keys are (org_id, way_id, captured_timestamp)

# COMMAND ----------

import io
from pyspark.sql.functions import (
    col,
    datediff,
    lit,
    current_date,
    least,
    coalesce,
    row_number,
    from_unixtime,
    regexp_replace,
)
from pyspark.sql.window import Window
import time
import boto3

# COMMAND ----------

boto3_session = boto3.Session(
    botocore_session=dbutils.credentials.getServiceCredentialsProvider(
        "samsara-safety-map-data-sources-readwrite"
    )
)
s3 = boto3_session.resource("s3")
client = boto3_session.client("s3")
bucket = "samsara-safety-map-data-sources"

# COMMAND ----------


def create_speed_limit_signs_metadata_prod_view():
    """
    Creates a temporary view for speed limit sign metadata from speedlimitdb.
    This view contains the current state of speed limit signs in production.
    """
    query = """
    CREATE OR REPLACE TEMPORARY VIEW speed_limit_signs_metadata_prod AS
    SELECT
      org_id,
      way_id,
      captured_timestamp,
      heading,
      s3_url,
      speed_limit_milliknots,
      latitude,
      longitude,
      image_quality,
      is_deleted
    FROM speedlimitsdb.speed_limit_signs_metadata
    """
    spark.sql(query)


def create_retention_config_view():
    """
    Creates a temporary view for retention configuration.
    This view contains retention policies for speed limit sign data.
    """
    query = """
    CREATE OR REPLACE TEMPORARY VIEW retention_config AS
    SELECT
      org_id,
      retain_days
    FROM retentiondb_shards.retention_config
    WHERE data_type = 4
    """
    spark.sql(query)


# List of organization IDs that have opted out
optoutOrgs = [
    34091,
    7058,
    45402,
    52973,
    56078,
    56580,
    51875,
    48624,
    49785,
    562949953421364,
    562949953421379,
    562949953421403,
    562949953421851,
    562949953421917,
    562949953422164,
    562949953423989,
    562949953425328,
    562949953425631,
    562949953425730,
    562949953425769,
    562949953426149,
    562949953426450,
    9000949,
    22756,
    22818,
    23813,
    24942,
    25715,
    37081,
    42057,
    43702,
    47523,
    49560,
    50226,
    58207,
    81134,
    55660,
    562949953426014,
    7003503,
    7003814,
    8003554,
    11002207,
    11000936,
    46766,
    78710,
    7003884,
    8005968,
    8004037,
    11004248,
    36048,
    54272,
    4004067,
    4006772,
    8004037,
    9005456,
    5005799,
    7005237,
    9002610,
    4006178,
    9003099,
    6004189,
    4008042,
    5007316,
    10006583,
    10007715,
    10007716,
    11003153,
    6002605,
    10005094,
    4001688,
    4003141,
    26486,
    428,
    1919,
    11008052,
    8006251,
    7006230,
    8002663,
    562949953427929,
    4004067,
    9007820,
]
optout_df = spark.createDataFrame([(org_id,) for org_id in optoutOrgs], ["org_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Data

# COMMAND ----------

# Flags records in the production dataset that need to be deleted based on retention policy:
# - Sets effective retention days as min(retention_days, 730) with default of 730 days
# - Marks records for deletion if they are older than retention period and not already deleted
def get_flagged_prod_records(prod_df, retention_df):
    prod_df = prod_df.withColumn("is_deleted", col("is_deleted").cast("boolean"))

    df = (
        prod_df.join(retention_df, "org_id", "left")
        .withColumn(
            "effective_retention_days",
            least(coalesce(col("retain_days"), lit(730)), lit(730)),
        )
        .withColumn(
            "needs_deleting",
            (
                datediff(
                    current_date(), from_unixtime(col("captured_timestamp") / 1000)
                )
                > col("effective_retention_days")
            )
            & (~col("is_deleted")),
        )
        .drop("retain_days", "effective_retention_days")
        .select(
            "org_id",
            "way_id",
            "captured_timestamp",
            "heading",
            "s3_url",
            "speed_limit_milliknots",
            "latitude",
            "longitude",
            "image_quality",
            "needs_deleting",
        )
    )
    return df


def get_valid_dojo_records(dojo_df, retention_df, prod_flagged_df, optout_df):
    # Filter out opt-out organizations first
    dojo_optin = dojo_df.join(optout_df, "org_id", "left_anti")

    # 1) Retention‐filter your incoming dojo data,
    dojo_retained = (
        dojo_optin.join(retention_df, "org_id", "left")
        .withColumn(
            "effective_retention_days",
            least(coalesce(col("retain_days"), lit(730)), lit(730)),
        )
        .withColumn(
            "is_outdated",
            datediff(current_date(), from_unixtime(col("file_timestamp_ms") / 1000))
            > col("effective_retention_days"),
        )
        .filter(col("is_outdated") == False)
        .filter(
            (col("heading").cast("double") > 0) & (col("heading").cast("double") < 360)
        )
        # Filter out rows with null or invalid speed_limit_milliknots
        .filter(col("speed_limit_milliknots").isNotNull())
        .filter(col("speed_limit_milliknots").cast("double") > 0)
        # Filter out rows with null image_score
        .filter(col("image_score").isNotNull())
        .drop("is_outdated", "retain_days", "effective_retention_days")
        .select(
            "org_id",
            "way_id",
            "speed_limit_milliknots",
            col("file_timestamp_ms").alias("captured_timestamp"),
            "heading",
            "latitude",
            "longitude",
            col("image_score").alias("image_quality"),
            col("prod_uri").alias("s3_url"),
        )
    )

    # 2) Rename dojo's is_deleted → needs_deleting
    dojo_prepared = dojo_retained.withColumn("needs_deleting", lit(False))

    # 3) Extract prod‐valid for comparison
    prod_valid = prod_flagged_df.select(
        "org_id",
        "way_id",
        "captured_timestamp",
        col("image_quality").alias("prod_image_quality"),
    )

    # 4) Left‐join dojo → prod_valid to compare image_quality: only keep entirely new or higher‐quality records
    dojo_vs_prod = (
        dojo_prepared.alias("dojo")
        .join(
            prod_valid.alias("prod"),
            on=["org_id", "way_id", "captured_timestamp"],
            how="left",
        )
        .filter(
            (col("prod.prod_image_quality").isNull())
            | (col("dojo.image_quality") > col("prod.prod_image_quality"))
        )
        .select("dojo.*")  # drop the prod_image_quality column
    )

    # 5) deduplication - Keeps only the top-ranked row in each group, dropping any duplicates with lower quality.
    window_spec = Window.partitionBy("org_id", "way_id", "captured_timestamp").orderBy(
        col("image_quality").desc()
    )

    dojo_deduped = (
        dojo_vs_prod.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    return dojo_deduped


def process_data():
    try:
        # Load dojo, prod, and retention config
        dojo_df = spark.table("dojo.speed_limit_sign_image_upload_published")
        print(
            f"Loaded {dojo_df.count()} records from speed_limit_sign_image_upload_published"
        )

        prod_df = spark.table("speed_limit_signs_metadata_prod")
        retention_df = spark.table("retention_config")

        prod_flagged = get_flagged_prod_records(prod_df, retention_df)

        # Pick only the outdated prod → needs deleting
        outdated_prod = prod_flagged.filter(col("needs_deleting") == True)

        # From dojo, keep only new or higher‐quality overrides
        dojo_valid = get_valid_dojo_records(
            dojo_df, retention_df, prod_flagged, optout_df
        )

        combined_df = outdated_prod.unionByName(dojo_valid)

        result_df = (
            combined_df.drop(col("is_deleted"))
            .withColumnRenamed("needs_deleting", "is_deleted")
            .select(
                "org_id",
                "way_id",
                "captured_timestamp",
                "heading",
                "s3_url",
                "speed_limit_milliknots",
                "latitude",
                "longitude",
                "image_quality",
                "is_deleted",
            )
        )

        # Transform S3 uri to use client-map-tiles.samsara.com domain
        result_with_format_url_df = result_df.withColumn(
            "s3_url",
            regexp_replace(
                col("s3_url"),
                "s3://samsara-client-map-tiles/speed-limit-signs/",
                "https://client-map-tiles.samsara.com/speed-limit-signs/",
            ),
        )

        print(
            f"Outdated prod records: {prod_flagged.filter(col('needs_deleting') == True).count()}"
        )
        print(f"Valid dojo records: {dojo_valid.count()}")
        print(f"Final deduplicated records: {result_with_format_url_df.count()}")

        return result_with_format_url_df

    except Exception as e:
        print(f"Error in processing data: {str(e)}")
        raise


# COMMAND ----------


def create_backend_speed_limit_sign_metadata_csv(df):
    """Create CSV for backend ingestion with required columns."""
    backend_df = df.select(
        "org_id",
        "way_id",
        "captured_timestamp",
        "heading",
        "s3_url",
        "speed_limit_milliknots",
        "latitude",
        "longitude",
        "image_quality",
        col("is_deleted").cast("int").alias("is_deleted"),
    )
    return backend_df


def make_speed_limit_sign_metadata_csv_key(now_ms, i):
    return f"maptiles/speed-limit-signs/{now_ms}-{i}.csv"


def persist_backend_csv(df):
    """Persist the backend CSV to the appropriate location."""

    dataset_df = df.toPandas()
    # Partition DF into separate files
    MAX_ROWS = 200000
    list_of_dfs = [
        dataset_df.loc[i : i + MAX_ROWS - 1, :]
        for i in range(0, len(dataset_df), MAX_ROWS)
    ]

    now_ms = int(round(time.time() * 1000))
    for i in range(0, len(list_of_dfs)):
        key = make_speed_limit_sign_metadata_csv_key(now_ms, i)

        stream = io.StringIO()
        list_of_dfs[i].to_csv(stream, index=False)

        obj = s3.Object(bucket, key)
        obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")
    print(f"Backend CSV written to: {key}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------


def main():
    # Create views for speed limit signs metadata and retention config
    create_speed_limit_signs_metadata_prod_view()
    create_retention_config_view()

    print("start process data")
    result_df = process_data()

    # Create backend CSV
    backend_df = create_backend_speed_limit_sign_metadata_csv(result_df)

    # Persist the CSV
    persist_backend_csv(backend_df)
    print("Speed limit signs metadata processing complete")


# COMMAND ----------

# Execute main function
main()
