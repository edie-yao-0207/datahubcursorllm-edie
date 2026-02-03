# Databricks notebook source
# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import date
from datetime import timedelta

QUERY_LIMIT = 100000
S3_BUCKET = "samsara-commercial-navigation-daily-driver-session-count"

try:
    # We don't have control over the filename when databricks writes the csv to s3 (databricks calls the csv file 'part-*'.)  This function renames the file
    def rename_databricks_file(prefix, new_filename):
        files_in_s3 = get_s3_client(
            "samsara-com-nav-daily-driver-session-count-read"
        ).list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix,)["Contents"]
        for file in files_in_s3:
            cur_filename = file["Key"].split("/")[-1]
            if cur_filename.startswith("part-"):
                dbutils.fs.mv(
                    f"s3://{S3_BUCKET}/{prefix}/{cur_filename}",
                    f"s3://{S3_BUCKET}/{prefix}/{new_filename}",
                )

    # pyspark entry point
    spark = SparkSession.builder.getOrCreate()

    # We want to use the most recent day of data. This will keep our writes to a minimum and ensure we are not going to run into duplicate entry conflicts when going to Dynamo.
    yesterdayDate = date.today() - timedelta(days=1)

    # Selecting records from the events data stream with a unique navSessionUUID in a particular day, grouped by org_id, driver_id, and date.
    queryString = f"""
  SELECT
    org_id as OrgId,
    `date` as `Date`,
    driver_id as DriverId,
    count(DISTINCT event_data:['navSessionUuid']) as NavigationSessionCount,
    UNIX_TIMESTAMP(DATEADD(day, 90, MAX(`date`))) as TTL
  From
    datastreams.mobile_nav_routing_events
  where
    event_data:['navSessionUuid'] IS NOT NULL
    and event_type = "NavState-activeNavigation"
    and org_id > 0
    and `date` = \"{yesterdayDate}\"
  group by
    OrgId,
    `Date`,
    DriverId
  order by
    `Date` desc,
    OrgId,
    NavigationSessionCount desc
  LIMIT {QUERY_LIMIT}
  """

    # Execute the query using the Databricks SQL API.
    dataframe = spark.sql(queryString)

    # Write CSV to the commercial-navigation-daily-driver-session-count S3 bucket
    rowCount = dataframe.count()
    if rowCount > 0:
        print(
            f"Writing {rowCount} records to S3 bucket {S3_BUCKET} for {yesterdayDate}."
        )

        s3FileLocation = f"s3://{S3_BUCKET}/daily_driver_session_count_{yesterdayDate}"

        dataframe.coalesce(1).write.format("csv").mode("overwrite").option(
            "header", True
        ).save(s3FileLocation)
        rename_databricks_file(
            f"daily_driver_session_count_{yesterdayDate}",
            f"daily_driver_session_count_{yesterdayDate}.csv",
        )

    else:
        print(f"No records found for {yesterdayDate}.")

except Exception as e:
    print(e)
