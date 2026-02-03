-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC %pip install jinja2==3.0.3

-- COMMAND ----------

-- MAGIC %run backend/backend/databricks_data_alerts/alerting_system

-- COMMAND ----------

-- MAGIC %run /backend/dataplatform/boto3_helpers

-- COMMAND ----------

-- Use dynamic partitioning when writing to trusted tables so that we avoid ingesting all past data
-- in S3 given to us by AT&T. Doing so has led to this job timing out in the past.
SET
  spark.sql.sources.partitionOverwriteMode = DYNAMIC;
SET
  spark.databricks.delta.schema.autoMerge.enabled = True;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import boto3
-- MAGIC from typing import Dict, List
-- MAGIC
-- MAGIC from datetime import datetime, date, timedelta, timezone
-- MAGIC from dateutil.relativedelta import relativedelta
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.types import (
-- MAGIC     DataType,
-- MAGIC     LongType,
-- MAGIC     StringType,
-- MAGIC     StructField,
-- MAGIC     StructType,
-- MAGIC     TimestampType,
-- MAGIC )
-- MAGIC
-- MAGIC def get_bucket_by_region():
-- MAGIC   region = boto3.session.Session().region_name
-- MAGIC   if region == "us-west-2":
-- MAGIC       return "samsara-att-sftp"
-- MAGIC   elif region == "eu-west-1":
-- MAGIC       return "samsara-eu-att-sftp"
-- MAGIC   else:
-- MAGIC       print("unsupported AWS region: " + region)
-- MAGIC       sys.exit(1)
-- MAGIC
-- MAGIC def spec_to_schema(spec: Dict[str, str]) -> StructType:
-- MAGIC     fields: List[StructField] = []
-- MAGIC     for colname, coltype in spec:
-- MAGIC         fieldname = (
-- MAGIC             colname.translate(str.maketrans(" -()", "____"))
-- MAGIC             .replace("___", "_")
-- MAGIC             .replace("__", "_")
-- MAGIC             .lower()
-- MAGIC         )
-- MAGIC         fieldtype: DataType = StringType()
-- MAGIC         if coltype == "Integer":
-- MAGIC             fieldtype = LongType()
-- MAGIC         elif coltype == "DateTime":
-- MAGIC             fieldtype = TimestampType()
-- MAGIC         fields.append(StructField(fieldname, fieldtype))
-- MAGIC     return StructType(fields)
-- MAGIC
-- MAGIC
-- MAGIC # Returns one month from today in yyyymm
-- MAGIC def get_prior_month(arbitrary_date):
-- MAGIC     last_month = arbitrary_date + relativedelta(months=-1)
-- MAGIC     return last_month.strftime("%Y%m")
-- MAGIC
-- MAGIC # Store current / prior year-month strings here for use in
-- MAGIC # data-usage and subscriber snapshot file path strings in
-- MAGIC # the future. For now, comment them out so that we can
-- MAGIC # convert the tables to use partitioning + add filedate column.
-- MAGIC today = datetime.utcnow().date()
-- MAGIC todayYearMonth = today.strftime("%Y%m")
-- MAGIC priorMonth = get_prior_month(today)
-- MAGIC todayYearMonthRegex = "{0}*".format(todayYearMonth)
-- MAGIC priorYearMonthRegex = "{0}*".format(priorMonth)
-- MAGIC s3Bucket = get_bucket_by_region()
-- MAGIC
-- MAGIC dbutils.widgets.text("processingStartDate", today.strftime("%Y-%m-%d"), "Enter Start Date (YYYY-MM-DD)")
-- MAGIC dbutils.widgets.text("processingEndDate", today.strftime("%Y-%m-%d"), "Enter End Date (YYYY-MM-DD)")
-- MAGIC
-- MAGIC def get_parsed_date(widget_name):
-- MAGIC     """
-- MAGIC     Retrieves and parses a date from a widget. Raises an exception if parsing fails.
-- MAGIC
-- MAGIC     Args:
-- MAGIC         widget_name (str): The name of the widget to retrieve the date from.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         datetime.date: The parsed date.
-- MAGIC     """
-- MAGIC     date_input = dbutils.widgets.get(widget_name)
-- MAGIC     print(f"{widget_name} Input: {date_input}")
-- MAGIC
-- MAGIC     try:
-- MAGIC         parsed_date = datetime.strptime(date_input, "%Y-%m-%d")
-- MAGIC         final_date = parsed_date.replace(tzinfo=timezone.utc).date()
-- MAGIC     except ValueError as e:
-- MAGIC         raise ValueError(f"Invalid date format for {widget_name}! Please use YYYY-MM-DD.") from e
-- MAGIC
-- MAGIC     print(f"{widget_name}: {final_date}")
-- MAGIC     return final_date
-- MAGIC
-- MAGIC # Process start date
-- MAGIC startDate = get_parsed_date("processingStartDate")
-- MAGIC startDateYearMonth = startDate.strftime("%Y%m")
-- MAGIC startDateYearMonthRegex = f"{startDateYearMonth}*"
-- MAGIC
-- MAGIC # Process end date
-- MAGIC endDate = get_parsed_date("processingEndDate")
-- MAGIC endDateYearMonth = endDate.strftime("%Y%m")
-- MAGIC endDateYearMonthRegex = f"{endDateYearMonth}*"
-- MAGIC
-- MAGIC if startDate > endDate:
-- MAGIC     raise Exception("Start date must be before end date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import boto3, re
-- MAGIC
-- MAGIC emails = ['ian.vernon@samsara.com', "alerts-ingestion-aaaad7ndq3nzakgbegyy7z6xwi@samsara.org.slack.com", "jasper-etl-databricks-notebook-job@samsara.pagerduty.com"]
-- MAGIC
-- MAGIC runbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5467684/Runbook+Jasper+Databricks+Notebooks+Failing"
-- MAGIC # We rely on ATT to send us reports in a timely manner. To ensure that we are using the latest data, this function will grab the latest reports from S3.
-- MAGIC # It will then trigger an alert if the data is more than 2 days old or missing altogether.
-- MAGIC def alert_on_stale_s3_files(bucket, prefix, filename, suffix):
-- MAGIC     s3 = get_s3_client("samsara-att-sftp-read")
-- MAGIC
-- MAGIC     # Paginate through all S3 objects since list_objects_v2 only returns up to 1000 per request
-- MAGIC     paginator = s3.get_paginator('list_objects_v2')
-- MAGIC     pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
-- MAGIC     all_contents = []
-- MAGIC     for page in pages:
-- MAGIC         if 'Contents' in page:
-- MAGIC             all_contents.extend(page['Contents'])
-- MAGIC
-- MAGIC     # If Contents is empty, that means we didn't find any files
-- MAGIC     if not all_contents:
-- MAGIC       alert = "no_{filename}_reports_for_this_month\nRunbook: {runbook}".format(filename=filename, runbook=runbook)
-- MAGIC       df = spark.createDataFrame([bucket], "string").toDF("bucket_name")
-- MAGIC       execute_alert(df, emails, [], alert)
-- MAGIC     else:
-- MAGIC       # Sort all filenames in the month ascending
-- MAGIC       files = sorted([x['Key'] for x in all_contents], reverse=True)
-- MAGIC
-- MAGIC       # Grab first file that has the filename and suffix. Get filedate and make sure its not more than 2 days old.
-- MAGIC       # Error out if the data is stale.
-- MAGIC       for file in files:
-- MAGIC         if filename in file and file.endswith(suffix):
-- MAGIC           match = re.search('JWCC_(\d{8})', file)
-- MAGIC           if match:
-- MAGIC             latest_filedate = match.group(1)
-- MAGIC             date = datetime.strptime(latest_filedate, '%Y%m%d').date()
-- MAGIC             if (today-date).days >= 2:
-- MAGIC               alert = filename + "_reports_stale_last_file_from_" + str(date) + "\nRunbook: " + runbook
-- MAGIC               df = spark.createDataFrame([str(date)], "string").toDF("filedate")
-- MAGIC               execute_alert(df, emails, [], alert)
-- MAGIC             break
-- MAGIC           else:
-- MAGIC             alert = "no_{filename}_reports_found\nRunbook: {runbook}".format(filename=filename,runbook=runbook)
-- MAGIC             df = spark.createDataFrame([bucket], "string").toDF("bucket_name")
-- MAGIC             execute_alert(df, emails, [], alert)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC alert_on_stale_s3_files(bucket=s3Bucket, prefix="att/e03227/Inbox/JWCC_{0}".format(todayYearMonth), filename="DataUsage",  suffix=".dat")
-- MAGIC alert_on_stale_s3_files(bucket=s3Bucket, prefix="att/e03227/Inbox/JWCC_{0}".format(todayYearMonth), filename="SubsSnapshot",  suffix=".dat")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import catalog
-- MAGIC from pyspark.sql import SQLContext
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from datetime import datetime, date, timedelta
-- MAGIC from dateutil.relativedelta import relativedelta
-- MAGIC import re
-- MAGIC
-- MAGIC db_name = "dataprep_cellular"
-- MAGIC table_name_data_usage_watermark = "att_data_usage_ingestion_watermark"
-- MAGIC
-- MAGIC # Chose the start of the most recently completed billing period relative to when filtering files based on explicit date was added.
-- MAGIC seed_date = datetime(2022,9,18).strftime("%Y-%m-%d")
-- MAGIC
-- MAGIC # Returns 18th of prior month in format yyyy-mm-dd
-- MAGIC def get_18th_prior_month(arbitrary_date):
-- MAGIC     last_month = arbitrary_date + relativedelta(months=-1)
-- MAGIC     return date(last_month.year, last_month.month, 18).strftime("%Y-%m-%d")
-- MAGIC
-- MAGIC def get_current_billing_period_start_date(today):
-- MAGIC     dd_int = int(today.strftime("%d"))
-- MAGIC     mm_int = int(today.strftime("%m"))
-- MAGIC     yy_int = int(today.strftime("%Y"))
-- MAGIC
-- MAGIC     if dd_int >= 1 and dd_int <= 18:
-- MAGIC         start_date = get_18th_prior_month(today)
-- MAGIC     else:
-- MAGIC         start_date = datetime(yy_int, mm_int, 18).date().strftime("%Y-%m-%d")
-- MAGIC
-- MAGIC     return start_date
-- MAGIC
-- MAGIC
-- MAGIC def get_high_watermark_date_from_table(db_name, table_name):
-- MAGIC   # TODO - SQLContext is deprecated in Spark 3.0. We will have to migrate this. I (Ian) tried
-- MAGIC   # to figure out how to do this but the alternative method I used (catalog.listTables(db_name))
-- MAGIC   # was unbearably slow compared to using SQLContext.
-- MAGIC   sqlContext = SQLContext(spark.sparkContext)
-- MAGIC   table_names_in_db = sqlContext.tableNames(db_name)
-- MAGIC
-- MAGIC   table_exists = table_name in table_names_in_db
-- MAGIC
-- MAGIC   # If the table exists, get the max watermark date.
-- MAGIC   if table_exists:
-- MAGIC     print("{0}.{1} exists, getting max date for high watermark".format(db_name, table_name))
-- MAGIC     max_date = spark.sql("SELECT MAX(most_recent_date_processed) as maxval FROM {0}.{1}".format(db_name, table_name)).collect()[0].asDict()['maxval']
-- MAGIC   else:
-- MAGIC     # Otherwise, create the table and seed it accordingly.
-- MAGIC     print("table doesn't exist, seeding table {0}.{1} with date {2}".format(db_name, table_name, seed_date))
-- MAGIC     spark.sql("CREATE TABLE {0}.{1} (most_recent_date_processed string)".format(db_name, table_name))
-- MAGIC     spark.sql("INSERT INTO {0}.{1}  VALUES (\"".format(db_name, table_name) + format(seed_date) + "\")")
-- MAGIC     max_date = seed_date
-- MAGIC
-- MAGIC   print("{0}.{1} high watermark date: {2}".format(db_name, table_name, max_date))
-- MAGIC   return max_date
-- MAGIC
-- MAGIC def update_high_watermark(db_name, table_name,  date_str):
-- MAGIC   print("updating high watermark for {0}.{1} to: {2}".format(db_name, table_name, date_str))
-- MAGIC   spark.sql("INSERT INTO {0}.{1} VALUES (\"".format(db_name, table_name) + format(date_str) + "\")")
-- MAGIC
-- MAGIC # get_min_max_data_usage_filedates_after_start_of_current_billing_period gets the oldest file date for the
-- MAGIC # current billing period (starts on the 18th), and the date in the name of the most recent Data Usage file.
-- MAGIC def get_files_in_current_billing_period():
-- MAGIC   cutoffDateLowerBound = get_current_billing_period_start_date(startDate) # startDate defaults to today's date if empty
-- MAGIC   cutoffDateUpperBound = endDate.strftime("%Y-%m-%d")
-- MAGIC
-- MAGIC   # List all files in the S3 bucket that contain `DataUsage` and end with `.dat` greater than cutoff date
-- MAGIC   s3 = get_s3_client("samsara-att-sftp-read")
-- MAGIC   paginator = s3.get_paginator('list_objects_v2')
-- MAGIC   pages = paginator.paginate(Bucket=s3Bucket, Prefix="att/e03227/Inbox/JWCC_")
-- MAGIC
-- MAGIC   dataUsageFileNames = []
-- MAGIC
-- MAGIC   # Get keys of all files and store them lists depending upon the dates in the
-- MAGIC   # filenames.
-- MAGIC   for page in pages:
-- MAGIC       for obj in page['Contents']:
-- MAGIC           keyName = obj['Key']
-- MAGIC           if "DataUsage" in keyName and keyName.endswith(".dat"):
-- MAGIC             match = re.search(r"(\d{8})", keyName)
-- MAGIC             date = datetime.strptime(match.group(), '%Y%m%d').strftime("%Y-%m-%d")
-- MAGIC             if date > cutoffDateLowerBound and date <= cutoffDateUpperBound:
-- MAGIC               dataUsageFileNames.append(keyName)
-- MAGIC
-- MAGIC   if len(dataUsageFileNames) == 0:
-- MAGIC     print("no data usage files to ingest")
-- MAGIC     return dataUsageFileNames
-- MAGIC
-- MAGIC   # Sort so we can get the most recent file for tracking high watermark.
-- MAGIC   dataUsageFileNames.sort()
-- MAGIC
-- MAGIC   print("data usage file names: {0}\n".format(dataUsageFileNames))
-- MAGIC   return dataUsageFileNames
-- MAGIC
-- MAGIC def get_min_max_data_usage_filedates_after_start_of_current_billing_period(dataUsageFileNames):
-- MAGIC
-- MAGIC   # Get max and min dates of files.
-- MAGIC   maxDateFileName = ""
-- MAGIC   if len(dataUsageFileNames) > 0:
-- MAGIC     maxDateFileName = dataUsageFileNames[len(dataUsageFileNames) - 1]
-- MAGIC
-- MAGIC   if maxDateFileName == "":
-- MAGIC     raise Exception("no max file date extracted from sets of files")
-- MAGIC   match = re.search(r"(\d{8})", maxDateFileName)
-- MAGIC   formattedMaxDate = datetime.strptime(match.group(), '%Y%m%d')
-- MAGIC
-- MAGIC   minDateFileName = ""
-- MAGIC   if len(dataUsageFileNames) > 0:
-- MAGIC     minDateFileName = dataUsageFileNames[0]
-- MAGIC
-- MAGIC   if minDateFileName == "":
-- MAGIC     raise Exception("no min file date extracted from sets of files")
-- MAGIC   match = re.search(r"(\d{8})", minDateFileName)
-- MAGIC   formattedMinDate = datetime.strptime(match.group(), '%Y%m%d')
-- MAGIC
-- MAGIC   return formattedMinDate, formattedMaxDate
-- MAGIC
-- MAGIC # Subtract a day because the file date is the day a report was generated, vs. we want to make sure
-- MAGIC # that we are overwriting in Databricks in some cases based on `record_received_date`, which is always for
-- MAGIC # the date prior to the file date.
-- MAGIC dataUsageFileNames = get_files_in_current_billing_period()
-- MAGIC
-- MAGIC if len(dataUsageFileNames) == 0:
-- MAGIC   # No files to ingest means we are at the start of the billing period.
-- MAGIC   dbutils.notebook.exit("no files to ingest, exiting early")
-- MAGIC
-- MAGIC oldestFileDate, newestFileDate = get_min_max_data_usage_filedates_after_start_of_current_billing_period(dataUsageFileNames)
-- MAGIC if oldestFileDate == "" or newestFileDate == "":
-- MAGIC   raise Exception("no file dates extracted from sets of files")
-- MAGIC
-- MAGIC minRecordReceivedDateString = (oldestFileDate - timedelta(days=1)).strftime("%Y-%m-%d")
-- MAGIC maxRecordReceivedDateString = (newestFileDate - timedelta(days=1)).strftime("%Y-%m-%d")
-- MAGIC minFileDateString = oldestFileDate.strftime("%Y-%m-%d")
-- MAGIC maxFileDateString = newestFileDate.strftime("%Y-%m-%d")
-- MAGIC
-- MAGIC print("minimum record received date: {0}".format(minRecordReceivedDateString))
-- MAGIC print("maximum record received date: {0}".format(maxRecordReceivedDateString))
-- MAGIC print("oldest filedate: {0}".format(minFileDateString))
-- MAGIC print("newest filedate: {0}".format(maxFileDateString))
-- MAGIC data_usage_high_watermark = get_high_watermark_date_from_table(db_name, table_name_data_usage_watermark)
-- MAGIC print("most recent file date of Data Usage report ingested into data lake: {0}".format(data_usage_high_watermark))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC data_usage_spec = [
-- MAGIC     ("data_traffic_detail_id", "Integer"),
-- MAGIC     ("iccid", "AlphaNumeric"),
-- MAGIC     ("msisdn", "AlphaNumeric"),
-- MAGIC     ("imsi", "AlphaNumeric"),
-- MAGIC     ("account_id", "Integer"),
-- MAGIC     ("billable_flag", "Alpha"),
-- MAGIC     ("billing_cycle", "Integer"),
-- MAGIC     ("sim_state", "Integer"),
-- MAGIC     ("service_type", "Alpha"),
-- MAGIC     ("assigned_rate_plan_id", "Integer"),
-- MAGIC     ("assigned_rating_zone_id", "Integer"),
-- MAGIC     ("offpeak_usage_flag", "Alpha"),
-- MAGIC     ("record_received_date", "DateTime"),
-- MAGIC     ("data_usage_raw_total", "Integer"),
-- MAGIC     ("data_usage_raw_uplink", "Integer"),
-- MAGIC     ("data_usage_raw_downlink", "Integer"),
-- MAGIC     ("data_usage_rounded", "Integer"),
-- MAGIC     ("apn", "AlphaNumeric"),
-- MAGIC     ("device_ip_address", "AlphaNumeric"),
-- MAGIC     ("operator_network", "Integer"),
-- MAGIC     ("record_open_time", "DateTime"),
-- MAGIC     ("session_duration", "Integer"),
-- MAGIC     ("record_sequence_number", "Integer"),
-- MAGIC     ("chargingid", "Integer"),
-- MAGIC     ("session_close_clause", "AlphaNumeric"),
-- MAGIC     ("tap_code", "AlphaNumeric"),
-- MAGIC     ("operator_account_id", "AlphaNumeric"),
-- MAGIC     ("rate_plan_version_id", "Integer"),
-- MAGIC     ("stream_id", "Integer"),
-- MAGIC     ("cgi_cell_global_identifier", "AlphaNumeric"),
-- MAGIC     ("sai_service_area_identifier", "AlphaNumeric"),
-- MAGIC     ("rai_routing_area_identifier", "AlphaNumeric"),
-- MAGIC     ("tai_tracking_area_identity", "AlphaNumeric"),
-- MAGIC     ("ecgi_e_utran_cgi_", "AlphaNumeric"),
-- MAGIC     ("lai_location_area_identifier", "AlphaNumeric"),
-- MAGIC     ("serving_sgsn", "AlphaNumeric"),
-- MAGIC     ("call_technology_type", "Integer"),
-- MAGIC     ("_corrupt_record", "AlphaNumeric")
-- MAGIC ]
-- MAGIC
-- MAGIC def get_num_columns(filename):
-- MAGIC     df = spark.read.option("lineSep", "\n").text(filename)
-- MAGIC     df = df.head(1)
-- MAGIC     # Add 1 to the number of delimiters because num_columns = num_delimiters + 1.
-- MAGIC     return df[0].value.count('|') + 1
-- MAGIC
-- MAGIC def generate_dynamic_schema(num_columns):
-- MAGIC     # Base schema with known columns, excluding the last one
-- MAGIC     base_schema = [
-- MAGIC         (col[0], col[1]) for col in data_usage_spec[:-1]
-- MAGIC     ]  # Exclude last column
-- MAGIC     # Add placeholders for extra columns
-- MAGIC     for extra_col_idx in range(num_columns - len(base_schema)):
-- MAGIC         base_schema.append(
-- MAGIC             (f"extra_column{extra_col_idx + 1}", "AlphaNumeric")
-- MAGIC         )  # Default type: AlphaNumeric
-- MAGIC     # Add the last column (e.g., `_corrupt_record`) back to the schema
-- MAGIC     base_schema.append(data_usage_spec[-1])
-- MAGIC     return base_schema
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC """Load the Jasper data with schema."""
-- MAGIC
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC reader_options = {
-- MAGIC     "header": "false",
-- MAGIC     "sep": "|",
-- MAGIC     "timestampFormat": "yyyyMMddHHmmss",
-- MAGIC     "columnNameOfCorruptRecord": "_corrupt_record",
-- MAGIC     "mode": "FAILFAST",
-- MAGIC }
-- MAGIC
-- MAGIC reader = spark.read.format("csv").options(**reader_options)
-- MAGIC
-- MAGIC dataframes = []
-- MAGIC for dataUsageFile in dataUsageFileNames:
-- MAGIC     s3FilePath = "s3://" + s3Bucket + "/" + dataUsageFile
-- MAGIC     numColumns = get_num_columns(s3FilePath)  # Get the number of columns in the file
-- MAGIC     print(f"Processing file: {s3FilePath}, Columns: {numColumns}")
-- MAGIC     # Generate dynamic schema for the current file
-- MAGIC     current_schema = spec_to_schema(generate_dynamic_schema(numColumns))
-- MAGIC     # Read the file with the dynamically generated schema
-- MAGIC     file_data = reader.schema(current_schema).load(s3FilePath)
-- MAGIC     # Identify and drop extra columns dynamically
-- MAGIC     extra_columns = [col for col in file_data.columns if col.startswith("extra_column")]
-- MAGIC     file_data = file_data.drop(*extra_columns)
-- MAGIC
-- MAGIC     file_data = (
-- MAGIC         file_data
-- MAGIC         .withColumn("filename", F.col("_metadata.file_path"))  # F.lit(s3FilePath) or F.input_file_name()
-- MAGIC         .withColumn(
-- MAGIC             "filedate",  # Extract and format the file date
-- MAGIC             F.to_date(F.regexp_extract("filename", r"JWCC_(\d{8})", 1), "yyyyMMdd"),
-- MAGIC         )
-- MAGIC     )
-- MAGIC     # Append the processed DataFrame to the list
-- MAGIC     dataframes.append(file_data)
-- MAGIC
-- MAGIC # Combine all individual DataFrames into a single DataFrame
-- MAGIC if dataframes:
-- MAGIC     att_csv_data = dataframes[0]
-- MAGIC     for df in dataframes[1:]:
-- MAGIC         att_csv_data = att_csv_data.union(df)
-- MAGIC else:
-- MAGIC     att_csv_data = spark.createDataFrame([], spec_to_schema(data_usage_spec))  # Empty DataFrame with default schema
-- MAGIC
-- MAGIC att_csv_data.createOrReplaceTempView("att_csv_data")
-- MAGIC


-- COMMAND ----------

CREATE
or REPLACE TEMP VIEW no_corrupt_rows as (
  SELECT
    *
  FROM
    att_csv_data
  WHERE
    _corrupt_record IS NULL
);
CREATE
OR REPLACE TEMP VIEW billing_month as (
  SELECT
    DATE_ADD(
      DATE_TRUNC(
        'month',
        DATE(record_received_date) - INTERVAL 18 days
      ),
      18
    ) as billing_month,
    *
  FROM
    no_corrupt_rows
);
CREATE
OR REPLACE TEMP VIEW deduped_sessions AS (
  SELECT
    DISTINCT billing_month,
    data_traffic_detail_id,
    iccid,
    tap_code,
    data_usage_raw_total,
    record_received_date,
    assigned_rate_plan_id,
    call_technology_type,
    operator_network,
    record_open_time
  FROM
    billing_month
  WHERE
    tap_code IS NOT NULL
    AND data_usage_raw_total IS NOT NULL
    AND record_received_date IS NOT NULL
    AND assigned_rate_plan_id IS NOT NULL
    AND iccid IS NOT NULL
    AND data_traffic_detail_id IS NOT NULL
    AND call_technology_type IS NOT NULL
    AND operator_network IS NOT NULL
);
CREATE
or replace temp view att_daily_usage as (
  SELECT
    billing_month,
    iccid,
    tap_code,
    call_technology_type,
    operator_network,
    SUM(data_usage_raw_total) AS data_usage,
    DATE(record_received_date) AS record_received_date,
    DATE(record_open_time) AS record_open_date
  FROM
    deduped_sessions
  GROUP BY
    billing_month,
    iccid,
    tap_code,
    call_technology_type,
    operator_network,
    DATE(record_received_date),
    DATE(record_open_time)
);
CREATE
OR REPLACE TEMP VIEW att_rate_plan AS (
  SELECT
    billing_month,
    iccid,
    MAX((record_received_date, assigned_rate_plan_id)).assigned_rate_plan_id AS assigned_rate_plan_id
  FROM
    deduped_sessions
  GROUP BY
    billing_month,
    iccid
);

-- COMMAND ----------

-- When upgrading the runtime away from 6.4.x-scala2.11, we found that without caching these tables, the creation of `dataprep_cellular.att_daily_usage`
-- would fail. We aren't sure why this is the case, but if it works, it works; we didn't want to sink too much time into figuring out errors deep in
-- the runtime layer.
CACHE TABLE att_csv_data;
CACHE TABLE no_corrupt_rows;
CACHE TABLE billing_month;
CACHE TABLE deduped_sessions;
CACHE TABLE att_daily_usage;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_cellular.att_daily_usage USING delta PARTITIONED BY (record_received_date) AS
SELECT
  *
FROM
  att_daily_usage
LIMIT
  0;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Use pyspark SQL to do delta overwrites to avoid having to rewrite
-- MAGIC # the entire table each time this notebook is ran. We go 28 days
-- MAGIC # in the past so that we are sure we only overwrite data that is for
-- MAGIC # sure in the past two months since we filter source data only
-- MAGIC # from the past two months. Since the shortest month (February) can
-- MAGIC # have at most 28 days, we want to avoid the situation where we
-- MAGIC # are at the start of March, and we go further than February first,
-- MAGIC # (i.e., we overwrite data in January that we haven't fetched from
-- MAGIC # S3).
-- MAGIC
-- MAGIC print("updating records where record_received_date between {0} and {1} (inclusive)".format(minRecordReceivedDateString, maxRecordReceivedDateString))
-- MAGIC spark.table("att_daily_usage") \
-- MAGIC     .filter(f"record_received_date >= '{minRecordReceivedDateString}' AND record_received_date <= '{maxRecordReceivedDateString}'") \
-- MAGIC     .write.format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .partitionBy("record_received_date") \
-- MAGIC     .option("replaceWhere", f"record_received_date >= '{minRecordReceivedDateString}' AND record_received_date <= '{maxRecordReceivedDateString}'") \
-- MAGIC     .option("mergeSchema", "true") \
-- MAGIC     .saveAsTable("dataprep_cellular.att_daily_usage")

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS dataprep_cellular.att_rate_plan USING delta PARTITIONED BY (billing_month) AS
SELECT
  *
FROM
  att_rate_plan
LIMIT
  0;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Overwrite data till the most recent billing month (including all data from the most recent 18th in the past before startDate until now).
-- MAGIC start_billing_month = spark.sql("SELECT MIN(billing_month) as minval FROM att_rate_plan").collect()[0].asDict()['minval']
-- MAGIC end_billing_month = spark.sql("SELECT MAX(billing_month) as maxval FROM att_rate_plan").collect()[0].asDict()['maxval']
-- MAGIC
-- MAGIC print(f"Updating records where billing_month between {start_billing_month} and {end_billing_month}")
-- MAGIC
-- MAGIC # start_billing_month is not inclusive since 18th would have a previous month billing date
-- MAGIC spark.table("att_rate_plan") \
-- MAGIC     .filter(f"billing_month > '{start_billing_month}' AND billing_month <= '{end_billing_month}'") \
-- MAGIC     .write.format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .partitionBy("billing_month") \
-- MAGIC     .option("replaceWhere", f"billing_month > '{start_billing_month}' AND billing_month <= '{end_billing_month}'") \
-- MAGIC     .option("mergeSchema", "true") \
-- MAGIC     .saveAsTable("dataprep_cellular.att_rate_plan")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC subscriber_snapshot_spec = [
-- MAGIC     ("iccid", "AlphaNumeric"),
-- MAGIC     ("msisdn", "AlphaNumeric"),
-- MAGIC     ("imsi", "AlphaNumeric"),
-- MAGIC     ("account_id", "Integer"),
-- MAGIC     ("sim_state", "Integer"),
-- MAGIC     ("assigned_rate_plan_id", "Integer"),
-- MAGIC     ("customer", "AlphaNumeric"),
-- MAGIC     ("device_id", "AlphaNumeric"),
-- MAGIC     ("modem_id", "AlphaNumeric"),
-- MAGIC     ("communication_plan_id", "Integer"),
-- MAGIC     ("initial_activation_date", "DateTime"),
-- MAGIC     ("custom1", "AlphaNumeric"),
-- MAGIC     ("custom2", "AlphaNumeric"),
-- MAGIC     ("custom3", "AlphaNumeric"),
-- MAGIC     ("line_payment_status", "AlphaNumeric"),
-- MAGIC     ("operator_account_id", "AlphaNumeric"),
-- MAGIC     ("custom4", "AlphaNumeric"),
-- MAGIC     ("custom5", "AlphaNumeric"),
-- MAGIC     ("custom6", "AlphaNumeric"),
-- MAGIC     ("custom7", "AlphaNumeric"),
-- MAGIC     ("custom8", "AlphaNumeric"),
-- MAGIC     ("custom9", "AlphaNumeric"),
-- MAGIC     ("custom10", "AlphaNumeric")
-- MAGIC ]
-- MAGIC
-- MAGIC """
-- MAGIC Generates a list of year-month strings (YYYYMM) for a range of months between start_date and end_date.
-- MAGIC
-- MAGIC Args:
-- MAGIC     start_date (date): The start date for the range.
-- MAGIC     end_date (date): The end date for the range.
-- MAGIC
-- MAGIC Returns:
-- MAGIC     list: A list of strings representing each year-month in the format YYYYMM.
-- MAGIC """
-- MAGIC def generate_date_range(start_date, end_date):
-- MAGIC     date_range = []
-- MAGIC     current_date = start_date
-- MAGIC
-- MAGIC     while current_date <= end_date:
-- MAGIC         year_month = current_date.strftime("%Y%m")
-- MAGIC         date_range.append(year_month)
-- MAGIC         current_date = current_date.replace(day=1) + relativedelta(months=1)
-- MAGIC
-- MAGIC     return date_range
-- MAGIC
-- MAGIC # Use pyspark SQL to do delta overwrites to avoid having to rewrite
-- MAGIC # the entire table each time this notebook is ran. We go 28 days
-- MAGIC # in the past so that we are sure we only overwrite data that is for
-- MAGIC # sure in the past two months since we filter source data only
-- MAGIC # from the past two months.
-- MAGIC
-- MAGIC LOOKBACK_DAYS = 28
-- MAGIC # Default scenario: current and prior month
-- MAGIC if (startDate is None and endDate is None) or (startDate == today and endDate == today):
-- MAGIC     path_current = "s3://{0}/att/e03227/Inbox/JWCC_{1}_SubsSnapshot*.dat".format(s3Bucket, todayYearMonthRegex)
-- MAGIC     path_prior = "s3://{0}/att/e03227/Inbox/JWCC_{1}_SubsSnapshot*.dat".format(s3Bucket, priorYearMonthRegex)
-- MAGIC     paths = f"{path_prior},{path_current}"
-- MAGIC     fallback_path = path_prior
-- MAGIC     is_backfill = False
-- MAGIC     filedate_filter_condition = f"filedate >= date_sub(current_date(), {LOOKBACK_DAYS})"
-- MAGIC else: # Backfilling scenario
-- MAGIC     date_range = generate_date_range(startDate, endDate)
-- MAGIC     paths = ",".join([f"s3://{s3Bucket}/att/e03227/Inbox/JWCC_{ym}*_SubsSnapshot*.dat" for ym in date_range])
-- MAGIC     fallback_path = None
-- MAGIC     is_backfill = True
-- MAGIC     startDateStr = startDate.strftime("%Y-%m-%d")
-- MAGIC     endDateStr = endDate.strftime("%Y-%m-%d")
-- MAGIC     filedate_filter_condition = f"filedate >= '{startDateStr}' AND filedate <= '{endDateStr}'"
-- MAGIC
-- MAGIC print("Loading data from files matching the following months: {0}".format(paths))
-- MAGIC print(f"Using the following filter condition: {filedate_filter_condition}")
-- MAGIC
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC reader_options = {
-- MAGIC     "header": "false",
-- MAGIC     "sep": "|",
-- MAGIC     "timestampFormat": "yyyyMMddHHmmss",
-- MAGIC     "columnNameOfCorruptRecord": "_corrupt_record",
-- MAGIC     "mode": "PERMISSIVE",
-- MAGIC }
-- MAGIC
-- MAGIC reader = spark.read.format("csv").options(**reader_options)
-- MAGIC
-- MAGIC try:
-- MAGIC   att_subscriber_snapshot_data = (
-- MAGIC     reader.schema(spec_to_schema(subscriber_snapshot_spec))
-- MAGIC     .load(paths.split(','))
-- MAGIC     .withColumn("filename", F.input_file_name())
-- MAGIC     .withColumn(
-- MAGIC         "filedate",
-- MAGIC         F.to_date(F.regexp_extract("filename", r"JWCC_(\d{8})", 1), "yyyyMMdd"),
-- MAGIC     )
-- MAGIC )
-- MAGIC # The above load attempt will fail on the first day of each month since there are not files for the new month
-- MAGIC # for part of the day. Try again with just the prior month's path instead.
-- MAGIC except Exception as e:
-- MAGIC     # Only fallback if not backfilling
-- MAGIC     if not is_backfill and fallback_path:
-- MAGIC         print("[WARN] Failed to load combined paths. Trying fallback with prior month only.")
-- MAGIC         att_subscriber_snapshot_data = (
-- MAGIC             reader.schema(spec_to_schema(subscriber_snapshot_spec))
-- MAGIC             .load(fallback_path)
-- MAGIC             .withColumn("filename", F.input_file_name())
-- MAGIC             .withColumn(
-- MAGIC                 "filedate",
-- MAGIC                 F.to_date(F.regexp_extract("filename", r"JWCC_(\d{8})", 1), "yyyyMMdd"),
-- MAGIC             )
-- MAGIC         )
-- MAGIC     else:
-- MAGIC         # If backfilling or no fallback available, re-raise the error
-- MAGIC         raise
-- MAGIC
-- MAGIC att_subscriber_snapshot_data.createOrReplaceTempView("att_subscriber_snapshot_data")

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW att_all_rate_plans AS (
  SELECT
    filedate,
    iccid,
    MAX((filedate, assigned_rate_plan_id)).assigned_rate_plan_id AS assigned_rate_plan_id
  FROM
    att_subscriber_snapshot_data
  GROUP BY
    filedate,
    iccid
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_cellular.att_all_rate_plans USING delta PARTITIONED BY (filedate) AS
SELECT
  *
FROM
  att_all_rate_plans
LIMIT
  0;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.table("att_all_rate_plans") \
-- MAGIC     .filter(filedate_filter_condition) \
-- MAGIC     .write.format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .partitionBy("filedate") \
-- MAGIC     .option("replaceWhere", filedate_filter_condition) \
-- MAGIC     .option("mergeSchema", "true") \
-- MAGIC     .saveAsTable("dataprep_cellular.att_all_rate_plans")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_cellular.att_subscriber_snapshot USING delta PARTITIONED BY (filedate) AS
SELECT
  *
FROM
  att_subscriber_snapshot_data
LIMIT
  0;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.table("att_subscriber_snapshot_data") \
-- MAGIC     .filter(filedate_filter_condition) \
-- MAGIC     .write.format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .partitionBy("filedate") \
-- MAGIC     .option("replaceWhere", filedate_filter_condition) \
-- MAGIC     .option("mergeSchema", "true") \
-- MAGIC     .saveAsTable("dataprep_cellular.att_subscriber_snapshot")

-- COMMAND ----------

-- We store the raw session-level data last because this operation takes the longest out of all operations.
-- It also isn't used in the downstream jasper_algo job, so if this step fails, that job will still be able
-- to process data in other trusted tables appended to above.
CREATE TABLE IF NOT EXISTS dataprep_cellular.att_raw_data USING delta PARTITIONED BY (filedate) AS
SELECT
  *
FROM
  no_corrupt_rows
LIMIT
  0;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC print(f"Replacing data where filedate is between {minFileDateString} and {maxFileDateString}")
-- MAGIC spark.table("no_corrupt_rows") \
-- MAGIC     .filter(f"filedate >= '{minFileDateString}' AND filedate <= '{maxFileDateString}'") \
-- MAGIC     .write.format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .partitionBy("filedate") \
-- MAGIC     .option("replaceWhere", f"filedate >= '{minFileDateString}' AND filedate <= '{maxFileDateString}'") \
-- MAGIC     .option("mergeSchema", "true") \
-- MAGIC     .saveAsTable("dataprep_cellular.att_raw_data")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Now that we have inserted all of the data from the reports listed above, we can set the watermark to the most recently uploaded file.
-- MAGIC data_usage_high_watermark_dt = datetime.strptime(data_usage_high_watermark, "%Y-%m-%d")
-- MAGIC max_file_date_dt = datetime.strptime(maxFileDateString, "%Y-%m-%d")
-- MAGIC print("data_usage_high_watermark_dt: {0}, max_file_date_dt: {1}".format(data_usage_high_watermark_dt, max_file_date_dt))
-- MAGIC
-- MAGIC # Update only when the latest file is newer than the current high watermark (ignore back-filling)
-- MAGIC if data_usage_high_watermark_dt < max_file_date_dt:
-- MAGIC     print("Updating data usage high watermark to {0}".format(maxFileDateString))
-- MAGIC     update_high_watermark("dataprep_cellular", "att_data_usage_ingestion_watermark", maxFileDateString)
