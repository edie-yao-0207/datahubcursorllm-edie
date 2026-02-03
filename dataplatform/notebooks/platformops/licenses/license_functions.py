# MAGIC %run /backend/platformops/aws

# COMMAND ----------

# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/platformops/licenses/classes

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from datetime import date, datetime, timedelta
from typing import List

from pyspark.sql import SparkSession
import pytz

NOTEBOOK_NAME = dbutils.widgets.get("NOTEBOOK_NAME")
LICENSE_VIEW = dbutils.widgets.get("LICENSE_VIEW")
LATEST_ROW_SYNCED_KEY = dbutils.widgets.get("LATEST_ROW_SYNCED_KEY")
SQS_URLS = {
    EU_AWS_REGION: "https://sqs.eu-west-1.amazonaws.com/947526550707/samsara_licenseingestionworker_input_queue",
    US_AWS_REGION: "https://sqs.us-west-2.amazonaws.com/781204942244/samsara_licenseingestionworker_input_queue",
}

S3_BUCKET = "platops-databricks-metadata"
S3_BUCKETS = {
    EU_AWS_REGION: f"samsara-eu-{S3_BUCKET}",
    US_AWS_REGION: f"samsara-{S3_BUCKET}",
}

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
CURRENT_REGION = get_current_aws_region()
sqs = get_sqs_client("netsuiteprocessor-sqs")
s3 = get_s3_client("samsara-platops-databricks-metadata-readwrite")

DIFF_QUERY_DB_TABLE_NAME = (
    "platops." + date.today().strftime("%-d%B%Y") + "DiffBasedLicenseIngestion"
)


def get_latest_row_synced_time() -> datetime:
    latest_synced_time_df = spark.sql(
        f"SELECT max(_fivetran_synced) as latest_sync_time FROM {LICENSE_VIEW}"
    )
    return pytz.utc.localize(latest_synced_time_df.toPandas()["latest_sync_time"][0])


def update_latest_synced_license_time(func):
    def wrapper(*args, **kwargs):
        out = func(*args, **kwargs)
        run_sam_numbers = (
            kwargs["run_sam_numbers"] if "run_sam_numbers" in kwargs else []
        )
        # If run_sam_numbers are passed in, do not update synced times
        if run_sam_numbers is not None and len(run_sam_numbers) > 0:
            return out
        latest_synced_license_time = get_latest_row_synced_time()
        put_latest_row_synced_time(
            s3,
            latest_synced_license_time,
            S3_BUCKETS[CURRENT_REGION],
            f"license/{LATEST_ROW_SYNCED_KEY}",
        )
        return out

    return wrapper


def get_sanitized_data(license_records_df):
    license_records_df["sam_number"] = license_records_df["sam_number"].fillna("")
    license_records_df["netsuite_transaction_id"] = (
        license_records_df["netsuite_transaction_id"].fillna(0).astype("int64")
    )
    license_records_df["netsuite_transaction_line_id"] = (
        license_records_df["netsuite_transaction_line_id"].fillna(0).astype("int64")
    )
    license_records_df["transaction_type"] = license_records_df[
        "transaction_type"
    ].fillna("")
    license_records_df["status"] = license_records_df["status"].fillna("")
    license_records_df["created_from_id"] = (
        license_records_df["created_from_id"].fillna(0).astype("int64")
    )
    license_records_df["order_number"] = license_records_df["order_number"].fillna("")
    license_records_df["return_number"] = license_records_df["return_number"].fillna("")
    license_records_df["sku"] = license_records_df["sku"].fillna("")
    license_records_df["quantity"] = (
        license_records_df["quantity"].fillna(0).astype("int32")
    )
    license_records_df["start_date"] = (
        license_records_df["start_date"].fillna("").astype("str")
    )
    license_records_df["end_date"] = (
        license_records_df["end_date"].fillna("").astype("str")
    )

    return license_records_df


def get_license_objects(license_records_df) -> [License]:
    license_records = license_records_df.to_dict("records")
    return [
        License(
            sam_number=r["sam_number"],
            netsuite_transaction_id=r["netsuite_transaction_id"],
            netsuite_transaction_line_id=r["netsuite_transaction_line_id"],
            transaction_type=r["transaction_type"],
            status=r["status"],
            created_from_id=r["created_from_id"],
            order_number=r["order_number"],
            return_number=r["return_number"],
            sku=r["sku"],
            quantity=r["quantity"],
            start_date=r["start_date"],
            end_date=r["end_date"],
        )
        for r in license_records
    ]


def write_results_to_temp_table(table_name, data):
    if len(data) > 0:
        df = spark.createDataFrame(data)
        df.write.format("delta").option("overwriteSchema", "true").mode(
            "append"
        ).saveAsTable(table_name)


def get_licenses_using_last_run_time_query(
    run_entire_backfill: bool = False,
    run_sam_numbers: List[str] = [],
) -> [License]:
    q = f"""
    SELECT
        sku,
        sam_number,
        netsuite_transaction_id,
        netsuite_transaction_line_id,
        return_number,
        order_number,
        quantity,
        transaction_type,
        status,
        created_from_id,
        start_date,
        end_date
    FROM {LICENSE_VIEW} AS license

    -- Filter out sam numbers that are not in the current region.
    WHERE license.sam_number IN (
        SELECT DISTINCT sfdc_accounts.sam_number
        FROM clouddb.sfdc_accounts
    )
    """

    if run_sam_numbers is not None and len(run_sam_numbers) > 0:
        sam_number_list_string = list(
            map(lambda sam_number: f"'{sam_number}'", run_sam_numbers)
        )
        joined_sam_numbers = ", ".join(sam_number_list_string)
        q += f" AND sam_number IN ({joined_sam_numbers})"
    elif not run_entire_backfill:
        previous_run_latest_sync_time = get_previous_run_latest_row_synced_time(
            s3, S3_BUCKETS[CURRENT_REGION], f"license/{LATEST_ROW_SYNCED_KEY}"
        )
        q += f" AND _fivetran_synced >= TIMESTAMP('{previous_run_latest_sync_time}')"

    license_records_df = spark.sql(q).toPandas()

    sanitized_data = get_sanitized_data(license_records_df)

    return get_license_objects(sanitized_data)


def get_licenses_using_diff_query(run_sam_numbers: List[str] = []) -> [License]:
    q = f"""
    SELECT
        sku,
        sam_number,
        netsuite_transaction_id,
        netsuite_transaction_line_id,
        return_number,
        order_number,
        quantity,
        transaction_type,
        status,
        created_from_id,
        start_date,
        end_date
    FROM {LICENSE_VIEW} AS netsuite_licenses

    -- Filter out previously ingested licenses.
    WHERE netsuite_licenses.netsuite_transaction_id NOT IN (SELECT DISTINCT licenses.netsuite_transaction_id from finopsdb.licenses)

      -- Filter licenses that might be in the process of syncing via the regular job. (True discrepencies should remain unsynced in multiple tries).
    AND netsuite_licenses._fivetran_synced < TIMESTAMP('{datetime.utcnow() - timedelta(hours=6)}')

    -- Filter out sam numbers that are not in the current region.
    AND netsuite_licenses.sam_number IN (
        SELECT DISTINCT sfdc_accounts.sam_number
        FROM clouddb.sfdc_accounts
    )
    """

    if isinstance(run_sam_numbers, list) and len(run_sam_numbers) > 0:
        sam_number_list_string = list(
            map(lambda sam_number: f"'{sam_number}'", run_sam_numbers)
        )
        joined_sam_numbers = ", ".join(sam_number_list_string)
        q += f" AND sam_number IN ({joined_sam_numbers})"

    license_records_df = spark.sql(q).toPandas()

    sanitized_data = get_sanitized_data(license_records_df)

    write_results_to_temp_table(DIFF_QUERY_DB_TABLE_NAME, sanitized_data)

    return get_license_objects(sanitized_data)


def get_licenses_using_diff_query_for_missing_licenses_metric() -> [License]:
    q = f"""
    SELECT
        sku,
        sam_number,
        netsuite_transaction_id,
        netsuite_transaction_line_id,
        return_number,
        order_number,
        quantity,
        transaction_type,
        status,
        created_from_id,
        start_date,
        end_date
    FROM {LICENSE_VIEW} AS netsuite_licenses

    -- Filter out previously ingested licenses.
    WHERE netsuite_licenses.netsuite_transaction_id NOT IN (SELECT DISTINCT licenses.netsuite_transaction_id from finopsdb.licenses)

    -- Filter licenses that might be in the process of syncing via the regular job. (True discrepencies should remain unsynced in multiple tries).
    -- Using a 25hr lookback because our SLA is 24 hrs (including 1 hr of processing time buffer).
    AND netsuite_licenses._fivetran_synced < TIMESTAMP('{datetime.utcnow() - timedelta(hours=25)}')

    -- Only filter in sam numbers that are in the current region
    -- AND that have been ingested at least 15 hours ago, allowing for enough of a buffer that the diff based ingestion has had a chance to detect it and ingest it.
    AND netsuite_licenses.sam_number IN (
        SELECT DISTINCT sfdc_accounts.sam_number
        FROM clouddb.sfdc_accounts
        WHERE sfdc_accounts.created_at < TIMESTAMP('{datetime.utcnow() - timedelta(hours=13)}')
    )
    """

    license_records_df = spark.sql(q).toPandas()

    sanitized_data = get_sanitized_data(license_records_df)

    return get_license_objects(sanitized_data)


@update_latest_synced_license_time
@log_function_duration(f"databricks.{NOTEBOOK_NAME}.notebook_run_time")
def main(*, run_entire_backfill=False, run_sam_numbers=[]):
    licenses = get_licenses_using_last_run_time_query(
        run_entire_backfill, run_sam_numbers
    )
    tags = [f"run_entire_backfill:{run_entire_backfill}"]
    if run_sam_numbers is not None and len(run_sam_numbers) > 0:
        run_sam_numbers_joined = ",".join(run_sam_numbers)
        tags.append(f"run_sam_numbers:{run_sam_numbers_joined}")
    log_datadog_metric(f"databricks.{NOTEBOOK_NAME}.count", len(licenses), tags)
    send_records_to_sqs(licenses, SQS_URLS[CURRENT_REGION])


def main_diff_query_ingestion(*, run_sam_numbers=[]):
    licenses = get_licenses_using_diff_query(run_sam_numbers)
    if len(licenses) > 0:
        send_records_to_sqs(licenses, SQS_URLS[CURRENT_REGION])


def register_missing_licenses_metric():
    missing_licenses = get_licenses_using_diff_query_for_missing_licenses_metric()
    joined_netsuite_transaction_ids = ",".join(
        [str(lic.netsuite_transaction_id) for lic in missing_licenses]
    )
    log_datadog_metric(
        f"databricks.missing_licenses.count",
        len(missing_licenses),
        [f"netsuite_transaction_ids:{joined_netsuite_transaction_ids}"],
    )
