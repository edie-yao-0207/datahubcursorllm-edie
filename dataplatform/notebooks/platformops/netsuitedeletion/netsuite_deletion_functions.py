# MAGIC %run /backend/platformops/aws

# COMMAND ----------

# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/platformops/netsuitedeletion/classes

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from base64 import b64encode
from datetime import datetime
import logging
from typing import List, Optional, Union

from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
import pytz

logger = logging.getLogger(__name__)
CURRENT_REGION = get_current_aws_region()
NOTEBOOK_NAME = dbutils.widgets.get("NOTEBOOK_NAME")
DATADOG_NAMESPACE = dbutils.widgets.get("DATADOG_NAMESPACE")
NETSUITE_DELETION_VIEW = dbutils.widgets.get("NETSUITE_DELETION_VIEW")
TRANSACTIONS_VIEW = dbutils.widgets.get("TRANSACTIONS_VIEW")
LATEST_ROW_SYNCED_KEY = dbutils.widgets.get("LATEST_ROW_SYNCED_KEY")
S3_BUCKET = "platops-databricks-metadata"
EU_AWS_REGION = "eu-west-1"
US_AWS_REGION = "us-west-2"
SQS_URLS = {
    EU_AWS_REGION: "https://sqs.eu-west-1.amazonaws.com/947526550707/samsara_netsuite_record_delete_queue",
    US_AWS_REGION: "https://sqs.us-west-2.amazonaws.com/781204942244/samsara_netsuite_record_delete_queue",
}

S3_BUCKETS = {
    EU_AWS_REGION: f"samsara-eu-{S3_BUCKET}",
    US_AWS_REGION: f"samsara-{S3_BUCKET}",
}

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
s3 = get_s3_client("samsara-platops-databricks-metadata-readwrite")
sqs = get_sqs_client("netsuiteprocessor-sqs")


def get_latest_row_synced_time(view) -> datetime:
    latest_synced_time_df = spark.sql(
        f"SELECT max(_fivetran_synced) as latest_sync_time FROM {view}"
    )
    return pytz.utc.localize(latest_synced_time_df.toPandas()["latest_sync_time"][0])


def get_previous_run_latest_row_synced_time() -> datetime:
    return get_s3_timestamp(
        s3,
        S3_BUCKETS[CURRENT_REGION],
        f"netsuite_deleter_history/{LATEST_ROW_SYNCED_KEY}",
    )


def update_latest_synced_deletion_history_time(arg):
    def inner_decorator(func):
        def wrapped(*args, **kwargs):
            response = func(*args, **kwargs)
            latest_synced_deletion_history_time = get_latest_row_synced_time(arg)
            put_latest_row_synced_time(
                s3,
                latest_synced_deletion_history_time,
                S3_BUCKETS[CURRENT_REGION],
                f"netsuite_deleter_history/{LATEST_ROW_SYNCED_KEY}",
            )
            return response

        return wrapped

    return inner_decorator


@update_latest_synced_deletion_history_time(TRANSACTIONS_VIEW)
def get_netsuite_deletion_data(run_entire_backfill):
    q = f"""SELECT
            transaction_id,
            sam_number,
            _fivetran_deleted,
            date_deleted
        FROM {NETSUITE_DELETION_VIEW}
        """

    if not run_entire_backfill:
        previous_run_latest_sync_time = get_previous_run_latest_row_synced_time()
        if previous_run_latest_sync_time:
            q += f"WHERE _fivetran_synced >= TIMESTAMP('{previous_run_latest_sync_time}')"

    df = spark.sql(q).toPandas()
    df["transaction_id"] = df["transaction_id"].fillna(0).astype("int64")
    df["date_deleted"] = df["date_deleted"].fillna("").astype("str")
    df["sam_number"] = df["sam_number"].fillna("").astype("str")

    deleted_records = df.to_dict("records")
    serialized_records = [
        DeletedRecord(
            transaction_id=r["transaction_id"],
            date_deleted=r["date_deleted"],
            sam_number=r["sam_number"],
        )
        for r in deleted_records
    ]
    return serialized_records


@log_function_duration(f"databricks.{NOTEBOOK_NAME}.notebook_run_time")
def main(*, run_entire_backfill=False):
    netsuite_transactions = get_netsuite_deletion_data(run_entire_backfill)
    tags = [f"run_entire_backfill:{run_entire_backfill}"]
    log_datadog_metric(f"{DATADOG_NAMESPACE}.count", len(netsuite_transactions), tags)
    send_records_to_sqs(netsuite_transactions, SQS_URLS[CURRENT_REGION])
