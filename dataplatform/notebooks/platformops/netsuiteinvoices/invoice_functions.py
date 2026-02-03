# The main function in this file is executed and run on a schedule by the
# ./invoice_runner.py file. We don't actually run main in this file because we
# want to run unit tests on these functions before calling main and if the
# tests fail we don't run anything.

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from base64 import b64encode
import concurrent.futures
from datetime import datetime
import functools
import json
import logging
from typing import List, Set

import boto3
from botocore.exceptions import ClientError
import datadog
import pandas as pd
from pyspark.sql import SparkSession
import pytz

NUM_WORKERS = 10

EU_CSV_BUCKET = "samsara-eu-databricks-netsuite-invoice-output"
US_CSV_BUCKET = "samsara-databricks-netsuite-invoice-output"
NOTEBOOK_NAME = dbutils.widgets.get("NOTEBOOK_NAME")
LATEST_ROW_SYNCED_KEY = dbutils.widgets.get("LATEST_ROW_SYNCED_KEY")
LATEST_SUCCESSFULLY_UPDATE_KEY = dbutils.widgets.get("LATEST_SUCCESSFULLY_UPDATE_KEY")
NETSUITE_CHILD_INVOICES_VIEW = dbutils.widgets.get("NETSUITE_CHILD_INVOICES_VIEW")
NETSUITE_CONSOLIDATED_INVOICES_VIEW = dbutils.widgets.get(
    "NETSUITE_CONSOLIDATED_INVOICES_VIEW"
)
CONSOLIDATED_INVOICES_MAP_VIEW = dbutils.widgets.get("CONSOLIDATED_INVOICES_MAP_VIEW")
CONSOLIDATED_INVOICE_MAP_CSV_KEY = dbutils.widgets.get(
    "CONSOLIDATED_INVOICE_MAP_CSV_KEY"
)

EU_SQS_QUEUE = "https://sqs.eu-west-1.amazonaws.com/947526550707/samsara_netsuiteinvoiceworker_input_queue"
US_SQS_QUEUE = "https://sqs.us-west-2.amazonaws.com/781204942244/samsara_netsuiteinvoiceworker_input_queue"

US_AWS_REGION = "us-west-2"
EU_AWS_REGION = "eu-west-1"

CONSOLIDATED_INVOICE_TYPE = "consolidated_invoice"
INVOICE_TYPE = "invoice"

CONSOLIDATED_INVOICE_STATUS_OPEN = "OPEN"
CONSOLIDATED_INVOICE_STATUS_PAID_IN_FULL = "PAID_IN_FULL"

INVOICE_CSV_COLUMNS = [
    "order_type",
    "create_date",
    "_fivetran_synced",
    "invoice_number",
    "transaction_id",
    "sam_number",
    "cost_amount",
    "shipping_amount",
    "tax_amount",
    "payment_amount",
    "symbol",
    "amount_unbilled",
    "exchange_rate",
    "invoice_status",
    "due_date",
    "payment_status",
    "po_number",
    "netsuite_internal_id",
    "order_number",
    "billing_addressee",
    "billing_attention",
    "bill_address_line_1",
    "bill_address_line_2",
    "bill_city",
    "bill_state",
    "bill_zip",
    "bill_country",
    "bill_phone_number",
    "ship_attention",
    "shipping_addressee",
    "ship_address_line_1",
    "ship_address_line_2",
    "ship_city",
    "ship_state",
    "ship_zip",
    "ship_country",
    "ship_phone_number",
    "consolidated_invoice_id",
    "total_amount",
    "charge_error_message",
]

CONSOLIDATED_INVOICE_CSV_COLUMNS = [
    "consolidated_invoice_num",
    "amount_remaining",
    "total_amount",
    "due_date",
    "create_date",
    "_fivetran_synced",
    "consolidated_invoice_id",
    "sam_number",
    "invoice_status",
    "currency_symbol",
    "payment_status",
    "charge_error_message",
]
CONSOLIDATED_INVOICE_MAP_CSV_COLUMNS = [
    "consolidated_invoice_id",
    "transaction_id",
    "sam_number",
]

CSV_INDEX_LABEL = "row_num"

# Set this to different levels to see different log info
LOG_LEVEL = logging.INFO
logger = logging.getLogger(__name__)

# Toggle to true if you want this notebook
# to process all invoices, not just the ones created since
# the last run.
RUN_ENTIRE_BACKFILL_FOR_ALL_SAM = False

# Set to False if you want to improve performance and just not
# send any metrics to datadog
LOG_METRICS_TO_DATADOG = True

# Populate this with a list of SAMS if you only want to process those sam numbers
ONLY_PROCESS_SAMS = []

# Set to true if you don't want to create or update PDFs for the invoice.
# This is helpful if you know the PDFs are already up to date for a given SAM number
# and you are only processing those sams via ONLY_PROCESS_SAMS
SKIP_PDF_GENERATION = False

SKIP_CONSOLIDATED_INVOICES = False

ONLY_CONSOLIDATED_INVOICES = False


def get_current_aws_region():
    return boto3.session.Session().region_name


class AwsResources:
    """
    A class to hold a set of AWS / boto3 resources you can use to interact with AWS

    Note that these resources are NOT thread safe, thus you should create a new instance of this
    class for each thread.
    """

    def __init__(self):
        self.aws_region = get_current_aws_region()
        self.s3 = get_s3_client("samsara-databricks-netsuite-invoice-output-readwrite")
        self.sqs = get_sqs_client("netsuiteprocessor-sqs")

    def get_s3_csv_bucket(self):
        return US_CSV_BUCKET if self.aws_region == US_AWS_REGION else EU_CSV_BUCKET

    def get_sqs_url(self):
        return US_SQS_QUEUE if self.aws_region == US_AWS_REGION else EU_SQS_QUEUE


def get_current_region_org_samnumbers_set(
    spark: SparkSession, sam_numbers: Set[str]
) -> Set[str]:
    # This query will only return sam_numbers that correspond to orgs in the
    # organizations table.
    # Any samnumber that is not returned can be assumed to be not part of this region.
    quoted_sams = [f"'{sam}'" for sam in sam_numbers]
    sam_numbers_in_clause = ",".join(quoted_sams)
    query = f"""
    SELECT DISTINCT sam_number
    FROM clouddb.org_sfdc_accounts as org_sfdc_accounts
    INNER JOIN clouddb.organizations as organizations ON
    organizations.id = org_sfdc_accounts.org_id
    INNER JOIN clouddb.sfdc_accounts as sfdc_accounts ON
    sfdc_accounts.id = org_sfdc_accounts.sfdc_account_id
    WHERE sfdc_accounts.sam_number IN ({sam_numbers_in_clause})
    """

    current_region_sam_numbers_list = spark.sql(query).collect()
    current_region_sam_numbers_set = {
        row["sam_number"] for row in current_region_sam_numbers_list
    }

    return current_region_sam_numbers_set


def initialize_datadog():
    if not LOG_METRICS_TO_DATADOG:
        logger.warning("Not sending any metrics to datadog")
        return

    ssm_client = get_ssm_client("standard-read-parameters-ssm")
    api_key = get_ssm_parameter(ssm_client, "DATADOG_API_KEY")
    app_key = get_ssm_parameter(ssm_client, "DATADOG_APP_KEY")
    datadog.initialize(api_key=api_key, app_key=app_key)


def should_update_start_and_end_times():
    return len(ONLY_PROCESS_SAMS) == 0 and not SKIP_PDF_GENERATION


def ms_between_times(start, end):
    total_time = end - start
    total_time_ms = total_time.total_seconds() * 1000

    return total_time_ms


def log_function_duration(metric_name: str):
    """This is meant to be used as a decorator to log function duration time

    Will log the duration of the function to datadog, in milliseconds.

    Parameters
    ----------
    metric_name : str
        The name of the metric to emit to datadog

    Usage
    ------
    @log_function_duration("my_datadog_metric")
    def foo():
        return 1 + 1
    """

    def log_function_duration_decorator(func):
        def wrapper(*args, **kwargs):
            data_dog_metric = metric_name

            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()

            if "invoice_type" in kwargs:
                invoice_type_tag = kwargs["invoice_type"]
                data_dog_metric = f"{data_dog_metric}.{invoice_type_tag}"

            send_datadog_metrics(
                [
                    {
                        "metric": data_dog_metric,
                        "points": ms_between_times(start_time, end_time),
                    }
                ]
            )
            return result

        return wrapper

    return log_function_duration_decorator


def send_datadog_metrics(metrics):
    if not LOG_METRICS_TO_DATADOG or len(metrics) == 0:
        return

    result = datadog.api.Metric.send(metrics=metrics)
    if result.get("errors") is not None:
        logger.warning(
            f"Error sending datadog metrics {[m['metric'] for m in metrics]}. "
            f"Errors are as follows: {result['errors']}'"
        )


def log_datadog_metric(metric, value):
    send_datadog_metrics([{"metric": metric, "points": value}])


def update_last_successful_run(time_started: datetime):
    # we don't want to update the last successful run time
    # if we only processed some sams

    if not should_update_start_and_end_times():
        return

    aws_resources = AwsResources()
    aws_resources.s3.put_object(
        Body=time_started.isoformat(),
        Bucket=aws_resources.get_s3_csv_bucket(),
        Key=LATEST_SUCCESSFULLY_UPDATE_KEY,
        ACL="bucket-owner-full-control",
    )
    get_previous_run_latest_row_synced_time.cache_clear()


def update_last_successful_run_time(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        out = func(*args, **kwargs)
        update_last_successful_run(start)
        return out

    return wrapper


def get_s3_body_str(s3obj: dict) -> str:
    return s3obj["Body"].read().decode("utf-8")


def s3get(key: str) -> dict:
    aws_resources = AwsResources()
    bucket = aws_resources.get_s3_csv_bucket()
    try:
        resp = aws_resources.s3.get_object(Bucket=bucket, Key=key)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            print(f"s3://{bucket}/{key} file not found")
            return None
        else:
            raise ex

    return resp


def get_latest_row_synced_time(spark: SparkSession, table: str) -> datetime:
    LATEST_SYNC_TIME = "latest_sync_time"
    latest_synced_time_df = spark.sql(
        f"SELECT max(_fivetran_synced) as {LATEST_SYNC_TIME} FROM {table}"
    )
    return latest_synced_time_df.toPandas()[LATEST_SYNC_TIME][0]


@functools.lru_cache(128)
def get_previous_run_latest_row_synced_time(invoice_type: str) -> datetime:
    resp = s3get(f"{invoice_type}/{LATEST_ROW_SYNCED_KEY}")
    if resp is None:
        return None
    timestamp = float(get_s3_body_str(resp))
    return datetime.fromtimestamp(timestamp, pytz.utc)


def put_latest_row_synced_time(invoice_type: str, d: datetime):
    aws_resources = AwsResources()
    bucket = aws_resources.get_s3_csv_bucket()
    aws_resources.s3.put_object(
        Body=f"{d.timestamp()}",
        Bucket=bucket,
        Key=f"{invoice_type}/{LATEST_ROW_SYNCED_KEY}",
        ACL="bucket-owner-full-control",
    )


def update_latest_synced_invoice_and_ci_time(func):
    def wrapper(*args, **kwargs):
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        latest_synced_invoice_time = get_latest_row_synced_time(
            spark,
            NETSUITE_CHILD_INVOICES_VIEW,
        )
        latest_synced_ci_time = get_latest_row_synced_time(
            spark, NETSUITE_CONSOLIDATED_INVOICES_VIEW
        )
        out = func(*args, **kwargs)
        put_latest_row_synced_time(INVOICE_TYPE, latest_synced_invoice_time)
        put_latest_row_synced_time(CONSOLIDATED_INVOICE_TYPE, latest_synced_ci_time)
        return out

    return wrapper


def get_consolidated_invoice_query() -> str:
    query = f"""
SELECT
    consolidated_invoice_num,
    amount_remaining,
    total_amount,
    due_date,
    create_date,
    _fivetran_synced,
    consolidated_invoice_id,
    sam_number,
    invoice_status,
    currency_symbol,
    payment_status,
    charge_error_message
FROM {NETSUITE_CONSOLIDATED_INVOICES_VIEW}
"""
    if RUN_ENTIRE_BACKFILL_FOR_ALL_SAM:
        return query

    if len(ONLY_PROCESS_SAMS):
        return limit_sams(query)

    sams_with_new_or_updated_ci_rows_query = limit_to_only_newly_created_rows(
        f"SELECT sam_number FROM {NETSUITE_CONSOLIDATED_INVOICES_VIEW}",
        CONSOLIDATED_INVOICE_TYPE,
    )
    sams_with_new_or_updated_invoice_rows_query = limit_to_only_newly_created_rows(
        f"SELECT sam_number FROM {NETSUITE_CHILD_INVOICES_VIEW}", INVOICE_TYPE
    )

    return f"{query} WHERE sam_number in ({sams_with_new_or_updated_ci_rows_query} UNION {sams_with_new_or_updated_invoice_rows_query})"


def limit_sams(query: str) -> str:
    quoted_sams = [f"'{sam}'" for sam in ONLY_PROCESS_SAMS]
    sam_numbers_in_clause = ",".join(quoted_sams)
    query += f" WHERE sam_number IN ({sam_numbers_in_clause})"
    return query


def limit_to_only_newly_created_rows(query: str, invoice_type: str) -> str:
    previous_run_latest_synced_time = get_previous_run_latest_row_synced_time(
        invoice_type
    )
    return f"{query} WHERE _fivetran_synced >= DATE('{previous_run_latest_synced_time.isoformat()}')"


@log_function_duration(
    f"databricks.{NOTEBOOK_NAME}.latency.get_invoice_data.consolidated_invoice"
)
def get_consolidated_invoice_data(spark: SparkSession) -> pd.DataFrame:
    logger.info(
        f"Getting consolidated invoice data with backfill status set to: {RUN_ENTIRE_BACKFILL_FOR_ALL_SAM}"
    )
    query = get_consolidated_invoice_query()

    try:
        df = spark.sql(query).toPandas()
    except:
        return pd.DataFrame()

    logger.debug("Completed fetching consolidated invoice data")
    return add_consolidated_status(df)


def add_consolidated_status(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["invoice_status"] = df.apply(
        lambda invoice: CONSOLIDATED_INVOICE_STATUS_PAID_IN_FULL
        if (invoice["amount_remaining"] == 0 or pd.isnull(invoice["amount_remaining"]))
        else CONSOLIDATED_INVOICE_STATUS_OPEN,
        axis=1,
    )
    return df


def get_invoice_query() -> str:
    query = f"""
SELECT
  order_type,
  create_date,
  _fivetran_synced,
  invoice_number,
  transaction_id,
  sam_number,
  cost_amount,
  shipping_amount,
  tax_amount,
  payment_amount,
  symbol,
  amount_unbilled,
  exchange_rate,
  invoice_status,
  due_date,
  payment_status,
  po_number,
  netsuite_internal_id,
  order_number,
  billing_addressee,
  billing_attention,
  bill_address_line_1,
  bill_address_line_2,
  bill_city,
  bill_state,
  bill_zip,
  bill_country,
  bill_phone_number,
  ship_attention,
  shipping_addressee,
  ship_address_line_1,
  ship_address_line_2,
  ship_city,
  ship_state,
  ship_zip,
  ship_country,
  ship_phone_number,
  consolidated_invoice_id,
  total_amount,
  charge_error_message
FROM {NETSUITE_CHILD_INVOICES_VIEW}
"""

    if RUN_ENTIRE_BACKFILL_FOR_ALL_SAM:
        return query

    if len(ONLY_PROCESS_SAMS):
        return limit_sams(query)

    sams_with_new_or_updated_rows_query = limit_to_only_newly_created_rows(
        f"SELECT sam_number FROM  {NETSUITE_CHILD_INVOICES_VIEW}", INVOICE_TYPE
    )
    return f"{query} WHERE sam_number in ({sams_with_new_or_updated_rows_query})"


@log_function_duration(f"databricks.{NOTEBOOK_NAME}.latency.get_invoice_data")
def get_invoice_data(spark: SparkSession) -> pd.DataFrame:
    logger.info(
        f"Getting invoice data with backfill status set to: {RUN_ENTIRE_BACKFILL_FOR_ALL_SAM}"
    )
    query = get_invoice_query()

    try:
        df = spark.sql(query).toPandas()
    except:
        return pd.DataFrame()

    logger.debug("Completed fetching invoice data")
    return df


# This is tiny data, so we are just going to pull all of it every time
def get_consolidated_invoices_map_query() -> str:
    return f"""
SELECT
  consolidated_invoice_id,
  transaction_id,
  sam_number
FROM {CONSOLIDATED_INVOICES_MAP_VIEW}
"""


@log_function_duration(
    f"databricks.{NOTEBOOK_NAME}.latency.get_consolidated_invoices_map_data"
)
def get_consolidated_invoices_map_data(spark: SparkSession) -> pd.DataFrame:
    logger.info(f"Getting consolidated_invoices_map data")
    query = get_consolidated_invoices_map_query()

    df = spark.sql(query).toPandas()
    logger.debug("Completed fetching consolidated_invoices_map data")
    return df


@log_function_duration(
    f"databricks.{NOTEBOOK_NAME}.latency.write_latency_metrics_to_datadog"
)
def write_latency_metrics_to_datadog(df: pd.DataFrame, invoice_type: str):
    if len(df.index) == 0:
        # if there are no invoices, there are no latency metrics to send
        return

    last_successful_run_time = get_previous_run_latest_row_synced_time(invoice_type)

    df["_fivetran_synced"] = df["_fivetran_synced"].map(pytz.utc.localize)
    df["create_date"] = df["create_date"].map(pytz.utc.localize)
    df = df[(df["_fivetran_synced"] > last_successful_run_time)]

    invoices = df[["sam_number", "_fivetran_synced", "create_date"]]
    # This is the time from when the invoice
    # was created in ns to the time fivetran synced it
    ns_to_fivetran_latencies = []
    #  This is the time from when the invoice was synced
    # from fivetran to the time that it was picked up by the notebook
    fivetran_to_databricks_latencies = []

    for i, row in invoices.iterrows():
        ns_to_fivetran_latencies.append(
            ms_between_times(row["create_date"], row["_fivetran_synced"])
        )
        fivetran_to_databricks_latencies.append(
            ms_between_times(row["_fivetran_synced"], pytz.utc.localize(datetime.now()))
        )

        if i % 1000 == 0:
            bulk_log_all_latencies(
                ns_to_fivetran_latencies, fivetran_to_databricks_latencies, invoice_type
            )
            ns_to_fivetran_latencies.clear()
            fivetran_to_databricks_latencies.clear()

    bulk_log_all_latencies(
        ns_to_fivetran_latencies, fivetran_to_databricks_latencies, invoice_type
    )


def bulk_log_all_latencies(
    ns_to_fivetran_latencies: list,
    fivetran_to_databricks_latencies: list,
    invoice_type: str,
):
    bulk_log_latencies(
        f"databricks.{NOTEBOOK_NAME}.notebook.{invoice_type}.latency.ns_to_fivetran",
        ns_to_fivetran_latencies,
    )
    bulk_log_latencies(
        f"databricks.{NOTEBOOK_NAME}.notebook.{invoice_type}.latency.fivetran_to_databricks",
        fivetran_to_databricks_latencies,
    )


def csv_columns_for_invoice_type(invoice_type: str):
    if invoice_type == CONSOLIDATED_INVOICE_TYPE:
        return CONSOLIDATED_INVOICE_CSV_COLUMNS
    return INVOICE_CSV_COLUMNS


def id_key_for_invoice_type(invoice_type: str):
    if invoice_type == CONSOLIDATED_INVOICE_TYPE:
        return "consolidated_invoice_id"
    return "netsuite_internal_id"


@log_function_duration(f"databricks.{NOTEBOOK_NAME}.latency.write_new_invoices_to_sqs")
def write_new_invoices_to_sqs(
    new_records_df: pd.DataFrame,
    sam_number: str,
    aws_resources: AwsResources,
    *,
    invoice_type: str,
):
    id_key = id_key_for_invoice_type(invoice_type)
    netsuite_internal_ids = [
        int(ns_internal_id)
        for ns_internal_id in new_records_df[id_key].unique().tolist()
    ]

    write_messages_to_sqs(
        netsuite_internal_ids, sam_number, aws_resources, invoice_type
    )


def write_messages_to_sqs(
    netsuite_internal_ids: List[int],
    sam_number: str,
    aws_resources: AwsResources,
    invoice_type: str,
):
    sqs = aws_resources.sqs

    sqs_messages = []
    for index, netsuite_internal_id in enumerate(netsuite_internal_ids):
        message_body = json.dumps(
            {
                "netsuite_internal_id": netsuite_internal_id,
                "sam_number": sam_number,
                "invoice_type": invoice_type,
            }
        )

        base_64_message_body = b64encode(message_body.encode("utf-8"))
        message = {
            "MessageBody": str(base_64_message_body, "utf-8"),
            "Id": str(index),
        }

        sqs_messages.append(message)

    # send_message_batch only lets you send 10 messages per each call
    sqs_batch_limit = 10
    for i in range(0, len(sqs_messages), sqs_batch_limit):
        sqs_messages_chunk = sqs_messages[i : i + sqs_batch_limit]

        try:
            sqs.send_message_batch(
                QueueUrl=aws_resources.get_sqs_url(), Entries=sqs_messages_chunk
            )
        except sqs.exceptions.InvalidMessageContents:
            logger.error(
                f"Message contents are invalid. Messages: {sqs_messages_chunk}"
            )


def bulk_log_latencies(metric, latencies):
    metrics = []
    for latency in latencies:
        metrics.append({"metric": metric, "points": latency})
    send_datadog_metrics(metrics)


def update_df_datatypes(df: pd.DataFrame, invoice_type: str) -> pd.DataFrame:
    df["_fivetran_synced"] = pd.to_datetime(df["_fivetran_synced"])
    df["create_date"] = pd.to_datetime(df["create_date"])
    df["due_date"] = pd.to_datetime(df["due_date"])

    return df


def get_csv_key(sam_number: str, invoice_type: str) -> str:
    return f"csvs/{invoice_type}/{sam_number}.csv"


@log_function_duration(f"databricks.{NOTEBOOK_NAME}.latency.write_df_csv_to_s3")
def write_df_csv_to_s3(
    updated_df: pd.DataFrame,
    sam_number: str,
    aws_resources: AwsResources,
    *,
    invoice_type: str,
) -> None:
    logger.debug(f"Writing updated {invoice_type} csv to s3 for SAM {sam_number}")

    aws_resources.s3.put_object(
        Body=updated_df.to_csv(
            columns=csv_columns_for_invoice_type(invoice_type),
            date_format="%Y-%m-%d %H:%m:%S.%f",
            index_label=CSV_INDEX_LABEL,
        ).encode(),
        Bucket=aws_resources.get_s3_csv_bucket(),
        Key=get_csv_key(sam_number, invoice_type),
        ACL="bucket-owner-full-control",
    )
    logger.debug(
        f"Completed writing {invoice_type} updated csv to s3 for SAM {sam_number}"
    )


def log_run_started():
    send_datadog_metrics(
        [
            {
                "metric": f"databricks.{NOTEBOOK_NAME}.notebook.run",
                "tags": ["status:started"],
                "points": 1,
            }
        ]
    )


def update_invoice_data(
    all_records_for_sam_df: pd.DataFrame,
    sam_number: str,
    invoice_type: str,
    aws_resources: AwsResources,
) -> None:
    if len(all_records_for_sam_df.index) == 0:
        logger.info(f"No {invoice_type} rows for sam: {sam_number}")
        return

    logger.debug(f"Started processing {invoice_type} for sam {sam_number}")

    write_df_csv_to_s3(
        all_records_for_sam_df, sam_number, aws_resources, invoice_type=invoice_type
    )

    last_successful_run_time = get_previous_run_latest_row_synced_time(invoice_type)
    all_records_for_sam_df["_fivetran_synced"] = all_records_for_sam_df[
        "_fivetran_synced"
    ].map(pytz.utc.localize)
    new_records_df = all_records_for_sam_df[
        (all_records_for_sam_df["_fivetran_synced"] > last_successful_run_time)
    ]
    number_of_new_records = len(new_records_df.index)
    log_datadog_metric(
        f"databricks.{NOTEBOOK_NAME}.notebook.new_{invoice_type}_count",
        number_of_new_records,
    )

    if not SKIP_PDF_GENERATION:
        records_to_send_to_sqs = all_records_for_sam_df
        if len(ONLY_PROCESS_SAMS) == 0 and not RUN_ENTIRE_BACKFILL_FOR_ALL_SAM:
            records_to_send_to_sqs = new_records_df

        write_new_invoices_to_sqs(
            records_to_send_to_sqs,
            sam_number,
            aws_resources,
            invoice_type=invoice_type,
        )

    logger.info(
        f"Finished successfully processing {invoice_type} for sam: {sam_number}"
    )


def process_invoices_for_sam(
    consolidated_invoice_df_for_sam: pd.DataFrame,
    invoice_df_for_sam: pd.DataFrame,
    sam_number: str,
) -> None:
    # boto3 resources are not thread safe, so we create a separate set
    # of AWS resources for each thread
    aws_resources = AwsResources()

    if not SKIP_CONSOLIDATED_INVOICES:
        update_invoice_data(
            consolidated_invoice_df_for_sam,
            sam_number,
            CONSOLIDATED_INVOICE_TYPE,
            aws_resources,
        )
    if not ONLY_CONSOLIDATED_INVOICES:
        update_invoice_data(
            invoice_df_for_sam,
            sam_number,
            INVOICE_TYPE,
            aws_resources,
        )


def update_ci_invoice_map(spark: SparkSession):
    aws_resources = AwsResources()

    ci_map_pd_df = get_consolidated_invoices_map_data(spark)

    aws_resources.s3.put_object(
        Body=ci_map_pd_df.to_csv(
            columns=CONSOLIDATED_INVOICE_MAP_CSV_COLUMNS, index_label=CSV_INDEX_LABEL
        ).encode(),
        Bucket=aws_resources.get_s3_csv_bucket(),
        Key=CONSOLIDATED_INVOICE_MAP_CSV_KEY,
        ACL="bucket-owner-full-control",
    )


def update_invoices(spark: SparkSession):
    number_of_records = 0

    if not ONLY_CONSOLIDATED_INVOICES:
        invoices_pd_df = get_invoice_data(spark)
        number_of_records = len(invoices_pd_df.index)
        logger.info(f"There are {number_of_records} total invoices to process")
    else:
        logger.warning("Skipping invoices since ONLY_CONSOLIDATED_INVOICES is set")

    number_of_consolidated_records = 0
    if not SKIP_CONSOLIDATED_INVOICES:
        consolidated_invoices_pd_df = get_consolidated_invoice_data(spark)
        number_of_consolidated_records = len(consolidated_invoices_pd_df.index)
        logger.info(
            f"There are {number_of_consolidated_records} total consolidated invoices to process"
        )
    else:
        logger.warning(
            "Skipping consolidated invoices since SKIP_CONSOLIDATED_INVOICES is set"
        )

    if number_of_records == 0 and number_of_consolidated_records == 0:
        return

    sams_to_update_list = []
    if not ONLY_CONSOLIDATED_INVOICES:
        sams_to_update_list = invoices_pd_df["sam_number"].unique().tolist()

    if not SKIP_CONSOLIDATED_INVOICES:
        sams_to_update_list += (
            consolidated_invoices_pd_df["sam_number"].unique().tolist()
        )
    sams_to_update_set = set(sams_to_update_list)

    current_region_org_sam_number_set = get_current_region_org_samnumbers_set(
        spark, sams_to_update_set
    )

    skipped_sams = set(sams_to_update_set) - current_region_org_sam_number_set
    if RUN_ENTIRE_BACKFILL_FOR_ALL_SAM:
        logger.warning(
            f"Skipping processing {len(skipped_sams)} SAM numbers since they are not in the current region"
        )
        logger.info(f"Processing {len(current_region_org_sam_number_set)} SAM numbers")
    else:
        logger.warning(
            f"Skipping processing the following {len(skipped_sams)} sam numbers since they are not in the current region: {skipped_sams}"
        )
        logger.info(
            f"Processing the following {len(current_region_org_sam_number_set)} SAM numbers: {current_region_org_sam_number_set}"
        )

    if SKIP_PDF_GENERATION:
        logger.warning("Skipping generating PDFs for all of these invoices")

    update_ci_invoice_map(spark)

    future_to_sam_number = {}
    # in parallel, process each sam number
    with (concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS)) as executor:
        for sam in current_region_org_sam_number_set:
            consolidated_invoices_pd_df_for_sam_number = None
            if not SKIP_CONSOLIDATED_INVOICES:
                consolidated_invoices_pd_df_for_sam_number = (
                    consolidated_invoices_pd_df[
                        consolidated_invoices_pd_df["sam_number"] == sam
                    ]
                )

            invoices_pd_df_for_sam_number = None
            if not ONLY_CONSOLIDATED_INVOICES:
                invoices_pd_df_for_sam_number = invoices_pd_df[
                    invoices_pd_df["sam_number"] == sam
                ]

            fut = executor.submit(
                process_invoices_for_sam,
                consolidated_invoices_pd_df_for_sam_number,
                invoices_pd_df_for_sam_number,
                sam,
            )
            future_to_sam_number[fut] = sam

    for future in concurrent.futures.as_completed(future_to_sam_number):
        completedSamNumber = future_to_sam_number[future]
        try:
            future.result()
        except Exception as exc:
            logger.error(
                f"Failure processing SAM {completedSamNumber}. Generated exception: {exc}"
            )

    logger.info("Uploading final metrics to datadog")
    # Write all latency metrics to SQS at the very end, so we don't slow down the actual
    # processing of data
    if not ONLY_CONSOLIDATED_INVOICES:
        write_latency_metrics_to_datadog(invoices_pd_df, INVOICE_TYPE)
    if not SKIP_CONSOLIDATED_INVOICES:
        write_latency_metrics_to_datadog(
            consolidated_invoices_pd_df,
            CONSOLIDATED_INVOICE_TYPE,
        )


def setup_logger():
    FORMAT = "%(asctime)-15s %(message)s"
    logging.basicConfig(format=FORMAT)
    logger.setLevel(LOG_LEVEL)


@update_latest_synced_invoice_and_ci_time
@update_last_successful_run_time
@log_function_duration(f"databricks.{NOTEBOOK_NAME}.notebook.time_ms")
def main() -> None:
    setup_logger()
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    initialize_datadog()
    log_run_started()

    update_invoices(spark)

    logger.info("Finished processing all invoices, updating last successful run time.")
    logger.info("Notebook complete!")
