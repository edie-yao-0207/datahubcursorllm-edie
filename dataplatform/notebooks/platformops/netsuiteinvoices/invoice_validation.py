# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------
from datetime import datetime
import time

import boto3
from botocore.exceptions import ClientError
import datadog
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import pytz

INVOICE_EXPORTS_BUCKET = "samsara-netsuite-invoice-exports"
LAST_SUCCESSFUL_INVOICE_COMPARISON_KEY = "last_successful_comparison.txt"
LAST_SUCCESSFUL_CONSOLIDATED_INVOICE_COMPARISON_KEY = (
    "last_successful_consolidated_comparison.txt"
)
INVOICE_EXPORT_KEY = dbutils.widgets.get("NETSUITE_EXPORT_S3_KEY")
INVOICE_EXPORTS_VIEW = "invoice_exports"
CALCULATED_INVOICE_DATA_VIEW = "invoice_csv_data"

CONSOLIDATED_INVOICE_EXPORT_KEY = "consolidated_invoices.csv"
CONSOLIDATED_INVOICE_EXPORTS_VIEW = "consolidated_invoice_exports"
CALCULATED_CONSOLIDATED_INVOICE_DATA_VIEW = "consolidated_invoice_csv_data"

NETSUITE_CONSOLIDATED_INVOICE_OUTPUT_PATH = (
    "s3://samsara-databricks-netsuite-invoice-output/csvs/consolidated_invoice/*.csv"
)

NETSUITE_INVOICE_OUTPUT_PATH = (
    "s3://samsara-databricks-netsuite-invoice-output/csvs/invoice/*.csv"
)

INVOICE_OUTPUT_CSV_SCHEMA = StructType(
    [
        StructField("_c0", IntegerType(), True),
        StructField("order_type", StringType(), True),
        StructField("create_date", StringType(), True),
        StructField("_fivetran_synced", StringType(), True),
        StructField("invoice_number", LongType(), True),
        StructField("transaction_id", DoubleType(), True),
        StructField("sam_number", StringType(), True),
        StructField("cost_amount", DoubleType(), True),
        StructField("shipping_amount", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("symbol", StringType(), True),
        StructField("amount_unbilled", StringType(), True),
        StructField("exchange_rate", DoubleType(), True),
        StructField("invoice_status", StringType(), True),
        StructField("due_date", StringType(), True),
        StructField("payment_status", DoubleType(), True),
        StructField("po_number", StringType(), True),
        StructField("netsuite_internal_id", DoubleType(), True),
        StructField("order_number", StringType(), True),
        StructField("billing_addressee", StringType(), True),
        StructField("billing_attention", StringType(), True),
        StructField("bill_address_line_1", StringType(), True),
        StructField("bill_address_line_2", IntegerType(), True),
        StructField("bill_city", StringType(), True),
        StructField("bill_state", StringType(), True),
        StructField("bill_zip", StringType(), True),
        StructField("bill_country", StringType(), True),
        StructField("bill_phone_number", StringType(), True),
        StructField("ship_attention", StringType(), True),
        StructField("shipping_addressee", StringType(), True),
        StructField("ship_address_line_1", StringType(), True),
        StructField("ship_address_line_2", StringType(), True),
        StructField("ship_city", StringType(), True),
        StructField("ship_state", StringType(), True),
        StructField("ship_zip", StringType(), True),
        StructField("ship_country", StringType(), True),
        StructField("ship_phone_number", StringType(), True),
        StructField("consolidated_invoice_id", StringType(), True),
        StructField("total_amount", DoubleType(), True),
    ]
)

CONSOLIDATED_INVOICE_OUTPUT_CSV_SCHEMA = StructType(
    [
        StructField("row_num", IntegerType(), True),
        StructField("consolidated_invoice_num", StringType(), True),
        StructField("amount_remaining", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("due_date", TimestampType(), True),
        StructField("create_date", TimestampType(), True),
        StructField("_fivetran_synced", TimestampType(), True),
        StructField("consolidated_invoice_id", DoubleType(), True),
        StructField("sam_number", StringType(), True),
        StructField("invoice_status", StringType(), True),
        StructField("currency_symbol", StringType(), True),
    ]
)

session = boto3.session.Session()
s3 = get_s3_client("samsara-netsuite-invoice-exports-readwrite")


def put_last_successful_comparison(key: str):
    s3.put_object(
        Body=f"{time.time()}",
        Bucket=INVOICE_EXPORTS_BUCKET,
        Key=key,
        ACL="bucket-owner-full-control",
    )


def put_csv(df: pd.DataFrame, bucket: str, key: str):
    s3.put_object(
        Body=df.to_csv().encode(),
        Bucket=bucket,
        Key=key,
        ACL="bucket-owner-full-control",
    )


def get_last_successful_comparison(key: str) -> datetime:
    resp = s3get(key)
    if resp is None:
        return None
    timestamp = float(get_s3_body_str(resp))
    return datetime.fromtimestamp(timestamp, pytz.utc)


def get_s3_body_str(s3obj: dict) -> str:
    return s3obj["Body"].read().decode("utf-8")


def s3get(file: str) -> dict:
    try:
        resp = s3.get_object(Bucket=INVOICE_EXPORTS_BUCKET, Key=file)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            print(f"s3://{INVOICE_EXPORTS_BUCKET}/{file} file not found")
            return None
        else:
            raise ex

    return resp


def get_export_last_modified(key: str) -> datetime:
    resp = s3get(key)
    if resp is None:
        return datetime.min
    return resp["LastModified"]


# the ci and invoice exports from NS have different column names.
# This will give us the correct samnumber column name for each.
def get_samnumber_col_name(view_name: str):
    # invoices col
    if view_name == INVOICE_EXPORTS_VIEW:
        return "sam"

    # consolidated invoices col
    return "samNumber"


# the ci and invoice exports from NS have different column names.
# This will give us the correct id column name for each.
def get_id_col_name(view_name: str):
    # invoices col
    if view_name == INVOICE_EXPORTS_VIEW:
        return "id"

    # consolidated invoices col
    return "ciId"


# the ci and invoice exports from NS have different column names.
# This will give us the correct column names for each.
def get_query_fields_for_view(view_name: str):
    # invoices col
    if view_name == INVOICE_EXPORTS_VIEW:
        return "raw.id,raw.sam,raw.totalAmountCents,raw.amountRemainingCents,raw.status"

    # consolidated invoices col
    return "raw.ciNumber,raw.ciId,raw.amountRemainingCents,raw.totalAmountCents,raw.dueDate,raw.samNumber,raw.paymentStatus,raw.currencySymbol"


def create_netsuite_export_temp_view(spark: SparkSession, view: str, key: str):
    # create view from netsuite export csv
    spark.sql(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW {view}_raw
        USING csv
        OPTIONS (
            path "s3://samsara-netsuite-invoice-exports/{key}",
            header "true",
            escape '"',
            inferSchema "true"
        );
    """
    )

    # limit to samnumbers in current region
    spark.sql(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW {view} AS (
            SELECT {get_query_fields_for_view(view)} FROM {view}_raw as raw
            INNER JOIN clouddb.sfdc_accounts on REPLACE(raw.{get_samnumber_col_name(view)}, '-', '') = sfdc_accounts.sam_number
        )
    """
    )


def create_csv_union_temp_view(
    spark: SparkSession, schema: StructType, path: str, view: str
):
    df = (
        spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(schema)
        .load(path)
    )

    df.createOrReplaceTempView(view)


def log_mismatched_invoices(spark: SparkSession):
    output = spark.sql(
        """
        SELECT
        i_view.sam_number,
        i_view._fivetran_synced,
        i_view.create_date,
        i_view.due_date,
        i_view.transaction_id as i_view_transaction_id,
        i_view.invoice_number,

        i_view.total_amount as i_view_total_amount,
        i_csv.total_amount as i_csv_total_amount,
        BROUND(exports.totalAmountCents / 100, 2) as exports_total_amount,

        i_view.payment_amount as i_view_payment_amount,
        i_csv.payment_amount as i_csv_payment_amount,
        BROUND(exports.totalAmountCents / 100, 2) - BROUND(
            exports.amountRemainingCents / 100,
            2
        ) as exports_payment_amount,

        i_view.invoice_status as i_view_invoice_status,
        i_csv.invoice_status as i_csv_invoice_status,
        exports.status as exports_invoice_status
        FROM
        netsuite_data.netsuite_child_invoices as i_view
        INNER JOIN invoice_csv_data as i_csv ON i_view.transaction_id = i_csv.transaction_id
        INNER JOIN invoice_exports as exports ON exports.id = CAST(i_view.transaction_id AS INT)
        WHERE
        i_csv.total_amount != BROUND(exports.totalAmountCents / 100, 2)
        OR BROUND(
            i_csv.total_amount - i_csv.payment_amount,
            2
        ) != BROUND(
            exports.amountRemainingCents / 100,
            2
        )
        OR i_csv.invoice_status != exports.status
        """
    ).toPandas()

    put_csv(output, INVOICE_EXPORTS_BUCKET, "invoice_export_diffs.csv")


def get_invoice_mismatch_ratio(spark: SparkSession) -> float:
    create_netsuite_export_temp_view(spark, INVOICE_EXPORTS_VIEW, INVOICE_EXPORT_KEY)
    create_csv_union_temp_view(
        spark,
        INVOICE_OUTPUT_CSV_SCHEMA,
        NETSUITE_INVOICE_OUTPUT_PATH,
        CALCULATED_INVOICE_DATA_VIEW,
    )

    output = spark.sql(
        f"""
    SELECT
      100 * SUM(
        IF(
          t.total_amount != BROUND(n.totalAmountCents / 100, 2) OR
          BROUND(
              t.total_amount - t.payment_amount,
              2
          ) != BROUND(
              n.amountRemainingCents / 100,
              2
          ) OR
          t.invoice_status != n.status,
          1,
          0
        )
      ) / count(*) AS mismatch_ratio
      FROM
       {INVOICE_EXPORTS_VIEW} as n
      INNER JOIN {CALCULATED_INVOICE_DATA_VIEW} as t
      ON n.id = t.transaction_id;
  """
    ).toPandas()

    return output["mismatch_ratio"][0]


def should_compare(last_comparison: datetime, last_export: datetime) -> bool:
    # if we have not run a comparison before or
    # if the invoice export has been updated since the last comparison
    return last_comparison is None or last_comparison < last_export


def compare_invoices_to_export(spark: SparkSession) -> float:
    last_comparison = get_last_successful_comparison(
        LAST_SUCCESSFUL_INVOICE_COMPARISON_KEY
    )
    last_export = get_export_last_modified(INVOICE_EXPORT_KEY)

    if not should_compare(last_comparison, last_export):
        print("skipping comparison because export has not been updated")
        print(f"invoice export last modified {last_export}")
        print(f"last successful comparison {last_comparison}")
        return 0.0

    invoice_mismatch_ratio = get_invoice_mismatch_ratio(spark)

    print(f"invoices match = {100 - invoice_mismatch_ratio}%")

    if invoice_mismatch_ratio >= 0.01:
        log_mismatched_invoices(spark)

    return invoice_mismatch_ratio


def log_mismatched_consolidated_invoices(spark: SparkSession):
    output = spark.sql(
        """
    SELECT
      views.consolidated_invoice_id,
      views.sam_number,

      BROUND(views.total_amount, 2) as views_total_amount,
      BROUND(csv.total_amount, 2) as csv_total_amount,
      BROUND(exports.totalAmountCents / 100, 2) as exports_total_amount,

      BROUND(views.amount_remaining, 2) as views_amount_remaining,
      BROUND(csv.amount_remaining, 2) as csv_amount_remaining,
      BROUND(exports.amountRemainingCents / 100, 2) as exports_amount_remaining

      FROM
       consolidated_invoice_exports as exports
      INNER JOIN consolidated_invoice_csv_data as csv
      ON exports.ciId = CAST(csv.consolidated_invoice_id AS INT)
      INNER JOIN netsuite_data.netsuite_consolidated_invoices as views
      ON views.consolidated_invoice_id = csv.consolidated_invoice_id
      WHERE
      BROUND(csv.total_amount, 2) != BROUND(exports.totalAmountCents / 100, 2) OR
      BROUND(csv.amount_remaining, 2) != BROUND(exports.amountRemainingCents / 100, 2)
       ;

    """
    ).toPandas()

    put_csv(output, INVOICE_EXPORTS_BUCKET, "consolidated_invoice_export_diffs.csv")


def get_consolidated_invoice_mismatch_ratio(spark: SparkSession) -> float:
    create_netsuite_export_temp_view(
        spark, CONSOLIDATED_INVOICE_EXPORTS_VIEW, CONSOLIDATED_INVOICE_EXPORT_KEY
    )
    create_csv_union_temp_view(
        spark,
        CONSOLIDATED_INVOICE_OUTPUT_CSV_SCHEMA,
        NETSUITE_CONSOLIDATED_INVOICE_OUTPUT_PATH,
        CALCULATED_CONSOLIDATED_INVOICE_DATA_VIEW,
    )

    output = spark.sql(
        f"""
    SELECT
      100 * SUM(
        IF(
          BROUND(t.total_amount, 2) != BROUND(n.totalAmountCents / 100, 2) OR
          BROUND(t.amount_remaining, 2) != BROUND(n.amountRemainingCents / 100, 2),
          1,
          0
        )
      ) / count(*) AS mismatch_ratio
      FROM
       {CONSOLIDATED_INVOICE_EXPORTS_VIEW} as n
      INNER JOIN {CALCULATED_CONSOLIDATED_INVOICE_DATA_VIEW} as t
      ON n.ciId = CAST(t.consolidated_invoice_id AS INT);
  """
    ).toPandas()

    return output["mismatch_ratio"][0]


def compare_consolidated_invoices_to_export(spark: SparkSession) -> float:
    last_comparison = get_last_successful_comparison(
        LAST_SUCCESSFUL_CONSOLIDATED_INVOICE_COMPARISON_KEY
    )
    last_export = get_export_last_modified(CONSOLIDATED_INVOICE_EXPORT_KEY)

    if not should_compare(last_comparison, last_export):
        print("skipping comparison because export has not been updated")
        print(f"consolidated invoice export last modified {last_export}")
        print(f"last successful comparison {last_comparison}")
        return 0.0

    ci_mismatch_ratio = get_consolidated_invoice_mismatch_ratio(spark)

    if ci_mismatch_ratio >= 0.01:
        log_mismatched_consolidated_invoices(spark)

    print(f"consolidated invoices match = {100 - ci_mismatch_ratio}%")
    return ci_mismatch_ratio


def main():
    # only runs in US b/c we only have one version of
    # the invoice export file in a US bucket
    if session.region_name != "us-west-2":
        return

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # compare_invoices_to_export will raise an assertion exception if the
    # invoice mismatch ratio is too high which will prevent the notebook from
    # writing a last successful run time.
    invoice_mismatch_percent = compare_invoices_to_export(spark)
    ci_mismatch_percent = compare_consolidated_invoices_to_export(spark)

    log_datadog_metrics(
        [
            {
                "metric": "databricks.invoice.accuracy_compared_to_netsuite",
                "points": 100 - invoice_mismatch_percent,
            },
            {
                "metric": "databricks.consolidated_invoice.accuracy_compared_to_netsuite",
                "points": 100 - ci_mismatch_percent,
            },
        ]
    )

    put_last_successful_comparison(LAST_SUCCESSFUL_INVOICE_COMPARISON_KEY)
    put_last_successful_comparison(LAST_SUCCESSFUL_CONSOLIDATED_INVOICE_COMPARISON_KEY)


if __name__ == "__main__":
    main()
