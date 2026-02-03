# MAGIC %run /backend/platformops/aws

# COMMAND ----------

# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run /backend/platformops/orderhistory/classes

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from datetime import datetime
from typing import List, Set

import pandas as pd
from pyspark.sql import SparkSession
import pytz

NOTEBOOK_NAME = dbutils.widgets.get("NOTEBOOK_NAME")
EXCHANGE_HISTORY_VIEW = dbutils.widgets.get("EXCHANGE_HISTORY_VIEW")
LATEST_ROW_SYNCED_KEY = dbutils.widgets.get("LATEST_ROW_SYNCED_KEY")

SQS_URLS = {
    EU_AWS_REGION: "https://sqs.eu-west-1.amazonaws.com/947526550707/samsara_exchangeingestionworker_input_queue",
    US_AWS_REGION: "https://sqs.us-west-2.amazonaws.com/781204942244/samsara_exchangeingestionworker_input_queue",
}

S3_BUCKET = "platops-databricks-metadata"
S3_BUCKETS = {
    EU_AWS_REGION: f"samsara-eu-{S3_BUCKET}",
    US_AWS_REGION: f"samsara-{S3_BUCKET}",
}

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
spark.sql("SET spark.sql.files.ignoreMissingFiles=true")
CURRENT_REGION = get_current_aws_region()
s3 = get_s3_client("samsara-platops-databricks-metadata-readwrite")
sqs = get_sqs_client("netsuiteprocessor-sqs")

SERIAL_CHAR_LENGTH = 12


def get_latest_row_synced_time() -> datetime:
    latest_synced_time_df = spark.sql(
        f"SELECT COALESCE(max(_fivetran_synced), timestamp(0)) as latest_sync_time FROM {EXCHANGE_HISTORY_VIEW}"
    )
    return pytz.utc.localize(latest_synced_time_df.toPandas()["latest_sync_time"][0])


def update_latest_synced_exchange_history_time(func):
    def wrapper(*args, **kwargs):
        out = func(*args, **kwargs)
        run_exchange_numbers = (
            kwargs["run_exchange_numbers"] if "run_exchange_numbers" in kwargs else []
        )
        run_sam_numbers = (
            kwargs["run_sam_numbers"] if "run_sam_numbers" in kwargs else []
        )
        # If run_exchange_numbers or run_sam_numbers are passed in, do not update synced times
        if run_exchange_numbers is not None and len(run_exchange_numbers) > 0:
            return out
        if run_sam_numbers is not None and len(run_sam_numbers) > 0:
            return out
        latest_synced_exchange_history_time = get_latest_row_synced_time()
        put_latest_row_synced_time(
            s3,
            latest_synced_exchange_history_time,
            S3_BUCKETS[CURRENT_REGION],
            f"exchange_history/{LATEST_ROW_SYNCED_KEY}",
        )
        return out

    return wrapper


def get_exchange_history_df(
    run_entire_backfill: bool,
    run_exchange_numbers: List[str],
    run_sam_numbers: List[str],
) -> pd.DataFrame:
    q = f"""
    SELECT
        transaction_id as exchange_id,
        transaction_type,
        status,
        transaction_line_id as line_id,
        return_number as exchange_number,
        order_number,
        sam_number,
        sku_product_returned as item_sku,
        name_product_returned as item_name,
        sku_product_received,
        name_product_received,
        initial_quantity as item_quantity,

        COALESCE(serial_number, return_serial_list_returned) as serial_number, -- Pull from list if inventory_numbers is null. This could be a single serial, or list depending on which col we pull from.

        delivery_carrier,
        shipping_contact,
        shipping_contact_email_address,
        requested_by_email,
        exchange_created_at,
        ship_city,
        ship_country,
        ship_state,
        ship_zip,
        ship_address_line_1,
        ship_address_line_2,
        ship_address_line_3,
        _fivetran_synced
    FROM {EXCHANGE_HISTORY_VIEW} as exchange_history

    -- Filter out orgs that are not in the current region.
    -- Orgs not in the current region will not be in the cloud
    -- tables in databricks.
    WHERE exchange_history.sam_number IN (
        SELECT DISTINCT sfdc_accounts.sam_number
        FROM clouddb.org_sfdc_accounts as org_sfdc_accounts
        INNER JOIN clouddb.organizations as organizations
            ON organizations.id = org_sfdc_accounts.org_id
        INNER JOIN clouddb.sfdc_accounts as sfdc_accounts
            ON sfdc_accounts.id = org_sfdc_accounts.sfdc_account_id
    )
    """
    if run_exchange_numbers is not None and len(run_exchange_numbers) > 0:
        exchange_number_list_string = list(
            map(lambda exchange_number: f"'{exchange_number}'", run_exchange_numbers)
        )
        joined_exchange_numbers = ", ".join(exchange_number_list_string)
        q += f" AND return_number IN ({joined_exchange_numbers})"
    elif run_sam_numbers is not None and len(run_sam_numbers) > 0:
        sam_number_list_string = list(
            map(lambda sam_number: f"'{sam_number}'", run_sam_numbers)
        )
        joined_sam_numbers = ", ".join(sam_number_list_string)
        q += f" AND sam_number IN ({joined_sam_numbers})"
    elif not run_entire_backfill:
        previous_run_latest_sync_time = get_previous_run_latest_row_synced_time(
            s3, S3_BUCKETS[CURRENT_REGION], f"exchange_history/{LATEST_ROW_SYNCED_KEY}"
        )
        q += f" AND _fivetran_synced >= TIMESTAMP('{previous_run_latest_sync_time}')"

    return spark.sql(q).toPandas()


def get_exchange_history(
    run_entire_backfill: bool = False,
    run_exchange_numbers: List[str] = [],
    run_sam_numbers: List[str] = [],
) -> [Exchange]:
    df = get_exchange_history_df(
        run_entire_backfill, run_exchange_numbers, run_sam_numbers
    )
    return exchange_df_to_exchange_list(df)


def exchange_df_to_exchange_list(df: pd.DataFrame) -> [Exchange]:
    exchanges_df = (
        df[
            [
                "exchange_id",
                "exchange_number",
                "sam_number",
                "delivery_carrier",
                "requested_by_email",
                "status",
                "exchange_created_at",
                "shipping_contact",
                "shipping_contact_email_address",
                "ship_city",
                "ship_country",
                "ship_state",
                "ship_zip",
                "ship_address_line_1",
                "ship_address_line_2",
                "ship_address_line_3",
                "line_id",
            ]
        ]
        .drop_duplicates(["exchange_id"])
        .set_index("exchange_id", drop=False)
    )

    exchange_ids = (
        exchanges_df[["exchange_id", "line_id"]]
        .set_index("exchange_id")
        .groupby(["exchange_id"])["line_id"]
        .apply(list)
    )

    lines_df = (
        df[
            [
                "exchange_id",
                "line_id",
                "exchange_number",
                "item_sku",
                "item_name",
                "item_quantity",
            ]
        ]
        .drop_duplicates(["exchange_id", "line_id"])
        .set_index(["exchange_id", "line_id"], drop=False)
    )

    exchange_line_ids = (
        lines_df[["exchange_id", "line_id"]]
        .set_index(["exchange_id"])
        .groupby(["exchange_id"])["line_id"]
        .apply(list)
    )

    serial_numbers_df = (
        df[["exchange_id", "line_id", "serial_number"]]
        .dropna(subset=["serial_number"])
        .drop_duplicates()
    )
    line_serial_numbers = serial_numbers_df.groupby(["exchange_id", "line_id"])[
        "serial_number"
    ].apply(list)

    exchange_ids = list(exchanges_df.index.values)

    exchanges: List[Exchange] = []
    for exchange_id in exchange_ids:
        exchange_row = exchanges_df.loc[exchange_id]
        if exchange_row is None:
            continue

        exchange_key = exchange_id

        line_ids: List[int] = []
        if exchange_key in exchange_line_ids:
            line_ids = exchange_line_ids.loc[exchange_key]

        product_details: List[ProductDetail] = []
        for line_id in line_ids:
            line_key = (exchange_id, line_id)
            line_row = lines_df.loc[line_key]
            if line_row is None:
                continue

            serial_numbers: List[Serial] = []
            serial_set: Set[str] = set()
            if line_key in line_serial_numbers:
                for serial_list in line_serial_numbers[line_key]:
                    for serial in serial_list.split():
                        if (
                            serial is not None
                            # filter invalid manual strings
                            and len(serial) == SERIAL_CHAR_LENGTH
                            and serial not in serial_set
                        ):
                            serial_numbers.append(Serial(serial))
                            serial_set.add(serial)

            product_details.append(
                ProductDetail(
                    sku=line_row["item_sku"],
                    product_name=line_row["item_name"],
                    quantity=line_row["item_quantity"],
                    serials=serial_numbers,
                )
            )

        shipping_group = ShippingGroup(
            delivery_status="",  # Delivery status not relevant on Exchanges since we cannot track incoming shipments. exchange_status is relevant for this purpose.
            shipping_address=Address(
                address_line_1=exchange_row["ship_address_line_1"],
                address_line_2=exchange_row["ship_address_line_2"],
                address_line_3=exchange_row["ship_address_line_3"],
                city=exchange_row["ship_city"],
                country=exchange_row["ship_country"],
                state=exchange_row["ship_state"],
                zip=exchange_row["ship_zip"],
            ),
            shipping_email=exchange_row["shipping_contact_email_address"],
            shipping_tracking_links=[],
            product_details=product_details,
        )

        exchanges.append(
            Exchange(
                netsuite_transaction_id=exchange_id,
                exchange_number=exchange_row["exchange_number"],
                sam_number=exchange_row["sam_number"],
                exchange_status=exchange_row["status"],
                shipping_groups=[
                    shipping_group
                ],  # An exchange will only have one shipping group.
                created_at=exchange_row["exchange_created_at"],
            )
        )
    return exchanges


@update_latest_synced_exchange_history_time
@log_function_duration(f"databricks.{NOTEBOOK_NAME}.notebook_run_time")
def main(*, run_entire_backfill=False, run_exchange_numbers=[], run_sam_numbers=[]):
    exchange_history = get_exchange_history(
        run_entire_backfill, run_exchange_numbers, run_sam_numbers
    )
    tags = [f"run_entire_backfill:{run_entire_backfill}"]
    if run_exchange_numbers is not None and len(run_exchange_numbers) > 0:
        run_exchange_numbers_joined = ",".join(run_exchange_numbers)
        tags.append(f"run_exchange_numbers:{run_exchange_numbers_joined}")
    elif run_sam_numbers is not None and len(run_sam_numbers) > 0:
        run_sam_numbers_joined = ",".join(run_sam_numbers)
        tags.append(f"run_sam_numbers:{run_sam_numbers_joined}")
    log_datadog_metric(f"databricks.{NOTEBOOK_NAME}.count", len(exchange_history), tags)
    send_records_to_sqs(exchange_history, SQS_URLS[CURRENT_REGION])
