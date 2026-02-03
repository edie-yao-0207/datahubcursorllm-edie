# MAGIC %run /backend/platformops/orderhistory/exchange_history_functions

# COMMAND ----------

# MAGIC %pip install deepdiff

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

from deepdiff import DeepDiff

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")


def get_manual_exchange_history_df() -> pd.DataFrame:
    q = f"""
    SELECT
        transaction_id as exchange_id,
        transaction_type,
        return_number as exchange_number,
        order_number,
        sku_product_returned as item_sku,
        name_product_returned as item_name,
        refunded_quantity as item_quantity,
        sku_product_received,
        name_product_received,
        initial_quantity,
        refunded_quantity,
        received_quantity,
        return_serial_list_returned,
        return_serial_list_received,
        return_status as status,
        tracking_number,
        return_serial_list_returned as serial_number,
        shipping_contact,
        shipping_contact_email_address,
        ship_city,
        ship_country,
        ship_state,
        ship_zip,
        ship_address_line_1,
        ship_address_line_2,
        ship_address_line_3,
        shipping_carrier as delivery_carrier,
        email,
        REPLACE(sam_number, '-', '') AS sam_number,
        requested_by_email,
        create_date as exchange_created_at,
        transaction_line_id as line_id
    FROM manual_exchanges_raw
    """

    return spark.sql(q).toPandas()


def get_all_manual_exchange_history() -> [Exchange]:
    df = get_manual_exchange_history_df()
    return exchange_df_to_exchange_list(df)


def create_manual_pull_view(csv_id: str):
    q = f"""
        CREATE OR REPLACE TEMPORARY VIEW manual_exchanges_raw
        USING csv
        OPTIONS (path "s3://{csv_id}", header "true", escape '"');
    """
    spark.sql(q)


def run_comparison():
    exchange_history = get_exchange_history(True)
    manual_exchange_history = get_all_manual_exchange_history()

    exchange_history_map = {}

    for exchange in exchange_history:
        key = exchange.exchange_number
        exchange_history_map[key] = exchange

    # Run json comparison
    off_sams = []
    match = 0
    no_match = 0

    not_in_platops_view = 0
    for manual_exchange in manual_exchange_history:
        exchange_number = exchange.exchange_number
        if exchange_number not in exchange_history_map:
            not_in_platops_view += 1
            continue
        exchange = exchange_history_map[exchange_number]

        res = DeepDiff(exchange.to_dict(), manual_exchange.to_dict(), ignore_order=True)
        if res:
            no_match += 1
            off_sams.append(exchange.sam_number)
            print("start")
            for key in res.keys():
                print(f"{key}: {res[key]}")
                print("---------------------")
            print("end")
        else:
            match += 1

    total = match + no_match
    print(f"{no_match*100/total}% mismatch")
    print(f"not_in_platops_view: {not_in_platops_view}")

    # Check if lengths are off
    manual_exchange_list_length = len(manual_exchange_history)
    exchange_list_length = len(exchange_history)

    diff = abs(manual_exchange_list_length - exchange_list_length)

    print(f"diff: {diff}")
    print(
        f"percent-ish: {100*diff/max(manual_exchange_list_length, exchange_list_length)}%"
    )


def main(manual_csv_id=False):
    create_manual_pull_view(manual_csv_id)
    run_comparison()
