# MAGIC %run ./invoice_functions

# COMMAND ----------

from datetime import datetime, date
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
DB_NAME = "platops." + date.today().strftime("%d%B%Y") + "InvoiceBackfill"
STAGGER_BACKFILL_IN_SECONDS = 1
INVOICE_TYPE = "invoice"
INVOICE_IDS_TO_SKIP = [2004294, 26440939]


def current_region_sams(spark, sam_numbers):
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


def get_records_in_backfill_table(spark):
    data_list = []
    try:
        select_query = """
        select sam_number, transaction_id, create_date
        from netsuite_data.netsuite_child_invoices
        where transaction_id not in
        (select netsuite_internal_id from finopsdb.netsuite_invoices_v2)
        """

        result = spark.sql(select_query)
        data_list = [
            (row.sam_number, row.transaction_id, row.create_date)
            for row in result.collect()
        ]
    except Exception as e:
        raise e
    return data_list


def get_sam_numbers_from_backfill(spark, data_list):
    return [t[0] for t in data_list]


def get_data_by_age(spark, data_list, age_in_days):
    filtered_data_list = []
    for elem in data_list:
        elem_date = elem[2]
        time_since_creation = datetime.now() - elem_date
        if time_since_creation.days > age_in_days:
            filtered_data_list.append(elem)
    return filtered_data_list


def get_invoices_to_backfill_by_sam(spark, data_list, sam_numbers_to_backfill):
    invoices_to_backfill = []
    for elem in data_list:
        sam_number = elem[0]
        if sam_number in sam_numbers_to_backfill:
            invoices_to_backfill.append(elem)
    return invoices_to_backfill


def write_results_to_temp_table(spark, table_name, invoice_data):
    # create DataFrame: When invoice created
    # fivetran_synced: When databricks got the record netsuite_data
    # s3 timestamp: we write fivetran_synced to s3.
    # if we do backfill, fivetran_synced
    df = spark.createDataFrame(
        invoice_data, ("sam_number", "netsuite_id", "created_date")
    )
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def build_backfill_table(spark):
    records_in_backfill = get_records_in_backfill_table(spark)
    all_sams_to_backfill = get_sam_numbers_from_backfill(spark, records_in_backfill)
    regional_sams = current_region_sams(spark, all_sams_to_backfill)

    sams_to_use = list(set(all_sams_to_backfill).intersection(set(regional_sams)))
    invoice_records = get_invoices_to_backfill_by_sam(
        spark, records_in_backfill, sams_to_use
    )
    if len(invoice_records) == 0:
        return []
    write_results_to_temp_table(spark, DB_NAME, invoice_records)
    return invoice_records


def get_column_from_backfill_table(spark, column_name, table_name):
    df = spark.read.table(table_name)
    column_data = df.select(["sam_number", "netsuite_id"]).collect()
    data = [(row[column_name], int(row["netsuite_id"])) for row in column_data]
    return data


def run_backfill():
    build_backfill_table(spark)
    subset_of_sams_to_run_backfill_on = get_column_from_backfill_table(
        spark, "sam_number", DB_NAME
    )
    aws_resources = AwsResources()
    for sam_number, invoice_number in subset_of_sams_to_run_backfill_on:
        if invoice_number in INVOICE_IDS_TO_SKIP:
            continue
        time.sleep(STAGGER_BACKFILL_IN_SECONDS)
        write_messages_to_sqs(
            [invoice_number],
            sam_number,
            aws_resources,
            INVOICE_TYPE,
        )
