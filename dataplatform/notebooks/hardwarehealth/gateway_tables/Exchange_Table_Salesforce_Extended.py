# Databricks notebook source
from pyspark.sql.functions import (
    regexp_replace,
    split,
    explode,
    upper,
    col,
    when,
    max,
    coalesce,
)
from pyspark.sql.window import Window

# Pull in data from salesforce opportunities and combine with accounts to get SAM number
df = spark.sql(
    f"""select distinct all_serial_numbers, SAM_Number_Undecorated__c as sam_number, account_id, warranty_exchange_reason, opportunity_type as type, date(opportunity_created_date) as rma_date, opportunity_created_date as created_datetime, zendesk_ticket_number as ticket_id, opportunity_id as rma_opp
from edw.salesforce_sterling.opportunity opp
left join sfdc_data.sfdc_accounts acc on acc.id = opp.account_id
where all_serial_numbers is not null
and opportunity_type in ('Warranty Exchange', 'Exchange')
and stage_name != 'Return Cancelled' """
)

# Clean up serial number field and explode into separate rows
df = df.withColumn(
    "all_serial_numbers", regexp_replace("all_serial_numbers", "\s+", "\n")
)  # Replace spaces with newline characters
df = df.withColumn(
    "all_serial_numbers", regexp_replace("all_serial_numbers", ",", "\n")
)  # Replace commas with newline characters
df = df.withColumn(
    "all_serial_numbers", split("all_serial_numbers", "\n")
)  # Split the string into an array of strings
df = df.withColumn(
    "serial_number", explode("all_serial_numbers")
)  # Create a new row for each serial number

# Filter by the correct serial number format
df = df.filter(
    (
        df.serial_number.rlike("^[A-Za-z0-9]{4}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$")
        | df.serial_number.rlike("^[A-Za-z0-9]{12}$")
        | df.serial_number.rlike("^[A-Za-z0-9]{11}$")
        | df.serial_number.rlike("^[A-Za-z0-9]{10}$")
    )
)  # only keep entries that look like serial numbers
df = df.withColumn(
    "serial", upper(regexp_replace(df.serial_number, "-", ""))
)  # make them all uppercase and remove dashes
df = df.drop("all_serial_numbers", "serial_number")
df = df.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F

# Define a window partitioned by 'serial' and 'sam_number' and ordered by 'created_datetime' in descending order
window_spec = Window.partitionBy("serial", "sam_number").orderBy(
    F.col("created_datetime").desc()
)

# Add a row number based on the window spec and filter to keep only the first row for each partition
windowed_df = (
    df.withColumn("row_number", F.row_number().over(window_spec))  # Add row number
    .filter(
        F.col("row_number") == 1
    )  # Keep only the row with the largest created_datetime
    .drop("row_number")
)

# COMMAND ----------

tickets_df = spark.sql(
    f"""
select ticket_id, status, subject, description, cast(created_at as DATE) as ticket_created_date, custom_jira_key, custom_hardware_area_internal, custom_reason_for_return, custom_rma_priority, custom_ticket_request_area, custom_select_safety_cameras_product_area as safety_category, custom_select_mobile_apps_product_area as app_category, custom_select_gateways_assets_accessories_cables_product_area as gateway_category, custom_select_dashboard_api_product_area as dashboard_category
from edw.silver.dim_customer_issues"""
)
tickets_df = tickets_df.replace("", None)
tickets_df = tickets_df.withColumn(
    "category",
    coalesce(
        col("gateway_category"),
        col("safety_category"),
        col("app_category"),
        col("dashboard_category"),
    ),
)
tickets_df = tickets_df.drop(
    "gateway_category", "safety_category", "app_category", "dashboard_category"
)

# COMMAND ----------

all_df = windowed_df.join(tickets_df, on="ticket_id", how="left")

# COMMAND ----------

from delta.tables import *

existing_table = DeltaTable.forName(spark, "hardware.hardware_exchanges_sf")
existing_table.alias("original").merge(
    all_df.alias("updates"),
    "original.serial = updates.serial and original.sam_number = updates.sam_number",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
