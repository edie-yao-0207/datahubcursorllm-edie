# Databricks notebook source
# MAGIC %md
# MAGIC Salesforce Data

# COMMAND ----------

from pyspark.sql.functions import (
    regexp_replace,
    split,
    explode,
    upper,
    col,
    row_number,
    when,
    max,
    coalesce,
)
from pyspark.sql.window import Window

# Pull in data from salesforce opportunities and combine with accounts to get SAM number
df = spark.sql(
    f"""select all_serial_numbers, SAM_Number_Undecorated__c as sam, account_id, warranty_exchange_reason, opportunity_type as type, date(opportunity_created_date) as created_date, opportunity_created_date as created_datetime, zendesk_ticket_number, return_products_to, return_reason, return_subreason, notes
from edw.salesforce_sterling.opportunity opp
left join sfdc_data.sfdc_accounts acc on acc.id = opp.account_id
where all_serial_numbers is not null
and (opportunity_type in ('Beta Program', 'Exchange', 'Warranty Exchange', 'Free Trial Return', 'Remorse Refund', 'Unplanned Return', 'Delinquent') or return_label_sent_at is not null)
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
    "serial", explode("all_serial_numbers")
)  # Create a new row for each serial number

# Filter by the correct serial number format
df = df.filter(
    (
        df.serial.rlike("^[A-Za-z0-9]{4}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$")
        | df.serial.rlike("^[A-Za-z0-9]{12}$")
        | df.serial.rlike("^[A-Za-z0-9]{11}$")
        | df.serial.rlike("^[A-Za-z0-9]{10}$")
    )
)  # only keep entries that look like serial numbers
df = df.withColumn(
    "serial_number", upper(regexp_replace(df.serial, "-", ""))
)  # make them all uppercase and remove dashes
df = df.drop("all_serial_numbers", "serial")

# COMMAND ----------

# Null out meaningless warranty exchange reasons
df = df.withColumn(
    "warranty_exchange_reason",
    when(col("warranty_exchange_reason").like("%DEVICE SERIAL%"), None).otherwise(
        col("warranty_exchange_reason")
    ),
)
df = df.withColumn(
    "warranty_exchange_reason",
    when(
        col("warranty_exchange_reason").like("%Email eliot.chang@samsara.com%"), None
    ).otherwise(col("warranty_exchange_reason")),
)

df = df.drop_duplicates()

# COMMAND ----------

# Filter out duplicates for the same serial + sam combo (for example, multiple RMAs made on accident or deliquents)
window = Window.partitionBy("serial_number", "sam")
# Add a new column 'max_date' with the maximum date over the window
df = df.withColumn("max_date", max("created_datetime").over(window))
# Keep only the rows where 'date' equals 'max_date'
df = df.filter(col("created_datetime") == col("max_date"))
# Drop the 'max_date' and 'datetime' columns
df = df.drop("max_date", "created_datetime")

# COMMAND ----------

# MAGIC %md
# MAGIC Netsuite Data

# COMMAND ----------

# Starting with entries with serial numbers in the return_serial_list. This is from netsuite transactions of type "Return Authorization"
df2 = spark.sql(
    f"""select distinct return_serial_list, exchange_created_at, date(exchange_created_at) as exchange_date, warranty_exchange_reason_c, return_type, sam_number from hardware.netsuite_rmas where return_serial_list is not null and sku_product_returned like 'HW%' and return_stage != 'Return Cancelled'"""
)

# Clean up list of serial numbers and split into separate rows
df2 = df2.withColumn(
    "return_serial_list", regexp_replace("return_serial_list", "\s+", "\n")
)  # Replace spaces with newline characters
df2 = df2.withColumn(
    "return_serial_list", regexp_replace("return_serial_list", ",", "\n")
)  # Replace commas with newline characters
df2 = df2.withColumn(
    "return_serial_list", split("return_serial_list", "\n")
)  # Split the string into an array of strings
df2 = df2.withColumn(
    "serial", explode("return_serial_list")
)  # Create a new row for each serial number

# Filter by the correct format
df2 = df2.filter(
    (
        df2.serial.rlike("^[A-Za-z0-9]{4}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$")
        | df2.serial.rlike("^[A-Za-z0-9]{12}$")
        | df2.serial.rlike("^[A-Za-z0-9]{11}$")
        | df2.serial.rlike("^[A-Za-z0-9]{10}$")
    )
)  # only keep entries that look like serial numbers
df2 = df2.withColumn(
    "serial", upper(regexp_replace(df2.serial, "-", ""))
)  # make them all uppercase and remove dashes
df2 = df2.drop("return_serial_list")

# Make sure we just have one entry per serial + sam combo
window_spec = Window.partitionBy("serial", "sam_number").orderBy(
    col("exchange_created_at").desc()
)
df2 = (
    df2.withColumn("row_number", row_number().over(window_spec))
    .filter(col("row_number") == 1)
    .drop("row_number")
)


# COMMAND ----------

# Next get entries in the serial_number column which are pulled from netsuite transactions of type "Item Receipt".
df3 = spark.sql(
    f"""select distinct serial_number as serial, last_modified_date, date(last_modified_date) as returned_date, date(exchange_created_at) as exchange_date, warranty_exchange_reason_c, return_type, sam_number from hardware.netsuite_rmas where serial_number is not null and sku_product_returned like 'HW%' and return_stage != 'Return Cancelled'"""
)

# Clean up and filter serial column
df3 = df3.withColumn("serial", upper(regexp_replace(df3.serial, "-", "")))
df3 = df3.filter(
    (
        df3.serial.rlike("^[A-Za-z0-9]{4}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$")
        | df3.serial.rlike("^[A-Za-z0-9]{12}$")
        | df3.serial.rlike("^[A-Za-z0-9]{11}$")
        | df3.serial.rlike("^[A-Za-z0-9]{10}$")
    )
)  # only keep entries that look like serial numbers

# COMMAND ----------

# Join the two tables from netsuite together, coalesce columns and remove duplicates
df4 = df2.join(
    df3,
    (df2.serial == df3.serial)
    & (df2.return_type == df3.return_type)
    & (df2.sam_number == df3.sam_number)
    & (df2.exchange_date == df3.exchange_date),
    how="full",
)
df4 = df4.withColumn("serial_number", coalesce(df2.serial, df3.serial))
df4 = df4.withColumn("type", coalesce(df2.return_type, df3.return_type))
df4 = df4.withColumn("sam", coalesce(df2.sam_number, df3.sam_number))
df4 = df4.withColumn("exchanged_date", coalesce(df2.exchange_date, df3.exchange_date))
df4 = df4.withColumn(
    "warranty_exchange_reason",
    coalesce(df2.warranty_exchange_reason_c, df3.warranty_exchange_reason_c),
)
df4 = df4.drop(
    "serial",
    "return_type",
    "warranty_exchange_reason_c",
    "sam_number",
    "exchange_date",
    "exchange_created_at",
)
df4 = df4.drop_duplicates()

# COMMAND ----------

# Filter out null 'Warranty Exchange Reasons' when there is a non-null option

# First let's make the useless warranty_exchange_reasons null so we don't keep them
df4 = df4.withColumn(
    "warranty_exchange_reason",
    when(col("warranty_exchange_reason").like("%DEVICE SERIAL%"), None).otherwise(
        col("warranty_exchange_reason")
    ),
)
df4 = df4.withColumn(
    "warranty_exchange_reason",
    when(
        col("warranty_exchange_reason").like("%Email eliot.chang@samsara.com%"), None
    ).otherwise(col("warranty_exchange_reason")),
)

# Define a window, one per serial + sam combo
window = Window.partitionBy("serial_number", "sam")

# Create a temporary column "warranty_exchange_reason_max" with the maximum non-null and non-empty value of "warranty_exchange_reason" for each serial, sam, type combo
df4 = df4.withColumn(
    "warranty_exchange_reason_max",
    max(
        when(
            col("warranty_exchange_reason").isNotNull()
            & (col("warranty_exchange_reason") != ""),
            col("warranty_exchange_reason"),
        )
    ).over(window),
)

# Drop the rows where "warranty_exchange_reason" is null or empty and "warranty_exchange_reason_max" is not null
df4 = df4.filter(
    ~(
        (
            col("warranty_exchange_reason").isNull()
            | (col("warranty_exchange_reason") == "")
        )
        & (col("warranty_exchange_reason_max").isNotNull())
    )
)

# Filter out duplicates for the same serial, type, sam combo (for example, multiple RMAs made on accident) by taking the max by exchanged_date and return_date
df4 = df4.withColumn("max_date", max("exchanged_date").over(window))
df4 = df4.filter(col("exchanged_date") == col("max_date"))
df4 = df4.drop("max_date")

df4 = df4.withColumn("max_date", max("returned_date").over(window))
df4 = df4.filter(
    (col("returned_date") == col("max_date")) | col("returned_date").isNull()
)
df4 = df4.drop("max_date")

# Drop the temporary columns
df4 = df4.drop("warranty_exchange_reason_max")

# COMMAND ----------

# Join the data from Saleforce and Netsuite together and coalesce columns

df_joined = df.join(
    df4,
    (df.serial_number == df4.serial_number) & (df.sam == df4.sam),
    how="full",
)
df_joined = df_joined.withColumn(
    "serial", coalesce(df.serial_number, df4.serial_number)
)
df_joined = df_joined.withColumn("return_type", coalesce(df.type, df4.type))
df_joined = df_joined.withColumn(
    "warranty_reason",
    coalesce(df.warranty_exchange_reason, df4.warranty_exchange_reason),
)
df_joined = df_joined.withColumn(
    "rma_date", coalesce(df.created_date, df4.exchanged_date)
)
df_joined = df_joined.withColumn("sam_number", coalesce(df.sam, df4.sam))

df_joined = df_joined.drop("type", "serial_number", "warranty_exchange_reason", "sam")
df_joined = df_joined.drop_duplicates(
    subset=["serial", "return_type", "sam_number", "rma_date"]
)

df_to_write = df_joined.select(
    "serial",
    "sam_number",
    "return_type",
    "rma_date",
    "returned_date",
    "warranty_reason",
    "return_reason",
    "return_subreason",
    "notes",
)

# update column names for consistency to previous version of table
df_to_write = df_to_write.withColumnRenamed("rma_date", "date")
df_to_write = df_to_write.withColumnRenamed(
    "warranty_reason", "warranty_exchange_reason"
)


# COMMAND ----------

from delta.tables import *

existing_table = DeltaTable.forName(spark, "hardware.gateways_return_summary")
existing_table.alias("original").merge(
    df_to_write.alias("updates"),
    "original.serial = updates.serial and original.sam_number = updates.sam_number",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
