# Databricks notebook source
import numpy as np
import pandas as pd
from pyspark.sql import functions as F


# COMMAND ----------

# List of return serials from customer requests

rdf = (
    spark.sql(
        """select return_serial_list, cast(exchange_created_at as date) as date, return_type, sam_number, warranty_exchange_reason_c
from playground.rma_details
where return_serial_list is not null
and return_stage <> 'Return Cancelled' """
    )
    .select("*")
    .toPandas()
)

rdf.drop_duplicates(inplace=True)
rdf["serial"] = rdf["return_serial_list"].str.split(pat="\r|\n| |:|,")

rdf = rdf.explode("serial")
rdf = rdf.reset_index()
rdf["serial"] = rdf["serial"].str.replace("-", "")
rdf["serial"] = rdf["serial"].str.replace(",", "")
rdf["serial"] = rdf["serial"].str.strip("()")
rdf["serial"] = rdf["serial"].str.upper()

rdf = rdf.drop(["return_serial_list", "index"], axis=1)

spark_sf_table = spark.createDataFrame(rdf)
spark_sf_table.createOrReplaceTempView("sf_req_ret_data")


# COMMAND ----------

# List of actual serial numbers

adf = (
    spark.sql(
        """select serial_number, cast(last_modified_date as date) as date, return_type, sam_number, warranty_exchange_reason_c
from playground.rma_details
where serial_number is not null
and return_stage <> 'Return Cancelled' """
    )
    .select("*")
    .toPandas()
)

adf.drop_duplicates(inplace=True)
adf["serial"] = adf["serial_number"].str.split(pat="\r|\n| |:|,")

adf = adf.explode("serial")
adf = adf.reset_index()
adf["serial"] = adf["serial"].str.replace("-", "")
adf["serial"] = adf["serial"].str.replace(",", "")
adf["serial"] = adf["serial"].str.strip("()")
adf["serial"] = adf["serial"].str.upper()

adf = adf.drop(["serial_number", "index"], axis=1)

spark_sf_table = spark.createDataFrame(adf)
spark_sf_table.createOrReplaceTempView("sf_act_ret_data")


# COMMAND ----------

rdf.replace("N/A", np.nan, inplace=True)
adf.replace("N/A", np.nan, inplace=True)

df = pd.concat([rdf, adf])
df.drop_duplicates(
    subset=["return_type", "sam_number", "serial", "warranty_exchange_reason_c"],
    keep="first",
    inplace=True,
    ignore_index=True,
)
df = df.drop(df[df.serial == ""].index)

# COMMAND ----------

spf = spark.createDataFrame(df)


# COMMAND ----------

# No repeat serial, sam number pairs
spf_count = spf.groupBy("sam_number", "serial").count().filter("count == 1")
spf_singles = spf.join(spf_count, on=["sam_number", "serial"]).selectExpr(
    "date",
    "serial",
    "sam_number",
    "return_type",
    "warranty_exchange_reason_c as warranty_exchange_reason",
)

# COMMAND ----------

# Repeats without reason
# TODO: change return type to be the last, not necessarily second
spf_count = spf.groupBy("sam_number", "serial").count().filter("count > 1")
spf_repeats = spf.join(spf_count, on=["sam_number", "serial"])
spf_repeats_no_reason = (
    spf_repeats.groupBy("sam_number", "serial")
    .agg(
        F.collect_list("warranty_exchange_reason_c").alias("warranty_exchange_reason"),
        F.collect_list("return_type")[1].alias("return_type"),
        F.max("date").alias("date"),
    )
    .filter("size(warranty_exchange_reason) = 0")
    .selectExpr(
        "date",
        "serial",
        "sam_number",
        "return_type",
        "null as warranty_exchange_reason",
    )
)

# COMMAND ----------

spf_repeats_reason = (
    spf_repeats.groupBy("sam_number", "serial")
    .agg(
        F.collect_list("warranty_exchange_reason_c").alias("warranty_exchange_reason"),
        F.collect_list("return_type")[1].alias("return_type"),
        F.max("date").alias("date"),
    )
    .filter("size(warranty_exchange_reason) > 0")
    .selectExpr(
        "date",
        "serial",
        "sam_number",
        "return_type",
        "warranty_exchange_reason[0] as warranty_exchange_reason",
    )
)

# COMMAND ----------

final_write = (
    spf_singles.union(spf_repeats_no_reason).union(spf_repeats_reason).sort("date")
)
final_write.write.format("delta").partitionBy("date").saveAsTable(
    "hardware.gateways_return_summary"
)
