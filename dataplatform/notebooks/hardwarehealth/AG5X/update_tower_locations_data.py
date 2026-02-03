# Databricks notebook source
# MAGIC %md
# MAGIC # Read new towers

# COMMAND ----------

from pyspark.sql.functions import explode, col
import requests
import pandas as pd
import numpy as np

# COMMAND ----------

df = spark.sql(
    f"""WITH ttab as (select id as device_id, product_id
                    FROM productsdb.devices
                    WHERE product_id in (124, 125))
                    SELECT value.proto_value.nordic_lte_debug.lte_cell_info.cell_id, 
                           value.proto_value.nordic_lte_debug.lte_cell_info.tac as area_code,
                           value.proto_value.nordic_lte_debug.lte_cell_info.mcc, 
                           value.proto_value.nordic_lte_debug.lte_cell_info.mnc
                    FROM (SELECT *
                    FROM kinesisstats.osdnordicltedebug
                    WHERE date == current_date()-1) AS gps
                    INNER JOIN ttab ON
                    ttab.device_id = gps.object_id"""
).select("*")
df = df.dropDuplicates().toPandas()
df.dropna(inplace=True)
df["radio_type"] = 4
df["lat"] = np.nan
df["long"] = np.nan

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.createOrReplaceTempView("temp_table_to_insert")
df = spark.sql(
    f"""SELECT cell_id, area_code, mcc, mnc, radio_type, lat, long from temp_table_to_insert
                    EXCEPT
                    SELECT cell_id, area_code, mcc, mnc, radio_type, null as lat, null as long from hardware.tower_locations"""
).select("*")
df = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Location of Towers

# COMMAND ----------

for index, row in df.iterrows():
    params = dict(
        key=dbutils.secrets.get(scope="meenu_creds", key="opencellid_key"),
        mcc=row["mcc"],
        mnc=row["mnc"],
        lac=row["area_code"],
        cellid=row["cell_id"],
        format="json",
    )
    r = requests.post("https://opencellid.org/cell/get", params=params)
    response = r.json()
    if "lat" in response:
        df.at[index, "lat"] = response["lat"]
        df.at[index, "long"] = response["lon"]


# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.insertInto("hardware.tower_locations")

# COMMAND ----------

"""
%sql
-- Write only the lat and long to corresponding tables

/*MERGE INTO hardware.tower_locations as h
USING temp_table_to_write as t
ON h.cell_id = t.cell_id and h.area_code = t.area_code and h.mcc = t.mcc and h.mnc = t.mnc and h.radio_type = t.radio_type
WHEN MATCHED THEN UPDATE SET h.lat = t.lat;

MERGE INTO hardware.tower_locations as h
USING temp_table_to_write as t
ON h.cell_id = t.cell_id and h.area_code = t.area_code and h.mcc = t.mcc and h.mnc = t.mnc and h.radio_type = t.radio_type
WHEN MATCHED THEN UPDATE SET h.long = t.long;*/
"""

# COMMAND ----------
