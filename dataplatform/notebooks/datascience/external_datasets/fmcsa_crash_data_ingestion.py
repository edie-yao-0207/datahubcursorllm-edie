# Databricks notebook source
# Use configurable file_name that can be passed
#  at job creation
file_name = dbutils.widgets.get("file_name")
full_path = "/dbfs/mnt/samsara-datasets/fmcsa/crash/" + file_name

db_name = "datascience"

raw_table_name = "raw_fmcsa_crash_file_ingestion"
dim_table_name = "dim_fmcsa_crash"
matched_table_name = "fmcsa_crash_matched_data"


# COMMAND ----------

from datetime import datetime
import time

import numpy as np
import pandas as pd
import pyspark.sql.functions as F

# Crash data is comma separated and encoded in ISO-8859-1
df = pd.read_csv(full_path, encoding="ISO-8859-1")
df.columns = [col.lower() for col in df.columns]
df["report_date"] = df.report_date.apply(lambda x: datetime.strptime(x, "%d-%b-%y"))

# COMMAND ----------


def y_n_parsing(x):
    if x == "Y":
        return True
    elif x == "N":
        return False
    return None


df["tow_away"] = df.tow_away.map(y_n_parsing)
df["hazmat_released"] = df.hazmat_released.map(y_n_parsing)


# COMMAND ----------


def clean_vins(vin):
    bad_vins = [str(i) * 17 for i in range(10)]
    bad_vins += [
        "UNKNOWN",
        "VINNOT10OR17",
    ]
    if vin in bad_vins or len(vin) != 17:
        return None
    return vin.upper()


df["vehicle_id_number"] = df.vehicle_id_number.map(str).map(clean_vins)

# COMMAND ----------

upload_ts = int(time.time() * 1000)
sdf = (
    spark.createDataFrame(df)
    .withColumn("file_name", F.lit(file_name))
    .withColumn("upload_ts", F.lit(upload_ts))
)

# COMMAND ----------

# Upload new data, dropping existing data from the same file
if spark._jsparkSession.catalog().tableExists(db_name, raw_table_name):
    sdf.write.format("delta").mode("overwrite").partitionBy("file_name").option(
        "replaceWhere", f"file_name = '{file_name}'"
    ).saveAsTable(f"{db_name}.{raw_table_name}")
else:
    sdf.write.format("delta").partitionBy("file_name").saveAsTable(
        f"{db_name}.{raw_table_name}"
    )

# COMMAND ----------

# Gen table to merge from latest file
# we need to ensure we're only pulling the
# latest seq_num from the file as this is
# the latest report data
dim_sdf = spark.sql(
    f"""
WITH latest_file_data AS (
  SELECT
    report_number,
    report_seq_no,
    vehicle_id_number,
    file_name,
    MAX(seq_num) AS latest_seq_num
  FROM {db_name}.{raw_table_name} fi
  WHERE
    fi.file_name = '{file_name}'
  GROUP BY
    report_number,
    report_seq_no,
    vehicle_id_number,
    file_name
)
SELECT
  fi.*
FROM {db_name}.{raw_table_name} fi
JOIN latest_file_data lfd
  ON lfd.report_number = fi.report_number
    AND lfd.report_seq_no = fi.report_seq_no
    AND lfd.vehicle_id_number = fi.vehicle_id_number
    AND lfd.file_name = fi.file_name
    AND lfd.latest_seq_num = fi.seq_num
"""
)

if spark._jsparkSession.catalog().tableExists(db_name, dim_table_name):
    dim_sdf.createOrReplaceTempView("to_merge")
    spark.sql(
        f"""
    MERGE INTO {db_name}.{dim_table_name} c
    USING to_merge fi
    ON fi.report_number = c.report_number
      AND fi.report_seq_no = c.report_seq_no
      AND fi.vehicle_id_number = c.vehicle_id_number
    WHEN MATCHED AND fi.seq_num > c.seq_num
      THEN UPDATE SET *
    WHEN NOT MATCHED
      THEN INSERT *
  """
    )
else:
    dim_sdf.write.format("delta").saveAsTable(f"{db_name}.{dim_table_name}")

# COMMAND ----------

# Generate FMCSA Matched Data
#  * Match new harsh events to devices
#  * Fuzzy match crahses to trips
#  * Fuzzy amtch harsh events to trips
#
#  Output fmcsa crash level data with matched info
matched_sdf = spark.sql(
    f"""
WITH fmcsa AS (
  SELECT
    f.report_number,
    f.report_seq_no,
    d.vin,
    f.seq_num,
    f.report_date,
    d.id AS device_id,
    d.org_id
  FROM {db_name}.{dim_table_name} f
  JOIN productsdb.devices d
    ON d.vin = f.vehicle_id_number
  WHERE f.file_name = '{file_name}'
    AND f.vehicle_id_number IS NOT NULL
),
trips AS (
SELECT
    trips.date,
    trips.device_id,
    TRUE AS had_trip_within_one_day,
    BOOL_OR(trips.date = f.report_date) AS had_trip_same_day
  from
    trips2db_shards.trips as trips
  JOIN fmcsa f
    ON f.device_id = trips.device_id
      AND (
        f.report_date = trips.date OR
        f.report_date = DATE_ADD(trips.date, -1) OR
        f.report_date = DATE_ADD(trips.date, 1)
      )
  WHERE
    trips.version = 101
  GROUP BY
    trips.date,
    trips.device_id
)
SELECT
  f.report_number,
  f.report_seq_no,
  f.vin,
  f.seq_num,
  f.report_date,
  f.device_id,
  f.org_id,
  t.had_trip_within_one_day,
  t.had_trip_same_day,
  BOOL_OR(se.date IS NOT NULL) AS had_safety_event_within_one_day,
  BOOL_OR(se.date IS NOT NULL AND se.date = f.report_date) AS safety_event_same_day,
  BOOL_OR(se.date IS NOT NULL AND se.detail_proto.accel_type = 5) AS had_crash_within_one_day,
  BOOL_OR(se.date IS NOT NULL AND se.date = f.report_date AND se.detail_proto.accel_type = 5) AS crash_same_day,
  COLLECT_LIST(
    STRUCT(
      se.date,
      se.event_ms,
      se.detail_proto.accel_type
    )
  ) AS safety_events
FROM fmcsa f
LEFT JOIN trips t
  ON t.date = f.report_date
    AND t.device_id = f.device_id
LEFT JOIN safetydb_shards.safety_events se
  ON f.report_date BETWEEN DATE_ADD(se.date, -1) AND DATE_ADD(se.date, 1)
    AND f.device_id = se.device_id
GROUP BY
  f.report_number,
  f.report_seq_no,
  f.vin,
  f.seq_num,
  f.report_date,
  f.device_id,
  f.org_id,
  t.had_trip_within_one_day,
  t.had_trip_same_day
"""
)

if spark._jsparkSession.catalog().tableExists(db_name, matched_table_name):
    matched_sdf.createOrReplaceTempView("to_upsert")
    spark.sql(
        f"""
    MERGE INTO {db_name}.{matched_table_name} a
    USING to_upsert u
    ON u.report_number = a.report_number
      AND u.report_seq_no = a.report_seq_no
      AND u.vin = a.vin
    WHEN MATCHED
      THEN UPDATE SET *
    WHEN NOT MATCHED
      THEN INSERT *
  """
    )
else:
    matched_sdf.write.format("delta").partitionBy("report_date").saveAsTable(
        f"{db_name}.{matched_table_name}"
    )
