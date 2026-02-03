# COMMAND ----------

# MAGIC %md
# MAGIC # Example of Running the Backend Crash Filter in DBX
# MAGIC In this notebook we
# MAGIC 1. Show where we've put the MLLib backend code such that it can be used in databricks
# MAGIC 1. Create a UDF to process traces and return a crash model result
# MAGIC 1. Write a query to prepare our traces for passing into the udf

# COMMAND ----------

df = spark.sql(
    """
WITH RAW as (
  SELECT
    t.date,
    t.time,
    t.org_id,
    t.object_id,
    CASE
      WHEN weights.max_weight_lbs < 6000 AND weights.max_weight_lbs > 0 THEN "passenger"
      WHEN weights.max_weight_lbs >= 6000
      AND weights.max_weight_lbs < 26000 THEN "light"
      WHEN weights.max_weight_lbs >= 26000 THEN "heavy"
      WHEN weights.max_weight_lbs IS NULL THEN "unknown"
      ELSE "unknown"
    END AS vehicle_type,
    value_list [0].proto_value.accelerometer_event.imu_harsh_event.algorithm_version AS algorithm_version,
    t.value_list [0].proto_value.accelerometer_event.event_id AS event_id,
    t.value_list [0].proto_value.accelerometer_event.harsh_accel_type AS harsh_accel_type,
    t.value_list [0].proto_value.accelerometer_event.ingestion_tag AS ingestion_tag,
    t.oriented_accel_lp_filtered_trace [0] AS oriented_accel_lp_filtered_trace,
    t.oriented_gyro_lp_filtered_trace [0] AS oriented_gyro_lp_filtered_trace,
    t.oriented_accel_raw_trace [0] AS oriented_accel_raw_trace,
    t.oriented_gyro_raw_trace [0] AS oriented_gyro_raw_trace,
    t.recent_gps_trace [0] AS recent_gps_trace
  FROM
    dataprep.osdaccelerometer_event_traces t
    LEFT JOIN productsdb.devices ON t.org_id = devices.org_id
    AND t.object_id = devices.id
    LEFT JOIN clouddb.vehicle_make_model_weights AS weights ON btrim(lcase(devices.make)) = btrim(lcase(weights.make))
    AND btrim(lcase(devices.model)) = btrim(lcase(weights.model))
  WHERE
    t.date = '2022-04-01'
    AND t.value_list [0].proto_value.accelerometer_event.ingestion_tag = 1
    AND (
      t.value_list [0].proto_value.accelerometer_event.harsh_accel_type IN (5, 28)
      OR AGGREGATE(
        TRANSFORM(
          t.value_list [0].proto_value.accelerometer_event.imu_harsh_event.secondary_events,
          x -> x.type
        ),
        FALSE,
        (acc, x) -> acc
        OR x IN (5, 28)
      )
    )
)
SELECT
  *,
  named_struct("vehicle", named_struct("type", vehicle_type)) AS metadata,
  named_struct(
    "imu_harsh_event",
    named_struct("algorithm_version", algorithm_version)
  ) AS accelerometer_event
FROM
  raw
"""
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Making code available
# MAGIC
# MAGIC 1. We have copied the `samsaraml` directory located at `/backend/python3/pkgs/samsaraml` to `/dbfs/mnt/samsara-databricks-workspace/datascience/python/apuzyk`
# MAGIC 1. We then add `/dbfs/mnt/samsara-databricks-workspace/datascience/python/apuzyk` to our python path using `sys.path.insert`

# COMMAND ----------

import sys

sys.path.insert(
    0, "/dbfs/mnt/samsara-databricks-workspace/datascience/python/apuzyk"
)  # fmt: skip
from typing import Any, Dict, List

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StructField, StructType

import samsaraml.models.SensorFusion.crash_detection as sfcd
import samsaraml.pipelines.crash_detection as pcd


# COMMAND ----------

# MAGIC %fs ls /mnt/samsara-databricks-workspace/datascience/python/apuzyk

# COMMAND ----------

# MAGIC %md
# MAGIC ## The UDF
# MAGIC
# MAGIC Some notes on the UDF
# MAGIC * We need to both add the path as well as import within the UDF.  This is because each node will have a different python path so we need to ensure the python path on the node is updated
# MAGIC * We check to ensure that we have at least 2 values in each of our arrays, this prevents arrays of `[None]` from being considered
# MAGIC * We utilize the spark schema from the BackendCrash object to ensure we're getting the correct schema back

# COMMAND ----------


return_type = sfcd.BackendCrash(sfcd.FirmwareHarshEvent()).get_spark_schema()
pipeline = pcd.get_crash_detection_pipeline(min_severity=pcd.BackendCrash.Severity.LOW)
_bc_pipeline = sc.broadcast(pipeline)


@udf(return_type)
def format_imu_data_dbx(
    metadata: Dict[str, Any],
    accelerometer_event: Dict[str, Any],
    oriented_accel_lp_filtered_trace: Dict[str, List[float]],
    oriented_gyro_lp_filtered_trace: Dict[str, List[float]],
    oriented_accel_raw_trace: Dict[str, List[float]],
    oriented_gyro_raw_trace: Dict[str, List[float]],
    recent_gps_trace: Dict[str, List[float]],
):
    sys.path.insert(
        0, "/dbfs/mnt/samsara-databricks-workspace/datascience/python/apuzyk"
    )
    import samsaraml.models.SensorFusion.crash_detection as sfcd
    import samsaraml.pipelines.datascience.crash_detection as pcd
    import samsaraml.datasets.datascience.sensor_algorithm_dataset as sad

    imu_df = {
        "unixtime_ms": oriented_accel_raw_trace["timestamp_ms"],
        "offset_from_event_ms": oriented_accel_raw_trace["offset_from_event_ms"],
        "a_x": oriented_accel_raw_trace["x_f"],
        "a_y": oriented_accel_raw_trace["y_f"],
        "a_z": oriented_accel_raw_trace["z_f"],
        "a_lp_x": oriented_accel_lp_filtered_trace["x_f"],
        "a_lp_y": oriented_accel_lp_filtered_trace["y_f"],
        "a_lp_z": oriented_accel_lp_filtered_trace["z_f"],
        "g_x": oriented_gyro_raw_trace["x_dps"],
        "g_y": oriented_gyro_raw_trace["y_dps"],
        "g_z": oriented_gyro_raw_trace["z_dps"],
        "g_lp_x": oriented_gyro_lp_filtered_trace["x_dps"],
        "g_lp_y": oriented_gyro_lp_filtered_trace["y_dps"],
        "g_lp_z": oriented_gyro_lp_filtered_trace["z_dps"],
    }
    for v in imu_df.items():
        if len(v) < 2:
            return None
    imu_df = pd.DataFrame(imu_df).fillna(0)

    gps_df = {
        "unixtime_ms": recent_gps_trace["timestamp_ms"],
        "offset_from_event_ms": recent_gps_trace["event_offset_ms"],
        "latitude_nd": recent_gps_trace["latitude"],
        "longitude_nd": recent_gps_trace["longitude"],
        "altitude_mm": recent_gps_trace["altitude"],
        "speed_milliknots": recent_gps_trace["speed"],
        "heading_md": recent_gps_trace["heading"],
        "gps_time_ms": recent_gps_trace["ts"],
    }
    for v in gps_df.values():
        if len(v) < 2:
            return None
    gps_df = pd.DataFrame(gps_df).fillna(0)
    sasd = sad.SensorAlgorithmSampleDatabricks(
        metadata, accelerometer_event, imu_df, gps_df
    )
    global _bc_pipeline
    o = _bc_pipeline.value.execute([sasd])[0].true_crash
    return None if o is None else o.to_dict()


# COMMAND ----------

# MAGIC %md
# MAGIC ## The query for input data
# MAGIC
# MAGIC * We use data available in
# MAGIC   * dataprep.osdaccelerometer_event_traces
# MAGIC   * productsdb.devices
# MAGIC   * clouddb.vehicle_make_model_weights
# MAGIC * We apply the following filters
# MAGIC   * ingestion_tag = 1: only include hev2 events
# MAGIC   * Events where the primary or a secondary event is a crash or a rollover
# MAGIC * We add values for the metadata and accelerometer event columns

# COMMAND ----------

run_date = "2022-01-01"
df = spark.sql(
    f"""
WITH RAW as (
  SELECT
    t.date,
    t.time,
    t.org_id,
    t.object_id,
    CASE
      WHEN weights.max_weight_lbs < 6000 AND weights.max_weight_lbs > 0 THEN "passenger"
      WHEN weights.max_weight_lbs >= 6000
      AND weights.max_weight_lbs < 26000 THEN "light"
      WHEN weights.max_weight_lbs >= 26000 THEN "heavy"
      WHEN weights.max_weight_lbs IS NULL THEN "unknown"
      ELSE "unknown"
    END AS vehicle_type,
    value_list [0].proto_value.accelerometer_event.imu_harsh_event.algorithm_version AS algorithm_version,
    t.value_list [0].proto_value.accelerometer_event.event_id AS event_id,
    t.value_list [0].proto_value.accelerometer_event.harsh_accel_type AS harsh_accel_type,
    t.value_list [0].proto_value.accelerometer_event.ingestion_tag AS ingestion_tag,
    t.oriented_accel_lp_filtered_trace [0] AS oriented_accel_lp_filtered_trace,
    t.oriented_gyro_lp_filtered_trace [0] AS oriented_gyro_lp_filtered_trace,
    t.oriented_accel_raw_trace [0] AS oriented_accel_raw_trace,
    t.oriented_gyro_raw_trace [0] AS oriented_gyro_raw_trace,
    t.recent_gps_trace [0] AS recent_gps_trace
  FROM
    dataprep.osdaccelerometer_event_traces t
    LEFT JOIN productsdb.devices ON t.org_id = devices.org_id
    AND t.object_id = devices.id
    LEFT JOIN clouddb.vehicle_make_model_weights AS weights ON btrim(lcase(devices.make)) =btrim(lcase(weights.make))
    AND btrim(lcase(devices.model)) = btrim(lcase(weights.model))
  WHERE
    t.date = '{run_date}'
    AND t.value_list [0].proto_value.accelerometer_event.ingestion_tag = 1
    AND (
      t.value_list [0].proto_value.accelerometer_event.harsh_accel_type IN (5, 28)
      OR AGGREGATE(
        TRANSFORM(
          t.value_list [0].proto_value.accelerometer_event.imu_harsh_event.secondary_events,
          x -> x.type
        ),
        FALSE,
        (acc, x) -> acc
        OR x IN (5, 28)
      )
    )
)
SELECT
  *,
  named_struct("vehicle", named_struct("type", vehicle_type)) AS metadata,
  named_struct(
    "imu_harsh_event",
    named_struct("algorithm_version", algorithm_version)
  ) AS accelerometer_event
FROM
  raw
"""
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding the crash column
# MAGIC Now we use everything we've written to add the crash column

# COMMAND ----------

df = df.withColumn(
    "crash",
    format_imu_data_dbx(
        df.metadata,
        df.accelerometer_event,
        df.oriented_accel_lp_filtered_trace,
        df.oriented_gyro_lp_filtered_trace,
        df.oriented_accel_raw_trace,
        df.oriented_gyro_raw_trace,
        df.recent_gps_trace,
    ),
)
df = df.select(
    df.date,
    df.time,
    df.org_id,
    df.object_id,
    df.event_id,
    df.crash,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Example use

# COMMAND ----------

df.groupBy(df.crash.severity).agg(F.count(F.lit(1))).show()
