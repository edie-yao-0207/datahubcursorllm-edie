# Databricks notebook source
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

SAMSARA_ML_PATH = (
    "/dbfs/mnt/samsara-databricks-workspace/datascience/python"  # fmt: skip
)
sys.path.insert(0, SAMSARA_ML_PATH)  # fmt: skip

import pandas as pd
import pyspark.sql.functions as F
import samsaraml.models.SensorFusion.crash_detection as sfcd
import samsaraml.pipelines.datascience.crash_detection as pcd
from pyspark.sql.types import ArrayType, DoubleType, LongType, StructField, StructType

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

return_type = sfcd.BackendCrash(sfcd.FirmwareHarshEvent()).get_spark_schema()
pipeline = pcd.get_crash_detection_pipeline(min_severity=sfcd.BackendCrash.Severity.LOW)
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
    sys.path.insert(0, SAMSARA_ML_PATH)
    import samsaraml.datasets.datascience.sensor_algorithm_dataset as sad
    import samsaraml.models.SensorFusion.crash_detection as sfcd
    import samsaraml.pipelines.datascience.crash_detection as pcd

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
    t.date BETWEEN '{START_DATE}' AND '{END_DATE}'
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
df.write.partitionBy("date").mode("overwrite").option(
    "replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'"
).saveAsTable("datascience.daily_crash_filter_predicitons")
