from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType

SOURCE_TABLE = "dataprep.osdaccelerometer_event_traces"
WRITE_TABLE = "datascience.backend_crash_model_traces"


START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
NUM_BATCHES = int(dbutils.widgets.get("num_batches")) or 5

run_dates = [d.strftime("%Y-%m-%d") for d in pd.date_range(START_DATE, END_DATE)]

#######################################################################
# EXACT COPY FROM OUR DATASCIENCE FEATURES FOLDER IN THE BACKEND      #
# REPO, NECESSARY BECAUSE WE CAN'T USE BACKEND CODE ON NON-INTERACTIVE#
# CLUSTERS                                                            #
#######################################################################


def gen_trace_array(
    gps_trace: Optional[List[Dict[str, List[float]]]],
    accel_trace: Optional[List[Dict[str, List[float]]]],
    gyro_trace: Optional[List[Dict[str, List[float]]]],
    desired_len: int = 9000,
) -> Optional[List[List[float]]]:
    """pyspark udf that generates a trace array from from gyro, accel, and gps data

    output array is of len desired len, with edge padding if input is not long enough
    since speed is sampled at 1hz vs 100hz for imu data we repeat gps speed 100 times for
    each entry.

    final output is of the form
    [
        speed in mps,
        x_f accel in gs,
        y_f accel in gs,
        z_f accel in gs,
        x_dps gyro,
        y_dps gyro,
        z_dps gyro
    ]

    args:
        gps_trace List of dict[str, List[float]] containing gps data (speed being in miliknots), only first entry of the list is used
        accel_trace: List of dict[str, List[float]] containing accel data, only first entry of the list is used
        gyro_trace: List of dict[str, List[float]] containing gyro data, only first entry of the list is used

    returns:
        Array[Array[float]] of the data
    """
    if (
        accel_trace is None
        or gyro_trace is None
        or gps_trace is None
        or accel_trace[0] is None
        or gyro_trace[0] is None
        or gps_trace[0] is None
        or gps_trace[0]["speed"] is None
    ):
        return None
    accel_trace_dict = accel_trace[0]
    gyro_trace_dict = gyro_trace[0]
    gps_trace_dict = gps_trace[0]
    gps_out = format_gps_speed(gps_trace_dict, desired_len)
    accel = format_accel(accel_trace_dict, desired_len)
    gyro = format_gyro(gyro_trace_dict, desired_len)

    return np.concatenate([[gps_out], accel, gyro]).tolist()


def format_gps_speed(
    gps_trace: Dict[str, List[float]],
    desired_len: int,
    hz_conversion: Tuple[int, int] = (1, 100),
) -> List[float]:
    speeds = gps_trace["speed"]
    conversion_rate = hz_conversion[1] // hz_conversion[0]
    gps_out_initial_len = min(desired_len, len(speeds) * conversion_rate)
    gps_out = [0.0 for _ in range(gps_out_initial_len)]
    for i in range(min(desired_len // conversion_rate, len(speeds))):
        for j in range(conversion_rate):
            to_add = 0 if i >= len(speeds) or speeds[i] is None else speeds[i]
            to_add = milliknots_to_mps(to_add)
            gps_out[int((i * conversion_rate) + j)] = float(to_add)
    if len(gps_out) < desired_len:
        gps_out = np.pad(
            gps_out, pad_width=[0, desired_len - len(gps_out)], mode="edge"
        )

    return gps_out


gen_trace_array_udf = udf(gen_trace_array, returnType=ArrayType(ArrayType(FloatType())))


def format_accel(
    accel_trace: Dict[str, List[float]], desired_len: int
) -> List[List[float]]:
    accel = [
        accel_trace["x_f"][:desired_len],
        accel_trace["y_f"][:desired_len],
        accel_trace["z_f"][:desired_len],
    ]
    if len(accel_trace["x_f"]) < desired_len:
        accel = np.pad(
            accel,
            pad_width=[(0, 0), (0, desired_len - len(accel_trace["x_f"]))],
            mode="edge",
        )

    return accel


def format_gyro(
    gyro_trace: Dict[str, List[float]], desired_len: int
) -> List[List[float]]:
    gyro = [
        gyro_trace["x_dps"][:desired_len],
        gyro_trace["y_dps"][:desired_len],
        gyro_trace["z_dps"][:desired_len],
    ]
    if len(gyro_trace["x_dps"]) < desired_len:
        gyro = np.pad(
            gyro,
            pad_width=[(0, 0), (0, desired_len - len(gyro_trace["x_dps"]))],
            mode="edge",
        )

    return gyro


def milliknots_to_mps(speed: float) -> float:
    return (speed / 1000) * 0.514444


df = spark.table(SOURCE_TABLE)
org_ids = [row["org_id"] for row in df.select("org_id").distinct().collect()]
random.shuffle(org_ids)
n = int(len(org_ids) / NUM_BATCHES) + 1
org_id_batches = [org_ids[i : i + n] for i in range(0, len(org_ids), n)]

df = df.withColumn(
    "trace_array",
    gen_trace_array_udf(
        df.recent_gps_trace, df.oriented_accel_raw_trace, df.oriented_gyro_raw_trace
    ),
)
for date in run_dates:
    for i, org_ids in enumerate(org_id_batches):
        print(f"Run Date: {date}; Org Id Batch: {i+1} of {NUM_BATCHES}")
        start = datetime.now()
        print(f"Starting at {start}...")
        to_write = df.where(df.date == date)
        to_write = to_write.where(to_write.org_id.isin(org_ids))
        to_write = to_write.select(
            to_write.date,
            to_write.org_id,
            to_write.object_id,
            to_write.object_type,
            to_write.stat_type,
            to_write.value_list[0].proto_value.accelerometer_event.event_id.alias(
                "event_id"
            ),
            to_write.trace_array,
        )
        to_write.write.partitionBy("date", "org_id").mode("overwrite").option(
            "replaceWhere",
            f"date = '{date}' AND org_id IN ({','.join(map(str, org_ids))})",
        ).saveAsTable(WRITE_TABLE)
        print(f"completed at {datetime.now()}...took {datetime.now() - start}")
