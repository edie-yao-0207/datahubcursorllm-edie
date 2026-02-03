# Databricks notebook source
# MAGIC %run /backend/safety/internal_retrievals/internal_retrievals

# COMMAND ----------

from datetime import datetime, timedelta

import pyspark.sql.functions as F
import pyspark.sql.utils

# Searching 9 to 4 hours ago
#  4 hour end time selected do to 3 hour delay
#  for RDS tables
#  We extend to 9 hours back incase there are
#  additional delays but deduplicate requests
#  to ensure we don't rerequest

ui_start_time = dbutils.widgets.get("start_time")
ui_end_time = dbutils.widgets.get("end_time")
if ui_start_time != "" and ui_end_time != "":
    start_time = datetime.strptime(ui_start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(ui_end_time, "%Y-%m-%d %H:%M:%S")
elif ui_start_time == "" and ui_end_time == "":
    start_time = datetime.now() - timedelta(hours=9)
    end_time = datetime.now() - timedelta(hours=4)
else:
    raise ValueError("Both start and end time must be defined if using the UI")


request_table = "datascience.hev2_python_not_go_crash_requests"

crashes = spark.table("safetydb_shards.safety_events")

crashes = crashes.where(
    (crashes.detail_proto.ingestion_tag == 1)
    # 5 = crash
    # 28 = rollover
    & crashes.detail_proto.accel_type.isin([5, 28])
)
crashes = crashes.select(
    crashes.date,
    crashes.detail_proto.event_id.alias("event_id"),
    crashes.org_id,
    crashes.device_id,
    F.lit(True).alias("has_crash_row"),
)

ds = spark.table("datastreams.crash_detector_inference_results").select(
    "date",
    "org_id",
    "device_id",
    "timestamp",
    "event_time_ms",
    "event_id",
    "severity",
    "event_max_g_ms",
    "threshold_g",
    "magnitude_g",
    "accel_energy",
    "gyro_energy",
)
ds = ds.where(ds.date.between(start_time.date(), end_time.date()))
ds = ds.where(ds.timestamp.between(start_time, end_time))

# limit to medium and high severity (2, 3)
ds = ds.where(ds.severity > 1)

ds = ds.join(crashes, on=["org_id", "date", "device_id", "event_id"], how="left")
ds = ds.where(ds.has_crash_row.isNull())
ds = ds.withColumn("start_ms", ds.event_time_ms - (1_000 * 5))
ds = ds.withColumn("end_ms", ds.event_time_ms + (1_000 * 5))
try:
    curr_table = spark.table(request_table)
    curr_table = curr_table.select(
        "date", "event_id", F.lit(1).alias("already_present")
    )
    ds = ds.join(curr_table, on=["date", "event_id"], how="left").select(
        ds.date,
        ds.org_id,
        ds.device_id,
        ds.timestamp,
        ds.event_time_ms,
        ds.event_id,
        ds.severity,
        ds.event_max_g_ms,
        ds.threshold_g,
        ds.magnitude_g,
        ds.accel_energy,
        ds.gyro_energy,
        ds.start_ms,
        ds.end_ms,
        curr_table.already_present,
    )
    ds = ds.where(ds.already_present.isNull())
    ds = ds.select(
        "date",
        "org_id",
        "device_id",
        "timestamp",
        "event_time_ms",
        "event_id",
        "severity",
        "event_max_g_ms",
        "threshold_g",
        "magnitude_g",
        "accel_energy",
        "gyro_energy",
        "start_ms",
        "end_ms",
    )
except pyspark.sql.utils.AnalysisException as e:
    check_string = f"Table or view not found: {request_table};"
    if not check_string in str(e):
        raise (e)

    ds = ds.select(
        "date",
        "org_id",
        "device_id",
        "timestamp",
        "event_time_ms",
        "event_id",
        "severity",
        "event_max_g_ms",
        "threshold_g",
        "magnitude_g",
        "accel_energy",
        "gyro_energy",
        "start_ms",
        "end_ms",
    )

requests = ds.select("org_id", "device_id", "start_ms", "end_ms").collect()

# Limit to 3k requests to prevent overloading prod cells
assert len(requests) <= 3000, f"More than 3,000 requests"
# Limit requests to be exactly 10 seconds in length
assert all(
    [(r["end_ms"] - r["start_ms"]) == 10_000 for r in requests]
), f"Request longer than 10 seconds"

retrievals = []

for request in requests:
    org_id = request["org_id"]
    device_id = request["device_id"]
    start_ms = request["start_ms"]
    end_ms = request["end_ms"]
    retrievals.append(
        InternalRetrievalRequest(
            org_id,
            device_id,
            start_ms,
            end_ms,
        )
    )

main(retrievals)
ds.write.partitionBy("date").mode("append").saveAsTable(request_table)

print("Completed", len(requests), "for", start_time, "to", end_time)
