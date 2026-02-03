from datetime import datetime, timedelta

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
SAMPLE_RATE = 0.02

HE_V1_CRASH_PREDICTIONS = "datascience.daily_crash_filter_predicitons"
LOW_SEV_CRASHES_TO_REQUEST = "datascience.low_sev_crash_samples_to_request"

crash_preds = spark.table(HE_V1_CRASH_PREDICTIONS)
low_sev_crashes = crash_preds.where(
    crash_preds.date.between(START_DATE, END_DATE) & (crash_preds.crash.severity == 1)
)

low_sev_crashes = low_sev_crashes.sample(SAMPLE_RATE)
low_sev_crashes = low_sev_crashes.withColumn(
    "start_ms", low_sev_crashes.time - 5_000
).withColumn("end_ms", low_sev_crashes.time + 5_000)

low_sev_crashes.select(
    low_sev_crashes.date,
    low_sev_crashes.time,
    low_sev_crashes.org_id,
    low_sev_crashes.object_id.alias("device_id"),
    low_sev_crashes.event_id,
    low_sev_crashes.start_ms,
    low_sev_crashes.end_ms,
).write.mode("overwrite").option(
    "replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'"
).saveAsTable(
    LOW_SEV_CRASHES_TO_REQUEST
)
