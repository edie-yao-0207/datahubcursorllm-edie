from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
DAILY_VIDEO_REQUESTS = "datascience.daily_video_requests"
WRITE_TABLE = "datascience.daily_videos_for_labeling"

hvr = spark.table("clouddb.historical_video_requests")
hvr = hvr.where(~hvr.completed_at_ms.isNull())

dv = spark.table("dataprep_ml.dashcam_videos")
dv = dv.groupBy("date", "vg_id", "asset_ms", "org_id", "event_id").agg(
    F.max(F.when(dv.direction == "forward", dv.video_url).otherwise(None)).alias(
        "forward_url"
    ),
    F.max(F.when(dv.direction == "inward", dv.video_url).otherwise(None)).alias(
        "inward_url"
    ),
)

requests = spark.table(DAILY_VIDEO_REQUESTS)

requests = requests.where(
    requests.date.between(F.date_sub(F.lit(START_DATE), 14), END_DATE)
)

requests = requests.join(hvr, on=["device_id", "start_ms", "end_ms"]).select(
    requests.date,
    requests.org_id,
    requests.device_id,
    requests.start_ms,
    requests.end_ms,
    requests.job_names,
    requests.event_ids,
    hvr.asset_ms,
    hvr.date.alias("historical_video_request_date"),
    hvr.id.alias("video_id"),
)
requests = requests.join(
    dv,
    on=(
        requests.historical_video_request_date.between(F.date_sub(dv.date, 14), dv.date)
        & (dv.event_id == requests.video_id)
        & (dv.org_id == requests.org_id)
    ),
).select(
    F.lit(END_DATE).alias("date"),
    requests.date.alias("request_date"),
    requests.org_id,
    requests.device_id,
    requests.start_ms,
    requests.end_ms,
    requests.asset_ms,
    requests.event_ids,
    requests.job_names,
    dv.event_id.alias("video_event_id"),
    dv.forward_url,
    dv.inward_url,
)
to_select_cols = requests.columns
try:
    written = spark.table(WRITE_TABLE)
    written = written.select(
        written.asset_ms.alias("written_asset_ms"),
        written.org_id.alias("written_org_id"),
        written.device_id.alias("written_device_id"),
        written.date.alias("written_date"),
    )
    requests = requests.join(
        written,
        on=(
            (requests.asset_ms == written.written_asset_ms)
            & (requests.org_id == written.written_org_id)
            & (requests.device_id == written.written_device_id)
            & (requests.date != written.written_date)
        ),
        how="left",
    )
    requests = requests.where(requests.written_asset_ms.isNull())
except AnalysisException as e:
    if not f"Table or view not found: {WRITE_TABLE};" in str(e):
        raise e

requests = requests.select(*to_select_cols)
requests.write.mode("overwrite").partitionBy("date").option(
    "replaceWhere", f"date = '{END_DATE}'"
).saveAsTable(WRITE_TABLE)
