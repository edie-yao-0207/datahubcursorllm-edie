from collections import namedtuple
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)

DAILY_VIDEO_REQUESTS = "datascience.daily_video_requests"

RequestTable = namedtuple("RequestTable", ["input_table", "job_name"])

REQUEST_TABLES = [
    RequestTable(
        "datascience.backend_k_means_cluster_for_sampling_items_to_request",
        "k_means_cluster",
    ),
    RequestTable(
        "datascience.low_sev_crash_samples_to_request",
        "low_sev_crashes",
    ),
    RequestTable(
        "datascience.backend_backup_events_items_to_request",
        "backup_events",
    ),
]

dfs = []
for table in REQUEST_TABLES:
    this_df = spark.table(table.input_table)
    this_df = this_df.withColumn("job_name", F.lit(table.job_name))
    this_df = this_df.where(this_df.date.between(START_DATE, END_DATE))
    this_df = this_df.select(
        "date",
        "org_id",
        "device_id",
        "event_id",
        "start_ms",
        "end_ms",
        "job_name",
    )
    vgs_with_cms = spark.table("dataprep_safety.cm_linked_vgs")
    vgs_with_cms = vgs_with_cms.select(
        vgs_with_cms.org_id, vgs_with_cms.vg_device_id.alias("device_id")
    ).distinct()
    this_df = this_df.join(vgs_with_cms, on=["org_id", "device_id"])
    dfs.append(this_df)

df = reduce(DataFrame.unionAll, dfs)
df = df.groupBy("date", "org_id", "device_id", "start_ms", "end_ms",).agg(
    F.collect_list("job_name").alias("job_names"),
    F.collect_set("event_id").alias("event_ids"),
)

df.write.partitionBy("date").mode("overwrite").option(
    "replaceWhere", f"date BETWEEN '{START_DATE}' AND '{END_DATE}'"
).saveAsTable(DAILY_VIDEO_REQUESTS)
