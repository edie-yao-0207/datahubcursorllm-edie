# MAGIC %run /backend/platformops/datadog

# COMMAND ----------
import boto3
from pyspark.sql.functions import sum as _sum, when, col, current_date, date_sub

oem_types = {
    "JohnDeere": 0,
    "CaterpillarVisionLink": 2,
    "Ford": 3,
    "Navistar": 4,
    "Volvo": 5,
    "Fca": 6,
    "ThermoKing": 7,
    "Komatsu": 8,
    "Gm": 9,
    "VolvoCareTrack": 12,
    "CaseSiteWatch": 13,
    "Vermeer": 14,
    "Bobcat": 15,
    "Carrier": 16,
    "Stellantis": 17,
    "Tesla": 18,
    "CaterpillarAemp": 19,
    "Hino": 20,
    "Drov": 21,
    "Continental": 22,
    "Rivian": 23,
    "Liebherr": 24,
    "Skf": 25,
    "CarrierEu": 26,
}


# Active devices with non-null device_id
active_devices = (
    table("oemdb_shards.oem_sources")
    .where((col("is_activated") == 1) & col("device_id").isNotNull())
    .select("org_id", "device_id", "oem_type")
)

# Devices that reported location in the last 24 hours
location_today = (
    table("default.kinesisstats.location")
    .where(col("date") >= date_sub(current_date(), 1))
    .select("org_id", "device_id")
    .distinct()
)

joined = (
    active_devices.alias("ad")
    .join(
        location_today.alias("ltd"),
        (col("ad.device_id") == col("ltd.device_id"))
        & (col("ad.org_id") == col("ltd.org_id")),
        "left",
    )
    .select(
        col("ad.oem_type"),
        col("ad.device_id"),
        when(col("ltd.device_id").isNotNull(), 1).otherwise(0).alias("reported_today"),
    )
)

df = joined.groupBy("oem_type").agg(
    _sum("reported_today").alias("reporting_today_count"),
)

case_expr = (
    "CASE oem_type \n"
    + "\n".join(
        f"WHEN {oem_type} THEN '{name}'" for name, oem_type in oem_types.items()
    )
    + "\nELSE CAST(oem_type AS STRING) END AS oem"
)

df = df.selectExpr("oem_type", case_expr, "reporting_today_count")

# COMMAND ----------

df.show()

# COMMAND ----------

region = boto3.session.Session().region_name
if region != "us-west-2" and region != "eu-west-1":
    raise Exception("bad region: " + region)

metrics = []

for row in df.rdd.collect():
    tags = [f"oemtype:{row.oem.lower()}", f"region:{region}"]
    metrics.append(
        {
            "metric": "databricks.oem.reporting_today.count",
            "points": row.reporting_today_count,
            "tags": tags,
        }
    )

log_datadog_metrics(metrics)
