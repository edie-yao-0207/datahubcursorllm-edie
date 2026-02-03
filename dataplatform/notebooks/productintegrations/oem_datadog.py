# MAGIC %run /backend/platformops/datadog

# COMMAND ----------
import boto3
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when

# When we add a new OEM Integration we should update this map. It would be nice to have this stored & automatically updated somewhere else so we don't have to do this manually.
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
oem_numbers = list(oem_types.values())

# Intended SQL Query to extract OEM type names and their activation/deactivation counts converted to pyspark:
# SELECT
# CASE oem_type
#   WHEN 0 THEN 'JohnDeere'
#   WHEN 2 THEN 'CaterpillarVisionLink'
#   ...
#   ELSE 'Other'
# END AS name, oem_type,
# SUM(CASE WHEN is_activated=1 THEN 1 ELSE 0 END) as active_sources,
# SUM(CASE WHEN is_activated=0 THEN 1 ELSE 0 END) as inactive_sources
# FROM oemdb_shards.oem_sources
# WHERE oem_type IN (0,1,2,3,4,5,6,7,8,9,12,13,14,15,16,18,19,20)
# GROUP BY oem_type

df = table("oemdb_shards.oem_sources")
df = (
    df.where(df.oem_type.isin(oem_numbers))
    .groupBy("oem_type")
    .agg(
        _sum(when(df.is_activated == 1, 1).otherwise(0)).alias("active_sources"),
        _sum(when(df.is_activated == 0, 1).otherwise(0)).alias("inactive_sources"),
    )
)
case_expr = (
    "CASE oem_type \n"
    + "\n".join(
        f"WHEN {oem_type} THEN '{name}'" for name, oem_type in oem_types.items()
    )
    + "ELSE 'Other' END AS name"
)

df = df.selectExpr("oem_type", case_expr, "active_sources", "inactive_sources")

# COMMAND ----------

df.show()

# COMMAND ----------

region = boto3.session.Session().region_name
if region != "us-west-2" and region != "eu-west-1":
    raise Exception("bad region: " + region)

metrics = []

for row in df.rdd.collect():
    tags = [f"oemtype:{row.name.lower()}", f"region:{region}"]
    metrics.append(
        {
            "metric": "databricks.oem.active_sources.count",
            "points": row.active_sources,
            "tags": tags,
        }
    )
    metrics.append(
        {
            "metric": "databricks.oem.inactive_sources.count",
            "points": row.inactive_sources,
            "tags": tags,
        }
    )

log_datadog_metrics(metrics)
