# Databricks notebook source
# MAGIC %run backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

# MAGIC %run backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %sql
# MAGIC SET
# MAGIC   spark.sql.broadcastTimeout = 100000;
# MAGIC SET
# MAGIC   spark.sql.autoBroadcastJoinThreshold = -1;

# COMMAND ----------

"""
This notebook takes in the following args:
* ARG_OSM_VERSION:
Must be specified in YYMMDD format
"""
dbutils.widgets.text(ARG_OSM_VERSION, "")
osm_version = dbutils.widgets.get(ARG_OSM_VERSION)
print(f"{ARG_OSM_VERSION}: {osm_version}")

"""
* ARG_TOMTOM_VERSION:
Must be specified in YYMM000 format
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: {tomtom_version}")

"""
* ARG_REGIONS:
Pass in "" to generate for DEFAULT_REGIONS
Pass in a comma-separated list of regions (ex: "EUR,USA")
"""
dbutils.widgets.text(ARG_REGIONS, serialize_regions([EUR]))
desired_regions = deserialize_regions(dbutils.widgets.get(ARG_REGIONS))
region = ""
if len(desired_regions) > 1:
    exit_notebook("only one region supported at a time")
else:
    region = desired_regions[0]
print(f"{ARG_REGIONS}: {region}")

"""
* ARG_IS_TOMTOM_DECOUPLED:
Boolean flag to indicate if tomtom data is decoupled
"""
dbutils.widgets.text(ARG_IS_TOMTOM_DECOUPLED, "")
is_tomtom_decoupled = dbutils.widgets.get(ARG_IS_TOMTOM_DECOUPLED).lower() == "true"
print(f"{ARG_IS_TOMTOM_DECOUPLED}: {is_tomtom_decoupled}")

# COMMAND ----------

if len(osm_version) == 0:
    exit_notebook("osm version must be specified")
if len(tomtom_version) == 0:
    exit_notebook("tomtom version must be specified")
if len(region) == 0:
    exit_notebook("region must be specified")

# COMMAND ----------

from pyspark.sql.functions import struct, udf
from pyspark.sql.types import IntegerType

KPH_IN_MPH = 1.60934


def osm_tomtom_speed_comparator(row):
    """
    * None if coverage is missing
    * 0 if osm and tomtom maxspeeds match
    * 1 if osm is null and tomtom limit exists (net-new)
    * 2 if osm has limit but tomtom does not
    * 3 if osm and tomtom differ (updated coverage)
    """
    # Normalize osm speed limit to kph
    osm_limit = osm_normalize_speed_kph(row["osm_maxspeed"])
    tomtom_limit = tomtom_normalize_speed_kph(
        row["tomtom_maxspeed"], row["tomtom_maxspeed_unit"]
    )
    if osm_limit is None and tomtom_limit is None:
        return None
    if osm_limit is None:
        return 1
    if tomtom_limit is None:
        return 2
    if osm_limit == tomtom_limit:
        return 0
    return 3


osm_tomtom_speed_comparator_udf = udf(osm_tomtom_speed_comparator, IntegerType())

assert (
    osm_tomtom_speed_comparator(
        {
            "osm_maxspeed": "10 mph",
            "tomtom_maxspeed": None,
            "tomtom_maxspeed_unit": None,
        }
    )
    == 2
)
assert (
    osm_tomtom_speed_comparator(
        {"osm_maxspeed": "10 mph", "tomtom_maxspeed": 10, "tomtom_maxspeed_unit": "mph"}
    )
    == 0
)
assert (
    osm_tomtom_speed_comparator(
        {"osm_maxspeed": "10", "tomtom_maxspeed": 10, "tomtom_maxspeed_unit": "kph"}
    )
    == 0
)
assert (
    osm_tomtom_speed_comparator(
        {"osm_maxspeed": "10", "tomtom_maxspeed": 10, "tomtom_maxspeed_unit": "mph"}
    )
    == 3
)
assert (
    osm_tomtom_speed_comparator(
        {"osm_maxspeed": None, "tomtom_maxspeed": 10, "tomtom_maxspeed_unit": "mph"}
    )
    == 1
)

# COMMAND ----------

mapmatch_table_name = DBX_TABLE.osm_tomtom_map_match(
    osm_version, region, tomtom_version
)
if is_tomtom_decoupled:
    mapmatch_table_name += "_decoupled"

mapmatch_sdf = sqlContext.table(mapmatch_table_name)
mapmatch_sdf = mapmatch_sdf.withColumn(
    "compare",
    osm_tomtom_speed_comparator_udf(
        struct([mapmatch_sdf[x] for x in mapmatch_sdf.columns])
    ),
)
mapmatch_sdf.createOrReplaceTempView("mapmatch_analysis")

# COMMAND ----------

coverage_analysis_sql = """
with total_rows as (
select count(*) as total_entries
from {map_match_table_name}
), no_coverage as (
select count(*) as no_coverage from {analysis_table_name} where compare is null
), osm_only_coverage as (
select count(*) as osm_only_coverage from {analysis_table_name} where compare = 2 and compare is not null
), new_coverage as (
select count(*) as new_coverage from {analysis_table_name} where compare = 1 and compare is not null
), updated_coverage as (
select count(*) as updated_coverage from {analysis_table_name} where compare = 3 and compare is not null
), matched_coverage as (
select count(*) as matched_coverage from {analysis_table_name} where compare = 0 and compare is not null
)
select
  round(no_coverage / total_entries,6)*100 as no_coverage_pct,
  round(osm_only_coverage / total_entries,6)*100 as osm_only_coverage_pct,
  round(new_coverage / total_entries,6)*100 as new_coverage_pct,
  round(updated_coverage / total_entries,6)*100 as updated_coverage_pct,
  round(matched_coverage / total_entries,6)*100 as matched_coverage_pct
from total_rows
cross join no_coverage
cross join osm_only_coverage
cross join new_coverage
cross join updated_coverage
cross join matched_coverage
""".format(
    map_match_table_name=mapmatch_table_name, analysis_table_name="mapmatch_analysis"
)

total_coverage_sdf = spark.sql(coverage_analysis_sql)
total_coverage_sdf.createOrReplaceTempView("mapmatch_total_coverage")

# COMMAND ----------

no_coverage_breakdown_sql = """
with total_rows as (
select count(*) as total_entries
from {map_match_table_name}
)
select
  osm_highway,
  round(count(*) / first(total_entries),6)*100 as pct_no_coverage
from {analysis_table_name}
cross join total_rows
where compare is null
group by osm_highway
order by pct_no_coverage desc
""".format(
    map_match_table_name=mapmatch_table_name, analysis_table_name="mapmatch_analysis"
)

no_coverage_breakdown_sdf = spark.sql(no_coverage_breakdown_sql)
no_coverage_breakdown_sdf.createOrReplaceTempView("no_coverage_breakdown")

# COMMAND ----------

coverage_res = list(map(lambda row: row.asDict(), total_coverage_sdf.collect()))
coverage_res = coverage_res[0]

no_coverage_res = list(
    map(lambda row: row.asDict(), no_coverage_breakdown_sdf.collect())
)
no_coverage_out = ""
for no_coverage in no_coverage_res:
    no_coverage_out += "* {0}: {1}%\n".format(
        no_coverage["osm_highway"], no_coverage["pct_no_coverage"]
    )
results = """
```
* no_coverage_pct: {0}%
* osm_only_coverage_pct: {1}%
* new_coverage_pct: {2}%
* updated_coverage_pct: {3}%
* matched_coverage_pct: {4}%

no_coverage_pct breakdown by tag type:
{5}
```
""".format(
    coverage_res["no_coverage_pct"],
    coverage_res["osm_only_coverage_pct"],
    coverage_res["new_coverage_pct"],
    coverage_res["updated_coverage_pct"],
    coverage_res["matched_coverage_pct"],
    no_coverage_out,
)
print(results)

# COMMAND ----------

emails = []
alert_name = "safety_mapdata_coverage_results"
slack_channels = ["safety-platform-alerts"]
execute_alert(
    "select 1",
    emails,
    slack_channels,
    alert_name,
    f"Total coverage for `{region}`, osm_version: `{osm_version}`, tomtom_version: `{tomtom_version}`\n"
    + results,
)
exit_notebook(None, None)

# COMMAND ----------
