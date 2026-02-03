# Databricks notebook source
# MAGIC %run /backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

import json

"""
vars:
tile_version
"""

"""
This notebook takes in the following args:
* ARG_TOMTOM_VERSION:
Pass in "" to return the latest downloaded version.
Pass in KEYWORD_FETCH_LATEST to redownload the newest dataset.
Pass in "YYYYMM000" to return skip downloading and return an older version.
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: {tomtom_version}")

"""
This notebook takes in the following args:
* ARG_OSM_VERSION:
Pass in "" to return the latest downloaded version.
Pass in KEYWORD_FETCH_LATEST to redownload the newest dataset.
Pass in "YYYYMMDD" to return skip downloading and return an older version.
"""
dbutils.widgets.text(ARG_OSM_VERSION, "")
osm_version = dbutils.widgets.get(ARG_OSM_VERSION)
print(f"{ARG_OSM_VERSION}: {osm_version}")

"""
* ARG_REGIONS:
Pass in a comma-separated list of regions (ex: "EUR,USA")
"""
DEFAULT_REGIONS = [CAN, EUR, MEX, USA]

dbutils.widgets.text(ARG_REGIONS, serialize_regions(DEFAULT_REGIONS))
desired_regions = dbutils.widgets.get(ARG_REGIONS)
if len(desired_regions) == 0:
    desired_regions = DEFAULT_REGIONS
else:
    desired_regions = deserialize_regions(desired_regions)
print(f"{ARG_REGIONS}: {desired_regions}")

"""
* ARG_TILE_VERSION
Pass in an int corresponding to the tile version for generation.
Production csvs (tilegen, backend) will be written for this tile version.
"""
dbutils.widgets.text(ARG_TILE_VERSION, "9999")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
if len(tile_version) == 0:
    tile_version = "9999"
print(f"{ARG_TILE_VERSION}: {tile_version}")

# COMMAND ----------

# DBTITLE 1,Fetch & Parquetize Latest OSM Data
if len(desired_regions) == 0:
    raise Exception("desired regions must be specified")

osm_download_res = run_notebook(
    "/backend/safety/speed_limits/osm/osm_download",
    {ARG_OSM_VERSION: osm_version, ARG_REGIONS: serialize_regions(desired_regions)},
)
osm_map_versions = osm_download_res[ARG_OSM_REGIONS_VERSION_MAP]
print(osm_map_versions)

# Parquetize OSM data
run_notebook(
    "/backend/safety/speed_limits/osm/osm_parquetizer",
    osm_download_res,
)

# Parquet to DBX
run_notebook(
    "/backend/safety/speed_limits/osm/osm_parquet_to_table",
    osm_download_res,
)

# COMMAND ----------

# DBTITLE 1,Fetch Latest TomTom Data
tomtom_download_res = run_notebook(
    "/backend/safety/speed_limits/tomtom/tomtom_download",
    {ARG_TOMTOM_VERSION: tomtom_version},
)
tomtom_map_version = tomtom_download_res[ARG_TOMTOM_VERSION]
print(tomtom_map_version)
print(desired_regions)
print(serialize_regions(desired_regions))

run_notebook(
    "/backend/safety/speed_limits/tomtom/tomtom_to_table",
    {
        ARG_TOMTOM_VERSION: tomtom_map_version,
        ARG_REGIONS: serialize_regions(desired_regions),
    },
)

# COMMAND ----------

# DBTITLE 1,Map-Match OSM + TomTom
run_notebook(
    "/backend/safety/speed_limits/map_match",
    {
        ARG_OSM_REGIONS_VERSION_MAP: json.dumps(osm_map_versions),
        ARG_TOMTOM_VERSION: tomtom_map_version,
        ARG_REGIONS: serialize_regions(desired_regions),
    },
)

# COMMAND ----------

# DBTITLE 1,Overall Coverage Metrics
notebook = "/backend/safety/speed_limits/analysis/coverage_analysis"
params = []
for region in desired_regions:
    params.append(
        {
            ARG_OSM_VERSION: osm_map_versions[region],
            ARG_TOMTOM_VERSION: tomtom_map_version,
            ARG_REGIONS: region,
        }
    )
res = submit_parallel_notebook_runs(notebook, params)
print(res)

# COMMAND ----------

# DBTITLE 1,Customer Ways Coverage Metrics
notebook = "/backend/safety/speed_limits/analysis/customer_ways_analysis"
for region in desired_regions:
    res = run_notebook(
        notebook,
        {
            ARG_OSM_VERSION: osm_map_versions[region],
            ARG_TOMTOM_VERSION: tomtom_map_version,
            ARG_REGIONS: region,
            ARG_TILE_VERSION: tile_version,
        },
    )
    print(res)

# COMMAND ----------

# DBTITLE 1,Write TileGen/Ingestion Datasets
serialized_regions = serialize_regions(desired_regions)

run_notebook(
    "/backend/safety/speed_limits/write_prod_datasets",
    {
        ARG_OSM_REGIONS_VERSION_MAP: json.dumps(osm_map_versions),
        ARG_TOMTOM_VERSION: tomtom_map_version,
        ARG_REGIONS: serialized_regions,
        ARG_TILE_VERSION: tile_version,
    },
)

# COMMAND ----------

emails = []
alert_name = "safety_mapdata_ingestion_completed"
slack_channels = ["safety-platform-alerts"]
execute_alert(
    "select 1",
    emails,
    slack_channels,
    alert_name,
    f"New map data ingestion completed\n* regions: `{desired_regions}`\n* osm_versions: `{osm_map_versions}`,\n* tomtom_version: `{tomtom_version}`\n* tile_version: `{tile_version}`",
)

# COMMAND ----------
