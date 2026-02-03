# Databricks notebook source
# MAGIC %run /backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/map_match_utils

# COMMAND ----------

"""
This notebook takes in the following args:
* ARG_OSM_REGIONS_VERSION_MAP:
JSON dict from region -> version_id
"""
dbutils.widgets.text(ARG_OSM_REGIONS_VERSION_MAP, "")
osm_regions_to_version = dbutils.widgets.get(ARG_OSM_REGIONS_VERSION_MAP)
if len(osm_regions_to_version) > 0:
    osm_regions_to_version = json.loads(osm_regions_to_version)
else:
    osm_regions_to_version = {}
print(f"{ARG_OSM_REGIONS_VERSION_MAP}: {osm_regions_to_version}")

"""
* ARG_TOMTOM_VERSION:
Must be specified in YYMM000 format
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: {tomtom_version}")

"""
* ARG_OSM_DATASET_VERSION:
OSM dataset version
"""
dbutils.widgets.text(ARG_OSM_DATASET_VERSION, "")
osm_dataset_version = dbutils.widgets.get(ARG_OSM_DATASET_VERSION)
print(f"{ARG_OSM_DATASET_VERSION}: {osm_dataset_version}")

"""
* ARG_TILE_VERSION:
Integer tile version
"""
dbutils.widgets.text(ARG_TILE_VERSION, "")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
print(f"{ARG_TILE_VERSION}: {tile_version}")


# COMMAND ----------

if len(osm_regions_to_version) == 0:
    exit_notebook("osm regions to version map must be specified")
if len(tomtom_version) == 0:
    exit_notebook("tomtom version must be specified")
if len(tile_version) == 0:
    exit_notebook("tile version must be specified")

# COMMAND ----------

# TODO Validate that we won't be overwriting any data for this tile version.
config.setCurrentPendingVersion(tile_version)

# COMMAND ----------

# Spark doesn't like it when we append to the same table currently being read from.
resolved_limits_table = DBX_TABLE.osm_tomtom_resolved_speed_limits(
    osm_regions_to_version, tomtom_version
)

# COMMAND ----------

# Set the datasource to be TomTom
speed_limit_data_source = SpeedLimitDataSource.VENDOR_TOM_TOM

# Create commercial speed limit backend CSV
backend_csl_df = create_backend_csl_csv(resolved_limits_table, speed_limit_data_source)

display(backend_csl_df)

# COMMAND ----------

validate_backend_csv(backend_csl_df)

# COMMAND ----------

# Persist commercial speed limit data to S3
persist_backend_csl_csv(backend_csl_df)

# COMMAND ----------

exit_notebook()
