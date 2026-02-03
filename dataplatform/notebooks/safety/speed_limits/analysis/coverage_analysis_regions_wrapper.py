# Databricks notebook source
# MAGIC %run backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

import json

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
print(f"{ARG_REGIONS}: {desired_regions}")

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


# COMMAND ----------

"""
The coverage_analysis notebook only handles one region at a time. This wrapper
will run that notebook for multiple regions in parallel!
"""

coverage_analysis_notebook = "/backend/safety/speed_limits/analysis/coverage_analysis"
params = []
for region in desired_regions:
    params.append(
        {
            ARG_OSM_VERSION: osm_regions_to_version[region],
            ARG_TOMTOM_VERSION: tomtom_version,
            ARG_REGIONS: region,
        }
    )

# results will be a list of strings, each of which is the output from the exit_notebook call of one of the runs
results = submit_parallel_notebook_runs(coverage_analysis_notebook, params)
print(results)

# If there was an error, let's propogate it
for result in results:
    if result is not None and result.get("error") is not None:
        exit_notebook(result["error"])

exit_notebook(None, None)
