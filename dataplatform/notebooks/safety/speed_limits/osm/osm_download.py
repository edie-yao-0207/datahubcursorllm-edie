# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

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

DEFAULT_REGIONS = [CAN, EUR, MEX, USA]

"""
* ARG_REGIONS:
Pass in "" to generate for DEFAULT_REGIONS
Pass in a comma-separated list of regions (ex: "EUR,USA")
"""
dbutils.widgets.text(ARG_REGIONS, serialize_regions(DEFAULT_REGIONS))
desired_regions = dbutils.widgets.get(ARG_REGIONS)
if len(desired_regions) == 0:
    desired_regions = DEFAULT_REGIONS
else:
    desired_regions = deserialize_regions(desired_regions)
print(f"{ARG_REGIONS}: {desired_regions}")

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get install wget -y

# COMMAND ----------

import datetime
import json
import os
import shutil
import subprocess
from typing import Dict, List

import boto3

REGIONS_TO_URLS = {
    CAN: "http://download.geofabrik.de/north-america/canada-latest.osm.pbf",
    MEX: "http://download.geofabrik.de/north-america/mexico-latest.osm.pbf",
    USA: "http://download.geofabrik.de/north-america/us-latest.osm.pbf",
    # NAM is only used in tilegen, so that we don't drop tiles near country borders.
    NAM: "https://download.geofabrik.de/north-america-latest.osm.pbf",
    # CAM is only used for ingestion path OSRM.
    CAM: "http://download.geofabrik.de/central-america-latest.osm.pbf",
    EUR: "http://download.geofabrik.de/europe-latest.osm.pbf",
}

# Dirctories
BASE_DIR = "/tmp/osm_pbf"


def get_consistent_versions_for_regions(
    desired_regions: List[str], version: str
) -> Dict[str, str]:
    """
    returns a consistent mapping of all desired_regions to the passed in version.
    """
    version = str(version)
    regions_to_versions_map = {}
    for region in desired_regions:
        regions_to_versions_map[region] = version
    return regions_to_versions_map


def get_newest_osm_version_id() -> str:
    """
    returns the most recent osm_version_id
    as the current date.
    """
    dt = datetime.datetime.now()
    return dt.strftime("%Y%m%d")


def parse_filename_from_url(url: str) -> str:
    """
    fetches a filename from an osm download url.
    """
    return url.split("/")[-1]


def download_file(url: str) -> str:
    """
    downloads a file using wget given a url.
    """
    print(f"downloading file from {url}")
    process = subprocess.Popen(
        ["wget", url, "-P", BASE_DIR], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        exit_notebook(str(stderr))
    print("done")
    return os.path.join(BASE_DIR, parse_filename_from_url(url))


def upload_file_to_s3(fpath: str, region: str):
    """
    uploads an osm base map at fpath to the correct
    s3 location given a region.
    """
    s3_file_path = OSM_S3.make_s3_key(get_newest_osm_version_id(), region)
    print(f"Uploading {fpath} to S3 bucket {OSM_S3.BUCKET}/{s3_file_path}")
    # TODO: Better error handling around S3 API?

    # boto3.client is only used for non-UC enabled clusters. (instance-profile: safetyplatform-cluster)
    # Please get s3 client from boto3_helpers.get_s3_client for UC enabled clusters. (instance-profile: unity-catalog-cluster)
    client = boto3.client("s3")
    client.upload_file(
        fpath,
        OSM_S3.BUCKET,
        s3_file_path,
        ExtraArgs={"ACL": "bucket-owner-full-control"},
    )


# COMMAND ----------


def run_download(desired_regions: List[str]):
    # Make a copy of desired_regions to ensure changes are not reflected in
    # the caller function.
    desired_regions = desired_regions.copy()
    if CAN in desired_regions or MEX in desired_regions or USA in desired_regions:
        # For tilegen, download the full NAM extract. This is neccessary so that we don't
        # unintentionally drop tiles near NAM country borders.
        # Also download Central America for ingestion-path OSRM.
        desired_regions.extend([NAM, CAM])
    try:
        for region in desired_regions:
            url = REGIONS_TO_URLS[region]
            filename = parse_filename_from_url(url)
            fpath = download_file(url)
            upload_file_to_s3(fpath, region)
            print(f"cleaning up {fpath}")
            os.remove(fpath)
    finally:
        shutil.rmtree(BASE_DIR)


# COMMAND ----------

latest_versions_map = get_manifest(OSM_S3.BUCKET, OSM_S3.S3_PREFIX, OSM_S3.BASE_OSM_KEY)

# Restrict the latest_versions_map to only contain regions
# specified in desired_regions.
if latest_versions_map is not None:
    latest_versions_map = {
        k: latest_versions_map[k] for k in desired_regions if k in latest_versions_map
    }

# If no latest version, attempt a download.
# If we explicitly want to fetch the latest, attempt a download.
if latest_versions_map is None or (osm_version) == KEYWORD_FETCH_LATEST:
    run_download(desired_regions)
elif len(osm_version) == 0:
    # If desired version is blank, return the latest downloaded version.
    exit_val = {ARG_OSM_REGIONS_VERSION_MAP: latest_versions_map}
    exit_notebook(None, exit_val)
else:
    # Pass-through, return desired osm_version for next step in the pipeline
    exit_val = {
        ARG_OSM_REGIONS_VERSION_MAP: get_consistent_versions_for_regions(
            desired_regions, osm_version
        )
    }
    exit_notebook(
        None,
        exit_val,
    )


# Upsert the latest version
version = get_newest_osm_version_id()
if latest_versions_map is None:
    latest_versions_map = {}
for region in desired_regions:
    latest_versions_map[region] = version

# Update manifest
update_manifest(
    OSM_S3.BUCKET, OSM_S3.S3_PREFIX, OSM_S3.BASE_OSM_KEY, latest_versions_map
)

exit_val = {ARG_OSM_REGIONS_VERSION_MAP: latest_versions_map}
exit_notebook(None, exit_val)
