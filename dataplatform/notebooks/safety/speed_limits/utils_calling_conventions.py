# Databricks notebook source
"""
Lib-Calling-Conventions contains shared helpers imported by several notebooks.
This includes:
 * Calling conventions when invoking the databricks workflow run API.
 * S3 / filesystem paths for data
 * Databricks talbe names
 * Normalizing speed limit units.
"""
# COMMAND ----------
# MAGIC %pip install /Volumes/s3/dataplatform-deployed-artifacts/wheels/service_credentials-1.0.1-py3-none-any.whl
# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------


import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Union

import boto3
import service_credentials  # required for ssm cloud credentials
from pyspark.sql import Row
from pyspark.sql.functions import col, regexp_extract, udf
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Standard arguments and values for sharing data between notebooks.
ARG_TOMTOM_VERSION = "tomtom_version"
ARG_TOMTOM_DATASET_VERSION = "tomtom_dataset_version"
ARG_OSM_VERSION = "osm_version"
ARG_OSM_REGIONS_VERSION_MAP = "osm_regions_versions"
ARG_REGIONS = "regions"
ARG_TILE_VERSION = "tile_version"
ARG_FORCE_RUN = "force_run"
ARG_OSM_DATASET_VERSION = "osm_dataset_version"
ARG_IS_TOMTOM_DECOUPLED = "is_tomtom_decoupled"
ARG_SPEED_LIMIT_DATASET_VERSION = "speed_limit_dataset_version"

KEYWORD_FETCH_LATEST = "latest"  # Indicates we should always re-fetch data.
MAX_SPEED_LIMIT_KPH = 200  # 200 kph is the cap speed limit
MAX_SPEED_LIMIT_MPH = 125  # 125 mph is the cap speed limit


def exit_notebook(
    error: Optional[str] = None, val: Dict[str, Union[Dict, str, int]] = None
):
    """
    Returning from a notebook only allows returning a string.
    These wrappers encode a return status as well as a return
    value to support returning errors.
    """
    if error is None or error is False:
        error = ""
    else:
        error = str(error)

    if val is None:
        val = {}
    retval = json.dumps({"error": error, "val": val})
    dbutils.notebook.exit(retval)


def run_notebook(
    loc: str, args: Dict[str, Union[Dict, str, int]], max_retries: int = 3
) -> Dict:
    """
    run_notebook runs the notebook at the given loc, passing in the provided args.
    Due to transient errors, we automatically retry exceptions, up to max_retries
    times. By default, max_retries is set to 3, meaning we could attempt to run
    a notebook up to 4 times.
    """
    res = None
    num_retries = 0
    # Convert args to a str -> str map by json encoding the value.
    for key, value in args.items():
        if value is not None and type(value) is not str:
            args[key] = json.dumps(value)
    while True:
        try:
            # Run without timeout
            res = dbutils.notebook.run(loc, 0, args)
            break
        except Exception as e:
            if num_retries >= max_retries:
                exit_notebook(e)
                break
            else:
                print("Retrying error:", e)
                num_retries += 1
    if res is None:
        return None
    res = json.loads(res)
    if res["error"] and len(res["error"]) > 0:
        exit_notebook(res["error"])
    if res["val"]:
        return res["val"]
    return None


# COMMAND ----------

"""
Helpers for regions, serialization & deserialization.
These region constants are used when deciding which regions to generate for.
"""
CAN = "CAN"
MEX = "MEX"
USA = "USA"
EUR = "EUR"
NAM = "NAM"  # NAM is only used for tilegen, since a country extract would cause us to lose data
CAM = "CAM"  # CAM is only used for ingestion path OSRM.


def deserialize_regions(regions_str: str) -> List[str]:
    """
    ex: "CAN,EUR,USA" -> ["CAN", "EUR", "USA"]
    """
    return sorted(regions_str.upper().split(","))


def serialize_regions(regions_list: List[str]) -> str:
    """
    ex: ["CAN", "EUR", "USA"] -> "CAN,EUR,USA"
    """
    regions_list = sorted(regions_list)
    return ",".join(regions_list).upper()


# COMMAND ----------

"""
Helpers for manifest utils.
Manifest.json files are used to keep track of inter-run persistent state, to avoid recomputing values.
Manifest files can be used by external polling workers to interact with the results of DBX runs.
"""


def get_manifest(bucket: str, s3_prefix: str, manifest_key: str):
    """
    Fetches the manifest value associated with a given key.
    If key is None, returns the whole manifest.
    """
    client = boto3.resource("s3")
    obj = client.Object(bucket, os.path.join(s3_prefix, "manifest.json"))
    try:
        res = obj.get()
    except client.meta.client.exceptions.NoSuchKey as e:
        print("manifest does not exist -- returning None")
        return None
    manifest = res["Body"].read()
    res["Body"].close()
    manifest = json.loads(manifest)
    if manifest_key is None:
        return manifest
    if manifest_key in manifest:
        return manifest[manifest_key]
    return None


def update_manifest(
    bucket: str, s3_prefix: str, key: str, value: Union[Dict, List, int, str, float]
):
    """
    updates the latest version in the manifest json
    """
    # upsert into the existing manifest
    manifest = get_manifest(bucket, s3_prefix, None)
    if manifest is None:
        manifest = {}
    manifest[key] = value
    if value is None:
        del manifest[key]
    stream = io.StringIO(json.dumps(manifest))
    client = boto3.resource("s3")
    obj = client.Object(bucket, os.path.join(s3_prefix, "manifest.json"))
    obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")


def get_manifest_uc(bucket: str, s3_prefix: str, manifest_key: str):
    """
    This is a copy of get_manifest, but for the tomtom download running in UC enabled clusters.
    Fetches the manifest value associated with a given key.
    If key is None, returns the whole manifest.
    """
    s3_resource = get_s3_resource("samsara-safety-map-data-sources-readwrite")
    obj = s3_resource.Object(bucket, os.path.join(s3_prefix, "manifest.json"))
    try:
        res = obj.get()
    except s3_resource.meta.client.exceptions.NoSuchKey as e:
        print("manifest does not exist -- returning None")
        return None
    manifest = res["Body"].read()
    res["Body"].close()
    manifest = json.loads(manifest)
    if manifest_key is None:
        return manifest
    if manifest_key in manifest:
        return manifest[manifest_key]
    return None


def update_manifest_uc(
    bucket: str, s3_prefix: str, key: str, value: Union[Dict, List, int, str, float]
):
    """
    This is a copy of update_manifest, but for the tomtom download running in UC enabled clusters.
    updates the latest version in the manifest json
    """
    # upsert into the existing manifest
    s3_resource = get_s3_resource("samsara-safety-map-data-sources-readwrite")
    # upsert into the existing manifest
    manifest = get_manifest_uc(bucket, s3_prefix, None)
    if manifest is None:
        manifest = {}
    manifest[key] = value
    if value is None:
        del manifest[key]
    stream = io.StringIO(json.dumps(manifest))
    obj = s3_resource.Object(bucket, os.path.join(s3_prefix, "manifest.json"))
    obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")


# COMMAND ----------


class OSM_S3:
    """
    OSM_S3 is a static class used for helper values related to osm s3 file locations.
    """

    VOLUME_NAME = "/Volumes/s3/safety-map-data-sources"
    BUCKET = "samsara-safety-map-data-sources"
    S3_PREFIX = "osm/"
    MOUNT_PATH = f"/dbfs/mnt/{BUCKET}"
    BASE_OSM_KEY = "base_osm"

    @staticmethod
    def make_s3_key(version_id: str, region: str) -> str:
        return os.path.join(
            OSM_S3.make_s3_prefix(version_id), OSM_S3.make_osm_pbf_filename(region)
        )

    @staticmethod
    def make_osm_pbf_filename(region: str) -> str:
        return f"{region}.osm.pbf"

    @staticmethod
    def make_s3_prefix(version_id: str) -> str:
        return os.path.join(OSM_S3.S3_PREFIX, version_id)

    @staticmethod
    def make_dbfs_mnt_path(version_id: str, region: str) -> str:
        return os.path.join(OSM_S3.MOUNT_PATH, OSM_S3.make_s3_key(version_id, region))

    @staticmethod
    def make_volume_path(version_id: str, region: str) -> str:
        return os.path.join(OSM_S3.VOLUME_NAME, OSM_S3.make_s3_key(version_id, region))


assert OSM_S3.make_s3_key("20200420", USA) == "osm/20200420/USA.osm.pbf"
assert OSM_S3.make_osm_pbf_filename(USA) == "USA.osm.pbf"
assert OSM_S3.make_s3_prefix("20200420") == "osm/20200420"
assert (
    OSM_S3.make_dbfs_mnt_path("20200420", USA)
    == "/dbfs/mnt/samsara-safety-map-data-sources/osm/20200420/USA.osm.pbf"
)
assert (
    OSM_S3.make_volume_path("20200420", USA)
    == "/Volumes/s3/safety-map-data-sources/osm/20200420/USA.osm.pbf"
)


class TOMTOM_S3:
    """
    TOMTOM_S3 is a static class used for helper values related to tomtom s3 file locations.
    """

    VOLUME_NAME = "/Volumes/s3/safety-map-data-sources"
    BUCKET = "samsara-safety-map-data-sources"
    S3_PREFIX = "vendored/tomtom/"
    S3_PREFIX_DECOUPLED = "vendored/tomtom/decoupled/"
    MOUNT_PATH = f"/dbfs/mnt/{BUCKET}"

    @staticmethod
    def make_s3_prefix(version_id: str, product_family: str, region: str) -> str:
        region = region.lower()
        return os.path.join(TOMTOM_S3.S3_PREFIX, version_id, product_family, region)

    @staticmethod
    def make_s3_prefix_decoupled(
        version_id: str, product_family: str, region: str
    ) -> str:
        region = region.lower()
        return os.path.join(
            TOMTOM_S3.S3_PREFIX_DECOUPLED, version_id, product_family, region
        )

    @staticmethod
    def make_dbfs_mnt_path(version_id: str, product_family: str, region: str) -> str:
        return os.path.join(
            TOMTOM_S3.MOUNT_PATH,
            TOMTOM_S3.make_s3_prefix(version_id, product_family, region),
        )

    @staticmethod
    def make_volume_path(version_id: str, product_family: str, region: str) -> str:
        return os.path.join(
            TOMTOM_S3.VOLUME_NAME,
            TOMTOM_S3.make_s3_prefix(version_id, product_family, region),
        )


assert (
    TOMTOM_S3.make_s3_prefix("2020-03-000", "multinet", EUR)
    == "vendored/tomtom/2020-03-000/multinet/eur"
)

assert (
    TOMTOM_S3.make_s3_prefix_decoupled("2020-03-000", "multinet", EUR)
    == "vendored/tomtom/decoupled/2020-03-000/multinet/eur"
)

assert (
    TOMTOM_S3.make_dbfs_mnt_path("2020-03-000", "multinet", EUR)
    == "/dbfs/mnt/samsara-safety-map-data-sources/vendored/tomtom/2020-03-000/multinet/eur"
)

assert (
    TOMTOM_S3.make_volume_path("2020-03-000", "multinet", EUR)
    == "/Volumes/s3/safety-map-data-sources/vendored/tomtom/2020-03-000/multinet/eur"
)


# COMMAND ----------


class DBX_TABLE:
    DB_NAME = "safety_map_data"

    @staticmethod
    def osm_full_ways(version_id: str, region: str) -> str:
        """
        osm_full_ways contains the full OSM ways extract with tags.
        """
        return f"{DBX_TABLE.DB_NAME}.osm_{version_id}__{region}__full_ways"

    @staticmethod
    def tomtom_full_ways(version_id: str, region: str) -> str:
        """
        tomtom_full_ways contains the full tomtom extract (multinet + logistics)
        """
        version_id = version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.tomtom_{version_id}__{region}__full_ways"

    @staticmethod
    def tomtom_full_ways_decoupled(version_id: str, region: str) -> str:
        """
        tomtom_full_ways contains the full tomtom extract (multinet + logistics)
        """
        version_id = version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.tomtom_{version_id}__{region}__full_ways_decoupled"

    @staticmethod
    def tomtom_logistics(version_id: str, region: str) -> str:
        """
        tomtom_logistics contains the logistics extract. This is used for intermediate
        computation to persist the converted raw tomtom files.
        """
        version_id = version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.tomtom_{version_id}__{region}__logistics_raw"

    @staticmethod
    def tomtom_logistics_decoupled(version_id: str, region: str) -> str:
        """
        tomtom_logistics contains the logistics extract. This is used for intermediate
        computation to persist the converted raw tomtom files.
        """
        version_id = version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.tomtom_{version_id}__{region}__logistics_raw_decoupled"

    @staticmethod
    def tomtom_multinet(version_id: str, region: str) -> str:
        """
        tomtom_multinet contains the multinet extract. This is used for intermediate
        computation to persist the converted raw tomtom files.
        """
        version_id = version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.tomtom_{version_id}__{region}__multinet_raw"

    @staticmethod
    def tomtom_multinet_decoupled(version_id: str, region: str) -> str:
        """
        tomtom_multinet contains the multinet extract. This is used for intermediate
        computation to persist the converted raw tomtom files.
        """
        version_id = version_id.replace("-", "")
        return (
            f"{DBX_TABLE.DB_NAME}.tomtom_{version_id}__{region}__multinet_raw_decoupled"
        )

    @staticmethod
    def osm_tomtom_matched_ways(
        osm_version_id: str, region: str, tomtom_version_id: str
    ) -> str:
        """
        osm_tomtom_matched_ways is an intermediate table used in map-matching that relates the OSM way_id with the tomtom_way_id.
        This is joined back with the original osm/tomtom tables for the map-match output.
        """
        tomtom_version_id = tomtom_version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.osm_{osm_version_id}__tomtom_{tomtom_version_id}__{region}__matched_ways"

    @staticmethod
    def osm_tomtom_map_match(osm_version_id: str, region: str, tomtom_version_id: str):
        """
        osm_tomtom_map_match contains the osm/tomtom map-matched result. This contains relevant columns from
        the original tomtom/osm dataset joined in one table.
        """
        tomtom_version_id = tomtom_version_id.replace("-", "")
        return f"{DBX_TABLE.DB_NAME}.osm_{osm_version_id}__tomtom_{tomtom_version_id}__{region}__map_match"

    @staticmethod
    def osm_tomtom_resolved_speed_limits(
        osm_regions_to_version: Dict[str, str], tomtom_version_id: str
    ):
        """
        intermediate table that stores resolved passenger and commercial speed limits for a given way id
        """
        tomtom_version_id = tomtom_version_id.replace("-", "")
        osm_versions = []
        for region, version in osm_regions_to_version.items():
            osm_versions.append(f"{region}_{version}")
        osm_regions_versions = "_".join(osm_versions)
        return f"{DBX_TABLE.DB_NAME}.osm_{osm_regions_versions}__tomtom_{tomtom_version_id}__resolved_speed_limits"

    @staticmethod
    def customer_ways_coverage_analysis():
        return f"{DBX_TABLE.DB_NAME}.customer_ways_coverage_analysis"

    @staticmethod
    def osm_ways_tiles(
        osm_regions_to_version: Dict[str, str], zoom_level: int = 13
    ) -> str:
        """
        osm_ways_tiles contains way_id and tile coordinates for each way.
        This table is used for tile-based speed limit processing.

        Args:
            osm_regions_to_version: Dictionary mapping region to version
            zoom_level: Zoom level for tile coordinates (default: 13)
        """
        osm_versions = []
        for region, version in osm_regions_to_version.items():
            osm_versions.append(f"{region}_{version}")
        osm_regions_versions = "_".join(osm_versions)
        return f"{DBX_TABLE.DB_NAME}.osm_{osm_regions_versions}__ways_tiles_zoom{zoom_level}"

    @staticmethod
    def is_table_exists(table_name: str):
        """
        wrapper to determine whether the table referenced by table_name exists.
        """
        try:
            spark.sql(f"DESCRIBE {table_name}")
        except:
            return False
        return True

    @staticmethod
    def delta_table(
        dataset_name: str, tile_version: str, dataset_version: str = "0"
    ) -> str:
        """
        delta_table contains the delta data between the resolved table and current max version.
        This table is used by downstream notebooks for processing delta changes.

        Args:
            dataset_name: Name of the dataset (e.g., "tomtom", "iowa_dot", "ml_cv")
            tile_version: Tile version string (e.g., "9999")
            dataset_version: Dataset version string (e.g., "0", "1", "2") - defaults to "0"
        """
        return (
            f"{DBX_TABLE.DB_NAME}.{dataset_name}_{tile_version}_{dataset_version}_delta"
        )


assert (
    DBX_TABLE.osm_full_ways("20200420", EUR)
    == "safety_map_data.osm_20200420__EUR__full_ways"
)
assert (
    DBX_TABLE.tomtom_full_ways("2020-03-000", EUR)
    == "safety_map_data.tomtom_202003000__EUR__full_ways"
)
assert (
    DBX_TABLE.tomtom_full_ways_decoupled("2020-03-000", EUR)
    == "safety_map_data.tomtom_202003000__EUR__full_ways_decoupled"
)
assert (
    DBX_TABLE.tomtom_logistics("2020-03-000", EUR)
    == "safety_map_data.tomtom_202003000__EUR__logistics_raw"
)
assert (
    DBX_TABLE.tomtom_logistics_decoupled("2020-03-000", EUR)
    == "safety_map_data.tomtom_202003000__EUR__logistics_raw_decoupled"
)
assert (
    DBX_TABLE.tomtom_multinet("2020-03-000", EUR)
    == "safety_map_data.tomtom_202003000__EUR__multinet_raw"
)
assert (
    DBX_TABLE.tomtom_multinet_decoupled("2020-03-000", EUR)
    == "safety_map_data.tomtom_202003000__EUR__multinet_raw_decoupled"
)
assert (
    DBX_TABLE.osm_tomtom_matched_ways("20200420", EUR, "2020-03-000")
    == "safety_map_data.osm_20200420__tomtom_202003000__EUR__matched_ways"
)
assert (
    DBX_TABLE.osm_tomtom_map_match("20200420", EUR, "2020-03-000")
    == "safety_map_data.osm_20200420__tomtom_202003000__EUR__map_match"
)
assert (
    DBX_TABLE.osm_tomtom_resolved_speed_limits(
        {EUR: "20200420", USA: "20201102"}, "2020-03-000"
    )
    == "safety_map_data.osm_EUR_20200420_USA_20201102__tomtom_202003000__resolved_speed_limits"
)
assert (
    DBX_TABLE.osm_ways_tiles({EUR: "20200420", USA: "20201102"})
    == "safety_map_data.osm_EUR_20200420_USA_20201102__ways_tiles_zoom13"
)
assert DBX_TABLE.is_table_exists("invalid_table") == False
assert DBX_TABLE.is_table_exists("kinesisstats.location") == True

# COMMAND ----------


def submit_parallel_notebook_runs(
    notebook: str, params_list: List[Dict[str, str]]
) -> List[str]:
    """
    This should be used for parallelizing unbalanced workloads, or workloads with high i/o latency.
    Note that there are diminishing returns for parallelizing already highly parallelizable notebooks.
    """

    def parallelize():
        # Creating too many notebooks in parallel can cause the driver to crash, so set a sensible default parallelization.
        with ThreadPoolExecutor(max_workers=4) as ec:
            return [ec.submit(run_notebook, notebook, params) for params in params_list]

    res = parallelize()
    result = [i.result(timeout=3600) for i in res]  # This is a blocking call.
    return result


# COMMAND ----------


def unitize_tomtom_passenger_speed_limit(
    max_speed_unit: str, passenger_speed_limit: Optional[int]
) -> str:
    """
    Unitizes the passed in passenger_speed_limit with the given max_speed_unit by
    by adding a space between the limit and the unit.
    """
    if passenger_speed_limit is None:
        return None

    return f"{passenger_speed_limit} {max_speed_unit}"


assert unitize_tomtom_passenger_speed_limit("kph", "30") == "30 kph"
assert unitize_tomtom_passenger_speed_limit("mph", "40") == "40 mph"
assert unitize_tomtom_passenger_speed_limit("kph", None) == None


def unitize_osm_passenger_speed_limit(osm_speed_limit: Optional[str]) -> Optional[str]:
    """
    unitizes the osm speed limit. Note that osm limits are strings, optionally containing
    the unit as a suffix. If no unit is specified, then the value is in kph.
    """
    if osm_speed_limit is None:
        return None

    if osm_speed_limit.find(" mph") > -1:
        return osm_speed_limit

    if osm_speed_limit.find(" kph") == -1:
        return f"{osm_speed_limit} kph"
    elif osm_speed_limit.find(" kph") > -1:
        return osm_speed_limit


assert unitize_osm_passenger_speed_limit("30") == "30 kph"
assert unitize_osm_passenger_speed_limit("25 mph") == "25 mph"
assert unitize_osm_passenger_speed_limit("30 kph") == "30 kph"
assert unitize_osm_passenger_speed_limit(None) == None


def unitize_tomtom_commercial_speed_limits(
    max_speed_unit: str, commercial_speed_limit_map: Optional[Dict[int, int]]
) -> Optional[Dict[int, str]]:
    """
    unitizes the commercial_speed_limit_map according to the max_max_speed_unit.
    """
    if commercial_speed_limit_map is None:
        return None

    unitized_map = {}
    for v_type, speed_limit in commercial_speed_limit_map.items():
        unitized_map[v_type] = f"{speed_limit} {max_speed_unit}"
    return unitized_map


unitize_tomtom_commercial_speed_limits_udf = udf(
    unitize_tomtom_commercial_speed_limits, MapType(IntegerType(), StringType())
)
assert unitize_tomtom_commercial_speed_limits("kph", None) == None
assert unitize_tomtom_commercial_speed_limits("mph", {1: 10, 2: 20}) == {
    1: "10 mph",
    2: "20 mph",
}
assert unitize_tomtom_commercial_speed_limits("kph", {1: 10, 2: 20}) == {
    1: "10 kph",
    2: "20 kph",
}

# COMMAND ----------


KPH_IN_MPH = 1.60934


def osm_passenger_limit_unit_fixer(
    region: str, maxspeed_str: Optional[str]
) -> Optional[str]:
    """
    OSM limits without a unit are assumed to be in kph. Sometimes data entry errors occur where
    the contributor omits the unit, treating it as kph rather than mph. In the US, assume that
    limits divisible by 5 are meant to be entered as mph, so add a "mph" unit suffix.
    """
    # Only apply this fix in the USA for now.
    if region != USA:
        return maxspeed_str
    if maxspeed_str is None:
        return None
    mph_unit = maxspeed_str.find(" mph")
    kph_unit = maxspeed_str.find(" kph")
    unit_exists = (mph_unit > -1) or (kph_unit > -1)
    if unit_exists:
        return maxspeed_str
    try:
        maxspeed_int = int(maxspeed_str)
        # If the maxspeed is divisible by 5, assume a data entry error
        # and treat it as an mph speed limit.
        if maxspeed_int % 5 == 0:
            return maxspeed_str + " mph"
        return maxspeed_str
    except ValueError:
        # Invalid speed limits will be handled downstream,
        # pass through the value for now.
        return None


osm_passenger_limit_unit_fixer_udf = udf(osm_passenger_limit_unit_fixer, StringType())


assert osm_passenger_limit_unit_fixer(USA, "30 kph") == "30 kph"
assert osm_passenger_limit_unit_fixer(USA, "30 mph") == "30 mph"
assert osm_passenger_limit_unit_fixer(USA, "30") == "30 mph"
assert osm_passenger_limit_unit_fixer(USA, "32") == "32"
assert osm_passenger_limit_unit_fixer(EUR, "30 kph") == "30 kph"
assert osm_passenger_limit_unit_fixer(EUR, "30 mph") == "30 mph"
assert osm_passenger_limit_unit_fixer(EUR, "30") == "30"
assert osm_passenger_limit_unit_fixer(EUR, "32") == "32"


def osm_normalize_speed_kph(maxspeed_str: Optional[str]) -> Optional[int]:
    """
    Normalize a maxspeed in the form of 'x mph', stripping the unit and converting to a rounded kph integer
    """
    if maxspeed_str is None:
        return None
    maxspeed_str = str(maxspeed_str)
    mph_unit = maxspeed_str.find(" mph")
    kph_unit = maxspeed_str.find(" kph")
    try:
        if (mph_unit == -1) and (kph_unit == -1):
            return int(maxspeed_str)
        if mph_unit != -1:
            return int(round(int(maxspeed_str[:mph_unit]) * KPH_IN_MPH, 0))
        if kph_unit != -1:
            return int(maxspeed_str[:kph_unit])
    except ValueError:
        # Invalid speedlimit, so return null
        return None
    return None


assert osm_normalize_speed_kph("30 mph") == 48
assert osm_normalize_speed_kph("30 kph") == 30
assert osm_normalize_speed_kph("30") == 30
assert osm_normalize_speed_kph(None) == None
assert osm_normalize_speed_kph("invalid") == None


def tomtom_normalize_speed_kph(
    maxspeed: Optional[int], unit: Optional[str]
) -> Optional[int]:
    if maxspeed is None:
        return None
    if unit == "kph":
        return int(maxspeed)
    if unit == "mph":
        return int(round(int(maxspeed) * KPH_IN_MPH, 0))
    return None


def osm_tomtom_speed_selector(
    osm_speed_limit: Optional[str],
    tomtom_speed_limit: Optional[int],
    tomtom_speed_unit: Optional[str],
) -> Row:
    """
    osm_tomtom_speed_selector picks the greater of the osm
    and tomtom speed limit
    """
    TOMTOM = "tomtom"
    OSM = "osm"

    # Normalize osm/tomtom speed limit to kph (int)
    normalized_osm = osm_normalize_speed_kph(osm_speed_limit)
    normalized_tomtom = tomtom_normalize_speed_kph(
        tomtom_speed_limit, tomtom_speed_unit
    )

    if normalized_osm is None and normalized_tomtom is None:
        return Row(None, None)

    if normalized_osm is None:
        return Row(
            unitize_tomtom_passenger_speed_limit(tomtom_speed_unit, tomtom_speed_limit),
            TOMTOM,
        )

    if normalized_tomtom is None:
        return Row(unitize_osm_passenger_speed_limit(osm_speed_limit), OSM)

    if normalized_tomtom >= normalized_osm:
        if normalized_tomtom > MAX_SPEED_LIMIT_KPH:
            if tomtom_speed_unit == "mph":
                return Row(f"{MAX_SPEED_LIMIT_MPH} {tomtom_speed_unit}", TOMTOM)
            return Row(f"{MAX_SPEED_LIMIT_KPH} {tomtom_speed_unit}", TOMTOM)
        return Row(
            unitize_tomtom_passenger_speed_limit(tomtom_speed_unit, tomtom_speed_limit),
            TOMTOM,
        )

    elif normalized_osm > normalized_tomtom:
        if normalized_osm > MAX_SPEED_LIMIT_KPH:
            if osm_speed_limit.find(" mph") > -1:
                return Row(f"{MAX_SPEED_LIMIT_MPH} mph", OSM)
            return Row(f"{MAX_SPEED_LIMIT_KPH} kph", OSM)
        return Row(unitize_osm_passenger_speed_limit(osm_speed_limit), OSM)


schema = StructType(
    [
        StructField("speed_limit", StringType(), True),
        StructField("data_source", StringType(), True),
    ]
)
osm_tomtom_speed_selector_udf = udf(osm_tomtom_speed_selector, schema)

assert osm_tomtom_speed_selector("30 mph", "30", "kph") == Row("30 mph", "osm")
assert osm_tomtom_speed_selector("30", "30", "mph") == Row("30 mph", "tomtom")
assert osm_tomtom_speed_selector("30", None, None) == Row("30 kph", "osm")
assert osm_tomtom_speed_selector("30 kph", None, None) == Row("30 kph", "osm")
assert osm_tomtom_speed_selector(None, "30", "kph") == Row("30 kph", "tomtom")
assert osm_tomtom_speed_selector("210 kph", "30", "kph") == Row(
    f"{MAX_SPEED_LIMIT_KPH} kph", "osm"
)
assert osm_tomtom_speed_selector("30 kph", "210", "mph") == Row(
    f"{MAX_SPEED_LIMIT_MPH} mph", "tomtom"
)


def should_upload_limit(
    osm_speed_limit: Optional[str],
    osm_raw_limit: Optional[str],
    selected_speed_limit: Optional[str],
) -> bool:
    """
    Pass in the osm speed limit and selected speed limit to determine whether
    the selected_speed_limit should be uploaded as part of the dataset.
    Note that we assume that we only update limits that have changed.

    osm_speed_limit is the osm limit with fixed units;
    osm_raw_limit is the uncorrected limit directly parsed from OSM.
    If these two parameters are different, then we have preprocessed the
    OSM limit to correct units and should upload the limit as an override
    for the ingestion path.

    This works by normalizing both the osm and tomtom limits to kph and comparing the normalized values.
    """
    osm_normalized = osm_normalize_speed_kph(osm_speed_limit)
    selected_normalized = osm_normalize_speed_kph(selected_speed_limit)
    if osm_normalized != selected_normalized:
        return True
    if osm_speed_limit != osm_raw_limit:
        return True
    return False


should_upload_limit_udf = udf(should_upload_limit, BooleanType())

assert should_upload_limit(None, None, "30 mph") == True
assert should_upload_limit("20", "20", None) == True
assert should_upload_limit("20", "20", "20 kph") == False
assert should_upload_limit("20", "20", "20 mph") == True
assert should_upload_limit(None, None, None) == False
assert should_upload_limit("20", "20 mph", None) == True
assert should_upload_limit("20 mph", "20", "20 mph") == True

# COMMAND ----------


def select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    dataset_name: str,
    dataset_speed_limit_milliknots: Optional[int],
    osmtomtom_resolved_datasource: Optional[str],
    osmtomtom_resolved_speed_limit: Optional[str],
) -> Row:
    """
    calculate the maximum speed limit between osmtomtom_resolved_speed_limit and dataset_speed_limit_milliknots
    return 2 things: speed_limit and data source

    dataset_speed_limit_milliknots is the speed limit from the decoupled speed limits dataset like regulatory or ml-cv
    """

    # Convert osmtomtom_resolved_speed_limit to kph
    osmtomtom_resolved_speed_limit_kph = osm_normalize_speed_kph(
        osmtomtom_resolved_speed_limit
    )

    # Convert dataset_speed_limit_milliknots to kph
    if dataset_speed_limit_milliknots is not None:
        dataset_speed_limit_milliknots_kph = int(
            round(dataset_speed_limit_milliknots * 0.001852)
        )
    else:
        dataset_speed_limit_milliknots_kph = None

    # Determine the maximum speed limit
    if (
        osmtomtom_resolved_speed_limit_kph is None
        and dataset_speed_limit_milliknots_kph is None
    ):
        return Row(None, None)

    if osmtomtom_resolved_speed_limit_kph is None:
        return Row(
            f"{int(round(dataset_speed_limit_milliknots_kph))} kph", dataset_name
        )

    if dataset_speed_limit_milliknots_kph is None:
        return Row(osmtomtom_resolved_speed_limit, osmtomtom_resolved_datasource)

    if dataset_speed_limit_milliknots_kph >= osmtomtom_resolved_speed_limit_kph:
        return Row(
            f"{int(round(dataset_speed_limit_milliknots_kph))} kph", dataset_name
        )

    return Row(osmtomtom_resolved_speed_limit, osmtomtom_resolved_datasource)


select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits_udf = udf(
    select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits, schema
)

assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 10000, "osm", "20 kph"
) == Row("20 kph", "osm")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 10799, "osm", "20 kph"
) == Row(
    "20 kph", "iowa_dot"
)  # prefer dataset when equal for proper attribution
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 20000, "osm", "20 kph"
) == Row("37 kph", "iowa_dot")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 10000, "osm", "20 mph"
) == Row("20 mph", "osm")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 17380, "osm", "20 mph"
) == Row(
    "32 kph", "iowa_dot"
)  # prefer dataset when equal for proper attribution
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 20000, "osm", "20 mph"
) == Row("37 kph", "iowa_dot")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", None, "osm", "20 mph"
) == Row("20 mph", "osm")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", None, None, None
) == Row(None, None)
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 20000, None, None
) == Row("37 kph", "iowa_dot")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 20000, None, "20 mph"
) == Row("37 kph", "iowa_dot")
assert select_max_speed_limit_amongst_osm_tomtom_decoupled_speed_limits(
    "iowa_dot", 20000, "tomtom", None
) == Row("37 kph", "iowa_dot")

# COMMAND ----------


def find_latest_tomtom_osm_resolved_speed_limit_table() -> str:
    """
    Returns the latest table name of TOMTOM and OSM resolved_speed_limits table
    e.g. osm_can_20240619_eur_20240619_mex_20240619_usa_20240619__tomtom_20240600370003__resolved_speed_limits
    """
    tables_df = spark.sql("SHOW TABLES IN safety_map_data")
    pattern = r"osm_(can|eur|mex|usa)_(\d{8})_.*__tomtom_(\d+)__resolved_speed_limits$"

    # Extract dates and filter relevant tables
    tables_with_dates_df = (
        tables_df.withColumn("osm_date", regexp_extract(col("tableName"), pattern, 2))
        .withColumn("tomtom_date", regexp_extract(col("tableName"), pattern, 3))
        .filter(col("osm_date").isNotNull() & col("tomtom_date").isNotNull())
    )

    # Order by dates and get the latest table
    latest_table_df = tables_with_dates_df.orderBy(
        col("osm_date").desc(), col("tomtom_date").desc()
    ).limit(1)

    latest_table_name = latest_table_df.collect()[0]["tableName"]
    return f"safety_map_data.{latest_table_name}"


def find_tomtom_osm_resolved_speed_limit_table_by_version(
    osm_dataset_version: str,
) -> str:
    """
    Returns a tomtom osm resolved speed limit table name based on the specified OSM dataset version.
    e.g. osm_can_20240619_eur_20240619_mex_20240619_usa_20240619__tomtom_20240600370003__resolved_speed_limits

    Args:
        osm_dataset_version: The OSM dataset version string. Expected format: YYYYMMDD

    Returns:
        The full table name including schema prefix

    Raises:
        ValueError: If osm_dataset_version format is invalid or no matching table is found
    """
    import re

    # Validate format: must be exactly 8 digits (YYYYMMDD)
    if not osm_dataset_version or not re.match(r"^\d{8}$", osm_dataset_version):
        raise ValueError(
            f"Invalid OSM dataset version format: '{osm_dataset_version}'. Expected format: YYYYMMDD (e.g., '20240619')"
        )

    tables_df = spark.sql("SHOW TABLES IN safety_map_data")
    # Pattern to match tables with the specific OSM version
    # This looks for tables where any of the region dates match the specified version
    pattern = (
        rf"osm_(can|eur|mex|usa)_(\d{{8}})_.*__tomtom_(\d+)__resolved_speed_limits$"
    )

    # Extract dates and filter relevant tables
    tables_with_dates_df = (
        tables_df.withColumn("osm_date", regexp_extract(col("tableName"), pattern, 2))
        .withColumn("tomtom_date", regexp_extract(col("tableName"), pattern, 3))
        .filter(col("osm_date").isNotNull() & col("tomtom_date").isNotNull())
        .filter(col("osm_date") == osm_dataset_version)
    )

    # Order by tomtom date (latest tomtom version for the specified OSM version)
    matching_table_df = tables_with_dates_df.orderBy(col("tomtom_date").desc()).limit(1)

    matching_table_rows = matching_table_df.collect()
    if not matching_table_rows:
        # No matching table found for the specified version
        raise ValueError(
            f"No tomtom osm resolved speed limit table found for OSM dataset version: {osm_dataset_version}"
        )

    table_name = matching_table_rows[0]["tableName"]
    return f"safety_map_data.{table_name}"


def find_latest_dataset_resolved_speed_limit_table(dataset_name: str) -> Optional[str]:
    """
    Returns the latest table name of dataset resolved_speed_limits table
    e.g. ml_cv_resolved_speed_limits__20250815
    """
    tables_df = spark.sql("SHOW TABLES IN safety_map_data")
    pattern = f"{dataset_name}_resolved_speed_limits__(\d{{8}})"

    # Extract dates and filter relevant tables
    tables_with_dates_df = tables_df.withColumn(
        "extracted_date", regexp_extract(col("tableName"), pattern, 1)
    ).filter(col("extracted_date").isNotNull())

    # Order by dates and get the latest table
    latest_table_df = tables_with_dates_df.orderBy(col("extracted_date").desc()).limit(
        1
    )

    latest_table_rows = latest_table_df.collect()
    if not latest_table_rows:
        # No matching table found
        return None
    else:
        latest_table_name = latest_table_df.collect()[0]["tableName"]
        return f"safety_map_data.{latest_table_name}"


def find_latest_incremental_dataset_resolved_speed_limit_table(
    dataset_name: str,
) -> Optional[str]:
    """
    Returns the latest table name of incremental dataset resolved_speed_limits table
    e.g. ml_cv_resolved_speed_limits__incremental_20260114
    """
    tables_df = spark.sql("SHOW TABLES IN safety_map_data")
    pattern = f"{dataset_name}_resolved_speed_limits__incremental_(\d{{8}})"

    # Extract dates and filter relevant tables
    tables_with_dates_df = tables_df.withColumn(
        "extracted_date", regexp_extract(col("tableName"), pattern, 1)
    ).filter(col("extracted_date").isNotNull())

    # Order by dates and get the latest table
    latest_table_df = tables_with_dates_df.orderBy(col("extracted_date").desc()).limit(
        1
    )

    latest_table_rows = latest_table_df.collect()
    if not latest_table_rows:
        # No matching table found
        return None
    else:
        latest_table_name = latest_table_df.collect()[0]["tableName"]
        return f"safety_map_data.{latest_table_name}"
