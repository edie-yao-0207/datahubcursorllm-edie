# Databricks notebook source
# MAGIC %md
# MAGIC # Lib OSM Parquetizer
# MAGIC
# MAGIC Run `parquetize_osm` to generate and upload parquet files from a given osm.pbf file. The generated parquet files are uploaded to the same s3 path as the source osm.pbf file.

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import glob
import json
import os
import shutil
import subprocess

import boto3
import botocore.exceptions

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

# Mount the samsara-dataplatform-deployed-artifacts bucket so that
# we can use the osm-parquetizer jar
aws_bucket_name = "samsara-dataplatform-deployed-artifacts"
mount_name = "samsara-dataplatform-deployed-artifacts"
try:
    dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
except Exception as e:
    print("Directory already mounted")

# COMMAND ----------


TMP_DIR = "/local_disk0/tmp/osm"


def check_parquet_exists(version_id: str, region: str):
    """
    checks whether parquet files for the given version_id
    and region exist.
    """
    filename = OSM_S3.make_osm_pbf_filename(region)
    key = os.path.join(OSM_S3.make_s3_prefix(version_id), filename)
    files = [key + ".way.parquet", key + ".node.parquet"]
    # boto3.client is only used for non-UC enabled clusters. (instance-profile: safetyplatform-cluster)
    # Please get s3 client from boto3_helpers.get_s3_client for UC enabled clusters. (instance-profile: unity-catalog-cluster)
    s3 = boto3.client("s3")
    for file in files:
        try:
            s3.head_object(Bucket=OSM_S3.BUCKET, Key=file)
        except botocore.exceptions.ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                # Not found
                return False
    return True


def parquetize_osm(version_id: str, region: str):
    """
    parquetize_osm generates 2 parquet tables, filename.way.parquet and filename.node.parquet from a given filename stored in s3.
    The output parquet tables are written to the same bucket and prefix as the input file.
    """
    # boto3.client is only used for non-UC enabled clusters. (instance-profile: safetyplatform-cluster)
    # Please get s3 client from boto3_helpers.get_s3_client for UC enabled clusters. (instance-profile: unity-catalog-cluster)
    client = boto3.client("s3")
    filename = OSM_S3.make_osm_pbf_filename(region)
    local_path = os.path.join(TMP_DIR, filename)
    s3_key = OSM_S3.make_s3_key(version_id, region)
    print(f"downloading {s3_key} to {local_path}")
    client.download_file(OSM_S3.BUCKET, s3_key, local_path)

    run_parquetize(local_path)

    print("uploading parquet tables")
    client.upload_file(
        local_path + ".way.parquet",
        OSM_S3.BUCKET,
        s3_key + ".way.parquet",
        {"ACL": "bucket-owner-full-control"},
    )
    client.upload_file(
        local_path + ".node.parquet",
        OSM_S3.BUCKET,
        s3_key + ".node.parquet",
        {"ACL": "bucket-owner-full-control"},
    )

    for fl in glob.glob(os.path.join(f"{TMP_DIR}", f"{region}*")):
        os.remove(fl)
    for fl in glob.glob(os.path.join(f"{TMP_DIR}", "/.*crc")):
        os.remove(fl)


def run_parquetize(local_path):
    print(f"parquetizing file at {local_path}")
    osm_parquetizer_jar_path = "/dbfs/mnt/samsara-dataplatform-deployed-artifacts/jars/osm-parquetizer-1.0.2.jar"
    # Skip generating relations.
    process = subprocess.Popen(
        [
            "java",
            "-jar",
            osm_parquetizer_jar_path,
            local_path,
            "--exclude-metadata",
            "--no-relations",
            "--pbf-threads",
            "7",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        exit_notebook(str(stderr))
    print("done")


# COMMAND ----------

try:
    print("making temp dir")
    os.mkdir(TMP_DIR)
    for region, version_id in osm_regions_to_version.items():
        if check_parquet_exists(version_id, region):
            print(f"{version_id}/{region} parquet files already exist. Skipping...")
            continue
        parquetize_osm(version_id, region)
finally:
    print(f"cleaning up {TMP_DIR}")
    shutil.rmtree(f"{TMP_DIR}")

# COMMAND ----------

exit_notebook()

# COMMAND ----------
