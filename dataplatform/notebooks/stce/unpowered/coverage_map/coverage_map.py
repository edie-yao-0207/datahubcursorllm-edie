# Databricks notebook source
# MAGIC %md
# MAGIC ## Readme
# MAGIC
# MAGIC This note book will process the `kinesisstats.location` location stats to GeoJson and convert it into vector tiles. The vector tiles are uploaded to samsara-network-coverage S3 bucket. The frontend fetches these vector tiles from S3 and render it as a Overlay.
# MAGIC
# MAGIC Scheduling this second Saturday of each month to keep the data updated with the latest location stats. The scheduled cron job should only run on weekend to fetch the week's stats on Wednesday and replace the vector tiles in the S3.
# MAGIC
# MAGIC Optional Input Parameter: Filter date - format "YYYY-MM-DD"
# MAGIC
# MAGIC For databricks alert refer to the runbook here: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4458943/Coverage+Map+-+Runbook#Databricks
# MAGIC
# MAGIC If the alert is during non-office hours, feel free to mute the alert and can be investigated during office hours.

# COMMAND ----------

# DBTITLE 1,Install GDAL library
# MAGIC %sh
# MAGIC rm -r /var/cache/apt/archives/* /var/lib/apt/lists/*
# MAGIC sudo apt-get clean
# MAGIC sudo add-apt-repository ppa:ubuntugis/ppa && apt-get update
# MAGIC sudo apt-get install -y gdal-bin libgdal-dev
# MAGIC # GDAL is used to convert GeoJson files to MVT: Mapbox Vector Tiles format

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install h3==4.1.2
# MAGIC %pip install geojson==3.1.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Register Imports
import h3
from pyspark.sql.types import ArrayType, StringType, DoubleType

spark.udf.register("geo_to_h3", h3.latlng_to_cell)
spark.udf.register("h3_to_geo", h3.cell_to_latlng, ArrayType(DoubleType()))
spark.udf.register("cell_area", h3.cell_area)
spark.udf.register("h3_to_parent", h3.cell_to_parent, StringType())

# COMMAND ----------

# DBTITLE 1,Create network_coverage_location_stats table and delete previous data if exists
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS connectedequipment_dev.network_coverage_location_stats(
# MAGIC   date DATE,
# MAGIC   hex_id string,
# MAGIC   latitude double,
# MAGIC   longitude double,
# MAGIC   orgs bigint,
# MAGIC   device_hours bigint
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Function for parsing filter date param
# Define filter_date widget with a default value if it doesn't exist
if not "filter_date" in dbutils.widgets.getAll():
    dbutils.widgets.text("filter_date", "")

from datetime import datetime


def parse_date(date_string, date_format="%Y-%m-%d"):
    """
    Parses a date from a string. Handles empty or invalid date strings.

    Parameters:
    date_string (str): The date string to parse.
    date_format (str): The expected format of the date string (default is "%Y-%m-%d").

    Returns:
    datetime.date: Parsed date object, or None if input is empty or invalid.
    """
    if not date_string:
        return ""

    try:
        return datetime.strptime(date_string, date_format).date()
    except ValueError:
        return ""


# COMMAND ----------

# DBTITLE 1,Process whether to update the table with current filter data
# Define the table name and the condition to check
table_name = "connectedequipment_dev.network_coverage_location_stats"
param_filter_date = parse_date(dbutils.widgets.get("filter_date"))
current_filter_date = spark.sql(
    """SELECT DATE_ADD(CURRENT_DATE, CASE
        WHEN DAYOFWEEK(CURRENT_DATE) = 1 THEN 3 -- Sunday
        WHEN DAYOFWEEK(CURRENT_DATE) = 2 THEN 2 -- Monday
        WHEN DAYOFWEEK(CURRENT_DATE) = 3 THEN 1 -- Tuesday
        WHEN DAYOFWEEK(CURRENT_DATE) = 4 THEN 0 -- Wednesday
        WHEN DAYOFWEEK(CURRENT_DATE) = 5 THEN -1 -- Thursday
        WHEN DAYOFWEEK(CURRENT_DATE) = 6 THEN -2 -- Friday
        WHEN DAYOFWEEK(CURRENT_DATE) = 7 THEN -3 -- Saturday
    END) AS filter_date"""
).collect()[0]["filter_date"]
filter_date = spark.sql(
    f"SELECT COALESCE(NULLIF('{param_filter_date}', ''), '{current_filter_date}') AS filter_date"
).collect()[0]["filter_date"]

# Check if data exists in the table
row_exists_query = f"SELECT COUNT(1) FROM {table_name} WHERE date = '{filter_date}'"
row_count = spark.sql(row_exists_query).collect()[0][0]

# Conditionally truncate the table
if row_count == 0:
    print(f"No rows with current filter date found. Updating data in {table_name}.")
    spark.sql(f"TRUNCATE TABLE {table_name}")
else:
    print(f"Row with filter date exists. Skip updating data in {table_name}.")

# set parameter to decide whether to update or skip data processing to table
dbutils.widgets.text("update_data", str(row_count))
# set the updated filter date
dbutils.widgets.text("updated_filter_date", filter_date)

# COMMAND ----------

# DBTITLE 1,Convert location states to H3 hex index and insert to network_coverage_location_stats table
# MAGIC %sql
# MAGIC INSERT INTO connectedequipment_dev.network_coverage_location_stats
# MAGIC         SELECT
# MAGIC     date,
# MAGIC     hex_id,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     COUNT(DISTINCT org_id) AS orgs,
# MAGIC     COUNT(DISTINCT CONCAT(ROUND(time / 1000 / 60 / 60), device_id)) AS device_hours
# MAGIC FROM
# MAGIC     (
# MAGIC     SELECT
# MAGIC         data.date,
# MAGIC         geo_to_h3(data.value.latitude, data.value.longitude, 9) AS hex_id,
# MAGIC         h3_to_geo(geo_to_h3(data.value.latitude, data.value.longitude, 9))[0] AS latitude,
# MAGIC         h3_to_geo(geo_to_h3(data.value.latitude, data.value.longitude, 9))[1] AS longitude,
# MAGIC         data.org_id,
# MAGIC         data.device_id,
# MAGIC         data.time
# MAGIC     FROM
# MAGIC         kinesisstats.location data
# MAGIC     JOIN
# MAGIC         clouddb.organizations organizations ON data.org_id = organizations.id
# MAGIC     JOIN
# MAGIC         clouddb.gateways gateways ON data.org_id = gateways.org_id AND data.device_id = gateways.device_id
# MAGIC     LEFT JOIN
# MAGIC         datamodel_core_silver.stg_devices_settings_first_on_date spoofs
# MAGIC         ON spoofs.device_id = data.device_id AND spoofs.date = data.date
# MAGIC     WHERE
# MAGIC         (${update_data}) = 0
# MAGIC         AND data.date = '${updated_filter_date}'
# MAGIC         AND data.org_id NOT IN (1, 5289, 2000016)
# MAGIC         AND data.value.latitude IS NOT NULL
# MAGIC         AND data.value.longitude IS NOT NULL
# MAGIC         AND organizations.internal_type = 0
# MAGIC         AND gateways.product_id IN (53, 24, 35, 89, 125, 68, 83, 178, 90, 140, 143, 144)
# MAGIC         AND spoofs.device_config.gps_config.gps_spoof_entries IS NULL
# MAGIC )
# MAGIC GROUP BY
# MAGIC     date, hex_id, latitude, longitude;

# COMMAND ----------

# DBTITLE 1,Create View for Resolution 9
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW crux_coverage_stats_r9 as
# MAGIC SELECT date, 9 as hex_res, hex_id, latitude, longitude,
# MAGIC   sqrt(cell_area(hex_id, 'm^2')/pi()) as radius_m,
# MAGIC   orgs, device_hours
# MAGIC FROM connectedequipment_dev.network_coverage_location_stats
# MAGIC WHERE orgs > 1

# COMMAND ----------

# DBTITLE 1,Create View for Resolution 5
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW crux_coverage_stats_r5 as
# MAGIC with res5 as (
# MAGIC select
# MAGIC     date,
# MAGIC     5 as hex_res,
# MAGIC     h3_to_parent(hex_id, 5) as hex_id,
# MAGIC     device_hours,
# MAGIC     case
# MAGIC       when device_hours > 1000 then 5
# MAGIC       when device_hours > 100 then 4
# MAGIC       when device_hours > 50 then 3
# MAGIC       when device_hours > 24 then 2
# MAGIC       when device_hours > 1 then 1
# MAGIC       else 0
# MAGIC     end as device_hours_level
# MAGIC   from crux_coverage_stats_r9
# MAGIC )
# MAGIC select
# MAGIC   date,
# MAGIC   hex_res,
# MAGIC   hex_id,
# MAGIC   h3_to_geo(hex_id)[0] as latitude,
# MAGIC   h3_to_geo(hex_id)[1] as longitude,
# MAGIC   sqrt(cell_area(hex_id, 'm^2')/pi()) as radius_m,
# MAGIC   max(device_hours) max_device_hours,
# MAGIC   max(device_hours_level) max_device_hours_level
# MAGIC from res5
# MAGIC group by 1,2,3,4,5

# COMMAND ----------

# DBTITLE 1,Transforming Res 9 Spatial Data into GeoJSON Format
import json


def geojson_r9(row):
    geojson_dict = {
        "type": "Feature",
        "id": row.int_h3_id,
        "geometry": {"type": "Point", "coordinates": [row.longitude, row.latitude]},
        "properties": {
            "longitude": row.longitude,
            "latitude": row.latitude,
            "device_hours": row.device_hours,
        },
    }

    return json.dumps(geojson_dict)


spark.udf.register("geojson_r9", geojson_r9)

r9df = spark.sql(
    """with int_id (
  select
    conv(hex_id, 16, 10) as int_h3_id,
    *
  from crux_coverage_stats_r9
)
select geojson_r9(struct(*)) as geojson
from int_id
"""
)

r9_geojson_ld = "\n".join(row.geojson for row in r9df.collect())

# COMMAND ----------

# DBTITLE 1,Transforming Res 5 Spatial Data into GeoJSON Format
import geojson

r5_output = []
for row in spark.sql(
    """
                     SELECT conv(hex_id, 16, 10) as int_h3_id, *
                     FROM crux_coverage_stats_r5
                     """
).collect():
    geojson_dict = {
        "type": "Feature",
        "id": row.int_h3_id,
        "geometry": {"type": "Point", "coordinates": [row.longitude, row.latitude]},
        "properties": {
            "longitude": row.longitude,
            "latitude": row.latitude,
            "radius_m": row.radius_m,
            "max_device_hours": row.max_device_hours,
            "max_device_hours_level": row.max_device_hours_level,
        },
    }
    r5_output.append(geojson.dumps(geojson_dict))
    r5_geojson_ld = ("\n").join(r5_output)

# COMMAND ----------

# DBTITLE 1,Writing GeoJSON Data to a File in local Disk
# Write GeoJson files to local disk
with open("r5_geojson_ld.geojson", "w") as file:
    file.write(r5_geojson_ld)

with open("r9_geojson_ld.geojson", "w") as file:
    file.write(r9_geojson_ld)

# COMMAND ----------

# DBTITLE 1,Convert GeoJson to Mapbox Vector tiles and output to mvt directory
# MAGIC %sh
# MAGIC ogr2ogr -f MVT mvt r5_geojson_ld.geojson -dsco FORMAT=DIRECTORY -dsco TILE_EXTENSION=mvt -dsco MINZOOM=0 -dsco MAXZOOM=10 -dsco COMPRESS=NO
# MAGIC
# MAGIC # Convert GeoJson to Mapbox Vector tiles for resolution 5 and zoom levels 0-10 - output to directory (mvt)
# MAGIC # https://gdal.org/drivers/vector/mvt.html#examples

# COMMAND ----------

# DBTITLE 1,Upload the Vector tiles in MVT directory to S3 bucket
import os
import sys

# Specify the S3 bucket volume
bucket_volume_path = "/Volumes/s3/network-coverage/root/"

# Get the absolute file path
directory_path = os.path.abspath("mvt")


def uploadDirectory(directory_path):
    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            s3_key = os.path.relpath(os.path.join(root, filename))
            volume_file_path = os.path.join(bucket_volume_path, s3_key)

            # Ensure the directory exists
            os.makedirs(os.path.dirname(volume_file_path), exist_ok=True)

            # Copy file to the volume
            file_path = os.path.join(root, filename)
            dbutils.fs.cp(f"file:{file_path}", volume_file_path)

            print(f"File {file_path} uploaded to {volume_file_path}")


uploadDirectory(directory_path)


# COMMAND ----------

# DBTITLE 1,Remove MVT directory
# MAGIC %sh
# MAGIC rm -rf mvt

# COMMAND ----------

# DBTITLE 1,Convert GeoJson to Mapbox Vector tiles and output to Coverage directory
# MAGIC %sh
# MAGIC ogr2ogr -f MVT mvt r9_geojson_ld.geojson -dsco FORMAT=DIRECTORY -dsco TILE_EXTENSION=mvt -dsco MINZOOM=9 -dsco MAXZOOM=14 -dsco COMPRESS=NO
# MAGIC # Convert GeoJson to Mapbox Vector tiles for resolution 9 and zoom levels 9-14 - output to directory (mvt)

# COMMAND ----------

# DBTITLE 1,Upload the Vector tiles in MVT directory to S3 bucket
import os
import sys

# Specify the S3 bucket volume
bucket_volume_path = "/Volumes/s3/network-coverage/root/"

# Get the absolute file path
directory_path = os.path.abspath("mvt")


def uploadDirectory(directory_path):
    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            s3_key = os.path.relpath(os.path.join(root, filename))
            volume_file_path = os.path.join(bucket_volume_path, s3_key)

            # Ensure the directory exists
            os.makedirs(os.path.dirname(volume_file_path), exist_ok=True)

            # Copy file to the volume
            file_path = os.path.join(root, filename)
            dbutils.fs.cp(f"file:{file_path}", volume_file_path)

            print(f"File {file_path} uploaded to {volume_file_path}")


uploadDirectory(directory_path)

# COMMAND ----------

# DBTITLE 1,Remove all local files created for the process
# MAGIC %sh
# MAGIC rm -rf mvt
# MAGIC rm -rf r5_geojson_ld.geojson
# MAGIC rm -rf r9_geojson_ld.geojson
