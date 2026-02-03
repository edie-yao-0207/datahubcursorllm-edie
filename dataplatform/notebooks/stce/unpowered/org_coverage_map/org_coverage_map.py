# Databricks notebook source
# MAGIC %md
# MAGIC This notebook generates mbtiles for the org coverage heat map shown on the Asset Tag Overview report. The finished tiles can be found in the smart-maps S3 bucket under "org_ble_coverage".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup

# COMMAND ----------

# MAGIC %pip install 'h3>=3.7,<4' geojson==2.5.0

# COMMAND ----------

# MAGIC %md
# MAGIC Install modules and restart

# COMMAND ----------

import h3
from pyspark.sql.types import ArrayType, StringType, DoubleType

spark.udf.register("geo_to_h3", h3.geo_to_h3)
spark.udf.register("h3_to_geo", h3.h3_to_geo, ArrayType(DoubleType()))
spark.udf.register("h3_to_parent", h3.h3_to_parent, StringType())
spark.udf.register("cell_area", h3.cell_area)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch all asset tag locations from the last 30 days and group by orgId. These are snapped to the center of the res 9 h3 tiles.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- location_count is the number of locations in the hex at res 9
# MAGIC CREATE OR REPLACE TEMP VIEW org_coverage_locations_r9 AS
# MAGIC with snapped AS (
# MAGIC   SELECT
# MAGIC     data.date,
# MAGIC     geo_to_h3(data.latitude, data.longitude, 9) AS hex_id,
# MAGIC     get(
# MAGIC       h3_to_geo(geo_to_h3(data.latitude, data.longitude, 9)), 0
# MAGIC     ) AS latitude,
# MAGIC     get(
# MAGIC       h3_to_geo(geo_to_h3(data.latitude, data.longitude, 9)), 1
# MAGIC     ) AS longitude,
# MAGIC     data.org_id
# MAGIC   FROM datastreams_history.crux_aggregated_locations data
# MAGIC   WHERE
# MAGIC     data.date >= date_sub(current_date(), 30)
# MAGIC     AND data.latitude IS NOT NULL
# MAGIC     AND data.longitude IS NOT NULL
# MAGIC     AND data.product_id IN (172, 221)
# MAGIC )
# MAGIC SELECT
# MAGIC   date,
# MAGIC   org_id,
# MAGIC   hex_id,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   COUNT(*) AS location_count,
# MAGIC   9 as hex_res
# MAGIC FROM snapped
# MAGIC GROUP BY
# MAGIC   date, org_id, hex_id, latitude, longitude;

# COMMAND ----------

# MAGIC %md
# MAGIC Create view for res 5.

# COMMAND ----------

# DBTITLE 1,Create view for resolution 5
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW org_coverage_locations_r5 AS
# MAGIC WITH res5 as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     org_id,
# MAGIC     h3_to_parent(hex_id, 5) as hex_id,
# MAGIC     location_count
# MAGIC   FROM org_coverage_locations_r9
# MAGIC ),
# MAGIC aggregated AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     5 AS hex_res,
# MAGIC     hex_id,
# MAGIC     org_id,
# MAGIC     SUM(location_count) as location_count
# MAGIC   FROM res5
# MAGIC   GROUP BY date, hex_id, org_id
# MAGIC )
# MAGIC SELECT
# MAGIC   date,
# MAGIC   hex_res,
# MAGIC   hex_id,
# MAGIC   org_id,
# MAGIC   location_count,
# MAGIC   h3_to_geo(hex_id)[0] as latitude,
# MAGIC   h3_to_geo(hex_id)[1] as longitude
# MAGIC FROM aggregated;
# MAGIC
# MAGIC SELECT * FROM org_coverage_locations_r5 LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC Join together the res 9 and res 5 aggregated tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW org_coverage_agg AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   date,
# MAGIC   conv(hex_id, 16, 10) as int_hex_id,
# MAGIC   hex_res,
# MAGIC   location_count,
# MAGIC   longitude,
# MAGIC   latitude
# MAGIC FROM org_coverage_locations_r5
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   date,
# MAGIC   conv(hex_id, 16, 10) as int_hex_id,
# MAGIC   hex_res,
# MAGIC   location_count,
# MAGIC   longitude,
# MAGIC   latitude
# MAGIC FROM org_coverage_locations_r9;

# COMMAND ----------

# MAGIC %md
# MAGIC Create the GeoJSON file and write to the S3 Staging folder.

# COMMAND ----------

import os
from datetime import datetime
import geojson
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)
from pyspark.sql.functions import udf, array_join, collect_list, lit


def create_geojson_feature(row):
    """
    Create a GeoJSON feature from a row.
    """
    properties = {
        "lng": row["longitude"],
        "lat": row["latitude"],
        "location_count": row["location_count"],
    }
    return geojson.Feature(
        id=row["int_hex_id"],
        geometry=geojson.Point((row["longitude"], row["latitude"])),
        properties=properties,
    )


@udf(returnType=StringType())
def create_geojson_feature_udf(
    longitude,
    latitude,
    location_count,
    int_hex_id,
):
    row = {
        "longitude": longitude,
        "latitude": latitude,
        "location_count": location_count,
        "int_hex_id": int_hex_id,
    }
    feature = create_geojson_feature(row)
    return geojson.dumps(feature)


@udf(returnType=StringType())
def s3_vol_path(base_dir, org_id, res):
    return os.path.join(base_dir, str(org_id), f"{res}.geojson")


@udf(
    returnType=StructType(
        [
            StructField("minzoom", IntegerType(), True),
            StructField("maxzoom", IntegerType(), True),
        ]
    )
)
def zoom_levels(res):
    minzoom, maxzoom = None, None
    if res == 5:
        minzoom, maxzoom = 4, 5
    elif res == 9:
        minzoom, maxzoom = 9, 9
    return minzoom, maxzoom


# Write geojson files to s3
def write_geojson(partition):
    for row in partition:
        file_path = row["s3_vol_path"]
        geo_json = row["str_geojson_feature"]

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Will overwrite if file exists
        with open(file_path, "w") as f:
            f.write(geo_json)


# Load the data
df = spark.sql(
    """
    SELECT
        hex_res,
        int_hex_id,
        org_id, longitude, latitude,
        location_count
    FROM org_coverage_agg
"""
)

# set up staging directory for org coverage geojson
generated_on_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_dir = (
    f"/Volumes/s3/smart-maps/root/staging/org_ble_coverage/geojson/{generated_on_date}"
)

# create a df with geojson_feature column
geo_json_df = df.withColumn(
    "geojson_feature",
    create_geojson_feature_udf(
        df.longitude, df.latitude, df.location_count, df.int_hex_id
    ),
).withColumn("s3_vol_path", s3_vol_path(lit(output_dir), df.org_id, df.hex_res))

# Add minzoom and maxzoom columns
geo_json_df = geo_json_df.withColumn("min_max_zoom", zoom_levels(df.hex_res))
geo_json_df = (
    geo_json_df.withColumn("minzoom", geo_json_df.min_max_zoom.minzoom)
    .withColumn("maxzoom", geo_json_df.min_max_zoom.maxzoom)
    .drop("min_max_zoom")
)

# Group all geojson data by s3 file path so we can write geojson per file
from pyspark.sql.functions import col

# Keep one row per feature; ensure all rows for a given file land in the same partition
geo_json_to_write_df = (
    geo_json_df.select("s3_vol_path", "geojson_feature")
    .repartition(col("s3_vol_path"))
    .sortWithinPartitions("s3_vol_path")
)


def write_geojson_stream(partition):
    import os

    current_path = None
    f = None
    try:
        for row in partition:
            path = row["s3_vol_path"]
            feature = row["geojson_feature"]

            # rotate file handle when path changes
            if path != current_path:
                if f is not None:
                    f.close()
                os.makedirs(os.path.dirname(path), exist_ok=True)
                f = open(path, "w")
                current_path = path
                first = True
            else:
                first = False

            if not first:
                f.write("\n")
            f.write(feature)
    finally:
        if f is not None:
            f.close()


geo_json_to_write_df.foreachPartition(write_geojson_stream)

# Create a dataframe with unique file metadata for MBTiles conversion.
# This preserves org_id and hex_res which are needed downstream.
mbtiles_input_df = geo_json_df.select("s3_vol_path", "org_id", "hex_res").distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC Convert GeoJson files into MBTiles and write to S3 bucket

# COMMAND ----------

import os
import shutil
from pyspark.sql.functions import pandas_udf, collect_list, col
from pyspark.sql.types import IntegerType
import subprocess
from datetime import timedelta
import pandas as pd

# Define the resolution to zoom mapping
res_to_zoom = {5: 5, 9: 9}

# Drop the geojson, we don't need it since we retrieve it from S3
df = mbtiles_input_df


today = datetime.now()
version = today.strftime("%Y%m%d")


# Define the UDF to process GeoJSON files
# Using a PandasUDF b/c it will batch rows together so we can minimize serializing data
@pandas_udf(IntegerType())
def convert_to_mbtiles(s3_vol_path_series, org_id_series, hex_res_series):
    results = []

    for s3_vol_path, org_id, hex_res in zip(
        s3_vol_path_series, org_id_series, hex_res_series
    ):
        local_mbtile_path = f"/tmp/mbtiles/{org_id}/res_{hex_res}.mbtiles"
        os.makedirs(os.path.dirname(local_mbtile_path), exist_ok=True)

        # Construct the ogr2ogr command for converting geospatial data to MBTiles
        ogr2ogr_command = [
            "ogr2ogr",  # GDAL command-line tool for geospatial data conversion
            # Output format and destination
            "-f",
            "MVT",  # Specify the output format as Mapbox Vector Tiles (MVT)
            local_mbtile_path,  # output file path (local to Spark cluster)
            # Input data source (assumed to be an S3-mounted volume path)
            s3_vol_path,
            # Dataset creation options (control how the MBTiles file is generated)
            "-dsco",
            "FORMAT=MBTILES",  # Use the MBTiles format for storing vector tiles
            "-dsco",
            "TILE_EXTENSION=mvt",  # Store tiles with the .mvt extension
            # Set the zoom levels dynamically based on hexagonal resolution
            "-dsco",
            f"MINZOOM={res_to_zoom[hex_res]}",  # Minimum zoom level
            "-dsco",
            f"MAXZOOM={res_to_zoom[hex_res]}",  # Maximum zoom level
            # Optimize file size and performance
            "-dsco",
            "COMPRESS=YES",  # Enable compression for smaller tile sizes
            "-dsco",
            "MAX_SIZE=2000000",  # Set max tile size to 2MB -> This is for the mvt tiles, not the mbtiles
            # Ensure no buffer around points to prevent unwanted duplication in frontend aggregation
            "-dsco",
            "BUFFER=0",
        ]

        try:
            process = subprocess.Popen(
                ogr2ogr_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                print(f"Error processing {s3_vol_path}: {stderr}")
                results.append(-1)  # ogr2ogr error
                continue
        except Exception as e:
            print(f"Unexpected error: {e}")
            results.append(-2)  # unexpected error with subprocess
            continue

        # Write data from Spark to S3
        s3_mbtile_path = os.path.join(
            "/Volumes/s3/smart-maps/root/",
            f"org_id={org_id}/layer=org_ble_coverage/version={version}/z={res_to_zoom[hex_res]}/data.mbtiles",
        )
        os.makedirs(os.path.dirname(s3_mbtile_path), exist_ok=True)
        try:
            # Will overwrite an existing file
            shutil.move(local_mbtile_path, s3_mbtile_path)
        except FileNotFoundError:
            results.append(2)
        except PermissionError:
            results.append(3)
        except IsADirectoryError:
            results.append(4)
        except shutil.SameFileError:
            results.append(5)
        except Exception as e:
            results.append(6)
        else:
            results.append(1)  # success
    return pd.Series(results)


num_partitions = 32
df = df.repartition(num_partitions)

# Apply the UDF to convert GeoJSON to mbtiles
processed_files = df.withColumn(
    "mbtiles_result", convert_to_mbtiles(df["s3_vol_path"], df["org_id"], df["hex_res"])
)
display(processed_files)

# COMMAND ----------

# Show results and collect errors if needed
display(processed_files.groupBy(col("mbtiles_result")).count())
