# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Runbook!!
# MAGIC
# MAGIC https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5724094/Idling+Heatmap+-+Data+Updates?atlOrigin=eyJpIjoiZGU0NTY1NzdiMDYyNDI2ZTg5MmM4MDFjZWZkMmM0MTUiLCJwIjoiY29uZmx1ZW5jZS1jaGF0cy1pbnQifQ

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup pre-requisites

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster events into h3 cells at 4 different resolutions (6, 7, 9, 11)

# COMMAND ----------

# MAGIC  %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW unproductive_idling_events AS
# MAGIC WITH cfg_raw AS (
# MAGIC  SELECT
# MAGIC    org_id,
# MAGIC    minimum_duration_ms,
# MAGIC    air_temperature_min_milli_c,
# MAGIC    air_temperature_max_milli_c,
# MAGIC    include_unknown_air_temperature,
# MAGIC    include_pto_on,
# MAGIC    include_pto_off,
# MAGIC    include_diesel_regen_active,
# MAGIC    include_diesel_regen_inactive,
# MAGIC    max_diesel_regen_active_duration_ms,
# MAGIC    updated_at,
# MAGIC    _timestamp
# MAGIC  FROM engineactivitydb_shards.unproductive_idling_config
# MAGIC ),
# MAGIC
# MAGIC cfg_latest AS (
# MAGIC  SELECT *
# MAGIC  FROM (
# MAGIC    SELECT c.*,
# MAGIC           ROW_NUMBER() OVER (
# MAGIC             PARTITION BY org_id
# MAGIC             ORDER BY updated_at DESC
# MAGIC           ) AS rn
# MAGIC    FROM cfg_raw c
# MAGIC  ) s
# MAGIC  WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- These values should be ept in sync with the values that the engine activity models define.
# MAGIC -- Currently located in go/src/samsaradev.io/fleet/engineactivity/engineactivitymodels/unproductive_idling_config.go
# MAGIC defaults AS (
# MAGIC  SELECT
# MAGIC    CAST(2 * 60 * 1000 AS BIGINT)  AS minimum_duration_ms,
# MAGIC    CAST(0         AS BIGINT)  AS air_temperature_min_milli_c,     -- 0 mC  (32°F)
# MAGIC    CAST(38000     AS BIGINT)  AS air_temperature_max_milli_c,     -- 38,000 mC (100°F)
# MAGIC    TRUE   AS include_unknown_air_temperature,
# MAGIC    FALSE  AS include_pto_on,
# MAGIC    TRUE   AS include_pto_off,
# MAGIC    TRUE   AS include_diesel_regen_active,
# MAGIC    TRUE   AS include_diesel_regen_inactive,
# MAGIC    CAST((24 * 60 * 60 * 1000) AS BIGINT)
# MAGIC           AS max_diesel_regen_active_duration_ms
# MAGIC ),
# MAGIC
# MAGIC effective AS (
# MAGIC  -- Compute effective config per org (fallback to defaults when org has no row)
# MAGIC  SELECT
# MAGIC    e.org_id,
# MAGIC    COALESCE(c.minimum_duration_ms,                 d.minimum_duration_ms)                  AS minimum_duration_ms,
# MAGIC    COALESCE(c.air_temperature_min_milli_c,         d.air_temperature_min_milli_c)          AS air_temperature_min_milli_c,
# MAGIC    COALESCE(c.air_temperature_max_milli_c,         d.air_temperature_max_milli_c)          AS air_temperature_max_milli_c,
# MAGIC
# MAGIC    COALESCE(c.include_unknown_air_temperature != 0, d.include_unknown_air_temperature)      AS include_unknown_air_temperature,
# MAGIC    COALESCE(c.include_pto_on                  != 0, d.include_pto_on)                       AS include_pto_on,
# MAGIC    COALESCE(c.include_pto_off                 != 0, d.include_pto_off)                      AS include_pto_off,
# MAGIC    COALESCE(c.include_diesel_regen_active     != 0, d.include_diesel_regen_active)          AS include_diesel_regen_active,
# MAGIC    COALESCE(c.include_diesel_regen_inactive   != 0, d.include_diesel_regen_inactive)        AS include_diesel_regen_inactive,
# MAGIC
# MAGIC    COALESCE(c.max_diesel_regen_active_duration_ms, d.max_diesel_regen_active_duration_ms)  AS max_diesel_regen_active_duration_ms
# MAGIC  FROM (SELECT DISTINCT org_id FROM engineactivitydb_shards.engine_idle_events) e
# MAGIC  LEFT JOIN cfg_latest c ON e.org_id = c.org_id
# MAGIC  CROSS JOIN defaults d
# MAGIC ),
# MAGIC
# MAGIC -- Normalization step to align with Go implementation:
# MAGIC -- 1) Reclassify long-duration ACTIVE diesel regen events as INACTIVE using
# MAGIC --    cfg.max_diesel_regen_active_duration_ms. This mirrors Go logic that treats
# MAGIC --    extended regen activity as effectively inactive for unproductive idling.
# MAGIC -- 2) UNKNOWN state inclusion is handled in the final WHERE clause, and is only
# MAGIC --    included when both include flags for the respective signal are enabled.
# MAGIC normalized_states AS (
# MAGIC  SELECT
# MAGIC    ev.*,
# MAGIC    CASE
# MAGIC      WHEN ev.diesel_regen_state = 1 AND ev.duration_ms > cfg.max_diesel_regen_active_duration_ms THEN 2
# MAGIC      ELSE ev.diesel_regen_state
# MAGIC    END AS diesel_regen_state_effective
# MAGIC  FROM engineactivitydb_shards.engine_idle_events ev
# MAGIC  JOIN effective cfg
# MAGIC   ON cfg.org_id = ev.org_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC  ev.*
# MAGIC FROM normalized_states ev
# MAGIC JOIN effective cfg
# MAGIC  ON cfg.org_id = ev.org_id
# MAGIC WHERE
# MAGIC  -- 1) Duration
# MAGIC  ev.duration_ms >= cfg.minimum_duration_ms
# MAGIC
# MAGIC  -- 2) Temperature window (or unknown if allowed)
# MAGIC  AND (
# MAGIC    ev.ambient_temperature_milli_celsius BETWEEN cfg.air_temperature_min_milli_c
# MAGIC                                             AND cfg.air_temperature_max_milli_c
# MAGIC    OR (ev.ambient_temperature_milli_celsius IS NULL AND cfg.include_unknown_air_temperature)
# MAGIC  )
# MAGIC
# MAGIC  -- 3) PTO state (0=UNKNOWN, 1=ACTIVE, 2=INACTIVE)
# MAGIC  AND (
# MAGIC    -- If both include flags are false, do not filter by PTO (allow all)
# MAGIC    NOT (cfg.include_pto_on OR cfg.include_pto_off)
# MAGIC    OR (
# MAGIC      (ev.pto_state = 1 AND cfg.include_pto_on)
# MAGIC      OR (ev.pto_state = 2 AND cfg.include_pto_off)
# MAGIC      OR (ev.pto_state = 0 AND (cfg.include_pto_on AND cfg.include_pto_off))
# MAGIC    )
# MAGIC  )
# MAGIC
# MAGIC  -- 4) Diesel regen (0=UNKNOWN, 1=ACTIVE, 2=INACTIVE)
# MAGIC  AND (
# MAGIC    -- If both include flags are false, do not filter by diesel regen (allow all)
# MAGIC    NOT (cfg.include_diesel_regen_active OR cfg.include_diesel_regen_inactive)
# MAGIC    OR (
# MAGIC      (ev.diesel_regen_state_effective = 1 AND cfg.include_diesel_regen_active)
# MAGIC      OR
# MAGIC      (ev.diesel_regen_state_effective = 2 AND cfg.include_diesel_regen_inactive)
# MAGIC      OR
# MAGIC      (ev.diesel_regen_state_effective = 0 AND (cfg.include_diesel_regen_active AND cfg.include_diesel_regen_inactive))
# MAGIC    )
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC All Orgs that have at least 1 Idling Event

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW org_info AS
# MAGIC WITH real_orgs AS (
# MAGIC SELECT org_id
# MAGIC FROM datamodel_core.dim_organizations
# MAGIC WHERE date = DATE_SUB(now(), 2)
# MAGIC ),
# MAGIC
# MAGIC active_device_count AS (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     COUNT(*) as device_count
# MAGIC   FROM datamodel_core.lifetime_device_activity
# MAGIC   WHERE date = DATE_SUB(now(), 2)
# MAGIC   AND latest_date >= DATE_SUB(now(), 8)
# MAGIC   GROUP BY org_id
# MAGIC ),
# MAGIC
# MAGIC org_with_active_device_count AS (
# MAGIC SELECT
# MAGIC   real_orgs.org_id,
# MAGIC   device_count
# MAGIC FROM real_orgs
# MAGIC JOIN active_device_count ON real_orgs.org_id = active_device_count.org_id
# MAGIC ),
# MAGIC
# MAGIC max_min_device_count AS (
# MAGIC   SELECT
# MAGIC   MIN(device_count) AS min_device_count,
# MAGIC   MAX(device_count) AS max_device_count
# MAGIC   FROM org_with_active_device_count
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC org_with_normalization_factor AS (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     device_count,
# MAGIC     (device_count - min_device_count) / (max_device_count - min_device_count) AS normalized_device_count
# MAGIC   FROM org_with_active_device_count, max_min_device_count
# MAGIC ),
# MAGIC
# MAGIC orgs_with_atleast_one_idle_event_in_period AS (
# MAGIC     SELECT
# MAGIC         org_id
# MAGIC     FROM
# MAGIC         unproductive_idling_events
# MAGIC     WHERE latitude IS NOT NULL AND longitude IS NOT NULL
# MAGIC     AND date >= date_sub(now(), 9)
# MAGIC     AND date <= DATE_SUB(now(), 1)
# MAGIC     GROUP BY org_id
# MAGIC ),
# MAGIC
# MAGIC final_org_set AS (
# MAGIC   SELECT
# MAGIC     org_with_normalization_factor.org_id,
# MAGIC     normalized_device_count,
# MAGIC     device_count
# MAGIC    FROM org_with_normalization_factor
# MAGIC   JOIN orgs_with_atleast_one_idle_event_in_period ON org_with_normalization_factor.org_id = orgs_with_atleast_one_idle_event_in_period.org_id
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM final_org_set

# COMMAND ----------

# MAGIC %md
# MAGIC Create view of localised events for selected orgs at res 13

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW date_localised_idling_events_at_res_13_previous_week AS WITH org_timezones AS (
# MAGIC   SELECT
# MAGIC     organization_id AS org_id,
# MAGIC     timezone
# MAGIC   FROM
# MAGIC     default.clouddb.groups
# MAGIC   WHERE
# MAGIC     organization_id IN (SELECT org_id FROM org_info)
# MAGIC ),
# MAGIC idling_events_for_selected_orgs AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     unproductive_idling_events
# MAGIC   WHERE
# MAGIC     date > date_sub(next_day(now(), 'MO'), 15) -- Buffer added for backward shifts
# MAGIC     AND date < date_add(next_day(now(), 'MO'), 6) -- Buffer added for forward shifts
# MAGIC     AND org_id IN (SELECT org_id FROM org_info)
# MAGIC     AND latitude IS NOT NULL
# MAGIC     AND longitude IS NOT NULL
# MAGIC ),
# MAGIC idling_events_with_localised_date AS (
# MAGIC   SELECT
# MAGIC     date_trunc(
# MAGIC       'day',
# MAGIC       from_utc_timestamp(
# MAGIC         to_timestamp(start_ms / 1000),
# MAGIC         org_timezones.timezone
# MAGIC       )
# MAGIC     ) AS local_date,
# MAGIC     idling_events_for_selected_orgs.*
# MAGIC   FROM
# MAGIC     idling_events_for_selected_orgs
# MAGIC     INNER JOIN org_timezones ON idling_events_for_selected_orgs.org_id = org_timezones.org_id
# MAGIC ),
# MAGIC idling_events_from_last_week AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     idling_events_with_localised_date
# MAGIC   WHERE
# MAGIC     local_date >= date_sub(next_day(now(), 'MO'), 14) -- Include from start of last week's Monday
# MAGIC     AND local_date < date_sub(next_day(now(), 'MO'), 7) -- Inlclude up to end of last week's Sunday
# MAGIC ),
# MAGIC idling_events_res_13 AS (
# MAGIC   SELECT
# MAGIC     local_date as date,
# MAGIC     org_id,
# MAGIC     DENSE_RANK() OVER (PARTITION BY org_id ORDER BY device_id) AS device_id,
# MAGIC     fuel_consumed_ml,
# MAGIC     geo_to_h3(latitude, longitude, 13) AS hex_id,
# MAGIC     h3_to_geo(
# MAGIC       geo_to_h3(latitude, longitude, 13)
# MAGIC     ) [0] AS latitude,
# MAGIC     h3_to_geo(
# MAGIC       geo_to_h3(latitude, longitude, 13)
# MAGIC     ) [1] AS longitude,
# MAGIC     (duration_ms) / (1000 * 60) AS idling_minutes
# MAGIC   FROM
# MAGIC     idling_events_from_last_week
# MAGIC )
# MAGIC SELECT
# MAGIC   date,
# MAGIC   org_id,
# MAGIC   hex_id,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   ARRAY_DISTINCT(ARRAY_AGG(device_id)) AS device_ids,
# MAGIC   COUNT(*) AS number_idling_events,
# MAGIC   SUM(fuel_consumed_ml / 1000) AS total_fuel_wastage_l,
# MAGIC   MIN(idling_minutes) AS min_idling_minutes,
# MAGIC   SUM(idling_minutes) AS sum_idling_minutes,
# MAGIC   MAX(idling_minutes) AS max_idling_minutes
# MAGIC FROM
# MAGIC   idling_events_res_13
# MAGIC GROUP BY
# MAGIC   date,
# MAGIC   org_id,
# MAGIC   hex_id,
# MAGIC   latitude,
# MAGIC   longitude;

# COMMAND ----------

# MAGIC %md
# MAGIC Create aggregated views at 4 different resolutions

# COMMAND ----------

# Define a function to create temporary views for a resolution
def create_idling_events_view(res):
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW idling_events_r{res} AS
    WITH idling_events AS (
        SELECT
            date,
            org_id,
            {res} as hex_res,
            h3_to_parent(hex_id, {res}) as hex_id,
            latitude,
            longitude,
            device_ids,
            number_idling_events,
            total_fuel_wastage_l,
            min_idling_minutes,
            sum_idling_minutes,
            max_idling_minutes
        FROM
            date_localised_idling_events_at_res_13_previous_week
    ),
    idling_events_agg AS (
        SELECT
            date,
            org_id,
            hex_res,
            hex_id,
            h3_to_geo(hex_id)[0] as latitude,
            h3_to_geo(hex_id)[1] as longitude,
            array_distinct(FLATTEN(ARRAY_AGG(device_ids))) AS device_ids,
            SUM(number_idling_events) AS number_idling_events,
            SUM(total_fuel_wastage_l) AS total_fuel_wastage_l,
            MIN(min_idling_minutes) AS min_idling_minutes,
            SUM(sum_idling_minutes) AS sum_idling_minutes,
            MAX(max_idling_minutes) AS max_idling_minutes
        FROM idling_events
        GROUP BY date, org_id, hex_res, hex_id, h3_to_geo(hex_id)[0], h3_to_geo(hex_id)[1]
    ),
    idling_events_agg_with_sum_by_day AS (
        SELECT
            org_id,
            hex_res,
            hex_id,
            latitude,
            longitude,
            array_distinct(FLATTEN(ARRAY_AGG(device_ids))) AS device_ids,
            SUM(number_idling_events) AS number_idling_events,
            SUM(total_fuel_wastage_l) AS total_fuel_wastage_l,
            MIN(min_idling_minutes) AS min_idling_minutes,
            SUM(sum_idling_minutes) AS sum_idling_minutes,
            MAX(max_idling_minutes) AS max_idling_minutes,
            ARRAY_AGG((date_format(date, 'yyyy-MM-dd'), sum_idling_minutes)) AS sum_by_day_idling_minutes
        FROM idling_events_agg
        GROUP BY
            org_id, hex_res, hex_id, latitude, longitude
    )

    SELECT
        idling_events_agg_with_sum_by_day.org_id,
        normalized_device_count,
        hex_res,
        hex_id,
        latitude,
        longitude,
        device_ids,
        number_idling_events,
        total_fuel_wastage_l,
        (sum_idling_minutes / number_idling_events) AS avg_idling_minutes,
        min_idling_minutes,
        sum_idling_minutes,
        max_idling_minutes,
        sum_by_day_idling_minutes
    FROM idling_events_agg_with_sum_by_day
    JOIN org_info ON idling_events_agg_with_sum_by_day.org_id = org_info.org_id
    """
    )


# Create views for resolutions 6, 7, and 9
resolutions = [6, 7, 9, 11]
for res in resolutions:
    create_idling_events_view(res)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform into GeoJSON

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW idling_events_at_res AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   6 AS hex_res,
# MAGIC   conv(hex_id, 16, 10) as int_h3_id,
# MAGIC   longitude,
# MAGIC   latitude,
# MAGIC   normalized_device_count,
# MAGIC   total_fuel_wastage_l,
# MAGIC   device_ids,
# MAGIC   sum_by_day_idling_minutes
# MAGIC FROM
# MAGIC   idling_events_r6
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   7 AS hex_res,
# MAGIC   conv(hex_id, 16, 10) as int_h3_id,
# MAGIC   longitude,
# MAGIC   latitude,
# MAGIC   normalized_device_count,
# MAGIC   total_fuel_wastage_l,
# MAGIC   device_ids,
# MAGIC   sum_by_day_idling_minutes
# MAGIC FROM
# MAGIC   idling_events_r7
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   9 AS hex_res,
# MAGIC   conv(hex_id, 16, 10) as int_h3_id,
# MAGIC   longitude,
# MAGIC   latitude,
# MAGIC   normalized_device_count,
# MAGIC   total_fuel_wastage_l,
# MAGIC   device_ids,
# MAGIC   sum_by_day_idling_minutes
# MAGIC FROM
# MAGIC   idling_events_r9
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   11 AS hex_res,
# MAGIC   conv(hex_id, 16, 10) as int_h3_id,
# MAGIC   longitude,
# MAGIC   latitude,
# MAGIC   normalized_device_count,
# MAGIC   total_fuel_wastage_l,
# MAGIC   device_ids,
# MAGIC   sum_by_day_idling_minutes
# MAGIC FROM
# MAGIC   idling_events_r11

# COMMAND ----------

# MAGIC %md
# MAGIC Create GeoJSON and Write to S3 Staging Folder

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
        "normalizedDeviceCount": row["normalized_device_count"],
        "fuelWastedL": row["total_fuel_wastage_l"],
        "deviceIds": ",".join([str(id) for id in row["device_ids"]]),
        "idlingTimeMinsSumByDay": ",".join(
            f"{str(day['col1'])}:{str(day['sum_idling_minutes'])}"
            for day in row["sum_by_day_idling_minutes"]
        ),
    }
    return geojson.Feature(
        id=row["int_h3_id"],
        geometry=geojson.Point((row["longitude"], row["latitude"])),
        properties=properties,
    )


@udf(returnType=StringType())
def create_geojson_feature_udf(
    longitude,
    latitude,
    normalized_device_count,
    total_fuel_wastage_l,
    device_ids,
    sum_by_day_idling_minutes,
    int_h3_id,
):
    row = {
        "longitude": longitude,
        "latitude": latitude,
        "normalized_device_count": normalized_device_count,
        "total_fuel_wastage_l": total_fuel_wastage_l,
        "device_ids": device_ids,
        "sum_by_day_idling_minutes": sum_by_day_idling_minutes,
        "int_h3_id": int_h3_id,
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
    if res == "6":
        minzoom, maxzoom = 5, 5
    elif res == "7":
        minzoom, maxzoom = 7, 7
    elif res == "9":
        minzoom, maxzoom = 9, 9
    elif res == "11":
        minzoom, maxzoom = 11, 11
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
        int_h3_id,
        org_id, longitude, latitude,
        normalized_device_count,
        total_fuel_wastage_l,
        device_ids,
        sum_by_day_idling_minutes
    FROM idling_events_at_res
"""
)

# set up staging directory for idling geojson
generated_on_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_dir = f"/Volumes/s3/smart-maps/root/staging/idling/geojson/{generated_on_date}"

# create a df with geojson_feature column
geo_json_df = df.withColumn(
    "geojson_feature",
    create_geojson_feature_udf(
        df.longitude,
        df.latitude,
        df.normalized_device_count,
        df.total_fuel_wastage_l,
        df.device_ids,
        df.sum_by_day_idling_minutes,
        df.int_h3_id,
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
aggregated_geo_json_df = geo_json_df.groupBy(["s3_vol_path", "org_id", "hex_res"]).agg(
    array_join(collect_list("geojson_feature"), "\n").alias("str_geojson_feature")
)
aggregated_geo_json_df.foreachPartition(write_geojson)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert GeoJson to MBTiles and Write to S3

# COMMAND ----------

import os
import shutil
from pyspark.sql.functions import pandas_udf, collect_list, col
from pyspark.sql.types import IntegerType
import subprocess
from datetime import timedelta
import pandas as pd

# Define the resolution to zoom mapping
res_to_zoom = {6: 5, 7: 7, 9: 9, 11: 11}

# Drop the geojson, we don't need it since we retrieve it from S3
df = aggregated_geo_json_df[["s3_vol_path", "org_id", "hex_res"]]

# Calculate the most recent Monday
# Format the most recent Monday as YYYYMMDD and that is our version
today = datetime.now()
most_recent_monday = today - timedelta(days=today.weekday())

version = most_recent_monday.strftime("%Y%m%d")

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
            f"org_id={org_id}/layer=idling/version={version}/z={res_to_zoom[hex_res]}/data.mbtiles",
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


num_partitions = spark.sparkContext.defaultParallelism * 4
df = df.repartition(num_partitions)

# Apply the UDF to convert GeoJSON to mbtiles
processed_files = df.withColumn(
    "mbtiles_result", convert_to_mbtiles(df["s3_vol_path"], df["org_id"], df["hex_res"])
)

# COMMAND ----------

# Show results and collect errors if needed
display(processed_files.groupBy(col("mbtiles_result")).count())
