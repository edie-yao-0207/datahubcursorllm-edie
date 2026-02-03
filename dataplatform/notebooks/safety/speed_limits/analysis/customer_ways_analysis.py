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

from datetime import date, timedelta

LOOKBACK_DAYS = 7

today = date.today()
data_start = today - timedelta(days=LOOKBACK_DAYS)

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
    raise Exception("only one region supported at a time")
else:
    region = desired_regions[0]
print(f"{ARG_REGIONS}: {region}")

"""
* ARG_TILE_VERSION:
Integer tile version
"""
dbutils.widgets.text(ARG_TILE_VERSION, "")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
print(f"{ARG_TILE_VERSION}: {tile_version}")

"""
* ARG_IS_TOMTOM_DECOUPLED:
Boolean flag to indicate if tomtom data is decoupled
"""
dbutils.widgets.text(ARG_IS_TOMTOM_DECOUPLED, "")
is_tomtom_decoupled = dbutils.widgets.get(ARG_IS_TOMTOM_DECOUPLED).lower() == "true"
print(f"{ARG_IS_TOMTOM_DECOUPLED}: {is_tomtom_decoupled}")

# COMMAND ----------

# Map map region to country codes stored on location object stats.
region_to_revgeo_countries = {
    USA: ["US"],
    MEX: ["MX"],
    CAN: ["CA"],
    EUR: [
        "GB",
        "FR",
        "DE",
        "NL",
        "IE",
        "BE",
        "IT",
        "ES",
        "SE",
        "PL",
        "CZ",
        "AT",
        "CH",
        "DK",
        "SK",
        "RO",
        "HU",
        "LU",
        "FI",
        "NO",
        "BG",
        "PT",
        "IM",
        "AD",
        "VA",
    ],
}


def revgeo_countries_to_sql(region: str) -> str:
    """
    Convert a map region to a list of comma-separated revgeo regions for use in a sql query.
    """
    countries = region_to_revgeo_countries[region]
    countries = map(lambda x: '"' + x + '"', countries)
    res = ", ".join(countries)
    return res


# COMMAND ----------

if len(osm_version) == 0:
    exit_notebook("osm version must be specified")
if len(tomtom_version) == 0:
    exit_notebook("tomtom version must be specified")
if len(region) == 0:
    exit_notebook("region must be specified")
if len(tile_version) == 0:
    exit_notebook("tile version must be specified")

# COMMAND ----------

mapmatch_table_name = DBX_TABLE.osm_tomtom_map_match(
    osm_version, region, tomtom_version
)
if is_tomtom_decoupled:
    mapmatch_table_name += "_decoupled"

loc_data_sql = """
  SELECT
    loc.org_id,
    device_id,
    date,
    time,
    value.latitude AS lat,
    value.longitude AS lng,
    upper(loc.value.revgeo_state) AS state,
    upper(loc.value.revgeo_country) AS country,
    COALESCE(loc.value.ecu_speed_meters_per_second, loc.value.gps_speed_meters_per_second) * 2.23694 AS speed_mph,
    COALESCE(loc.value.ecu_speed_meters_per_second, loc.value.gps_speed_meters_per_second) * 3.6 AS speed_kph,
    value.way_id,
    value.has_speed_limit IS NOT NULL AS has_speed_limit,
    round(value.speed_limit_meters_per_second*2.23694/5, 0)*5 AS speed_limit_mph,
    round(value.speed_limit_meters_per_second*3.6/5,0)*5 AS speed_limit_kph,
    CASE WHEN matched.tomtom_maxspeed IS NULL THEN false ELSE true END AS has_tomtom_speed_limit,
    matched.tomtom_maxspeed AS tomtom_speed_limit,
    matched.tomtom_maxspeed_unit AS tomtom_speed_unit
  FROM kinesisstats.location AS loc
  JOIN clouddb.organizations orgs
    ON orgs.id = loc.org_id
    AND orgs.internal_type <> 1
    AND orgs.quarantine_enabled <> 1
  JOIN productsdb.devices devices
    ON loc.device_id = devices.id
    AND devices.product_id IN (7, 17, 24, 35)
  LEFT JOIN {map_match_table} AS matched
    ON matched.osm_way_id = loc.value.way_id
  WHERE
    loc.value.revgeo_country IN ({revgeo_countries})
    AND loc.value.has_fix = true
    AND loc.date >= date_sub(current_date(), {lookback_days})
    -- exclude speed less than 5mph
    AND round(coalesce(value.ecu_speed_meters_per_second, value.gps_speed_meters_per_second)*2.23694/5, 0)*5 >= 5
""".format(
    map_match_table=mapmatch_table_name,
    revgeo_countries=revgeo_countries_to_sql(region),
    lookback_days=LOOKBACK_DAYS,
)

loc_data_sdf = spark.sql(loc_data_sql)
loc_data_sdf.createOrReplaceTempView("loc_data")

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW exploded_ways AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     LAG(way_id) OVER (
# MAGIC       PARTITION BY org_id,
# MAGIC       device_id
# MAGIC       ORDER BY
# MAGIC         time ASC
# MAGIC     ) AS prior_way,
# MAGIC     LEAD(way_id) OVER (
# MAGIC       PARTITION BY org_id,
# MAGIC       device_id
# MAGIC       ORDER BY
# MAGIC         time ASC
# MAGIC     ) AS next_way,
# MAGIC     time - (
# MAGIC       LAG(time) OVER (
# MAGIC         PARTITION BY org_id,
# MAGIC         device_id
# MAGIC         ORDER BY
# MAGIC           time ASC
# MAGIC       )
# MAGIC     ) AS time_since_last_point,
# MAGIC     (
# MAGIC       LEAD(time) OVER (
# MAGIC         PARTITION BY org_id,
# MAGIC         device_id
# MAGIC         ORDER BY
# MAGIC           time ASC
# MAGIC       )
# MAGIC     ) - time AS time_until_next_point
# MAGIC   FROM
# MAGIC     loc_data
# MAGIC );

# COMMAND ----------

# MAGIC %sql -- Note that we take the greatest of the existing AND new speed limits WHEN evaluating new speeding.
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW way_time_speeding AS (
# MAGIC   with time_speeding_mph AS (
# MAGIC     -- only consider US + UK as mph
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = false
# MAGIC         OR (speed_mph - speed_limit_mph) <= 0 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_not_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_mph - speed_limit_mph) > 0
# MAGIC         AND (speed_mph - speed_limit_mph) < 5 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_light_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_mph - speed_limit_mph) >= 5
# MAGIC         AND (speed_mph - speed_limit_mph) < 10 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_moderate_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_mph - speed_limit_mph) >= 10
# MAGIC         AND (speed_mph - speed_limit_mph) < 15 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_heavy_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_mph - speed_limit_mph) >= 15 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_severe_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = false
# MAGIC           AND has_tomtom_speed_limit = false
# MAGIC         )
# MAGIC         OR (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) <= 0 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_not_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) > 0
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) < 5 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_light_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) >= 5
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) < 10 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_moderate_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) >= 10
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) < 15 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_heavy_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_mph - greatest(tomtom_speed_limit, speed_limit_mph)
# MAGIC         ) >= 15 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_severe_speeding
# MAGIC     FROM
# MAGIC       exploded_ways
# MAGIC     WHERE
# MAGIC       country IN ("US", "GB")
# MAGIC   ),
# MAGIC   time_speeding_kph AS (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = false
# MAGIC         OR (speed_kph - speed_limit_kph) <= 0 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_not_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_kph - speed_limit_kph) > 0
# MAGIC         AND (speed_kph - speed_limit_kph) < 10 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_light_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_kph - speed_limit_kph) >= 10
# MAGIC         AND (speed_kph - speed_limit_kph) < 20 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_moderate_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_kph - speed_limit_kph) >= 20
# MAGIC         AND (speed_kph - speed_limit_kph) < 30 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_heavy_speeding,
# MAGIC       CASE
# MAGIC         WHEN has_speed_limit = true
# MAGIC         AND (speed_kph - speed_limit_kph) >= 30 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS loc_severe_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = false
# MAGIC           AND has_tomtom_speed_limit = false
# MAGIC         )
# MAGIC         OR (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) <= 0 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_not_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) > 0
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) < 10 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_light_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) >= 10
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) < 20 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_moderate_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) >= 20
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) < 30 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_heavy_speeding,
# MAGIC       CASE
# MAGIC         WHEN (
# MAGIC           has_speed_limit = true
# MAGIC           OR has_tomtom_speed_limit = true
# MAGIC         )
# MAGIC         AND (
# MAGIC           speed_kph - greatest(tomtom_speed_limit, speed_limit_kph)
# MAGIC         ) >= 30 THEN time_until_next_point
# MAGIC         ELSE 0
# MAGIC       END AS new_severe_speeding
# MAGIC     FROM
# MAGIC       exploded_ways
# MAGIC     WHERE
# MAGIC       country NOT IN ("US", "GB")
# MAGIC   ),
# MAGIC   combined AS(
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       time_speeding_mph
# MAGIC     UNION
# MAGIC     SELECT
# MAGIC       *
# MAGIC     FROM
# MAGIC       time_speeding_kph
# MAGIC   )
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     combined
# MAGIC   WHERE
# MAGIC     time_until_next_point < 1000 * 30
# MAGIC     AND time_until_next_point IS NOT NULL
# MAGIC )

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW way_intervals AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         org_id,
# MAGIC         device_id,
# MAGIC         way_id,
# MAGIC         prior_way,
# MAGIC         has_speed_limit,
# MAGIC         time AS last_time_on_way,
# MAGIC         lat,
# MAGIC         lng,
# MAGIC         state,
# MAGIC         speed_limit_mph,
# MAGIC         speed_limit_kph,
# MAGIC         has_tomtom_speed_limit,
# MAGIC         tomtom_speed_limit,
# MAGIC         tomtom_speed_unit,
# MAGIC         LAG(time) OVER (
# MAGIC           PARTITION BY org_id,
# MAGIC           device_id
# MAGIC           ORDER BY
# MAGIC             time ASC
# MAGIC         ) AS first_time_on_way,
# MAGIC         LAG(lat) OVER (
# MAGIC           PARTITION BY org_id,
# MAGIC           device_id
# MAGIC           ORDER BY
# MAGIC             time ASC
# MAGIC         ) AS first_lat_on_way,
# MAGIC         LAG(lng) OVER (
# MAGIC           PARTITION BY org_id,
# MAGIC           device_id
# MAGIC           ORDER BY
# MAGIC             time ASC
# MAGIC         ) AS first_lng_on_way
# MAGIC       FROM
# MAGIC         exploded_ways
# MAGIC       WHERE
# MAGIC         (
# MAGIC           way_id != next_way -- consider our time on way however long we spent on the way until we saw a change
# MAGIC           OR next_way IS NULL
# MAGIC         )
# MAGIC         AND (
# MAGIC           time_since_last_point < 1000 * 60 * 5
# MAGIC           OR time_since_last_point IS NULL
# MAGIC         )
# MAGIC     )
# MAGIC   WHERE
# MAGIC     way_id = prior_way
# MAGIC );

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW total_time_spent_by_way AS (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     device_id,
# MAGIC     way_id,
# MAGIC     max((first_time_on_way, state)).state AS state,
# MAGIC     max((first_time_on_way, speed_limit_mph)).speed_limit_mph AS speed_limit_mph,
# MAGIC     FIRST(has_tomtom_speed_limit) AS has_tomtom_speed_limit,
# MAGIC     FIRST(tomtom_speed_limit) AS tomtom_speed_limit,
# MAGIC     FIRST(tomtom_speed_unit) AS tomtom_speed_unit,
# MAGIC     CASE
# MAGIC       WHEN LAST(has_speed_limit) IS NULL THEN false
# MAGIC       ELSE LAST(has_speed_limit)
# MAGIC     END AS has_speed_limit,
# MAGIC     sum(last_time_on_way - first_time_on_way) AS total_time_on_way_ms,
# MAGIC     sum(
# MAGIC       111.045 * DEGREES(
# MAGIC         ACOS(
# MAGIC           COS(RADIANS(lat)) * COS(RADIANS(first_lat_on_way)) * COS(
# MAGIC             RADIANS(lng) - RADIANS(first_lng_on_way)
# MAGIC           ) + SIN(RADIANS(lat)) * SIN(RADIANS(first_lat_on_way))
# MAGIC         )
# MAGIC       )
# MAGIC     ) AS total_distance_on_way_km
# MAGIC   FROM
# MAGIC     way_intervals
# MAGIC   GROUP BY
# MAGIC     org_id,
# MAGIC     device_id,
# MAGIC     way_id
# MAGIC );

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW way_info AS (
# MAGIC   SELECT
# MAGIC     way_id,
# MAGIC     FIRST(state) AS state,
# MAGIC     max(has_speed_limit) AS has_speed_limit,
# MAGIC     avg(speed_limit_mph) AS speed_limit_mph,
# MAGIC     sum(total_distance_on_way_km) AS total_distance_km,
# MAGIC     sum(total_time_on_way_ms) AS total_time_ms,
# MAGIC     FIRST(has_tomtom_speed_limit) AS has_tomtom_speed_limit,
# MAGIC     FIRST(tomtom_speed_limit) AS tomtom_speed_limit,
# MAGIC     FIRST(tomtom_speed_unit) AS tomtom_speed_unit
# MAGIC   FROM
# MAGIC     total_time_spent_by_way
# MAGIC   GROUP BY
# MAGIC     way_id
# MAGIC );
# MAGIC
# MAGIC CACHE TABLE way_info;

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW distance_with_limits_all AS WITH total_distance AS (
# MAGIC   SELECT
# MAGIC     SUM(total_distance_km) AS tot_dist
# MAGIC   FROM
# MAGIC     way_info
# MAGIC ),
# MAGIC location_has_speed_limit AS (
# MAGIC   SELECT
# MAGIC     has_speed_limit AS has_speed_limit,
# MAGIC     round(SUM(total_distance_km) / FIRST(tot_dist), 6) * 100 AS pct_dist_with_location_limit
# MAGIC   FROM
# MAGIC     way_info
# MAGIC     CROSS JOIN total_distance
# MAGIC   GROUP BY
# MAGIC     has_speed_limit
# MAGIC ),
# MAGIC tomtom_has_speed_limit AS (
# MAGIC   SELECT
# MAGIC     has_tomtom_speed_limit AS has_speed_limit,
# MAGIC     round(SUM(total_distance_km) / FIRST(tot_dist), 6) * 100 AS pct_dist_with_tomtom_limit
# MAGIC   FROM
# MAGIC     way_info
# MAGIC     CROSS JOIN total_distance
# MAGIC   GROUP BY
# MAGIC     has_tomtom_speed_limit
# MAGIC )
# MAGIC SELECT
# MAGIC   a.has_speed_limit,
# MAGIC   pct_dist_with_location_limit,
# MAGIC   pct_dist_with_tomtom_limit
# MAGIC FROM
# MAGIC   location_has_speed_limit AS a
# MAGIC   JOIN tomtom_has_speed_limit AS b ON a.has_speed_limit = b.has_speed_limit

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW distance_with_limits_state AS WITH total_distance AS (
# MAGIC   SELECT
# MAGIC     state,
# MAGIC     SUM(total_distance_km) AS tot_dist
# MAGIC   FROM
# MAGIC     way_info
# MAGIC   GROUP BY
# MAGIC     state
# MAGIC ),
# MAGIC location_has_speed_limit AS (
# MAGIC   SELECT
# MAGIC     total_distance.state,
# MAGIC     has_speed_limit AS has_speed_limit,
# MAGIC     round(SUM(total_distance_km) / FIRST(tot_dist), 6) * 100 AS pct_dist_with_location_limit
# MAGIC   FROM
# MAGIC     way_info
# MAGIC     INNER JOIN total_distance on way_info.state = total_distance.state
# MAGIC   GROUP BY
# MAGIC     total_distance.state,
# MAGIC     has_speed_limit
# MAGIC ),
# MAGIC tomtom_has_speed_limit AS (
# MAGIC   SELECT
# MAGIC     total_distance.state,
# MAGIC     has_tomtom_speed_limit AS has_speed_limit,
# MAGIC     round(SUM(total_distance_km) / FIRST(tot_dist), 6) * 100 AS pct_dist_with_tomtom_limit
# MAGIC   FROM
# MAGIC     way_info
# MAGIC     INNER JOIN total_distance on way_info.state = total_distance.state
# MAGIC   GROUP BY
# MAGIC     total_distance.state,
# MAGIC     has_tomtom_speed_limit
# MAGIC )
# MAGIC SELECT
# MAGIC   a.state,
# MAGIC   a.has_speed_limit,
# MAGIC   pct_dist_with_location_limit,
# MAGIC   pct_dist_with_tomtom_limit
# MAGIC FROM
# MAGIC   location_has_speed_limit AS a
# MAGIC   JOIN tomtom_has_speed_limit AS b ON a.has_speed_limit = b.has_speed_limit
# MAGIC   AND a.state = b.state
# MAGIC WHERE
# MAGIC   a.has_speed_limit = true
# MAGIC   and a.state is not null
# MAGIC ORDER BY
# MAGIC   a.state ASC

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW change_in_speeding AS WITH sum_speeding AS (
# MAGIC   SELECT
# MAGIC     SUM(time_until_next_point) AS total_time,
# MAGIC     SUM(loc_not_speeding) AS loc_not_speeding,
# MAGIC     SUM(loc_light_speeding) AS loc_light_speeding,
# MAGIC     SUM(loc_moderate_speeding) AS loc_moderate_speeding,
# MAGIC     SUM(loc_heavy_speeding) AS loc_heavy_speeding,
# MAGIC     SUM(loc_severe_speeding) AS loc_severe_speeding,
# MAGIC     SUM(new_not_speeding) AS new_not_speeding,
# MAGIC     SUM(new_light_speeding) AS new_light_speeding,
# MAGIC     SUM(new_moderate_speeding) AS new_moderate_speeding,
# MAGIC     SUM(new_heavy_speeding) AS new_heavy_speeding,
# MAGIC     SUM(new_severe_speeding) AS new_severe_speeding
# MAGIC   FROM
# MAGIC     way_time_speeding
# MAGIC )
# MAGIC SELECT
# MAGIC   round(loc_not_speeding / total_time, 6) * 100 AS pct_loc_not_speeding,
# MAGIC   round(loc_light_speeding / total_time, 6) * 100 AS pct_loc_light_speeding,
# MAGIC   round(loc_moderate_speeding / total_time, 6) * 100 AS pct_loc_moderate_speeding,
# MAGIC   round(loc_heavy_speeding / total_time, 6) * 100 AS pct_loc_heavy_speeding,
# MAGIC   round(loc_severe_speeding / total_time, 6) * 100 AS pct_loc_severe_speeding,
# MAGIC   round(new_not_speeding / total_time, 6) * 100 AS pct_new_not_speeding,
# MAGIC   round(new_light_speeding / total_time, 6) * 100 AS pct_new_light_speeding,
# MAGIC   round(new_moderate_speeding / total_time, 6) * 100 AS pct_new_moderate_speeding,
# MAGIC   round(new_heavy_speeding / total_time, 6) * 100 AS pct_new_heavy_speeding,
# MAGIC   round(new_severe_speeding / total_time, 6) * 100 AS pct_new_severe_speeding
# MAGIC FROM
# MAGIC   sum_speeding;
# MAGIC
# MAGIC CACHE TABLE change_in_speeding;
# MAGIC CACHE TABLE distance_with_limits_all;
# MAGIC CACHE TABLE distance_with_limits_state;

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMPORARY VIEW change_in_speeding_state AS WITH sum_speeding AS (
# MAGIC   SELECT
# MAGIC     state,
# MAGIC     SUM(time_until_next_point) AS total_time,
# MAGIC     SUM(loc_not_speeding) AS loc_not_speeding,
# MAGIC     SUM(loc_light_speeding) AS loc_light_speeding,
# MAGIC     SUM(loc_moderate_speeding) AS loc_moderate_speeding,
# MAGIC     SUM(loc_heavy_speeding) AS loc_heavy_speeding,
# MAGIC     SUM(loc_severe_speeding) AS loc_severe_speeding,
# MAGIC     SUM(new_not_speeding) AS new_not_speeding,
# MAGIC     SUM(new_light_speeding) AS new_light_speeding,
# MAGIC     SUM(new_moderate_speeding) AS new_moderate_speeding,
# MAGIC     SUM(new_heavy_speeding) AS new_heavy_speeding,
# MAGIC     SUM(new_severe_speeding) AS new_severe_speeding
# MAGIC   FROM
# MAGIC     way_time_speeding
# MAGIC   GROUP BY
# MAGIC     state
# MAGIC )
# MAGIC SELECT
# MAGIC   state,
# MAGIC   round(loc_not_speeding / total_time, 6) * 100 AS pct_loc_not_speeding,
# MAGIC   round(loc_light_speeding / total_time, 6) * 100 AS pct_loc_light_speeding,
# MAGIC   round(loc_moderate_speeding / total_time, 6) * 100 AS pct_loc_moderate_speeding,
# MAGIC   round(loc_heavy_speeding / total_time, 6) * 100 AS pct_loc_heavy_speeding,
# MAGIC   round(loc_severe_speeding / total_time, 6) * 100 AS pct_loc_severe_speeding,
# MAGIC   round(new_not_speeding / total_time, 6) * 100 AS pct_new_not_speeding,
# MAGIC   round(new_light_speeding / total_time, 6) * 100 AS pct_new_light_speeding,
# MAGIC   round(new_moderate_speeding / total_time, 6) * 100 AS pct_new_moderate_speeding,
# MAGIC   round(new_heavy_speeding / total_time, 6) * 100 AS pct_new_heavy_speeding,
# MAGIC   round(new_severe_speeding / total_time, 6) * 100 AS pct_new_severe_speeding
# MAGIC FROM
# MAGIC   sum_speeding
# MAGIC WHERE
# MAGIC   state IS NOT NULL
# MAGIC ORDER BY
# MAGIC   state ASC;
# MAGIC
# MAGIC CACHE TABLE change_in_speeding_state;

# COMMAND ----------

change_data_sql = """
  SELECT
    "{today}" as date,
    "{start_date}" as start_date,
    "{region}" as region,
    {tile_version} as tile_version,
    dist.pct_dist_with_location_limit,
    dist.pct_dist_with_tomtom_limit,
    change.*
  FROM change_in_speeding as change
  CROSS JOIN distance_with_limits_all as dist
  WHERE dist.has_speed_limit = true
""".format(
    today=today.strftime("%Y-%m-%d"),
    start_date=data_start.strftime("%Y-%m-%d"),
    region=region,
    tile_version=tile_version,
)

spark.sql(change_data_sql).createOrReplaceTempView("change_data")

customer_ways_table = DBX_TABLE.customer_ways_coverage_analysis()

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {customer_ways_table}
USING DELTA
PARTITIONED BY (date)
AS
SELECT * FROM change_data
"""
)

spark.sql(
    f"""
MERGE INTO {customer_ways_table} AS target
USING change_data AS updates
ON target.date = updates.date
AND target.region = updates.region
AND target.tile_version = updates.tile_version
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
)

# COMMAND ----------

coverage_res = list(
    map(lambda row: row.asDict(), spark.table("distance_with_limits_all").collect())
)
has_limit_locations = "0%"
has_limit_tomtom = "0%"
no_limit_locations = "0%"
no_limit_tomtom = "0%"

for c in coverage_res:
    locations = c["pct_dist_with_location_limit"]
    tomtom = c["pct_dist_with_tomtom_limit"]
    if c["has_speed_limit"]:
        has_limit_locations = f"{locations:.4f}%"
        has_limit_tomtom = f"{tomtom:.4f}%"
    else:
        no_limit_locations = f"{locations:.4f}%"
        no_limit_tomtom = f"{tomtom:.4f}%"

coverage_state = list(
    map(lambda row: row.asDict(), spark.table("distance_with_limits_state").collect())
)
state_coverage_info = ""
for c in coverage_state:
    state = c["state"]
    locations = c["pct_dist_with_location_limit"]
    tomtom = c["pct_dist_with_tomtom_limit"]
    change = tomtom - locations
    sign = "+" if change > 0 else "-"
    change = abs(change)
    state_coverage_info += (
        f"* {state}: {tomtom:.4f}% (prev: {locations:.4f}%) => {sign}{change:.4f}%\n"
    )

speeding_state = list(
    map(lambda row: row.asDict(), spark.table("change_in_speeding_state").collect())
)
state_speeding_info = ""
for ss in speeding_state:
    state = ss["state"]
    state_speeding_info += f"* {state}:\n"
    state_speeding_info += f"  - Not Speeding: {ss['pct_new_not_speeding']:.4f}% (prev: {ss['pct_loc_not_speeding']:.4f}%)\n"
    state_speeding_info += f"  - Light Speeding: {ss['pct_new_light_speeding']:.4f}% (prev: {ss['pct_loc_light_speeding']:.4f}%)\n"
    state_speeding_info += f"  - Moderate Speeding: {ss['pct_new_moderate_speeding']:.4f}% (prev: {ss['pct_loc_moderate_speeding']:.4f}%)\n"
    state_speeding_info += f"  - Heavy Speeding: {ss['pct_new_heavy_speeding']:.4f}% (prev: {ss['pct_loc_heavy_speeding']:.4f}%)\n"
    state_speeding_info += f"  - Severe Speeding: {ss['pct_new_severe_speeding']:.4f}% (prev: {ss['pct_loc_severe_speeding']:.4f}%)\n"

s = list(map(lambda row: row.asDict(), spark.table("change_in_speeding").collect()))[0]
results = """
```
{data_start} - {today}
% of customer distance driven with speed limits (full region):
* Has Limit: {has_limit_tomtom} (prev: {has_limit_location})
* No Limit: {no_limit_tomtom} (prev: {no_limit_location})

% of customer distance driven with speed limits (by state):
{state_coverage_info}

% of time spent speeding by category (full region):
* Not Speeding: {new_not_speeding:.4f}% (prev: {loc_not_speeding:.4f}%)
* Light Speeding: {new_light_speeding:.4f}% (prev: {loc_light_speeding:.4f}%)
* Moderate Speeding: {new_moderate_speeding:.4f}% (prev: {loc_moderate_speeding:.4f}%)
* Heavy Speeding: {new_heavy_speeding:.4f}% (prev: {loc_heavy_speeding:.4f}%)
* Severe Speeding: {new_severe_speeding:.4f}% (prev: {loc_severe_speeding:.4f}%)

% of time spent speeding by category (by state):
{state_speeding_info}```
""".format(
    data_start=data_start.strftime("%b %d, %Y"),
    today=today.strftime("%b %d, %Y"),
    has_limit_tomtom=has_limit_tomtom,
    has_limit_location=has_limit_locations,
    no_limit_tomtom=no_limit_tomtom,
    no_limit_location=no_limit_locations,
    state_coverage_info=state_coverage_info,
    state_speeding_info=state_speeding_info,
    new_not_speeding=s["pct_new_not_speeding"],
    loc_not_speeding=s["pct_loc_not_speeding"],
    new_light_speeding=s["pct_new_light_speeding"],
    loc_light_speeding=s["pct_loc_light_speeding"],
    new_moderate_speeding=s["pct_new_moderate_speeding"],
    loc_moderate_speeding=s["pct_loc_moderate_speeding"],
    new_heavy_speeding=s["pct_new_heavy_speeding"],
    loc_heavy_speeding=s["pct_loc_heavy_speeding"],
    new_severe_speeding=s["pct_new_severe_speeding"],
    loc_severe_speeding=s["pct_loc_severe_speeding"],
)
print(results)

# COMMAND ----------

emails = []
alert_name = "safety_mapdata_customer_ways_results"
slack_channels = ["safety-platform-alerts"]
execute_alert(
    "select 1",
    emails,
    slack_channels,
    alert_name,
    f"Speed Limits Coverage by Customer Ways Driven for `{region}`, osm_version: `{osm_version}`, tomtom_version: `{tomtom_version}`\n"
    + results,
)
exit_notebook(None, None)

# COMMAND ----------
