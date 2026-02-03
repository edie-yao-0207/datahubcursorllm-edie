import boto3
import googlemaps
import pandas as pd

from datetime import datetime, timedelta, timezone
from pyspark.sql import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from typing import List

# COMMAND ----------

# Schema for rows to update later. We need to create a schema so that
# rows get correctly merged into the scheduled_distances table.
schema = StructType(
    [
        StructField("route_id", LongType(), False),
        StructField("job_id", LongType(), False),
        StructField("route_scheduled_start_ms", LongType(), False),
        StructField("route_scheduled_or_updated_ms", LongType(), False),
        StructField("route_last_updated_time", TimestampType(), False),
        StructField("segment_distance", LongType(), False),
        StructField("route_distance", LongType(), False),
        StructField("error", StringType(), False),
    ]
)

# COMMAND ----------


# Given an array of points and a split size, create a 2d array with points
# split into groups of at most the split size. The first point of an array
# will be the last point of the previous array (this let's us compute
# route distances without any gaps). The last array can potentially have
# fewer points. Points should contain at least 2 elements and split_size
# must be greater than 1.
# ex: split size 3, points = [a, b, c, d, e, f] =>
# [[a, b, c], [c, d, e], [e, f]]
def _split_points(points: List[str], split_size: int):
    while len(points) > 1:
        chunk, points = points[:split_size], points[split_size - 1 :]
        yield chunk


# Provided a google maps api response and the waypoints used to make the api
# call, validates the response contents, raising a ValueError if an issue
# is found.
def _validate_google_maps_response(route_id: int, resp, chunk: List[str]):
    if not resp:
        raise ValueError(f"Empty response for route {route_id}")
    route = resp[0]
    if "waypoint_order" not in route or "legs" not in route:
        raise ValueError(
            f"Malformed response for route {route_id}: no waypoint_order or legs fields"
        )
    if (
        len(route["waypoint_order"]) != len(chunk)
        or len(route["legs"]) != len(chunk) + 1
    ):
        raise ValueError(
            f"Malformed response for route {route_id}: waypoint or legs inconsistent with chunk"
        )


# Recalculate_route will compute the scheduled distances for all rows
# associated with a single route.
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def recalculate_route(route_df):
    output_cols = [
        "route_id",
        "job_id",
        "route_scheduled_start_ms",
        "route_scheduled_or_updated_ms",
        "route_last_updated_time",
        "segment_distance",
        "route_distance",
        "error",
    ]
    # For error cases, we will return a dataframe with error field populated
    # and most other fields set at default values. This allows us to retrieve
    # the errors outside of the UDF.
    err_row = {
        "route_id": 0,
        "job_id": 0,
        "route_scheduled_start_ms": 0,
        "route_scheduled_or_updated_ms": 0,
        "route_last_updated_time": time_now,
        "segment_distance": 0,
        "route_distance": 0,
        "error": "",
    }
    if route_df.empty:
        err_row["error"] = "Input dataframe for route is empty"
        return pd.DataFrame([err_row], columns=output_cols)

    # route_df contains rows for a single route.
    route_id = route_df["route_id"].values[0]

    # Set route_id in the error dataframe for debugging purposes.
    err_row["route_id"] = route_id

    # Sort job rows for route.
    sorted_df = route_df.sort_values(
        ["scheduled_arrival_time", "job_id"], ascending=[True, True]
    )

    destinations = []
    job_ids = []
    origin = ""
    scheduled_start_ms = 0
    route_scheduled_or_updated_ms = 0
    totalMeters = 0
    leg_distances = []

    # Loop over jobs for the route.
    for _, coord_info in sorted_df.iterrows():
        if origin == "":
            origin = f"{coord_info.start_lat},{coord_info.start_lng}"
            destinations.append(origin)
            scheduled_start_ms = coord_info.scheduled_start_ms
            route_scheduled_or_updated_ms = coord_info.route_scheduled_or_updated_ms

        job_ids.append(coord_info.job_id)
        destinations.append(f"{coord_info.dest_lat},{coord_info.dest_lng}")

    # Make requests to gmaps, storing distances along the way.
    # We are limited to 10 waypoints for standard billing. The first and last
    # points are used as the origin and destination parameters to the API call
    # so are not included in waypoints.
    for chunk in _split_points(destinations, 12):
        if len(chunk) < 2:
            err_row[
                "error"
            ] = f"Malformed request for route {route_id}: chunk with too few elements"
            return pd.DataFrame([err_row], columns=output_cols)

        origin = chunk.pop(0)
        dest = chunk.pop(-1)
        resp = None
        try:
            resp = gmaps.directions(origin, dest, waypoints=chunk, mode="driving")
        except googlemaps.exceptions.ApiError as e:
            # https://googlemaps.github.io/google-maps-services-python/docs/#module-googlemaps.exceptions
            # In either of these cases we just want to skip the route, not
            # error out.
            if e.status == "NOT_FOUND" or e.status == "ZERO_RESULTS":
                err_row["error"] = f"API Error {e.status} for route {route_id}"
                return pd.DataFrame([err_row], columns=output_cols)
            raise

        # Do some sanity checking of the gmaps response and return an
        # error row if the response is malformed.
        try:
            _validate_google_maps_response(route_id, resp, chunk)
        except ValueError as e:
            err_row["error"] = str(e)
            return pd.DataFrame([err_row], columns=output_cols)

        legs = resp[0]["legs"]
        for leg in legs:
            if "distance" not in leg or "value" not in leg["distance"]:
                err_row[
                    "error"
                ] = f"Malformed response for route {route_id}: missing distance or value fields"
                return pd.DataFrame([err_row], columns=output_cols)

            leg_distance = leg["distance"]["value"]
            leg_distances.append(leg_distance)
            totalMeters += leg_distance

    if len(job_ids) != len(leg_distances):
        err_row[
            "error"
        ] = f"Inconsistent result processed for route {route_id}: job_ids doesn't match leg_distances"
        return pd.DataFrame([err_row], columns=output_cols)

    rows_to_insert = []
    for job_id, segment_distance in zip(job_ids, leg_distances):
        # Specifying values as dict; names and order must be the same.

        # Previous spec with Row(job_id=job_id ...)
        # led to a jumbled column order write.
        dist_at_job = {
            "route_id": route_id,
            "job_id": job_id,
            "route_scheduled_start_ms": scheduled_start_ms,
            "route_scheduled_or_updated_ms": route_scheduled_or_updated_ms,
            "route_last_updated_time": time_now,
            "segment_distance": segment_distance,
            "route_distance": totalMeters,
            "error": "",
        }
        rows_to_insert.append(dist_at_job)

    # create a dataframe of rows that need to update
    return pd.DataFrame(rows_to_insert, columns=output_cols)


# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

# START initial setup #

# Create google maps client:
# 1. Key is stored with our databricks instance. Added via the secrets CLI.
# 2. Timeout on connection and read per request.
# 3. Queries per second to make sure we don't exceed our contract QPS (threshold 125 queries/s globally)
# See https://googlemaps.github.io/google-maps-services-python/docs/index.html
#   for client documentation
# See https://developers.google.com/maps/documentation/directions/usage-and-billing#other-usage-limits
#   for details on the API limits
ssm_client = get_ssm_client("standard-read-parameters-ssm", use_region=False)
gmaps = googlemaps.Client(
    key=get_ssm_parameter(ssm_client, "DISPATCH_ROUTES_GOOGLE_MAPS_KEY"),
    timeout=60,
    queries_per_second=1,
)

# Single definition of when update happened
time_now = datetime.now(timezone.utc)
time_now_ms = int(time_now.timestamp() * 1e3)

# Chunk the route data so that we limit how much we data we process
# in parallel at any point in time
batch_duration_ms = 30 * 60 * 1000

create_scheduled_distances_query = """
    CREATE TABLE IF NOT EXISTS dataprep_routing.scheduled_distances(
      route_id BIGINT NOT NULL,
      job_id BIGINT NOT NULL,
      route_scheduled_start_ms BIGINT NOT NULL,
      route_scheduled_or_updated_date DATE NOT NULL,
      route_last_updated_time TIMESTAMP,
      segment_distance BIGINT,
      route_distance BIGINT
    )
    USING delta
    PARTITIONED BY (route_scheduled_or_updated_date)
"""
spark.sql(create_scheduled_distances_query)

# Setup spark tables and loop agnostic views outside of loop
dispatch_routes_table = spark.table("dispatchdb.dispatch_routes").selectExpr(
    "id as route_id",
    "scheduled_start_ms",
    "TO_DATE(from_unixtime(scheduled_start_ms / 1000)) AS route_scheduled_or_updated_date",
    "scheduled_meters",
    "start_location_lat",
    "start_location_lng",
    "start_location_address_id",
    "created_at as route_created_at",
    "updated_at as route_updated_at",
)

dispatch_jobs_table = spark.table("dispatchdb.dispatch_jobs").selectExpr(
    "dispatch_route_id as route_id",
    "id as job_id",
    "destination_lat",
    "destination_lng",
    "destination_address_id",
    "created_at as job_created_at",
    "updated_at as job_updated_at",
    "scheduled_arrival_time",
    "job_state",
)

addresses_table = spark.table("clouddb.addresses")

# getting all completed routes based on job state
# completed routes may contain all completed jobs, some skipped some completed, but not all skipped
# 3 -> completed
# 4 -> skipped
completed_route_ids = (
    dispatch_jobs_table.selectExpr("route_id", "job_state")
    .groupBy("route_id")
    .agg({"job_state": "min", "job_state": "max"})
    .where("min(job_state)=3 and max(job_state)<=4")
    .select("route_id")
)

completed_routes = dispatch_routes_table.join(completed_route_ids, "route_id")

min_scheduled_start_ms = (
    dispatch_routes_table.where("scheduled_start_ms >= 0")
    .agg({"scheduled_start_ms": "min"})
    .collect()[0]["min(scheduled_start_ms)"]
)
# Add 1 to the max_scheduled_start_ms since we will fetch routes exclusive of the end time
max_scheduled_start_ms = (
    dispatch_routes_table.where(f"scheduled_start_ms <= {time_now_ms}")
    .agg({"scheduled_start_ms": "max"})
    .collect()[0]["max(scheduled_start_ms)"]
    + 1
)

current_start_ms = min_scheduled_start_ms
current_end_ms = min(current_start_ms + batch_duration_ms, max_scheduled_start_ms)

# In order to control the query rate to the Google
# directions API, we batch processing into smaller
# time chunks
while current_start_ms < max_scheduled_start_ms:
    # Restrict to routes which have scheduled_start_ms within the current time window
    selected_routes = completed_routes.where(
        f"scheduled_meters is null and scheduled_start_ms >= {current_start_ms} and scheduled_start_ms < {current_end_ms}"
    ).selectExpr(
        "route_id",
        "scheduled_start_ms",
        "route_scheduled_or_updated_date",
        "start_location_lat",
        "start_location_lng",
        "start_location_address_id",
        "route_created_at",
        "route_updated_at",
    )

    selected_routes_and_jobs = selected_routes.join(
        dispatch_jobs_table, "route_id"
    ).select(
        "route_id",
        "job_id",
        "scheduled_arrival_time",
        "start_location_lat",
        "start_location_lng",
        "start_location_address_id",
        "destination_lat",
        "destination_lng",
        "destination_address_id",
        "scheduled_start_ms",
        "route_scheduled_or_updated_date",
        "route_created_at",
        "route_updated_at",
        "job_created_at",
        "job_updated_at",
    )

    # checking if addresses exist in clouddb.addresses
    # first join to get the lat/lng for all starting points
    # then join again to get lat/lng for all destinations
    route_address_ids = selected_routes_and_jobs.join(
        addresses_table,
        selected_routes_and_jobs.start_location_address_id == addresses_table.id,
        "left",
    ).selectExpr(
        "route_id",
        "job_id",
        "scheduled_arrival_time",
        "start_location_lat",
        "start_location_lng",
        "start_location_address_id",
        "destination_lat",
        "destination_lng",
        "destination_address_id",
        "scheduled_start_ms",
        "route_scheduled_or_updated_date",
        "route_created_at",
        "route_updated_at",
        "job_created_at",
        "job_updated_at",
        "latitude as route_clouddb_lat",
        "longitude as route_clouddb_lng",
    )

    job_address_ids = route_address_ids.join(
        addresses_table,
        route_address_ids.destination_address_id == addresses_table.id,
        "left",
    ).selectExpr(
        "route_id",
        "job_id",
        "scheduled_arrival_time",
        "start_location_lat",
        "start_location_lng",
        "start_location_address_id",
        "destination_lat",
        "destination_lng",
        "destination_address_id",
        "scheduled_start_ms",
        "route_scheduled_or_updated_date",
        "route_created_at",
        "route_updated_at",
        "job_created_at",
        "job_updated_at",
        "route_clouddb_lat",
        "route_clouddb_lng",
        "latitude as job_clouddb_lat",
        "longitude as job_clouddb_lng",
    )

    # complete all routes and jobs
    completed_routes_and_jobs_addr = job_address_ids.selectExpr(
        "route_id",
        "job_id",
        "scheduled_start_ms",
        "CAST(to_unix_timestamp(route_scheduled_or_updated_date) AS BIGINT) * 1000 AS route_scheduled_or_updated_ms",
        "coalesce(route_clouddb_lat, start_location_lat) as start_lat",
        "coalesce(route_clouddb_lng, start_location_lng) as start_lng",
        "coalesce(job_clouddb_lat, destination_lat) as dest_lat",
        "coalesce(job_clouddb_lng, destination_lng) as dest_lng",
        "scheduled_arrival_time",
    )

    # group by route_id and recalculate scheduled distances per group
    calculated_rows = completed_routes_and_jobs_addr.groupby("route_id").apply(
        recalculate_route
    )

    updates = calculated_rows.where(calculated_rows["error"] == "").selectExpr(
        "route_id",
        "job_id",
        "route_scheduled_start_ms",
        "TO_DATE(from_unixtime(route_scheduled_or_updated_ms / 1000)) AS route_scheduled_or_updated_date",
        "route_last_updated_time",
        "segment_distance",
        "route_distance",
    )

    updates.createOrReplaceTempView("updates")
    query = """
        MERGE INTO dataprep_routing.scheduled_distances AS distances
        USING updates
        ON distances.route_id = updates.route_id AND distances.job_id = updates.job_id
        WHEN matched THEN update SET *
        WHEN NOT matched THEN INSERT *
    """
    spark.sql(query)

    current_start_ms = current_end_ms
    current_end_ms = min(current_start_ms + batch_duration_ms, max_scheduled_start_ms)

# COMMAND ----------
