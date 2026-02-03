from datetime import datetime, timedelta

DATE = str((datetime.utcnow() - timedelta(days=1)).date())
query = f"""
WITH raw AS (
    SELECT
        device_id,
        org_id,
        o.name as org_name,
        o.internal_type,
        proto.trip_distance.distance_meters AS distance_meters,
        (end_ms - start_ms)/1000 AS trip_seconds
    FROM trips2db_shards.trips t
    JOIN clouddb.organizations o
        ON o.id = t.org_id
    WHERE date = '{DATE}'
        AND end_ms - start_ms < (24*60*60*1000)
        AND t.version = 101
),
agged AS (
    SELECT
        device_id,
        org_name,
        org_id,
        internal_type,
        SUM(distance_meters) AS total_distance_meters,
        SUM(distance_meters)/SUM(trip_seconds) AS avg_speed_mps,
        SUM(trip_seconds) as total_trip_seconds,
        SUM(distance_meters)/COUNT(1) AS avg_trip_distance_meters,
        count(1) AS num_trips
    FROM raw
    GROUP BY
        device_id,
        org_name,
        org_id,
        internal_type
)
SELECT
    '{DATE}' AS date,
    a.*,
    LOG(avg_trip_distance_meters + 1) AS log_avg_trip_distance_meters,
    CASE
        WHEN avg_speed_mps < (-11/12) * LOG(avg_trip_distance_meters + 1) + 11 THEN 0
        WHEN avg_speed_mps < (-5/6) * LOG(avg_trip_distance_meters + 1) + 29.5 THEN 1
        ELSE 2
    END AS avg_trip_len_and_speed_category,
    CASE
        WHEN avg_speed_mps < (-11/12) * LOG(avg_trip_distance_meters + 1) + 11 THEN 'low'
        WHEN avg_speed_mps < (-5/6) * LOG(avg_trip_distance_meters + 1) + 29.5 THEN 'medium'
        ELSE 'high'
    END AS avg_trip_len_and_speed_category_name
FROM agged a
"""
df = spark.sql(query)
df.where(df.date == DATE).write.partitionBy("date").format("delta").option(
    "replaceWhere", f"date = '{DATE}'"
).mode("overwrite").saveAsTable("datascience.daily_harsh_events_trip_and_speed_strata")
