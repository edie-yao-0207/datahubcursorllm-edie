# Databricks notebook source
date = dbutils.widgets.get("date")
final_df = {}
final_df["date"] = date

# COMMAND ----------

total_active_devices = (
    spark.sql(
        f"""WITH ttab as (SELECT d.device_id
FROM(select id as device_id, org_id
FROM productsdb.devices
WHERE product_id in (125, 140, 143, 144)) as d
JOIN clouddb.organizations AS o ON
  d.org_id = o.id
WHERE o.internal_type <> 1)

select date, count(distinct(a.device_id)) as total_active_devices
FROM dataprep.active_devices as a
inner join ttab on
ttab.device_id = a.device_id
WHERE date == '{date}' and active_heartbeat IS TRUE
group by date"""
    )
    .rdd.collect()[0]
    .asDict()
)

final_df.update(total_active_devices)

# COMMAND ----------

gps_all_data = spark.sql(
    f"""
with gps as (
    select gpd.*, array_contains(value.proto_value.nordic_gps_debug.no_fix, true) as no_fix,
           lag(time) over(partition by object_id order by time asc) as gps_lag_time
    from kinesisstats.osdnordicgpsdebug as gpd
    where date == '{date}' and gpd.org_id <> 1 and
          (value.is_start is true or (value.is_end is false and value.is_databreak is false))
    ),

-- final powered and trips data
powered as (
    select gps.object_id, gps.org_id, gps.date, gps.value.time, 
           value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd[0]/1000000000 as lat,
           value.proto_value.nordic_gps_debug.gps_fix_info.longitude_nd[0]/1000000000 as lon,
           value.proto_value.nordic_gps_debug.gps_fix_info.time_to_fix_ms[0] as ttff,
           value.proto_value.nordic_gps_debug.no_fix[0] as fix,
           value.proto_value.nordic_gps_debug.gps_fix_info.execution_time_ms[0] as exec_time,
           value.proto_value.nordic_gps_debug.gps_scan_duration_ms[0] as gps_scan_time,
           value.proto_value.nordic_gps_debug.gps_fix_info.speed_mm_per_s[0] as gps_speed,
           b.start_time as power_started_ms, round(gps.time/300000,0) as trip_chunk_rank,
           b.end_time as power_removed_ms, 
           b.end_time - b.start_time as duration_ms,
           b.trip_rank
    from gps 
    inner join (select * from hardware_dev.meenu_ag52_trips_table where date == '{date}') as b 
    on b.object_id = gps.object_id 
    where gps.time between b.start_time and b.end_time and 
          value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd[0] is not null)

select *, 
      lag(lat) over(partition by object_id, trip_rank order by time asc) as lag_lat,
      lag(lon) over(partition by object_id, trip_rank order by time asc) as lag_lon,
      lag(time) over(partition by object_id, trip_rank order by time asc) as lag_time
from powered"""
)

gps_all_data.createOrReplaceTempView("gps_all_data")

gps_all_data.cache()


# COMMAND ----------

rrc_chunk_data = spark.sql(
    f"""
with rrc as (
    select object_id, org_id, date, value.time,
           value.received_delta_seconds,
           value.proto_value.nordic_lte_debug_connection_status.connection_type,
           value.proto_value.nordic_lte_debug_connection_status.rrc_connected_duration_ms
    from kinesisstats.osdnordicltedebugconnectionstatus
    where date == '{date}'
    ),

-- final powered and trips data
powered as (
    select rrc.*, round(rrc.time/300000,0) as trip_chunk_rank,
           b.start_time as power_started_ms, 
           b.end_time as power_removed_ms, 
           b.start_time - b.start_time as duration_ms,
           b.trip_rank
    from rrc 
    inner join (select * from hardware_dev.meenu_ag52_trips_table where date == '{date}') as b 
    on b.object_id = rrc.object_id 
    where rrc.time between b.start_time and b.end_time)

select object_id, org_id, date, trip_rank, trip_chunk_rank, duration_ms, power_started_ms, power_removed_ms, 
       sum(rrc_connected_duration_ms)/60000 as rrc_connected_mins, 
       sum(rrc_connected_duration_ms)*100/(60000*5) as perc_trip_rrc_connected
from powered
group by object_id, org_id, date, trip_rank, trip_chunk_rank, duration_ms, power_started_ms, power_removed_ms"""
)

rrc_chunk_data.createOrReplaceTempView("rrc_chunk_data")
rrc_chunk_data.cache()


# COMMAND ----------

# DBTITLE 1,Total trip time and trip count
total_trip_time_count = (
    spark.sql(
        f"select sum(duration_s)/3600 as total_trip_duration_hr, count(1) as total_trip, count(distinct(object_id)) as total_trip_devices from (select * from hardware_dev.meenu_ag52_trips_table where date == '{date}')"
    )
    .rdd.collect()[0]
    .asDict()
)

final_df.update(total_trip_time_count)

# COMMAND ----------

# DBTITLE 1,Del time and del distance percentiles
del_time_del_dist = (
    spark.sql(
        f"""select date, 
       percentile_approx(del_time, 0.5) del_time_p50,
       percentile_approx(del_time, 0.90) del_time_p90,
       percentile_approx(del_time, 0.95) del_time_p95,
       percentile_approx(del_time, 0.99) del_time_p99,
       percentile_approx(hav_m, 0.5) hav_m_p50,
       percentile_approx(hav_m, 0.90) hav_m_p90,
       percentile_approx(hav_m, 0.95) hav_m_p95,
       percentile_approx(hav_m, 0.99) hav_m_p99
from (select *,
       (time - lag_time)/1000 as del_time, 
       2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) + 
                                   cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
from gps_all_data)
group by date"""
    )
    .rdd.collect()[0]
    .asDict()
)

final_df.update(del_time_del_dist)

# COMMAND ----------

# DBTITLE 1,% Trip with gaps
try:
    perc_trip_with_gaps = (
        spark.sql(
            f"""with gps_lag_data as 
            (select *,
                   (time - lag_time)/1000 as del_time, 
                   2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) + 
                                               cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
            from gps_all_data),

            tc as (
            select date, count(distinct(object_id, trip_rank)) as total_trips
            from gps_lag_data
            group by date),

            filt1 as (
            select date, count(distinct(object_id, trip_rank)) as trip_with_time_gap
            from gps_lag_data
            where del_time > 300
            group by date
            ),

            filt2 as (
            select date, count(distinct(object_id, trip_rank)) as trip_with_2dist_gap
            from gps_lag_data
            where hav_m > 2000
            group by date
            ),

            filt3 as (
            select date, count(distinct(object_id, trip_rank)) as trip_with_5dist_gap
            from gps_lag_data
            where hav_m > 5000
            group by date
            ),

            filt4 as (
            select date, count(distinct(object_id, trip_rank)) as trip_with_8dist_gap
            from gps_lag_data
            where hav_m > 8000
            group by date
            )

            select tc.date, 
                   trip_with_time_gap*100/tc.total_trips as trip_with_time_gap, 
                   trip_with_2dist_gap*100/tc.total_trips as trip_with_2dist_gap,
                   trip_with_5dist_gap*100/tc.total_trips as trip_with_5dist_gap, 
                   trip_with_8dist_gap*100/tc.total_trips as trip_with_8dist_gap
            from tc
            join filt1
            on tc.date = filt1.date
            join filt2
            on tc.date = filt2.date
            join filt3
            on tc.date = filt3.date
            join filt4
            on tc.date = filt4.date"""
        )
        .rdd.collect()[0]
        .asDict()
    )

except:
    perc_trip_with_gaps = (
        spark.sql(
            f"""select cast(null as double) as trip_with_time_gap, 
                   cast(null as double) as trip_with_2dist_gap,
                   cast(null as double) as trip_with_5dist_gap, 
                   cast(null as double) as trip_with_8dist_gap"""
        )
        .rdd.collect()[0]
        .asDict()
    )


final_df.update(perc_trip_with_gaps)


# COMMAND ----------

# DBTITLE 1,% Trips hr with gaps
try:
    perc_trip_hr_with_gaps = (
        spark.sql(
            f"""with gps_lag_data as 
              (select *, round(time/3600000,0) as trip_hr_rank,
                     (time - lag_time)/1000 as del_time, 
                     2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) + 
                                                 cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
              from gps_all_data),

              tc as (
              select date, count(distinct(object_id, trip_rank, trip_hr_rank)) as total_trip_hrs
              from gps_lag_data
              group by date),

              filt1 as (
              select date, count(distinct(object_id, trip_rank, trip_hr_rank)) as trip_hr_with_time_gap
              from gps_lag_data
              where del_time > 300
              group by date
              ),

              filt2 as (
              select date, count(distinct(object_id, trip_rank, trip_hr_rank)) as trip_hr_with_2dist_gap
              from gps_lag_data
              where hav_m > 2000
              group by date
              ),

              filt3 as (
              select date, count(distinct(object_id, trip_rank, trip_hr_rank)) as trip_hr_with_5dist_gap
              from gps_lag_data
              where hav_m > 5000
              group by date
              ),

              filt4 as (
              select date, count(distinct(object_id, trip_rank, trip_hr_rank)) as trip_hr_with_8dist_gap
              from gps_lag_data
              where hav_m > 8000
              group by date
              )

              select tc.date, 
                     trip_hr_with_time_gap*100/total_trip_hrs as trip_hr_with_time_gap, 
                     trip_hr_with_2dist_gap*100/total_trip_hrs as trip_hr_with_2dist_gap, 
                     trip_hr_with_5dist_gap*100/total_trip_hrs as trip_hr_with_5dist_gap, 
                     trip_hr_with_8dist_gap*100/total_trip_hrs as trip_hr_with_8dist_gap
              from tc
              join filt1
              on tc.date = filt1.date
              join filt2
              on tc.date = filt2.date
              join filt3
              on tc.date = filt3.date
              join filt4
              on tc.date = filt4.date"""
        )
        .rdd.collect()[0]
        .asDict()
    )
except:
    perc_trip_hr_with_gaps = (
        spark.sql(
            f"""select cast(null as double) as trip_hr_with_time_gap, 
                     cast(null as double) as trip_hr_with_2dist_gap, 
                     cast(null as double) as trip_hr_with_5dist_gap, 
                     cast(null as double) as trip_hr_with_8dist_gap"""
        )
        .rdd.collect()[0]
        .asDict()
    )


final_df.update(perc_trip_hr_with_gaps)

# COMMAND ----------

# DBTITLE 1,% Devices with trip gaps
try:
    perc_devices_with_gaps = (
        spark.sql(
            f"""with gps_lag_data as 
              (select *,
                     (time - lag_time)/1000 as del_time, 
                     2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) + 
                                                 cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
              from gps_all_data),

              tc as (
              select date, count(distinct(object_id)) as total_devices
              from gps_lag_data
              group by date),

              filt1 as (
              select date, count(distinct(object_id)) as devices_with_time_gap
              from gps_lag_data
              where del_time > 300
              group by date
              ),

              filt2 as (
              select date, count(distinct(object_id)) as devices_with_2dist_gap
              from gps_lag_data
              where hav_m > 2000
              group by date
              ),

              filt3 as (
              select date, count(distinct(object_id)) as devices_with_5dist_gap
              from gps_lag_data
              where hav_m > 5000
              group by date
              ),

              filt4 as (
              select date, count(distinct(object_id)) as devices_with_8dist_gap
              from gps_lag_data
              where hav_m > 8000
              group by date
              )

              select tc.date, 
                     devices_with_time_gap*100/total_devices as devices_with_time_gap, 
                     devices_with_2dist_gap*100/total_devices as devices_with_2dist_gap, 
                     devices_with_5dist_gap*100/total_devices as devices_with_5dist_gap, 
                     devices_with_8dist_gap*100/total_devices as devices_with_8dist_gap
              from tc
              join filt1
              on tc.date = filt1.date
              join filt2
              on tc.date = filt2.date
              join filt3
              on tc.date = filt3.date
              join filt4
              on tc.date = filt4.date"""
        )
        .rdd.collect()[0]
        .asDict()
    )

except:
    perc_devices_with_gaps = (
        spark.sql(
            f"""select cast(null as double) as devices_with_time_gap, 
                     cast(null as double) as devices_with_2dist_gap, 
                     cast(null as double) as devices_with_5dist_gap, 
                     cast(null as double) as devices_with_8dist_gap"""
        )
        .rdd.collect()[0]
        .asDict()
    )

final_df.update(perc_devices_with_gaps)

# COMMAND ----------

# DBTITLE 1,% Successful fixes per trip


# COMMAND ----------

# DBTITLE 1,percentile rrc connected per trip chunk
try:
    perc_rrc_connected = (
        spark.sql(
            f"""select percentile_approx(perc_trip_rrc_connected, 0.5) as p50_rrc_connect,
        percentile_approx(perc_trip_rrc_connected, 0.90) as p90_rrc_connect,
        percentile_approx(perc_trip_rrc_connected, 0.95) as p95_rrc_connect,
        percentile_approx(perc_trip_rrc_connected, 0.99) as p99_rrc_connect
  from rrc_chunk_data
  group by date"""
        )
        .rdd.collect()[0]
        .asDict()
    )

except:
    perc_rrc_connected = (
        spark.sql(
            f"""select cast(null as decimal(38,18)) as p50_rrc_connect,
        cast(null as decimal(38,18)) as p90_rrc_connect,
        cast(null as decimal(38,18)) as p95_rrc_connect,
        cast(null as decimal(38,18)) as p99_rrc_connect"""
        )
        .rdd.collect()[0]
        .asDict()
    )

final_df.update(perc_rrc_connected)

# COMMAND ----------

# DBTITLE 1,%trip chunks with %RRC > x%
try:
    perc_trip_chunk_rrc_over = (
        spark.sql(
            f"""with tc as (
              select date, count(1) as total_chunks
              from rrc_chunk_data
              group by date),

              ov25 as (select date, count(1) as o25
              from rrc_chunk_data
              where perc_trip_rrc_connected > 25
              group by date),

              ov50 as (select date, count(1) as o50
              from rrc_chunk_data
              where perc_trip_rrc_connected > 50
              group by date),

              ov75 as (select date, count(1) as o75
              from rrc_chunk_data
              where perc_trip_rrc_connected > 75
              group by date)


              select tc.date, 
                     o25*100/total_chunks as perc_chunk_o25, 
                     o50*100/total_chunks as perc_chunk_o50, 
                     o75*100/total_chunks as perc_chunk_o75
              from tc
              join ov25 on
              tc.date = ov25.date
              join ov50 on
              tc.date = ov50.date
              join ov75 on
              tc.date = ov75.date"""
        )
        .rdd.collect()[0]
        .asDict()
    )
except:
    perc_trip_chunk_rrc_over = (
        spark.sql(
            f"""select null as perc_chunk_o25, 
                     null as perc_chunk_o50, 
                     null as perc_chunk_o75"""
        )
        .rdd.collect()[0]
        .asDict()
    )

final_df.update(perc_trip_chunk_rrc_over)

# COMMAND ----------

spark_df = spark.createDataFrame(
    [final_df],
    schema=sql("select * from hardware_dev.meenu_ag52_live_ping_metric").schema,
)
spark_df.write.option("replaceWhere", f"date = '{date}'").mode("overwrite").saveAsTable(
    "hardware_dev.meenu_ag52_live_ping_metric"
)
