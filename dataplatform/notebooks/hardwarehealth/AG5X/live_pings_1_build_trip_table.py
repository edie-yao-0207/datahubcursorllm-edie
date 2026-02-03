# Databricks notebook source
date = dbutils.widgets.get("date")

# COMMAND ----------

prel_trips_data = spark.sql(
    f"""with a as ( -- get AG52 devices
    SELECT d.device_id, d.product_id, d.org_id, o.name
    FROM(select id as device_id, org_id, product_id
         FROM productsdb.devices
         WHERE product_id in (125, 140, 143, 144)) as d
    JOIN clouddb.organizations AS o ON
          d.org_id = o.id
    WHERE o.internal_type <> 1
    ),
z as (select object_id, org_id, time, date,
        coalesce(array_max(value.proto_value.auxiliary_voltages.aux_mv),0) as voltage,
        lead(coalesce(array_max(value.proto_value.auxiliary_voltages.aux_mv),0)) 
                      over(partition by object_id order by time asc) as next_voltage,
        lag(coalesce(array_max(value.proto_value.auxiliary_voltages.aux_mv),0)) 
                      over(partition by object_id order by time asc) as prev_voltage
   from kinesisstats.osdauxiliaryvoltages
   where date == '{date}' and value.is_end is false and value.is_databreak is false),

x as (select object_id, org_id, date, to_timestamp(time/1000),
           lag(time) over(partition by object_id order by time asc) as start_time,
           time as end_time,     
           prev_voltage, voltage, next_voltage
    from z 
    where (prev_voltage < 6000 and voltage >= 9000) or 
          (next_voltage < 6000 and voltage >= 9000) or 
          (next_voltage is null and voltage >=9000) or 
          (prev_voltage is null and voltage >=9000))

select object_id, a.name as org_name, date, start_time, end_time, (end_time - start_time)/1000 as duration_s,
    RANK() OVER(PARTITION BY x.object_id ORDER BY start_time asc) as trip_rank -- unique ascending trip number
from x  
inner join a on a.device_id = x.object_id  
where ((next_voltage < 6000 and voltage >= 9000 ) or 
     (next_voltage is null and voltage >= 9000)) and 
     start_time is not null and prev_voltage >= 9000 and
     (end_time - start_time)/1000 > 300
order by object_id, trip_rank"""
)

prel_trips_data.createOrReplaceTempView("prel_trips_data")


# COMMAND ----------

trips_data = spark.sql(
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
    select b.*, org_id, value.proto_value.nordic_gps_debug.gps_fix_info.speed_mm_per_s[0] as gps_speed
    from gps 
    inner join prel_trips_data as b 
    on b.object_id = gps.object_id 
    where gps.time between b.start_time and b.end_time and 
          value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd[0] is not null)

select cast(date as date) as date, org_id, org_name, object_id, start_time, end_time, duration_s, trip_rank, 
       percentile_approx(gps_speed, 0.5) as p50_speed, avg(gps_speed) as avg_speed, count(1) as gps_cnt
from powered
group by object_id, org_id, org_name, date, start_time, end_time, duration_s, trip_rank
having p50_speed > 4470 or avg_speed > 4470 and gps_cnt > 0"""
)


# COMMAND ----------

trips_data.write.partitionBy("date").option("replaceWhere", f"date = '{date}'").mode(
    "overwrite"
).saveAsTable("hardware_dev.meenu_ag52_trips_table")

# COMMAND ----------
