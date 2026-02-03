# Databricks notebook source

from datetime import datetime, timedelta, date
from delta.tables import *

metrics_date = date.today() - timedelta(days=1)

# COMMAND ----------

df = spark.sql(
    f"""select x.date, x.time as start_time, null as end_time, x.org_id, x.object_id, g.gateway_id, g.product_id, 'HWRA-122' as signature_key
        from (select tr.date, min(gps.time) as time, tr.device_id as object_id, tr.org_id,
                min_by(gps.value.proto_value.nordic_gps_debug.gps_fix_info.speed_accuracy_mm_per_s[0], gps.time) as speed
            from (select t.date, t.device_id, t.org_id, t.start_ms, t.end_ms
                from trips2db_shards.trips as t
                join productsdb.devices as d
                on d.id = t.device_id
                where d.product_id = 125
                and t.`date` = '{metrics_date}'
                and t.version = 101 ) as tr
            left join (select * from kinesisstats.osdnordicgpsdebug where date = '{metrics_date}')  as gps
            on tr.device_id = gps.object_id
            where gps.time between tr.start_ms and tr.end_ms
            group by tr.date, tr.device_id, tr.org_id, tr.start_ms, tr.end_ms
            having speed > 22352) as x
        join hardware.active_gateways as g
        on x.date = g.date
        and x.object_id = g.object_id
        and x.org_id = g.org_id"""
).select("*")


new_data = df.selectExpr(
    "date",
    "start_time",
    "end_time",
    "org_id",
    "object_id",
    "gateway_id",
    "product_id",
    "signature_key",
)
exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.date = updates.date \
    and original.start_time = updates.start_time \
    and original.end_time = updates.end_time \
    and original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.product_id = updates.product_id \
    and original.signature_key = updates.signature_key",
).whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "start_time": "updates.start_time",
        "end_time": "updates.end_time",
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "signature_key": "updates.signature_key",
    }
).execute()


# COMMAND ----------

df = spark.sql(
    f"""select x.date, x.time as start_time, null as end_time, x.org_id, x.object_id, g.gateway_id, g.product_id, 'HWRA-123' as signature_key
        from (select tr.date, max(gps.time) as time, tr.device_id as object_id, tr.org_id,
                max_by(gps.value.proto_value.nordic_gps_debug.gps_fix_info.speed_accuracy_mm_per_s[0], gps.time) as speed
            from (select t.date, t.device_id, t.org_id, t.start_ms, t.end_ms
                from trips2db_shards.trips as t
                join productsdb.devices as d
                on d.id = t.device_id
                where d.product_id = 125
                and t.`date` = '{metrics_date}'
                and t.version = 101) as tr
            left join (select * from kinesisstats.osdnordicgpsdebug where date = '{metrics_date}')  as gps
            on tr.device_id = gps.object_id
            where gps.time between tr.start_ms and tr.end_ms
            group by tr.date, tr.device_id, tr.org_id, tr.start_ms, tr.end_ms
            having speed > 22352) as x
        join hardware.active_gateways as g
        on x.date = g.date
        and x.object_id = g.object_id
        and x.org_id = g.org_id"""
).select("*")

new_data = df.selectExpr(
    "date",
    "start_time",
    "end_time",
    "org_id",
    "object_id",
    "gateway_id",
    "product_id",
    "signature_key",
)
exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.date = updates.date \
    and original.start_time = updates.start_time \
    and original.end_time = updates.end_time \
    and original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.product_id = updates.product_id \
    and original.signature_key = updates.signature_key",
).whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "start_time": "updates.start_time",
        "end_time": "updates.end_time",
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "signature_key": "updates.signature_key",
    }
).execute()


# COMMAND ----------

df = spark.sql(
    f"""select cast(date as date) as date, org_id, device_id as object_id, gateway_id, product_id, start_time, end_time, 'HWRA-129' as signature_key
    from (select serial, org_id, device_id, gateway_id, product_id, min(date) as date, trip_starts as trip_id, min(time) as start_time, max(time) as end_time, (max(time)-min(time))/3600000 as time_hrs,
    sum(gps_null) as cnt_gps_nulls, sum(ecu_null) as cnt_ecu_nulls, count(*) as locations,
    sum(gps_null) *100 / count(*) as perc_gps_null,
    sum(ecu_null) *100 / count(*) as perc_ecu_null
    from (
    select *, trip_ends+trip_starts as trip_id
    from (select *, sum(trip_start) over (partition by serial, device_id, org_id order by time) as trip_starts,
        sum(trip_end) over (partition by serial, device_id, org_id order by time) as trip_ends
        from (select *,
                    case when prev_ecu < 4.5 and ecu_speed_meters_per_second > 4.5 then 1 else 0 end trip_start,
                    case when prev_ecu > 4.5 and ecu_speed_meters_per_second < 4.5 then -1 else 0 end trip_end,
                    case when gps_speed_meters_per_second is null then 1 else 0 end as gps_null,
                    case when ecu_speed_meters_per_second is null then 1 else 0 end as ecu_null
            from (select d.serial, l.date, l.org_id, l.device_id, l.time, d.gateway_id, d.product_id,
                        value.has_fix, 
                        value.latitude,
                        value.longitude,
                        value.gps_speed_meters_per_second,
                        value.ecu_speed_meters_per_second,
                        value.hdop,
                        value.vdop,
                        value.accuracy_millimeters,
                        lag(value.ecu_speed_meters_per_second) over (partition by serial, device_id, l.org_id order by time) as prev_ecu
                    from kinesisstats.location as l
                    join clouddb.organizations as o
                    on l.org_id = o.id
                    join (select h.date, h.object_id, h.org_id, h.serial, h.gateway_id, h.product_id
                        from hardware.active_gateways as h
                        where h.org_id not in (1, 54869)
                        and h.product_id in (178, 53, 24)
                        and h.date = '{metrics_date}') as d
                    on l.date = d.date
                    and l.device_id = d.object_id
                    and l.org_id = d.org_id
                    where o.internal_type = 0
                    and l.date >= '{metrics_date}')
            )
        )
    )
    where trip_id <> 0
    group by serial, org_id, device_id, gateway_id, trip_id, trip_starts, product_id
    having cnt_gps_nulls - cnt_ecu_nulls > 4
    and cnt_gps_nulls > 1
    and perc_gps_null > 50 
    and perc_ecu_null < 35
    order by device_id, perc_gps_null desc, perc_ecu_null)
"""
)

new_data = df.selectExpr(
    "date",
    "start_time",
    "end_time",
    "org_id",
    "object_id",
    "gateway_id",
    "product_id",
    "signature_key",
)
exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.date = updates.date \
    and original.start_time = updates.start_time \
    and original.end_time = updates.end_time \
    and original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.product_id = updates.product_id \
    and original.signature_key = updates.signature_key",
).whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "start_time": "updates.start_time",
        "end_time": "updates.end_time",
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "signature_key": "updates.signature_key",
    }
).execute()

# COMMAND ----------
## SOC drop >2% in 24 hours
df = spark.sql(
    f"""
    select z.date, z.object_id, z.org_id, g.id as gateway_id, g.serial as serial, max_by(soc_diff, time_diff) as soc_diff, 'HWRA-167' as signature_key, 125 as product_id
          from (select x.date, x.object_id, x.org_id, (x.time-y.time)/3600000 as time_diff, y.soc - x.soc as soc_diff, x.time as end_time, y.time as start_time
              from (select date, max(time) as time, b.object_id, b.org_id, max_by(value.proto_value.battery_info.fuel_gauge.soc, time) as soc
                      from kinesisstats.osdbatteryinfo as b
                      join productsdb.devices as d
                      on d.id = b.object_id
                      where d.product_id = 125
                      and b.date = '{metrics_date}'
                      group by b.date, b.object_id, b.org_id) as x
              join (select date, time, b.object_id, b.org_id, value.proto_value.battery_info.fuel_gauge.soc
                      from kinesisstats.osdbatteryinfo as b
                      join productsdb.devices as d
                      on d.id = b.object_id
                      where d.product_id = 125
                      and b.date = date_sub('{metrics_date}', 1)) as y
              on x.object_id = y.object_id
              and x.org_id = y.org_id
              where (x.time-y.time)/3600000 between 20 and 25) as z
          join clouddb.organizations as o
          on o.id = z.org_id
          join productsdb.gateways as g
          on z.object_id = g.device_id
          where o.internal_type = 0
          and o.id not in (select org_id from internaldb.simulated_orgs)
          group by z.date, z.object_id, z.org_id, g.id, g.serial
          having soc_diff > 2
    """
)

new_data = df.selectExpr(
    "date",
    "null as start_time",
    "null as end_time",
    "org_id",
    "object_id",
    "gateway_id",
    "product_id",
    "signature_key",
)
exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.date = updates.date \
    and original.start_time = updates.start_time \
    and original.end_time = updates.end_time \
    and original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.product_id = updates.product_id \
    and original.signature_key = updates.signature_key",
).whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "start_time": "updates.start_time",
        "end_time": "updates.end_time",
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "signature_key": "updates.signature_key",
    }
).execute()


# COMMAND ----------

df = spark.sql(
    f"""
    select date, min(time) as start_time, max(time) as end_time, peripheral_org_id as org_id, peripheral_device_id as object_id, peripheral_asset_id as gateway_id, 172 as product_id, 'HWRA-261' as signature_key
    from (select *
    from hardware_analytics.at11_data
    where date == '{metrics_date}'
    and protocol_version is null)
    group by date, peripheral_org_id, peripheral_device_id, peripheral_asset_id
    """
)

new_data = df.selectExpr(
    "date",
    "start_time",
    "end_time",
    "org_id",
    "object_id",
    "gateway_id",
    "product_id",
    "signature_key",
)
exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.date = updates.date \
    and original.start_time = updates.start_time \
    and original.end_time = updates.end_time \
    and original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.product_id = updates.product_id \
    and original.signature_key = updates.signature_key",
).whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "start_time": "updates.start_time",
        "end_time": "updates.end_time",
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "signature_key": "updates.signature_key",
    }
).execute()

# COMMAND ----------

# NOTE: This query has been commented out because kinesisstats.osdcruxproxylocationdebug
# and crux_proxy_location_debug have been removed and reserved. The new osdcruxdebug table has a
# different schema that doesn't include scanner_data or adv_data fields. This query needs to be
# rewritten to use the new data sources (e.g., osdcruxdebug, generic_proxy_readings_debug, or
# datastreams.crux_unaggregated_locations) with appropriate joins to get scanner location data.

# df = spark.sql(
#     f"""
#     select date, min(time) as start_time, max(time) as end_time, mac as gateway_id, 'HWRA-262' as signature_key
#     from (select c.date, c.time, c.value.proto_value.crux_proxy_location_debug.adv_data.mac
# from kinesisstats.osdcruxproxylocationdebug as c
# left join productsdb.gateways as g
# on c.value.proto_value.crux_proxy_location_debug.adv_data.mac = g.id
# where c.date == '{metrics_date}'
#     and g.id is null)
#     group by date, mac
#     """
# )
# MAC does not have org_id or object_id because it is not recorgnized
# Null product_id because it can expected to be AG51/AT11 or any other future peripheral in gateways table
# new_data = df.selectExpr(
#     "date",
#     "start_time",
#     "end_time",
#     "null as org_id",
#     "null as object_id",
#     "gateway_id",
#     "null as product_id",
#     "signature_key",
# )
# exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
# exist_heart.alias("original").merge(
#     new_data.alias("updates"),
#     "original.date = updates.date \
#     and original.start_time = updates.start_time \
#     and original.end_time = updates.end_time \
#     and original.org_id = updates.org_id \
#     and original.object_id = updates.object_id \
#     and original.gateway_id = updates.gateway_id \
#     and original.product_id = updates.product_id \
#     and original.signature_key = updates.signature_key",
# ).whenNotMatchedInsert(
#     values={
#         "date": "updates.date",
#         "start_time": "updates.start_time",
#         "end_time": "updates.end_time",
#         "org_id": "updates.org_id",
#         "object_id": "updates.object_id",
#         "gateway_id": "updates.gateway_id",
#         "product_id": "updates.product_id",
#         "signature_key": "updates.signature_key",
#     }
# ).execute()

# COMMAND ----------
df = spark.sql(
    f"""
    select date, min(time) as start_time, max(time) as end_time, peripheral_org_id as org_id, peripheral_device_id as object_id, peripheral_asset_id as gateway_id, 172 as product_id, 'HWRA-126' as signature_key
    from (select *
    from hardware_analytics.at11_data
    where date == '{metrics_date}'
    and tx_power =111)
    group by date, peripheral_org_id, peripheral_device_id, peripheral_asset_id
    """
)

new_data = df.selectExpr(
    "date",
    "start_time",
    "end_time",
    "org_id",
    "object_id",
    "gateway_id",
    "product_id",
    "signature_key",
)
exist_heart = DeltaTable.forName(spark, "hardware_analytics.field_signatures")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.date = updates.date \
    and original.start_time = updates.start_time \
    and original.end_time = updates.end_time \
    and original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.product_id = updates.product_id \
    and original.signature_key = updates.signature_key",
).whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "start_time": "updates.start_time",
        "end_time": "updates.end_time",
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "signature_key": "updates.signature_key",
    }
).execute()

# COMMAND ----------

# COMMAND ----------
