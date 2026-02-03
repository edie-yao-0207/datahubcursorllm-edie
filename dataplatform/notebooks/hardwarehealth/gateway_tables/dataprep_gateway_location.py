# Databricks notebook source

from datetime import datetime, timedelta, date
from delta.tables import *

metrics_date = date.today() - timedelta(days=1)

df = spark.sql(
    f"""select l.org_id, l.device_id as object_id, h.gateway_id, h.serial, h.product_id, l.date, l.time, l.location_stats.latitude, l.location_stats.longitude, l.location_stats.gps_fix_timestamp_utc_ms, l.location_stats.has_fix
    from (select org_id,
                device_id,
                max(time) as time,
                max(date) as date,
                max_by(value, time) as location_stats 
            from kinesisstats.location
            where date == '{metrics_date}'
            group by org_id, device_id) as l
    left join hardware.gateways_heartbeat as h
    on l.device_id = h.object_id
    and l.org_id = h.org_id
    where l.date >= h.first_heartbeat_date
    and l.date <= h.last_heartbeat_date"""
).select("*")


new_data = df.selectExpr(
    "org_id",
    "object_id",
    "gateway_id",
    "serial",
    "product_id",
    "date",
    "time",
    "latitude",
    "longitude",
    "gps_fix_timestamp_utc_ms",
    "has_fix",
)
exist_heart = DeltaTable.forName(spark, "hardware.gateways_last_reported_location")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.serial = updates.serial \
    and original.product_id = updates.product_id",
).whenMatchedUpdate(
    set={
        "date": "updates.date",
        "time": "updates.time",
        "latitude": "updates.latitude",
        "longitude": "updates.longitude",
        "gps_fix_timestamp_utc_ms": "updates.gps_fix_timestamp_utc_ms",
        "has_fix": "updates.has_fix",
    }
).whenNotMatchedInsert(
    values={
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "serial": "updates.serial",
        "date": "updates.date",
        "time": "updates.time",
        "latitude": "updates.latitude",
        "longitude": "updates.longitude",
        "gps_fix_timestamp_utc_ms": "updates.gps_fix_timestamp_utc_ms",
        "has_fix": "updates.has_fix",
    }
).execute()
