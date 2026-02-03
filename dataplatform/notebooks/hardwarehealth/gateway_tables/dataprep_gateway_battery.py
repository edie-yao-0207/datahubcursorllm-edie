# Databricks notebook source

from datetime import datetime, timedelta, date
from delta.tables import *

metrics_date = date.today() - timedelta(days=1)

df = spark.sql(
    f"""select b.org_id, b.object_id, h.gateway_id, h.serial, h.product_id, b.date, b.time, b.battery_stats
        from (select org_id,
                    object_id,
                    max(time) as time,
                    max(date) as date,
                    max_by(value.proto_value.battery_info.cell.mv, time) as battery_stats
                from kinesisstats.osdbatteryinfo
                where date == '{metrics_date}'
                group by org_id, object_id) as b
        left join hardware.gateways_heartbeat as h
        on b.object_id = h.object_id
        and b.org_id = h.org_id
        where b.date >= h.first_heartbeat_date
        and b.date <= h.last_heartbeat_date"""
).select("*")


new_data = df.selectExpr(
    "org_id",
    "object_id",
    "gateway_id",
    "serial",
    "product_id",
    "date",
    "time",
    "battery_stats",
)
exist_heart = DeltaTable.forName(spark, "hardware.gateways_last_reported_battery")
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
        "battery_stats": "updates.battery_stats",
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
        "battery_stats": "updates.battery_stats",
    }
).execute()
