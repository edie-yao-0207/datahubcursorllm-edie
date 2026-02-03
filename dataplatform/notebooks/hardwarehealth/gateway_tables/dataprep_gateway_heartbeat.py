# Databricks notebook source

from datetime import datetime, timedelta, date
from delta.tables import *

metrics_date = date.today() - timedelta(days=1)

df = spark.sql(
    f"""select h.org_id, h.object_id, h.gateway_id, 
                g.serial, g.product_id, 
                h.first_heartbeat_date, h.last_heartbeat_date, h.first_heartbeat_time, h.last_heartbeat_time
        from (select min(date) as first_heartbeat_date,
                    max(date) as last_heartbeat_date,
                    min(time) as first_heartbeat_time,
                    max(value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms) as last_heartbeat_time,
                    org_id,
                    object_id,
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                from kinesisstats.osdhubserverdeviceheartbeat
                where date >= '{metrics_date}'
                group by org_id, object_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id) as h
            left join productsdb.gateways as g
            on h.gateway_id = g.id"""
).select("*")


new_data = df.selectExpr(
    "org_id",
    "object_id",
    "gateway_id",
    "serial",
    "product_id",
    "first_heartbeat_date",
    "last_heartbeat_date",
    "first_heartbeat_time",
    "last_heartbeat_time",
)
exist_heart = DeltaTable.forName(spark, "hardware.gateways_heartbeat")
exist_heart.alias("original").merge(
    new_data.alias("updates"),
    "original.org_id = updates.org_id \
    and original.object_id = updates.object_id \
    and original.gateway_id = updates.gateway_id \
    and original.serial = updates.serial \
    and original.product_id = updates.product_id",
).whenMatchedUpdate(
    set={
        "last_heartbeat_date": "updates.last_heartbeat_date",
        "last_heartbeat_time": "updates.last_heartbeat_time",
    }
).whenNotMatchedInsert(
    values={
        "org_id": "updates.org_id",
        "object_id": "updates.object_id",
        "gateway_id": "updates.gateway_id",
        "product_id": "updates.product_id",
        "serial": "updates.serial",
        "first_heartbeat_date": "updates.first_heartbeat_date",
        "first_heartbeat_time": "updates.first_heartbeat_time",
        "last_heartbeat_date": "updates.last_heartbeat_date",
        "last_heartbeat_time": "updates.last_heartbeat_time",
    }
).execute()
