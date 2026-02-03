# Databricks notebook source

from datetime import datetime, timedelta, date
from delta.tables import *

metrics_date = date.today() - timedelta(days=1)

df = spark.sql(
    f"""select h.org_id, h.object_id, h.gateway_id,
                g.serial, g.product_id, h.date, h.time, h.last_boot_count, h.last_reported_build
        from (select date,
                    max(time) as time,
                    max_by(value.proto_value.hub_server_device_heartbeat.connection.device_hello.build, time) as last_reported_build,
                    max_by(value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count, time) as last_boot_count,
                    org_id,
                    object_id,
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                from kinesisstats.osdhubserverdeviceheartbeat
                where date == '{metrics_date}'
                and value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id is not null
                group by date, org_id, object_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id) as h
            left join clouddb.gateways as g
            on h.gateway_id = g.id"""
).select("*")


new_data = df.selectExpr(
    "org_id",
    "object_id",
    "gateway_id",
    "serial",
    "product_id",
    "date",
    "time",
    "last_reported_build",
    "last_boot_count",
)
exist_heart = DeltaTable.forName(spark, "hardware.gateways_last_reported_build")
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
        "last_reported_build": "updates.last_reported_build",
        "last_boot_count": "updates.last_boot_count",
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
        "last_reported_build": "updates.last_reported_build",
        "last_boot_count": "updates.last_boot_count",
    }
).execute()
