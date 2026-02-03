# Databricks notebook source
import tqdm
import pandas as pd
from datetime import datetime, timedelta, date
from delta.tables import *

datelist = pd.date_range(
    dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
)
datelist = datelist.strftime("%Y-%m-%d").to_list()


# COMMAND ----------
try:
    date_l = dbutils.widgets.get("start_date")
    df = spark.sql(
        f"""select h.org_id, h.object_id, h.gateway_id, 
                    g.serial, g.product_id, 
                    h.date as first_heartbeat_date, h.date as last_heartbeat_date, 
                    h.first_heartbeat_time, h.last_heartbeat_time
            from (select date,
                        min(time) as first_heartbeat_time,
                        max(value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms) as last_heartbeat_time,
                        org_id,
                        object_id,
                        value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                    from kinesisstats_history.osdhubserverdeviceheartbeat
                    where date == '{date_l}'
                    group by date, org_id, object_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id) as h
                left join productsdb.gateways as g
                on h.gateway_id = g.id"""
    ).select("*")

    df.write.format("delta").saveAsTable("hardware.gateways_heartbeat")
except Exception as e:
    print(e)

# COMMAND ----------

for date_l in tqdm.tqdm(datelist):
    df = spark.sql(
        f"""select h.org_id, h.object_id, h.gateway_id, 
                    g.serial, g.product_id, 
                    h.date, h.first_heartbeat_time, h.last_heartbeat_time
            from (select date,
                        min(time) as first_heartbeat_time,
                        max(value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms) as last_heartbeat_time,
                        org_id,
                        object_id,
                        value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                  from kinesisstats_history.osdhubserverdeviceheartbeat
                  where date == '{date_l}'
                  group by date, org_id, object_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id) as h
             left join productsdb.gateways as g
             on h.gateway_id = g.id"""
    ).select("*")

    new_data = df.selectExpr(
        "org_id",
        "object_id",
        "gateway_id",
        "serial",
        "product_id",
        "date",
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
            "last_heartbeat_date": "updates.date",
            "last_heartbeat_time": "updates.last_heartbeat_time",
        }
    ).whenNotMatchedInsert(
        values={
            "org_id": "updates.org_id",
            "object_id": "updates.object_id",
            "gateway_id": "updates.gateway_id",
            "product_id": "updates.product_id",
            "serial": "updates.serial",
            "first_heartbeat_date": "updates.date",
            "first_heartbeat_time": "updates.first_heartbeat_time",
            "last_heartbeat_date": "updates.date",
            "last_heartbeat_time": "updates.last_heartbeat_time",
        }
    ).execute()
