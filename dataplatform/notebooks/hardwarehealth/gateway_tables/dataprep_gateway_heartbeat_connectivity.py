# Databricks notebook source

from datetime import datetime, timedelta, date

metrics_date = date.today() - timedelta(days=1)

df = spark.sql(
    f"""select date, org_id, object_id, gateway_id, time as start_time, 
            case when next_heartbeat_time < (last_heartbeat_at_ms + (2*heartbeat_period_sec * 1000)) then next_heartbeat_time 
                else (last_heartbeat_at_ms + (2*heartbeat_period_sec * 1000)) end as end_time
            from (SELECT date, h.org_id, h.object_id, 
                value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id, time, 
                value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms, 
                lead (time) over (partition by value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id, h.object_id order by time) as next_heartbeat_time,
                value.proto_value.hub_server_device_heartbeat.heartbeat_period_sec
            FROM kinesisstats_history.osdhubserverdeviceheartbeat as h
            where h.date >= '{metrics_date}' ) -- so that over heartbeat connectivity that overlap to next day is counted
            where date == '{metrics_date}'
        """
)

df.write.partitionBy("date").format("delta").option(
    "replaceWhere", f"date = '{metrics_date}'"
).mode("overwrite").saveAsTable("hardware.gateways_connectivity_intervals")


df = spark.sql(
    f"""select date, org_id, object_id, gateway_id, time as start_time, 
            case when next_heartbeat_time < (last_heartbeat_at_ms + (2*heartbeat_period_sec * 1000)) then next_heartbeat_time 
                else (last_heartbeat_at_ms + (2*heartbeat_period_sec * 1000)) end as end_time
            from (SELECT date, h.org_id, h.object_id, 
                value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id, time, 
                value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms, 
                lead (time) over (partition by value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id, h.object_id order by time) as next_heartbeat_time,
                value.proto_value.hub_server_device_heartbeat.heartbeat_period_sec
            FROM kinesisstats_history.osdhubserverdeviceheartbeat as h
            where h.date >= '{metrics_date}' ) -- so that over heartbeat connectivity that overlap to next day is counted
            where date > '{metrics_date}'
        """
)

df.write.partitionBy("date").format("delta").option(
    "replaceWhere", f"date = '{date.today()}'"
).mode("overwrite").saveAsTable("hardware.gateways_connectivity_intervals")
