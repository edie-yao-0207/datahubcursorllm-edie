# Databricks notebook source

from datetime import datetime, timedelta, date

metrics_date = date.today() - timedelta(days=1)

df = spark.sql(
    f"""select hbeat.*, ga.serial, ga.product_id
        from
            (select date, org_id, object_id,
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id as gateway_id,
                    max_by(value.proto_value.hub_server_device_heartbeat.connection.device_hello.build, time) as fw_build
            from kinesisstats.osdhubserverdeviceheartbeat 
            where date == '{metrics_date}'
            group by date, org_id, object_id, gateway_id) as hbeat
        left join (select id, serial, product_id
                    from clouddb.gateways) as ga
        on ga.id = hbeat.gateway_id
   """
)

df.write.partitionBy("date").format("delta").option(
    "replaceWhere", f"date = '{metrics_date}'"
).mode("overwrite").saveAsTable("hardware.gateways_build")
