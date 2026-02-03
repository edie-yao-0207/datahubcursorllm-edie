# Databricks notebook source

from datetime import datetime, timedelta, date

metrics_date = date.today() - timedelta(days=1)


df = spark.sql(
    f"""select hbeat.date, hbeat.org_id, hbeat.object_id, hbeat.gateway_id, int((hbeat.reboots_on_day - coalesce(csr.cum_sum_reboot,0))) as reboots_on_day, ga.serial, ga.product_id
        from
            (select date, org_id, object_id,
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id as gateway_id,
                    max_by(value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count, time) as reboots_on_day
            from kinesisstats.osdhubserverdeviceheartbeat 
            where date == '{metrics_date}'
            group by date, org_id, object_id, gateway_id) as hbeat
        left join (select id, serial, product_id
                    from clouddb.gateways) as ga
        on ga.id = hbeat.gateway_id
        left join (select object_id, gateway_id, sum(reboots_on_day) as cum_sum_reboot
                from hardware.gateways_reboots_on_day
                group by object_id, gateway_id) as csr
        on hbeat.object_id = csr.object_id
        and hbeat.gateway_id = csr.gateway_id
    """
)

df.write.partitionBy("date").format("delta").option(
    "replaceWhere", f"date = '{metrics_date}'"
).mode("overwrite").saveAsTable("hardware.gateways_reboots_on_day")
