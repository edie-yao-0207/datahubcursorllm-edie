# Databricks notebook source
import pandas as pd
from delta.tables import *
from datetime import datetime, timedelta, date

datelist = pd.date_range(
    dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
)
datelist = datelist.strftime("%Y-%m-%d").to_list()
datelist.reverse()

# COMMAND ----------
try:
    d = dbutils.widgets.get("end_date")
    df = spark.sql(
        f"""select hbeat.*, ga.serial, ga.product_id, ga.associated_at
            from
                (select date, org_id, object_id, 
                        value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id gateway_id
                from kinesisstats.osdhubserverdeviceheartbeat 
                where date == '{d}'
                group by date, org_id, object_id, gateway_id) as hbeat
            left join (select id, serial, product_id, associated_at
                        from productsdb.gateways) as ga
            on ga.id = hbeat.gateway_id
            """
    )
    df.write.partitionBy("date").format("delta").saveAsTable("hardware.active_gateways")

except Exception as e:
    print(e)


# COMMAND ----------

for d in datelist:
    df = spark.sql(
        f"""select hbeat.*, ga.serial, ga.product_id, ga.associated_at
        from
            (select date, org_id, object_id, 
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id gateway_id
            from kinesisstats.osdhubserverdeviceheartbeat 
            where date == '{d}'
            group by date, org_id, object_id, gateway_id) as hbeat
        left join (select id, serial, product_id, associated_at
                    from productsdb.gateways) as ga
        on ga.id = hbeat.gateway_id
        """
    )

    df.write.partitionBy("date").format("delta").option(
        "replaceWhere", f"date = '{d}'"
    ).mode("overwrite").saveAsTable("hardware.active_gateways")

# COMMAND ----------
