import tqdm
import pandas as pd
from datetime import datetime, timedelta, date
from delta.tables import *
from pyspark.sql.functions import expr, array_union


datelist = pd.date_range(
    dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
)
datelist = datelist.strftime("%Y-%m-%d").to_list()

existing_table = DeltaTable.forName(spark, "hardware.life_summary")

# Read and update from shipped_summary for gateways
sfdata = spark.sql(
    f"""select dp.name, sam_number, sh.serial, min(sh.ship_date) as ship_date, collect_set(type) as order_type
    from (select sam_number, serial, close_date as ship_date, type
        from hardware.shipped_summary
        where close_date between {dbutils.widgets.get("start_date")} and {dbutils.widgets.get("end_date")}
        and sam_number is not null) as sh
    join productsdb.gateways as g
    on sh.serial = g.serial
    join definitions.products as dp
    on g.product_id = dp.product_id
    group by dp.name, sh.sam_number, g.id, sh.serial"""
)

existing_table.alias("original").merge(
    sfdata.alias("updates"),
    "original.name = updates.name \
    and original.sam_number = updates.sam_number \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    set={
        "ship_date": expr(
            "CASE WHEN updates.ship_date < original.ship_date OR original.ship_date IS NULL THEN updates.ship_date ELSE original.ship_date END"
        ),
        "order_type": array_union("original.order_type", "updates.order_type"),
    }
).whenNotMatchedInsert(
    values={
        "name": "updates.name",
        "sam_number": "updates.sam_number",
        "serial": "updates.serial",
        "ship_date": "updates.ship_date",
        "order_type": "updates.order_type",
    }
).execute()

# Read and update from shipped_summary for widgets
sfdata = spark.sql(
    f"""select dp.name, sam_number, sh.serial, min(sh.ship_date) as ship_date, collect_set(type) as order_type
    from (select sam_number, serial, close_date as ship_date, type
        from hardware.shipped_summary
        where close_date between {dbutils.widgets.get("start_date")} and {dbutils.widgets.get("end_date")}
        and sam_number is not null) as sh
    join productsdb.widgets as g
    on sh.serial = g.serial
    join definitions.products as dp
    on g.product_id = dp.product_id
    group by dp.name, sh.sam_number, g.id, sh.serial"""
)

existing_table.alias("original").merge(
    sfdata.alias("updates"),
    "original.name = updates.name \
    and original.sam_number = updates.sam_number \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    set={
        "ship_date": expr(
            "CASE WHEN updates.ship_date < original.ship_date OR original.ship_date IS NULL THEN updates.ship_date ELSE original.ship_date END"
        ),
        "order_type": array_union("original.order_type", "updates.order_type"),
    }
).whenNotMatchedInsert(
    values={
        "name": "updates.name",
        "sam_number": "updates.sam_number",
        "serial": "updates.serial",
        "ship_date": "updates.ship_date",
        "order_type": "updates.order_type",
    }
).execute()


# TODO: Change table for sam_number to a better one
# TODO: External orgs that do not have a sam number alert
# Read and write kinesisstats table for first and last heartbeat
df = spark.sql(
    f"""select  dp.name, h.sam_number, g.serial, 
                min(h.date) as first_heartbeat_date, max(h.date) as last_heartbeat_date
        from (select date,
                    psam.sam_number,
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                from kinesisstats_history.osdhubserverdeviceheartbeat as h1
                join platops.org_name_to_samnumber as psam
                on h1.org_id = psam.id
                join clouddb.organizations as o
                on h1.org_id = o.id
                where date between {dbutils.widgets.get("start_date")} and {dbutils.widgets.get("end_date")}
                and o.internal_type = 0
                and psam.internal_type = 0
                and org_id not in (select org_id from internaldb.simulated_orgs)
                group by date, sam_number, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id) as h
            join productsdb.gateways as g
            on h.gateway_id = g.id
            join definitions.products as dp
            on g.product_id = dp.product_id
        group by dp.name, h.sam_number, g.serial"""
).select("*")

existing_table.alias("original").merge(
    df.alias("updates"),
    "original.name = updates.name \
    and original.sam_number = updates.sam_number \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    set={
        "last_heartbeat_date": expr(
            "CASE WHEN updates.last_heartbeat_date > original.last_heartbeat_date OR original.last_heartbeat_date IS NULL THEN updates.last_heartbeat_date ELSE original.last_heartbeat_date END"
        ),
        "first_heartbeat_date": expr(
            "CASE WHEN updates.first_heartbeat_date < original.first_heartbeat_date OR original.first_heartbeat_date IS NULL THEN updates.first_heartbeat_date ELSE original.first_heartbeat_date END"
        ),
    }
).whenNotMatchedInsert(
    values={
        "name": "updates.name",
        "sam_number": "updates.sam_number",
        "serial": "updates.serial",
        "first_heartbeat_date": "updates.first_heartbeat_date",
        "last_heartbeat_date": "updates.last_heartbeat_date",
    }
).execute()

# Read and write widgets table for first and last heartbeat
df = spark.sql(
    f"""select dp.name, h.sam_number,
                h.serial, min(h.date) as first_heartbeat_date, max(h.date) as last_heartbeat_date
        from (select date,
                    psam.sam_number,
                    serial, product_id
                from hardware.active_monitors as h1
                join platops.org_name_to_samnumber as psam
                on h1.org_id = psam.id
                join clouddb.organizations as o
                on h1.org_id = o.id
                where date between {dbutils.widgets.get("start_date")} and {dbutils.widgets.get("end_date")}
                and o.internal_type = 0
                and psam.internal_type = 0
                and org_id not in (select org_id from internaldb.simulated_orgs)
                group by date, sam_number, serial, product_id) as h
            join definitions.products as dp
            on h.product_id = dp.product_id
        group by dp.name, h.sam_number, h.serial"""
).select("*")

existing_table.alias("original").merge(
    df.alias("updates"),
    "original.name = updates.name \
    and original.sam_number = updates.sam_number \
    and original.gateway_id = updates.gateway_id \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    set={
        "last_heartbeat_date": expr(
            "CASE WHEN updates.last_heartbeat_date > original.last_heartbeat_date OR original.last_heartbeat_date IS NULL THEN updates.last_heartbeat_date ELSE original.last_heartbeat_date END"
        ),
        "first_heartbeat_date": expr(
            "CASE WHEN updates.first_heartbeat_date < original.first_heartbeat_date OR original.first_heartbeat_date IS NULL THEN updates.first_heartbeat_date ELSE original.first_heartbeat_date END"
        ),
    }
).whenNotMatchedInsert(
    values={
        "name": "updates.name",
        "sam_number": "updates.sam_number",
        "serial": "updates.serial",
        "first_heartbeat_date": "updates.first_heartbeat_date",
        "last_heartbeat_date": "updates.last_heartbeat_date",
    }
).execute()


# Read and write returns table
df = spark.sql(
    f"""select name, sam_number, serial, max(date) as return_date, max_by(return_type, date) as return_type
from (select dp.name, r.sam_number, r.serial, coalesce(r.date, r.returned_date) as date, r.return_type
        from hardware.gateways_return_summary as r
        join productsdb.gateways as g
        on r.serial = g.serial
        join definitions.products as dp
        on g.product_id = dp.product_id
        )
group by sam_number, serial, name"""
).select("*")

existing_table.alias("original").merge(
    df.alias("updates"),
    "original.sam_number = updates.sam_number \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    set={"return_date": "updates.return_date", "return_type": "updates.return_type"}
).whenNotMatchedInsert(
    values={
        "name": "updates.name",
        "sam_number": "updates.sam_number",
        "serial": "updates.serial",
        "return_date": "updates.return_date",
        "return_type": "updates.return_type",
    }
).execute()


df = spark.sql(
    f"""select name, sam_number, serial, max(date) as return_date, max_by(return_type, date) as return_type
from (select dp.name, r.sam_number, r.serial, coalesce(r.date, r.returned_date) as date, r.return_type
        from hardware.gateways_return_summary as r
        join productsdb.widgets as g
        on r.serial = g.serial
        join definitions.products as dp
        on g.product_id = dp.product_id
        )
group by sam_number, serial, name"""
).select("*")

existing_table.alias("original").merge(
    df.alias("updates"),
    "original.sam_number = updates.sam_number \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    set={"return_date": "updates.return_date", "return_type": "updates.return_type"}
).whenNotMatchedInsert(
    values={
        "name": "updates.name",
        "sam_number": "updates.sam_number",
        "serial": "updates.serial",
        "return_date": "updates.return_date",
        "return_type": "updates.return_type",
    }
).execute()
