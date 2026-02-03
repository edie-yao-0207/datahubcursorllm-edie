import pandas as pd
from datetime import datetime, timedelta, date
from delta.tables import *
from pyspark.sql.functions import expr, array_union

# 40 day lookback because shipped summary & active_monitors lookback looksback 10 days
metrics_date = date.today() - timedelta(days=15)

workspace_name = str.split(spark.conf.get("spark.databricks.workspaceUrl"), ".")[0]
region_name = str.replace(workspace_name, "samsara-dev-", "")

if region_name == "us-west-2":
    sim_org_string = "and org_id not in (select org_id from internaldb.simulated_orgs)"
else:
    sim_org_string = ""


existing_table = DeltaTable.forName(spark, "hardware.life_summary")

# Read and update from shipped_summary for gateways
sfdata_gateways = spark.sql(
    f"""select dp.name, sam_number, sh.serial, min(sh.ship_date) as ship_date, collect_set(type) as order_type
    from (select sam_number, serial, close_date as ship_date, type
        from hardware.shipped_summary
        where close_date >= '{metrics_date}'
        and sam_number is not null) as sh
    join productsdb.gateways as g
    on sh.serial = g.serial
    join definitions.products as dp
    on g.product_id = dp.product_id
    group by dp.name, sh.sam_number, sh.serial"""
)

existing_table.alias("original").merge(
    sfdata_gateways.alias("updates"),
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
sfdata_widgets = spark.sql(
    f"""select dp.name, sam_number, sh.serial, min(sh.ship_date) as ship_date, collect_set(type) as order_type
    from (select sam_number, serial, close_date as ship_date, type
        from hardware.shipped_summary
        where close_date >= '{metrics_date}'
        and sam_number is not null) as sh
    join productsdb.widgets as g
    on sh.serial = g.serial
    join definitions.products as dp
    on g.product_id = dp.product_id
    group by dp.name, sh.sam_number, sh.serial"""
)

existing_table.alias("original").merge(
    sfdata_widgets.alias("updates"),
    "original.name = updates.name \
    and original.sam_number = updates.sam_number \
    and original.serial = updates.serial",
).whenMatchedUpdate(
    condition="updates.ship_date < original.ship_date",
    set={
        "ship_date": expr(
            "CASE WHEN updates.ship_date < original.ship_date OR original.ship_date IS NULL THEN updates.ship_date ELSE original.ship_date END"
        ),
        "order_type": array_union("original.order_type", "updates.order_type"),
    },
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
heartbeat_gateways = spark.sql(
    f"""select  dp.name, h.sam_number, g.serial, 
                min(h.date) as first_heartbeat_date, max(h.date) as last_heartbeat_date
        from (select date,
                    psam.sam_number,
                    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                from kinesisstats.osdhubserverdeviceheartbeat as h1
                join platops.org_name_to_samnumber as psam
                on h1.org_id = psam.id
                join clouddb.organizations as o
                on h1.org_id = o.id
                where date>= '{metrics_date}'
                and o.internal_type = 0
                and psam.internal_type = 0
                {sim_org_string}
                group by date, sam_number, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id) as h
            join productsdb.gateways as g
            on h.gateway_id = g.id
            join definitions.products as dp
            on g.product_id = dp.product_id
        group by dp.name, h.sam_number, g.serial"""
).select("*")

existing_table.alias("original").merge(
    heartbeat_gateways.alias("updates"),
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
active_monitors = spark.sql(
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
                where date >= '{metrics_date}'
                and o.internal_type = 0
                and psam.internal_type = 0
                {sim_org_string}
                group by date, sam_number, serial, product_id) as h
            join definitions.products as dp
            on h.product_id = dp.product_id
        group by dp.name, h.sam_number, h.serial"""
).select("*")

existing_table.alias("original").merge(
    active_monitors.alias("updates"),
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

# Connection for ATs cause they are not in heartbeat or widgets
active_asset_tags = spark.sql(
    f"""select dp.name, psam.sam_number,
                h1.peripheral_serial as serial, min(first_observation_date) as first_heartbeat_date, max(last_observation_date) as last_heartbeat_date
                from hardware_analytics.AT_summary_data as h1
                join platops.org_name_to_samnumber as psam
                on h1.peripheral_org_id = psam.id
                join clouddb.organizations as o
                on h1.peripheral_org_id = o.id
                join definitions.products as dp
                on h1.peripheral_product_id = dp.product_id
                where o.internal_type = 0
                and psam.internal_type = 0
                --{sim_org_string} TODO: Cause it has to be peripheral_org_id and not just org_id
                group by dp.name, psam.sam_number, h1.peripheral_serial
                """
).select("*")

existing_table.alias("original").merge(
    active_asset_tags.alias("updates"),
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


# Read and write returns table
returns_gateways = spark.sql(
    f"""select name, sam_number, serial, max(date) as return_date, max_by(return_type, date) as return_type
from (select dp.name, r.sam_number, r.serial, r.date, r.return_type
        from hardware.gateways_return_summary as r
        join productsdb.gateways as g
        on r.serial = g.serial
        join definitions.products as dp
        on g.product_id = dp.product_id
        )
where sam_number is not null
group by sam_number, serial, name"""
).select("*")

existing_table.alias("original").merge(
    returns_gateways.alias("updates"),
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


returns_widgets = spark.sql(
    f"""select name, sam_number, serial, max(date) as return_date, max_by(return_type, date) as return_type
from (select dp.name, r.sam_number, r.serial, r.date, r.return_type
        from hardware.gateways_return_summary as r
        join productsdb.widgets as g
        on r.serial = g.serial
        join definitions.products as dp
        on g.product_id = dp.product_id
        )
where sam_number is not null
group by sam_number, serial, name"""
).select("*")

existing_table.alias("original").merge(
    returns_widgets.alias("updates"),
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
