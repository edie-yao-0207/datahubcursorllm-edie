import pandas as pd
import tqdm
from delta.tables import *
from datetime import datetime, timedelta, date

end_date = date.today() - timedelta(days=1)
start_date = date.today() - timedelta(days=10)

datelist = pd.date_range(start_date, end_date)

datelist = datelist.strftime("%Y-%m-%d").to_list()
# datelist.reverse()

# COMMAND ----------

tables_widget = (
    spark.sql("show tables from kinesisstats like 'osw*'")
    .select("tableName")
    .toPandas()["tableName"]
    .to_list()
)
tables_widget.remove("oswconndevicerssi")  # added later
tables_widget.remove("oswstreamchannelstats")  # object_ids are org_ids. Not sure why
tables_widget.remove("oswanomalyevent")  # don't think these are actual stats
tables_widget.remove("oswadvertisementstatistics")

# COMMAND ----------
for d in tqdm.tqdm(datelist):
    df = spark.sql(
        f"""select tbl.date, tbl.org_id, tbl.object_id, m.id as monitor_id, m.serial, m.product_id, 'oswconndevicerssi' as stat
                        from (select date, org_id, object_id, to_timestamp(time/1000) as time
                            from kinesisstats_history.oswconndevicerssi 
                            where date == '{d}'
                            ) as tbl
                        join productsdb.monitor_widget_history as wh
                        on tbl.object_id = wh.widget_id
                        join productsdb.monitors as m
                        on wh.monitor_id = m.id
                        -- join (select w.id, w.group_id, gr.organization_id
                        --       from productsdb.widgets as w
                        --       join clouddb.groups as gr
                        --       on w.group_id = gr.id
                        -- ) as w
                        -- on tbl.object_id = w.id
                        -- and tbl.org_id = w.organization_id
                        where tbl.time >= wh.updated_at
                        group by tbl.date, tbl.org_id, tbl.object_id, m.id, m.serial, m.product_id
        """
    )
    df.write.partitionBy("date").format("delta").option(
        "replaceWhere", f"date = '{d}'"
    ).mode("overwrite").saveAsTable("hardware.active_monitors")
    for tbl_w in tables_widget:
        tf = spark.sql(
            f"""select tbl.date, tbl.org_id, tbl.object_id, m.id as monitor_id, m.serial, m.product_id, '{tbl_w}' as stat
                        from (select date, org_id, object_id, to_timestamp(time/1000) as time
                            from kinesisstats_history.{tbl_w}
                            where date == '{d}'
                            ) as tbl
                        join productsdb.monitor_widget_history as wh
                        on tbl.object_id = wh.widget_id
                        join productsdb.monitors as m
                        on wh.monitor_id = m.id
                        -- join (select w.id, w.group_id, gr.organization_id
                        --       from productsdb.widgets as w
                        --       join clouddb.groups as gr
                        --       on w.group_id = gr.id
                        -- ) as w
                        -- on tbl.object_id = w.id
                        -- and tbl.org_id = w.organization_id
                        where tbl.time >= wh.updated_at
                        group by tbl.date, tbl.org_id, tbl.object_id, m.id, m.serial, m.product_id 
                            """
        )

        exist_heart = DeltaTable.forName(spark, "hardware.active_monitors")
        exist_heart.alias("original").merge(
            tf.alias("updates"),
            "original.date = updates.date \
            and original.org_id = updates.org_id \
            and original.object_id = updates.object_id \
            and original.monitor_id = updates.monitor_id \
            and original.serial = updates.serial \
            and original.product_id = updates.product_id",
        ).whenNotMatchedInsert(
            values={
                "date": "updates.date",
                "org_id": "updates.org_id",
                "object_id": "updates.object_id",
                "monitor_id": "updates.monitor_id",
                "product_id": "updates.product_id",
                "serial": "updates.serial",
                "stat": "updates.stat",
            }
        ).execute()
