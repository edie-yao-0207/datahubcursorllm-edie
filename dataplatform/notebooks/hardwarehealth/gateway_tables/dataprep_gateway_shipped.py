## Packages needed
import pandas as pd
from delta.tables import *
from datetime import datetime, timedelta, date
from pyspark.sql.functions import expr

metrics_date = date.today() - timedelta(days=10)

datelist = pd.date_range(metrics_date, date.today())
datelist = datelist.strftime("%Y-%m-%d").to_list()

workspace_name = str.split(spark.conf.get("spark.databricks.workspaceUrl"), ".")[0]
region_name = str.replace(workspace_name, "samsara-dev-", "")

summary_table = DeltaTable.forName(spark, "hardware.shipped_summary")

if region_name == "us-west-2":
    table_name = "edw.salesforce_sterling.opportunity"
elif region_name == "eu-west-1":
    table_name = "biztech_edw_salesforce_silver.sfdc_opportunity_eu"
else:
    print("Invalid table name")

for date_l in datelist:
    # Read and update hardware.gateways_shipped for salesforce opportunities
    sfdata = spark.sql(
        f"""select to_date(went_to_closed_won_date) as close_date, account_id, opportunity_id, order_number, opportunity_type as type, went_to_ready_to_ship_date, promo_reason, shipping_state, shipping_country, total_opportunity_quantity, all_serial_numbers
                from {table_name}
                where is_closed is true
                and stage_name = 'Closed Won'
                and all_serial_numbers is not null
                and to_date(went_to_closed_won_date) = '{date_l}'
                                    """
    ).toPandas()

    sfdata["serial"] = sfdata["all_serial_numbers"].str.split(pat="\n|\r|\t| ")
    sfdata = sfdata.explode("serial")
    sfdata = sfdata.reset_index()
    sfdata["serial"] = sfdata["serial"].str.replace("-", "")
    sfdata["serial"] = sfdata["serial"].str.replace(" ", "")
    sfdata = sfdata.drop(["all_serial_numbers", "index"], axis=1)
    sfdata = sfdata.dropna(subset=["serial"])

    if len(sfdata) > 0:
        sfdata = sfdata.drop_duplicates()
        # convert to spark
        spark_sf_table = spark.createDataFrame(
            sfdata, schema=spark.table("hardware.shipped_salesforce").schema
        )
        # save to temp table
        spark_sf_table.write.format("delta").partitionBy("close_date").option(
            "replaceWhere", f"close_date = '{date_l}'"
        ).mode("overwrite").saveAsTable("hardware.shipped_salesforce")

# Read and update from hardware.shipped_salesforce for salesforce opportunities

sfdata = spark.sql(
    f"""select sh.type, sfdc.SAM_Number_Undecorated__c as sam_number, sh.serial, min(sh.close_date) as close_date
        from (select type, account_id, serial, close_date
            from hardware.shipped_salesforce
            where close_date >= '{metrics_date}') as sh
        left join sfdc_data.sfdc_accounts as sfdc
        on sh.account_id = sfdc.Id
        where sfdc.SAM_Number_Undecorated__c is not null
        group by sh.type, sfdc.SAM_Number_Undecorated__c, sh.serial"""
)
print(sfdata.count())
if sfdata.count() > 0:
    summary_table.alias("original").merge(
        sfdata.alias("updates"),
        "original.type = updates.type \
        and original.sam_number = updates.sam_number \
        and original.serial = updates.serial",
    ).whenMatchedUpdate(
        condition="updates.close_date < original.close_date",
        set={"original.close_date": "updates.close_date"},
    ).whenNotMatchedInsert(
        values={
            "type": "updates.type",
            "sam_number": "updates.sam_number",
            "serial": "updates.serial",
            "close_date": "updates.close_date",
        }
    ).execute()

# Read and write from dataprep.device_sold data when data not available in salesforce or biztech
# dataprep.devices_sold is the same name in NA & EU
dsfdata = spark.sql(
    f"""select order_type as type, sam_number, serial, min(transaction_date) as close_date
        from dataprep.devices_sold
        where product not like "%LIC%"
        and transaction_date >= '{metrics_date}'
        and transaction_type == 'Item Fulfillment'
        and sam_number is not null
        group by order_type, sam_number, serial"""
)
if dsfdata.count() > 0:
    summary_table.alias("original").merge(
        dsfdata.alias("updates"),
        "original.type = updates.type \
        and original.sam_number = updates.sam_number \
        and original.serial = updates.serial",
    ).whenMatchedUpdate(
        condition="updates.close_date < original.close_date",
        set={"original.close_date": "updates.close_date"},
    ).whenNotMatchedInsert(
        values={
            "type": "updates.type",
            "sam_number": "updates.sam_number",
            "serial": "updates.serial",
            "close_date": "updates.close_date",
        }
    ).execute()

# Read and write from edw.silver.dim_device_transaction data when data not available in salesforce or biztech
if region_name == "us-west-2":
    table_name = "edw.silver.dim_device_transaction"
elif region_name == "eu-west-1":
    table_name = "edw_delta_share.silver.dim_device_transaction"
else:
    print("Invalid table name")

ddfdata = spark.sql(
    f"""select order_type as type, sam_number, device_serial as serial, min(order_date) as close_date
        from {table_name}
        where order_date >= '{metrics_date}'
        and sam_number is not null
        and order_type is not null
        group by order_type, sam_number, serial"""
)
if ddfdata.count() > 0:
    summary_table.alias("original").merge(
        dsfdata.alias("updates"),
        "original.type = updates.type \
        and original.sam_number = updates.sam_number \
        and original.serial = updates.serial",
    ).whenMatchedUpdate(
        condition="updates.close_date < original.close_date",
        set={"original.close_date": "updates.close_date"},
    ).whenNotMatchedInsert(
        values={
            "type": "updates.type",
            "sam_number": "updates.sam_number",
            "serial": "updates.serial",
            "close_date": "updates.close_date",
        }
    ).execute()
