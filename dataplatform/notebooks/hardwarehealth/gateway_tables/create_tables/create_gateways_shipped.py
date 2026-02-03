# Databricks notebook source
## Packages needed
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
import pyspark
import tqdm

# COMMAND ----------

# Connect to Salesforce
print("Connecting to salesforce.")
sf = Salesforce(
    username=dbutils.secrets.get(scope="meenu_creds", key="email"),
    password=dbutils.secrets.get(scope="meenu_creds", key="pwd"),
    security_token=dbutils.secrets.get(scope="meenu_creds", key="salesforce_key"),
)
print("Connected to SF")

# COMMAND ----------

d = dbutils.widgets.get("end_date")
try:
    sfdata = []
    result = sf.query(
        f"""Select Id, Type, AccountId, All_Serial_Numbers__c, CloseDate, Went_to_Ready_to_Ship_Date__c, Shipping_Country__c, Shipping_State__c
                            from Opportunity 
                            where CloseDate = {d} and
                                    StageName = 'Closed Won' and
                                    AccountId <> '0014p00001jjTjt' and AccountId <> '0011P000012Dp92' 
                                    """
    )
    sfdata.append(pd.DataFrame(result["records"]).drop("attributes", axis=1))
    while "nextRecordsUrl" in result.keys():
        result = sf.query_more(result["nextRecordsUrl"], True)
        sfdata.append(pd.DataFrame(result["records"]).drop("attributes", axis=1))

    sfdata = pd.concat(sfdata)

    sfdata = sfdata.dropna(subset=["All_Serial_Numbers__c"])
    # sfdata = sfdata.query(f"Shipping_Country__c in ['United States', 'Canada', 'Mexico']")
    sfdata["serial"] = sfdata["All_Serial_Numbers__c"].str.split(pat="\n")
    sfdata = sfdata.explode("serial")
    sfdata = sfdata.reset_index()
    sfdata["serial"] = sfdata["serial"].str.replace("-", "")
    sfdata = sfdata.drop(["All_Serial_Numbers__c", "index"], axis=1)

    sfdata = sfdata.rename(
        columns={
            "Id": "opportunity_id",
            "Type": "opportunity_type",
            "AccountId": "sf_account_id",
            "CloseDate": "close_date",
            "Went_to_Ready_to_Ship_Date__c": "ship_date",
            "Shipping_Country__c": "shipping_country",
            "Shipping_State__c": "shipping_state",
        }
    )
    sfdata = sfdata[
        [
            "close_date",
            "ship_date",
            "opportunity_id",
            "opportunity_type",
            "sf_account_id",
            "serial",
            "shipping_country",
            "shipping_state",
        ]
    ]
    if len(sfdata) > 0:
        sfdata = sfdata.drop_duplicates()
        # convert to spark
        spark_sf_table = spark.createDataFrame(sfdata)
        # save to temp table
        spark_sf_table.write.format("delta").partitionBy("close_date").saveAsTable(
            "hardware.gateways_shipped"
        )
except Exception as e:
    print(e)

# COMMAND ----------

datelist = pd.date_range(
    dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")
)
datelist = datelist.strftime("%Y-%m-%d").to_list()
datelist.reverse()
for d in tqdm.tqdm(datelist):
    try:
        sfdata = []
        result = sf.query(
            f"""Select Id, Type, AccountId, All_Serial_Numbers__c, CloseDate, Went_to_Ready_to_Ship_Date__c, Shipping_Country__c, Shipping_State__c
                                from Opportunity 
                                where CloseDate = {d} and
                                        StageName = 'Closed Won' and
                                        AccountId <> '0014p00001jjTjt' and AccountId <> '0011P000012Dp92' 
                                        """
        )
        sfdata.append(pd.DataFrame(result["records"]).drop("attributes", axis=1))
        while "nextRecordsUrl" in result.keys():
            result = sf.query_more(result["nextRecordsUrl"], True)
            sfdata.append(pd.DataFrame(result["records"]).drop("attributes", axis=1))

        sfdata = pd.concat(sfdata)

        sfdata = sfdata.dropna(subset=["All_Serial_Numbers__c"])
        # sfdata = sfdata.query(f"Shipping_Country__c in ['United States', 'Canada', 'Mexico']")
        sfdata["serial"] = sfdata["All_Serial_Numbers__c"].str.split(pat="\n")
        sfdata = sfdata.explode("serial")
        sfdata = sfdata.reset_index()
        sfdata["serial"] = sfdata["serial"].str.replace("-", "")
        sfdata["serial"] = sfdata["serial"].str.replace("\r", "")
        sfdata["serial"] = sfdata["serial"].str.replace(" ", "")
        sfdata["serial"] = sfdata["serial"].str.replace("\t", "")
        sfdata = sfdata.drop(["All_Serial_Numbers__c", "index"], axis=1)

        sfdata = sfdata.rename(
            columns={
                "Id": "opportunity_id",
                "Type": "opportunity_type",
                "AccountId": "sf_account_id",
                "CloseDate": "close_date",
                "Went_to_Ready_to_Ship_Date__c": "ship_date",
                "Shipping_Country__c": "shipping_country",
                "Shipping_State__c": "shipping_state",
            }
        )
        sfdata = sfdata[
            [
                "close_date",
                "ship_date",
                "opportunity_id",
                "opportunity_type",
                "sf_account_id",
                "serial",
                "shipping_country",
                "shipping_state",
            ]
        ]
        if len(sfdata) > 0:
            sfdata = sfdata.drop_duplicates()
            # convert to spark
            spark_sf_table = spark.createDataFrame(sfdata)
            # save to temp table
            spark_sf_table.write.format("delta").partitionBy("close_date").option(
                "replaceWhere", f"close_date = '{d}'"
            ).mode("overwrite").saveAsTable("hardware.gateways_shipped")
    except Exception as e:
        print(e)
