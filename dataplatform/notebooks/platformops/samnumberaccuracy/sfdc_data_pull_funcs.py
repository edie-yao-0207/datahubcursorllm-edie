# MAGIC %pip install simple-salesforce

# COMMAND ----------

# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run ./sfdc_helpers

# COMMAND ----------


from datetime import datetime

from pandas import json_normalize
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()


@log_function_duration(
    "databricks.samnumber_accuracy.pull_and_store_sfdc_account_data.run_time"
)
def pull_and_store_sfdc_account_data(sfdc_connection: SfdcConnection):
    accounts_soql = "SELECT Id, Name, CS_POC_Email_Address__c, SAM_Number_Undecorated__c FROM Account"

    accounts_table_create_sql = """
        CREATE OR REPLACE TABLE sfdc_data.sfdc_accounts
        USING DELTA
        AS

        SELECT
        Id,
        Name,
        OwnerEmail,
        SAM_Number_Undecorated__c
        FROM sfdc_accounts_temp
    """

    account_info = sfdc_connection.get_results_from_call(accounts_soql)

    # Rename the owner email
    for account in account_info:
        account["OwnerEmail"] = account["CS_POC_Email_Address__c"]

    account_raw_df = json_normalize(account_info)

    # Drop duplicates and unnecessary data
    account_df = account_raw_df[
        [
            "Id",
            "Name",
            "OwnerEmail",
            "SAM_Number_Undecorated__c",
        ]
    ].drop_duplicates(
        [
            "Id",
            "Name",
            "OwnerEmail",
            "SAM_Number_Undecorated__c",
        ]
    )

    account_df_spark = spark.createDataFrame(account_df)
    account_df_spark.createOrReplaceTempView("sfdc_accounts_temp")

    spark.sql(accounts_table_create_sql)


@log_function_duration(
    "databricks.samnumber_accuracy.pull_and_store_sfdc_oppty_data.run_time"
)
def pull_and_store_sfdc_oppty_data(sfdc_connection: SfdcConnection):
    opptys_soql = "SELECT Id, AccountId, Order_Number__c from Opportunity"

    oppty_table_create_sql = """
        CREATE OR REPLACE TABLE sfdc_data.sfdc_opportunities
        USING DELTA
        AS

        SELECT
        Id,
        AccountId,
        Order_Number__c
        FROM sfdc_opportunities_temp;
    """

    oppty_info = sfdc_connection.get_results_from_call(opptys_soql)

    oppty_raw_df = json_normalize(oppty_info)

    oppty_df = oppty_raw_df[["Id", "AccountId", "Order_Number__c",]].drop_duplicates(
        [
            "Id",
            "AccountId",
            "Order_Number__c",
        ]
    )

    oppty_df_spark = spark.createDataFrame(oppty_df)
    oppty_df_spark.createOrReplaceTempView("sfdc_opportunities_temp")

    spark.sql(oppty_table_create_sql)


@log_function_duration(
    "databricks.samnumber_accuracy.pull_and_store_sfdc_line_items_data.run_time"
)
def pull_and_store_sfdc_line_items_data(sfdc_connection: SfdcConnection):
    line_items_soql = "SELECT OpportunityId, Product_Code_Copy__c, Quantity FROM OpportunityLineItem WHERE Product_Code_Copy__c LIKE 'LIC-%' AND Product_Code_Copy__C != 'LIC-SVC-CM-Review-ENT'"

    line_items_table_create_sql = """
        CREATE OR REPLACE TABLE sfdc_data.sfdc_line_items
        USING DELTA
        AS

        SELECT
            OpportunityId,
            Product_Code_Copy__c,
            Quantity
        FROM sfdc_line_items_temp;
    """

    line_items_info = sfdc_connection.get_results_from_call(line_items_soql)

    line_items_raw_df = json_normalize(line_items_info)

    line_items_df = line_items_raw_df[
        [
            "OpportunityId",
            "Product_Code_Copy__c",
            "Quantity",
        ]
    ].drop_duplicates(
        [
            "OpportunityId",
            "Product_Code_Copy__c",
            "Quantity",
        ]
    )

    line_items_df_spark = spark.createDataFrame(line_items_df)
    line_items_df_spark.createOrReplaceTempView("sfdc_line_items_temp")

    spark.sql(line_items_table_create_sql)


def run_sfdc_data_pull():
    sfdc_connection = SfdcConnection()
    pull_and_store_sfdc_oppty_data(sfdc_connection)
    pull_and_store_sfdc_account_data(sfdc_connection)
    pull_and_store_sfdc_line_items_data(sfdc_connection)
