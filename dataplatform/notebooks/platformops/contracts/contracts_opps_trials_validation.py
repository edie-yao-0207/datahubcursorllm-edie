# Databricks notebook source
# MAGIC %run /backend/platformops/datadog

# COMMAND ----------

# MAGIC %run backend/platformops/samnumberaccuracy/sfdc_data_pull_funcs

# COMMAND ----------

# MAGIC %md
# MAGIC # Fetch data from SFDC

# COMMAND ----------

import pyspark.sql.functions as F


def pull_and_store_sfdc_contracts(sfdc_connection: SfdcConnection):
    contracts_soql = """
		SELECT
			Id,
			ContractNumber,
			StartDate,
			EndDate,
			ContractTerm,
			LastModifiedDate,
			Auto_Renewal__c,
			Account.SAM_Number_Undecorated__c,
			SBQQ__Opportunity__c,
			SBQQ__RenewalOpportunity__c,
			Is_Active__c
		FROM
			Contract
	"""

    contracts_table_create_sql = """
        CREATE OR REPLACE TEMP VIEW sfdc_contracts
        AS

        SELECT
            Id as sfdc_id,
            ContractNumber as contract_number,
            StartDate as start_date,
            EndDate as end_date,
            ContractTerm as term_length_months,
            LastModifiedDate as sfdc_modified_at,
            Auto_Renewal__c as auto_renewal,
            `Account.SAM_Number_Undecorated__c` as sam_number,
            SBQQ__Opportunity__c as opportunity_sfdc_id,
            SBQQ__RenewalOpportunity__c as renewal_opportunity_sfdc_id,
            Is_Active__c as is_active
        FROM sfdc_contracts_temp;
    """

    contract_info = sfdc_connection.get_results_from_call(contracts_soql)
    contract_raw_df = json_normalize(contract_info)
    contract_fields = [
        "Id",
        "ContractNumber",
        "StartDate",
        "EndDate",
        "ContractTerm",
        "LastModifiedDate",
        "Auto_Renewal__c",
        "Account.SAM_Number_Undecorated__c",
        "SBQQ__Opportunity__c",
        "SBQQ__RenewalOpportunity__c",
        "Is_Active__c",
    ]
    contract_df = contract_raw_df[contract_fields].drop_duplicates(contract_fields)
    contract_df_spark = spark.createDataFrame(contract_df)
    contract_df_spark.createOrReplaceTempView("sfdc_contracts_temp")

    spark.sql(contracts_table_create_sql)


# COMMAND ----------


def pull_and_store_sfdc_opportunities(sfdc_connection: SfdcConnection):
    opptys_soql = """
		SELECT
			Id,
			StageName,
			Netsuite_Id__c,
			Checkout_Agreement_Link__c,
            Express_Agreement_Accepted_Date__c,
			Sub_Type__c,
			LastModifiedDate,
			Account.SAM_Number_Undecorated__c,
            Contract_ID_Link__c,
			Consolidated_Renewal_Opportunity_Id__c
		FROM
			Opportunity
        WHERE
			Type = 'Revenue Opportunity'
	"""

    oppty_table_create_sql = """
        CREATE OR REPLACE TEMP VIEW sfdc_opportunities
        AS

        SELECT
			Id as sfdc_id,
			StageName as stage_name,
			Netsuite_Id__c as netsuite_id,
			Checkout_Agreement_Link__c as agreement_link,
            Express_Agreement_Accepted_Date__c as agreement_accepted_date,
			Sub_Type__c as sub_type,
			LastModifiedDate as sfdc_modified_at,
			`Account.SAM_Number_Undecorated__c` as sam_number,
            Contract_ID_Link__c as amended_contract_sfdc_id,
            Consolidated_Renewal_Opportunity_Id__c as consolidated_renewal_opp_sfdc_id
        FROM sfdc_opportunities_temp;
    """

    oppty_info = sfdc_connection.get_results_from_call(opptys_soql)
    oppty_raw_df = json_normalize(oppty_info)
    oppty_fields = [
        "Id",
        "StageName",
        "Netsuite_Id__c",
        "Checkout_Agreement_Link__c",
        "Express_Agreement_Accepted_Date__c",
        "Sub_Type__c",
        "LastModifiedDate",
        "Account.SAM_Number_Undecorated__c",
        "Contract_ID_Link__c",
        "Consolidated_Renewal_Opportunity_Id__c",
    ]
    oppty_df = oppty_raw_df[oppty_fields].drop_duplicates(oppty_fields)

    oppty_df_spark = spark.createDataFrame(oppty_df)
    oppty_df_spark.createOrReplaceTempView("sfdc_opportunities_temp")

    spark.sql(oppty_table_create_sql)


# COMMAND ----------


def pull_and_store_sfdc_trials(sfdc_connection: SfdcConnection):
    trials_soql = """
		SELECT
			Id,
			Trial_Start_Date_new__c,
			Trial_End_Date__c,
			LastModifiedDate,
			Account.SAM_Number_Undecorated__c,
            Netsuite_Transfer_Order_ID__c
		FROM
			Opportunity
        WHERE
			Type = 'Free Trial Opportunity'
	"""

    trial_table_create_sql = """
        CREATE OR REPLACE TEMP VIEW sfdc_trials
        AS

        SELECT
			Id as sfdc_id,
			Trial_Start_Date_new__c as start_date,
			Trial_End_Date__c as end_date,
			LastModifiedDate as sfdc_modified_at,
            `Account.SAM_Number_Undecorated__c` as sam_number,
            Netsuite_Transfer_Order_ID__c as netsuite_transfer_order_id
        FROM sfdc_trials_temp;
    """

    trial_info = sfdc_connection.get_results_from_call(trials_soql)
    trial_raw_df = json_normalize(trial_info)
    trial_fields = [
        "Id",
        "Trial_Start_Date_new__c",
        "Trial_End_Date__c",
        "LastModifiedDate",
        "Account.SAM_Number_Undecorated__c",
        "Netsuite_Transfer_Order_ID__c",
    ]
    trial_df = trial_raw_df[trial_fields].drop_duplicates(trial_fields)

    trial_df_spark = spark.createDataFrame(trial_df)
    trial_df_spark.createOrReplaceTempView("sfdc_trials_temp")

    spark.sql(trial_table_create_sql)


# COMMAND ----------

# Run the helper functions to get temp view from sfdc
sfdc_connection = SfdcConnection()
pull_and_store_sfdc_contracts(sfdc_connection)
pull_and_store_sfdc_opportunities(sfdc_connection)
pull_and_store_sfdc_trials(sfdc_connection)

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary - compare fetched sfdc data to finops data

# COMMAND ----------


def print_results_and_log_datadog(
    object_type,
    total_num_sfdc,
    missing_in_sfdc,
    total_number_finops,
    missing_in_finops,
    diff_df,
):
    log_datadog_metrics(
        [
            {
                "metric": f"databricks.finops.{object_type.lower()}.sfdc_count",
                "points": total_num_sfdc,
            },
            {
                "metric": f"databricks.finops.{object_type.lower()}.finops_count",
                "points": total_number_finops,
            },
            {
                "metric": f"databricks.finops.{object_type.lower()}.missing_in_finops",
                "points": missing_in_finops,
            },
            {
                "metric": f"databricks.finops.{object_type.lower()}.missing_in_sfdc",
                "points": missing_in_sfdc,
            },
            {
                "metric": f"databricks.finops.{object_type.lower()}.difference",
                "points": len(diff_df),
            },
        ]
    )

    print(f"Summary of records for {object_type}")
    print("Number of records in sfdc: ", total_num_sfdc)
    print("Number of records missing from sfdc: ", missing_in_sfdc)
    print("Number of records in finops: ", total_number_finops)
    print("Number of records missing from finops: ", missing_in_finops)
    print("Number of records different between SFDC and Finops ", len(diff_df))
    print(
        f"Precentage difference from SFDC: {100*(len(diff_df)+missing_in_finops)/total_num_sfdc}%"
    )
    print()
    if len(diff_df) != 0:
        display(diff_df)


# COMMAND ----------


def get_contract_summary():
    # counts
    total_num_query_sfdc = "SELECT COUNT(*) FROM sfdc_contracts"
    total_num_sfdc = spark.sql(total_num_query_sfdc).toPandas().loc[0][0]

    total_num_query_finops = (
        "SELECT COUNT(*) FROM finopsdb.contracts WHERE sam_number NOT LIKE 'UAT_%'"
    )
    total_num_finops = spark.sql(total_num_query_finops).toPandas().loc[0][0]

    # missing in finops
    missing_in_finops_query = """
  SELECT count(*) FROM
    (SELECT sfdc_id from sfdc_contracts EXCEPT (
      SELECT sfdc_id from finopsdb.contracts WHERE sam_number NOT LIKE 'UAT_%'
      )
    )
  """
    missing_in_finops = spark.sql(missing_in_finops_query).toPandas().loc[0][0]

    # missing in sfdc
    missing_in_sfdc_query = """
  SELECT count(*) FROM
    (SELECT sfdc_id from finopsdb.contracts WHERE sam_number NOT LIKE 'UAT_%' EXCEPT (
      SELECT sfdc_id from sfdc_contracts
      )
    )
  """
    missing_in_sfdc = spark.sql(missing_in_sfdc_query).toPandas().loc[0][0]

    # differences between finops and sfdc
    diff_query = """
  SELECT
  s.sfdc_id,
  CAST(s.sfdc_modified_at AS STRING), CAST(finops_contracts.sfdc_modified_at AS STRING) as finops_modified_at,
  s.sam_number, finops_contracts.sam_number as finops_sam_number,
  s.contract_number, finops_contracts.contract_number as finops_contract_number,
  s.auto_renewal, CAST(finops_contracts.auto_renewal AS BOOLEAN) as finops_auto_renewal,
  s.start_date, CAST(finops_contracts.start_date as DATE) as finops_start_date,
  s.end_date, CAST(finops_contracts.end_date as DATE) as finops_end_date,
  s.term_length_months, finops_contracts.term_length_months as finops_term_length_months,
  s.opportunity_sfdc_id, finops_contracts.opportunity_sfdc_id as finops_opp_sfdc_id,
  s.renewal_opportunity_sfdc_id, finops_contracts.renewal_opportunity_sfdc_id as finops_renewal_opp_sfdc_id,
  s.is_active, CAST(finops_contracts.is_active AS BOOLEAN) as finops_is_active
  FROM sfdc_contracts s
  JOIN finopsdb.contracts AS finops_contracts ON s.sfdc_id = finops_contracts.sfdc_id
  WHERE
    (
     s.sam_number != finops_contracts.sam_number OR
     s.contract_number != finops_contracts.contract_number OR
     s.auto_renewal != CAST(finops_contracts.auto_renewal AS BOOLEAN) OR
     s.start_date != CAST(finops_contracts.start_date AS DATE) OR
     s.end_date != CAST(finops_contracts.end_date AS DATE) OR
     s.term_length_months != finops_contracts.term_length_months OR
     s.opportunity_sfdc_id != finops_contracts.opportunity_sfdc_id OR
     s.renewal_opportunity_sfdc_id != finops_contracts.renewal_opportunity_sfdc_id OR
     s.is_active != CAST(finops_contracts.is_active AS BOOLEAN)
    )
    AND
    finops_contracts.sam_number NOT LIKE 'UAT_%'
  """
    diff_df = spark.sql(diff_query).toPandas()

    print_results_and_log_datadog(
        "Contract",
        total_num_sfdc,
        missing_in_sfdc,
        total_num_finops,
        missing_in_finops,
        diff_df,
    )


get_contract_summary()

# COMMAND ----------


def get_opportunity_summary():
    # counts
    total_num_query_sfdc = "SELECT COUNT(*) FROM sfdc_opportunities"
    total_num_sfdc = spark.sql(total_num_query_sfdc).toPandas().loc[0][0]

    total_num_query_finops = (
        "SELECT COUNT(*) FROM finopsdb.opportunities WHERE sam_number NOT LIKE 'UAT_%'"
    )
    total_num_finops = spark.sql(total_num_query_finops).toPandas().loc[0][0]

    # missing in finops
    missing_in_finops_query = """
  SELECT count(*) FROM
    (SELECT sfdc_id from sfdc_opportunities EXCEPT (
      SELECT sfdc_id from finopsdb.opportunities WHERE sam_number NOT LIKE 'UAT_%'
      )
    )
  """
    missing_in_finops = spark.sql(missing_in_finops_query).toPandas().loc[0][0]

    # missing in sfdc
    missing_in_sfdc_query = """
  SELECT count(*) FROM
    (SELECT sfdc_id from finopsdb.opportunities WHERE sam_number NOT LIKE 'UAT_%' EXCEPT (
      SELECT sfdc_id from sfdc_opportunities
      )
    )
  """
    missing_in_sfdc = spark.sql(missing_in_sfdc_query).toPandas().loc[0][0]

    # differences between finops and sfdc
    diff_query = """
  SELECT
  so.sfdc_id,
  CAST(so.sfdc_modified_at AS STRING), CAST(fo.sfdc_modified_at AS STRING) as finops_modified_at,
  so.sam_number, fo.sam_number as finops_sam_number,
  so.stage_name, fo.stage_name as finops_stage_name,
  so.netsuite_id, fo.netsuite_id as finops_netsuite_id,
  so.agreement_link, fo.agreement_link as finops_agreement_link,
  so.sub_type, fo.sub_type as finops_sub_type,
  so.agreement_accepted_date, fo.agreement_accepted_date as finops_agreement_accepted_date,
  so.amended_contract_sfdc_id, fo.amended_contract_sfdc_id as finops_amended_contract_sfdc_id,
  so.consolidated_renewal_opp_sfdc_id, fo.consolidated_renewal_opp_sfdc_id as finops_consolidated_renewal_opp_sfdc_id
  FROM sfdc_opportunities so
  JOIN finopsdb.opportunities AS fo ON so.sfdc_id = fo.sfdc_id
  WHERE (
    so.sam_number != fo.sam_number OR
    so.stage_name != fo.stage_name OR
    so.netsuite_id != fo.netsuite_id OR
    so.agreement_link != fo.agreement_link OR
    so.sub_type != fo.sub_type OR
    so.agreement_accepted_date != fo.agreement_accepted_date OR
    so.amended_contract_sfdc_id != fo.amended_contract_sfdc_id OR
    so.consolidated_renewal_opp_sfdc_id != fo.consolidated_renewal_opp_sfdc_id
  )
  AND
  fo.sam_number NOT LIKE 'UAT_%'
  """

    diff_sql_result = spark.sql(diff_query)
    diff_df = diff_sql_result.select(
        *[
            F.col(c).cast("string").alias(c) if t == "timestamp" else F.col(c)
            for c, t in diff_sql_result.dtypes
        ]
    ).toPandas()

    print_results_and_log_datadog(
        "Opportunity",
        total_num_sfdc,
        missing_in_sfdc,
        total_num_finops,
        missing_in_finops,
        diff_df,
    )


get_opportunity_summary()


# COMMAND ----------


def get_trial_summary():
    # counts
    total_num_query_sfdc = "SELECT COUNT(*) FROM sfdc_trials"
    total_num_sfdc = spark.sql(total_num_query_sfdc).toPandas().loc[0][0]

    total_num_query_finops = (
        "SELECT COUNT(*) FROM finopsdb.trials WHERE sam_number NOT LIKE 'UAT_%'"
    )
    total_num_finops = spark.sql(total_num_query_finops).toPandas().loc[0][0]

    # missing in finops
    missing_in_finops_query = """
  SELECT count(*) FROM
    (SELECT sfdc_id from sfdc_trials EXCEPT (
      SELECT sfdc_id from finopsdb.trials WHERE sam_number NOT LIKE 'UAT_%'
      )
    )
  """
    missing_in_finops = spark.sql(missing_in_finops_query).toPandas().loc[0][0]

    # missing in sfdc
    missing_in_sfdc_query = """
  SELECT count(*) FROM
    (SELECT sfdc_id from finopsdb.trials WHERE sam_number NOT LIKE 'UAT_%' EXCEPT (
      SELECT sfdc_id from sfdc_trials
      )
    )
  """
    missing_in_sfdc = spark.sql(missing_in_sfdc_query).toPandas().loc[0][0]

    # differences between finops and sfdc
    diff_query = """
  SELECT
  st.sfdc_id,
  CAST(st.sfdc_modified_at AS STRING), CAST(ft.sfdc_modified_at AS STRING) as finops_modified_at,
  st.sam_number, ft.sam_number as finops_sam_number,
  st.start_date, ft.start_date as finops_start_date,
  st.end_date, ft.end_date as finops_end_date,
  st.netsuite_transfer_order_id, ft.netsuite_transfer_order_id as finops_netsuite_transfer_order_id
  FROM sfdc_trials st
  JOIN finopsdb.trials AS ft ON st.sfdc_id = ft.sfdc_id
  WHERE
    (st.sam_number != ft.sam_number OR
     st.start_date != ft.start_date OR
     st.end_date != ft.end_date OR
     st.netsuite_transfer_order_id != ft.netsuite_transfer_order_id
    )
    AND
    ft.sam_number NOT LIKE 'UAT_%'
  """
    diff_df = spark.sql(diff_query).toPandas()

    print_results_and_log_datadog(
        "Trial",
        total_num_sfdc,
        missing_in_sfdc,
        total_num_finops,
        missing_in_finops,
        diff_df,
    )


get_trial_summary()
