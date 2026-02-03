import pyspark
import pyspark.sql.functions as f
from datetime import datetime

db = "datascience"
start_date = "2022-01-01"
acv_daily_table = "acv_daily_snapshot"
org_acv_table = "org_acv_daily_snapshot"
metrics_table = "datapipeline_merge_statistics"
metrics_df = None


spark.sql(
    """
  SET spark.databricks.delta.schema.autoMerge.enabled = TRUE
"""
)


spark.sql(
    """
  create or replace temp view account_consolidated
  using bigquery
  options (
    table 'sfdc_standard.Account_Consolidated'
  )
"""
)


spark.sql(
    """
  create or replace temp view opportunity_consolidated
  using bigquery
  options (
    table 'sfdc_standard.Opportunity_Consolidated'
  )
"""
)


def check_table_exists(database, table_name):
    try:
        _ = spark.table(f"{database}.{table_name}")
    except pyspark.sql.utils.AnalysisException as x:
        if "not found" in x.desc:
            return False
        else:
            raise x
    return True


acv_daily_check = check_table_exists(db, acv_daily_table)
acv_org_check = check_table_exists(db, org_acv_table)
if acv_daily_check:
    curr_date = spark.sql(
        f"select date_add(max(date), 1) as latest_date from {db}.{acv_daily_table}"
    ).collect()[0]["latest_date"]
    if curr_date >= datetime.today().date():
        curr_date = datetime.today().date()
else:
    curr_date = start_date

print(f"running for acv snapshots for {curr_date}")


query = f"""
  with org_map as (
    select distinct
      sam_number,
      osa.org_id,
      lower(sa.sfdc_account_id) as sfdc_account_id
    from
      clouddb.org_sfdc_accounts osa
      join clouddb.sfdc_accounts sa on sa.id = osa.sfdc_account_id
  )
  , acv_bookings as (
    select account_id
      , substring(lower(account_id), 1, 15) as sfdc_account_id
      , sum(ACV_Bookings_USD_Converted) as customer_acv
    from opportunity_consolidated
    where 0 = 0
        and Stage in ('Closed Won', 'Refunded - Closed', 'Refunded Closed')
        and cast(Opportunity_Close_date as date) < cast('{curr_date}' as date)
    group by 1
  )
  select om.sam_number
    , om.org_id
    , acv.account_id
    , acv.sfdc_account_id
    , '{curr_date}' as date
    , current_timestamp() as _timestamp
    , round(sum(acv.customer_acv), 2) as lifetime_acv_usd
  from acv_bookings acv
  inner join org_map om on acv.sfdc_account_id = om.sfdc_account_id
  group by 1,2,3,4,5,6
"""


df_acv_daily = spark.sql(query)
df_acv_daily.createOrReplaceTempView(acv_daily_table)


df_org_acv_daily = df_acv_daily.groupby("sam_number", "org_id").agg(
    f.max(f.col("date")).alias("date"),
    f.max(f.col("_timestamp")).alias("_timestamp"),
    f.sum(f.col("lifetime_acv_usd")).alias("lifetime_acv_usd"),
)
df_org_acv_daily.createOrReplaceTempView(org_acv_table)


if acv_daily_check:

    df = spark.sql(
        f"""MERGE INTO {db}.{acv_daily_table} a
    USING {acv_daily_table} b
    ON a.sam_number = b.sam_number
      and a.org_id = b.org_id
      and a.account_id = b.account_id
      and a.sfdc_account_id = b.sfdc_account_id
      and a.date = b.date
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
  """
    )
    metrics_df = df.withColumn("operation", f.lit("insert and update")).withColumn(
        "table", f.lit(f"{db}.{acv_daily_table}")
    )

    df = spark.sql(
        f"""MERGE INTO {db}.{org_acv_table} a
    USING {org_acv_table} b
    ON a.sam_number = b.sam_number
      and a.org_id = b.org_id
      and a.date = b.date
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
  """
    )
    df = df.withColumn("operation", f.lit("insert and update")).withColumn(
        "table", f.lit(f"{db}.{org_acv_table}")
    )
    metrics_df = metrics_df.unionByName(df)

else:

    spark.sql(
        f"""
  create table if not exists {db}.{acv_daily_table} as (
    select *
    from {acv_daily_table}
  )
  """
    )

    spark.sql(
        f"""
  create table if not exists {db}.{org_acv_table} as (
    select *
    from {org_acv_table}
  )
  """
    )


if metrics_df:
    metrics_df = metrics_df.withColumn("_timestamp", f.current_timestamp())
    metrics_df.show()
    metrics_df.createOrReplaceTempView(metrics_table)
    spark.sql(
        f"""MERGE INTO {db}.{metrics_table} a
    USING {metrics_table} b
    ON a._timestamp = b._timestamp
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
  """
    )
