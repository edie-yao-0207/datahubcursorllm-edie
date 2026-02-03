# Databricks notebook source
# MAGIC %md
# MAGIC # _Setup_

# COMMAND ----------

# MAGIC %pip install fuzzywuzzy[speedup]

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view order_serials as (
# MAGIC     -- Collect the serial and list of the org IDs from the org_sam_mappings to get a list of potential org IDs that the
# MAGIC     -- device COULD be activated on to be considered valid as well as a list of org names and login domains of said org IDs
# MAGIC     select
# MAGIC       serial_sam_mapping.serial as serial,
# MAGIC       collect_list(org_sam_mapping.org_id) as org_ids,
# MAGIC       collect_list(org_details.org_name) as org_names,
# MAGIC       collect_list(org_details.org_login_domain) as org_login_domains
# MAGIC     from
# MAGIC       -- This will map serials to the SAM numbers associated with based on sales records in the finops DB
# MAGIC       (
# MAGIC         select
# MAGIC           serial,
# MAGIC           last(sam_number) as sam_number
# MAGIC         from
# MAGIC           (
# MAGIC             select
# MAGIC               serial,
# MAGIC               sam_number
# MAGIC             from
# MAGIC               finopsdb.product_serial_details
# MAGIC             order by
# MAGIC               created_at asc
# MAGIC           )
# MAGIC         group by
# MAGIC           serial
# MAGIC       ) as serial_sam_mapping
# MAGIC       -- Join the existing serials + their SAM numbers with the org IDs tied to that SAM number in the cloudDB
# MAGIC       join (
# MAGIC         select
# MAGIC           o.org_id as org_id,
# MAGIC           s.sam_number as sam_number
# MAGIC         from
# MAGIC           clouddb.sfdc_accounts as s
# MAGIC           join clouddb.org_sfdc_accounts as o on s.id = o.sfdc_account_id
# MAGIC       ) as org_sam_mapping on org_sam_mapping.sam_number = serial_sam_mapping.sam_number
# MAGIC       -- Join the org IDs found on the organizations table for login domain and name details as well for further testing
# MAGIC       join (
# MAGIC         select
# MAGIC             orgs.id as org_id,
# MAGIC             orgs.name as org_name,
# MAGIC             orgs.login_domain as org_login_domain
# MAGIC         from
# MAGIC           clouddb.organizations as orgs
# MAGIC           join clouddb.org_sfdc_accounts as accs on orgs.id = accs.org_id
# MAGIC       ) as org_details on  org_details.org_id = org_sam_mapping.org_id
# MAGIC     group by
# MAGIC       serial_sam_mapping.serial
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC # _Activating Serials_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: All-time

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view all_time_distinct_serial_activation_count AS (
# MAGIC   select
# MAGIC     COUNT(distinct order_serials.serial) as distinct_serial_activation_count
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from all_time_distinct_serial_activation_count;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view last_thirty_days_distinct_serial_activation_count AS (
# MAGIC   select
# MAGIC     COUNT(distinct order_serials.serial) as distinct_serial_activation_count
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC   where
# MAGIC     -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC     devices.updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -30,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and devices.updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from last_thirty_days_distinct_serial_activation_count;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view last_day_distinct_serial_activation_count AS (
# MAGIC   select
# MAGIC     COUNT(distinct order_serials.serial) as distinct_serial_activation_count
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC   where
# MAGIC     -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC     devices.updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and devices.updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from last_day_distinct_serial_activation_count;

# COMMAND ----------

# MAGIC %md
# MAGIC # _Activating Organizations_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: All-time

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view distinct_org_ids AS (
# MAGIC   select
# MAGIC     COUNT(distinct devices.org_id) as org_id_count,
# MAGIC     collect_set(devices.org_id) as org_id_set
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct_org_ids.org_id_count as distinct_org_activation_count from distinct_org_ids;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view last_thirty_days_distinct_org_ids AS (
# MAGIC   select
# MAGIC     COUNT(distinct devices.org_id) as org_id_count,
# MAGIC     collect_set(devices.org_id) as org_id_set
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC   where
# MAGIC     -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC     devices.updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -30,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and devices.updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select last_thirty_days_distinct_org_ids.org_id_count as distinct_org_activation_count from last_thirty_days_distinct_org_ids;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view last_day_distinct_org_ids AS (
# MAGIC   select
# MAGIC     COUNT(distinct devices.org_id) as org_id_count,
# MAGIC     collect_set(devices.org_id) as org_id_set
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC   where
# MAGIC     -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC     devices.updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and devices.updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select last_day_distinct_org_ids.org_id_count as distinct_org_activation_count from last_day_distinct_org_ids;

# COMMAND ----------

# MAGIC %md
# MAGIC # _Flagged Activations_

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temporary view raw_flagged_invalid_activations AS (
# MAGIC   select
# MAGIC     order_serials.serial,
# MAGIC     order_serials.org_ids as expected_org_ids,
# MAGIC     devices.org_id as actual_org_id,
# MAGIC     order_serials.org_names as expected_org_names,
# MAGIC     orgs.name as actual_org_name,
# MAGIC     order_serials.org_login_domains as expected_org_login_domains,
# MAGIC     orgs.login_domain as actual_org_login_domain,
# MAGIC     devices.updated_at as device_updated_at,
# MAGIC     devices.id as device_id
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     -- We only want to find activations for customer orgs and can ignore internal orgs entirely
# MAGIC     join clouddb.organizations as orgs on
# MAGIC       orgs.id = devices.org_id
# MAGIC       and orgs.internal_type = 0
# MAGIC     -- To find the actual flagged recrods, we need to also check that the expected orgs doesn't contain
# MAGIC     -- the actual org ID
# MAGIC     and not array_contains(
# MAGIC       order_serials.org_ids, devices.org_id
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # _Count of flagged activations_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: All-time

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   COUNT(distinct raw_flagged_invalid_activations.serial) as raw_incorrect_activation_count
# MAGIC from raw_flagged_invalid_activations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   COUNT(distinct raw_flagged_invalid_activations.serial) as raw_incorrect_activation_count
# MAGIC from raw_flagged_invalid_activations
# MAGIC where
# MAGIC     -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC     raw_flagged_invalid_activations.device_updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -30,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and raw_flagged_invalid_activations.device_updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running count: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   COUNT(distinct raw_flagged_invalid_activations.serial) as raw_incorrect_activation_count
# MAGIC from raw_flagged_invalid_activations
# MAGIC where
# MAGIC     -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC     raw_flagged_invalid_activations.device_updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and raw_flagged_invalid_activations.device_updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC # _Applying fuzzy-matching for false-positive checks_

# COMMAND ----------

from pyspark.sql import SparkSession

from fuzzywuzzy import fuzz

# Start a Spark Session
spark = SparkSession.builder.getOrCreate()

query = """
select * from raw_flagged_invalid_activations;
"""

df = spark.sql(query)
pd_df = df.toPandas()

# COMMAND ----------


fuzzy_ratio_threshold = 50


def compare_names(row):
    expected_org_name_list = str(row["expected_org_names"]).split(",")
    return max(
        [
            fuzz.token_sort_ratio(org_name, row["actual_org_name"])
            for org_name in expected_org_name_list
        ]
    )


def compare_login_domains(row):
    expected_login_domain_list = str(row["expected_org_login_domains"]).split(",")
    return max(
        [
            fuzz.token_sort_ratio(login_domain, row["actual_org_login_domain"])
            for login_domain in expected_login_domain_list
        ]
        + [0]
    )


def create_likely_false_positive_column(row):
    return (
        compare_login_domains(row) >= fuzzy_ratio_threshold
        or compare_names(row) >= fuzzy_ratio_threshold
    )


pd_df["org_name_similarity_score"] = pd_df.apply(compare_names, axis=1)
pd_df["domain_name_similarity_score"] = pd_df.apply(compare_login_domains, axis=1)
pd_df["likely_false_positive"] = pd_df.apply(
    create_likely_false_positive_column, axis=1
)


# COMMAND ----------

spark_df = spark.createDataFrame(pd_df)
spark_df.createOrReplaceTempView("invalid_activations_with_flagged_false_positives")

# COMMAND ----------

# MAGIC %md
# MAGIC # _Report with false-positive checks_

# COMMAND ----------

# MAGIC %md
# MAGIC ### True positives - Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from invalid_activations_with_flagged_false_positives where
# MAGIC   likely_false_positive = false and
# MAGIC   invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC   ORDER BY device_updated_at desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### False positives - Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from invalid_activations_with_flagged_false_positives where
# MAGIC   likely_false_positive = true and
# MAGIC   invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC   ORDER BY device_updated_at desc

# COMMAND ----------

# MAGIC %md
# MAGIC # _Report metadata_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incorrect activation percentages: All-time

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   CAST(raw_incorrect_activation_count/(total_activation_count) *100 as decimal(18,2)) as raw_incorrect_activation_percentage,
# MAGIC   CAST(true_positive_incorrect_activation_count/(total_activation_count) *100 as decimal(18,2)) as adjusted_incorrect_activation_percentage
# MAGIC from (
# MAGIC   select
# MAGIC     r.raw_incorrect_activation_count,
# MAGIC     tp.true_positive_incorrect_activation_count,
# MAGIC     t.total_activation_count
# MAGIC   from (
# MAGIC     select
# MAGIC       COUNT(distinct raw_flagged_invalid_activations.serial) as raw_incorrect_activation_count
# MAGIC     from raw_flagged_invalid_activations
# MAGIC   ) as r
# MAGIC     full join (
# MAGIC       select COUNT(distinct invalid_activations_with_flagged_false_positives.serial) as true_positive_incorrect_activation_count
# MAGIC       from invalid_activations_with_flagged_false_positives
# MAGIC         where
# MAGIC           invalid_activations_with_flagged_false_positives.likely_false_positive = false
# MAGIC     ) as tp
# MAGIC     full join (
# MAGIC       select all_time_distinct_serial_activation_count.distinct_serial_activation_count as total_activation_count from all_time_distinct_serial_activation_count
# MAGIC     ) as t
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incorrect activation percentages: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   CAST(raw_incorrect_activation_count/(total_activation_count) *100 as decimal(18,2)) as raw_incorrect_activation_percentage,
# MAGIC   CAST(true_positive_incorrect_activation_count/(total_activation_count) *100 as decimal(18,2)) as adjusted_incorrect_activation_percentage
# MAGIC from (
# MAGIC   select
# MAGIC     r.raw_incorrect_activation_count,
# MAGIC     tp.true_positive_incorrect_activation_count,
# MAGIC     t.total_activation_count
# MAGIC   from (
# MAGIC     select
# MAGIC       COUNT(distinct raw_flagged_invalid_activations.serial) as raw_incorrect_activation_count
# MAGIC     from raw_flagged_invalid_activations
# MAGIC     where
# MAGIC         -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC         raw_flagged_invalid_activations.device_updated_at >= DATEADD(
# MAGIC           DAY,
# MAGIC           -30,
# MAGIC           CAST(NOW() AS date)
# MAGIC         )
# MAGIC         and raw_flagged_invalid_activations.device_updated_at < CAST(
# MAGIC           NOW() AS DATE
# MAGIC         )
# MAGIC   ) as r
# MAGIC     full join (
# MAGIC       select COUNT(distinct invalid_activations_with_flagged_false_positives.serial) as true_positive_incorrect_activation_count
# MAGIC       from invalid_activations_with_flagged_false_positives
# MAGIC         where
# MAGIC           invalid_activations_with_flagged_false_positives.likely_false_positive = false
# MAGIC           and invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC             DAY,
# MAGIC             -30,
# MAGIC             CAST(NOW() AS date)
# MAGIC           )
# MAGIC           and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC             NOW() AS DATE
# MAGIC           )
# MAGIC     ) as tp
# MAGIC     full join (
# MAGIC       select last_thirty_days_distinct_serial_activation_count.distinct_serial_activation_count as total_activation_count from last_thirty_days_distinct_serial_activation_count
# MAGIC     ) as t
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incorrect activation percentages: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   CAST(raw_incorrect_activation_count/(total_activation_count) *100 as decimal(18,2)) as raw_incorrect_activation_percentage,
# MAGIC   CAST(true_positive_incorrect_activation_count/(total_activation_count) *100 as decimal(18,2)) as adjusted_incorrect_activation_percentage
# MAGIC from (
# MAGIC   select
# MAGIC     r.raw_incorrect_activation_count,
# MAGIC     tp.true_positive_incorrect_activation_count,
# MAGIC     t.total_activation_count
# MAGIC   from (
# MAGIC     select
# MAGIC       COUNT(distinct raw_flagged_invalid_activations.serial) as raw_incorrect_activation_count
# MAGIC     from raw_flagged_invalid_activations
# MAGIC     where
# MAGIC         -- We want to exclude records where the last activatation date is outside of the last day
# MAGIC         raw_flagged_invalid_activations.device_updated_at >= DATEADD(
# MAGIC           DAY,
# MAGIC           -1,
# MAGIC           CAST(NOW() AS date)
# MAGIC         )
# MAGIC         and raw_flagged_invalid_activations.device_updated_at < CAST(
# MAGIC           NOW() AS DATE
# MAGIC         )
# MAGIC   ) as r
# MAGIC     full join (
# MAGIC       select COUNT(distinct invalid_activations_with_flagged_false_positives.serial) as true_positive_incorrect_activation_count
# MAGIC       from invalid_activations_with_flagged_false_positives
# MAGIC         where
# MAGIC           invalid_activations_with_flagged_false_positives.likely_false_positive = false
# MAGIC           and invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC             DAY,
# MAGIC             -1,
# MAGIC             CAST(NOW() AS date)
# MAGIC           )
# MAGIC           and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC             NOW() AS DATE
# MAGIC           )
# MAGIC     ) as tp
# MAGIC     full join (
# MAGIC       select last_day_distinct_serial_activation_count.distinct_serial_activation_count as total_activation_count from last_day_distinct_serial_activation_count
# MAGIC     ) as t
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 50 false-positive orgs: All-time

# COMMAND ----------

# MAGIC %sql
# MAGIC select actual_org_id, first(actual_org_name) as actual_org_name, count(actual_org_id) as false_positive_count from invalid_activations_with_flagged_false_positives where
# MAGIC   likely_false_positive = true
# MAGIC   GROUP BY actual_org_id
# MAGIC   ORDER BY count(actual_org_id) desc
# MAGIC   LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 50 false-positive orgs: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC select actual_org_id, first(actual_org_name) as actual_org_name, count(actual_org_id) as false_positive_count from invalid_activations_with_flagged_false_positives where
# MAGIC   likely_false_positive = true and
# MAGIC   invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -30,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC   GROUP BY actual_org_id
# MAGIC   ORDER BY count(actual_org_id) desc
# MAGIC   LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 50 false-positive orgs: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select actual_org_id, first(actual_org_name) as actual_org_name, count(actual_org_id) as false_positive_count from invalid_activations_with_flagged_false_positives where
# MAGIC   likely_false_positive = true and
# MAGIC   invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -1,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC   GROUP BY actual_org_id
# MAGIC   ORDER BY count(actual_org_id) desc
# MAGIC   LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flagged activation percentages by org: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     devices.org_id as org_id,
# MAGIC     count(distinct order_serials.serial) as total_org_activation_count,
# MAGIC     count(distinct r_act.serial) as flagged_activation_count,
# MAGIC     CAST((count(distinct r_act.serial)/count(distinct order_serials.serial)) *100 as decimal(18,2)) as flagged_activation_percentage,
# MAGIC     count(distinct adj_act.serial) as adjusted_incorrect_activation_count,
# MAGIC     CAST((count(distinct adj_act.serial)/count(distinct order_serials.serial)) *100 as decimal(18,2)) as adjusted_incorrect_activation_percentage,
# MAGIC     count(distinct adj_act.serial) - count(distinct r_act.serial) adjusted_incorrect_activation_delta
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     join last_thirty_days_distinct_org_ids as d_orgs on
# MAGIC       array_contains(d_orgs.org_id_set, devices.org_id)
# MAGIC     join (
# MAGIC       select serial, actual_org_id from raw_flagged_invalid_activations
# MAGIC         where device_updated_at >= DATEADD(
# MAGIC           DAY,
# MAGIC           -30,
# MAGIC           CAST(NOW() AS date)
# MAGIC         )
# MAGIC         and device_updated_at < CAST(
# MAGIC           NOW() AS DATE
# MAGIC         )
# MAGIC     ) as r_act on r_act.actual_org_id = devices.org_id and replace(
# MAGIC           lower(order_serials.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         ) = replace(
# MAGIC           lower(r_act.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         )
# MAGIC     left join (
# MAGIC       select serial, actual_org_id from invalid_activations_with_flagged_false_positives
# MAGIC         where
# MAGIC         likely_false_positive = false
# MAGIC         and device_updated_at >= DATEADD(
# MAGIC           DAY,
# MAGIC           -30,
# MAGIC           CAST(NOW() AS date)
# MAGIC         )
# MAGIC         and device_updated_at < CAST(
# MAGIC           NOW() AS DATE
# MAGIC         )
# MAGIC     ) as adj_act on adj_act.actual_org_id = devices.org_id and replace(
# MAGIC           lower(order_serials.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         ) = replace(
# MAGIC           lower(adj_act.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         )
# MAGIC   where devices.updated_at >= DATEADD(
# MAGIC       DAY,
# MAGIC       -30,
# MAGIC       CAST(NOW() AS date)
# MAGIC     )
# MAGIC     and devices.updated_at < CAST(
# MAGIC       NOW() AS DATE
# MAGIC     )
# MAGIC   group by devices.org_id
# MAGIC   order by adjusted_incorrect_activation_percentage asc, adjusted_incorrect_activation_delta asc, total_org_activation_count desc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flagged activation percentages by org: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     devices.org_id as org_id,
# MAGIC     count(distinct order_serials.serial) as total_org_activation_count,
# MAGIC     count(distinct r_act.serial) as flagged_activation_count,
# MAGIC     CAST((count(distinct r_act.serial)/count(distinct order_serials.serial)) *100 as decimal(18,2)) as flagged_activation_percentage,
# MAGIC     count(distinct adj_act.serial) as adjusted_incorrect_activation_count,
# MAGIC     CAST((count(distinct adj_act.serial)/count(distinct order_serials.serial)) *100 as decimal(18,2)) as adjusted_incorrect_activation_percentage,
# MAGIC     count(distinct adj_act.serial) - count(distinct r_act.serial) adjusted_incorrect_activation_delta
# MAGIC   from
# MAGIC     order_serials
# MAGIC     -- Find the device records tied to the serials found in our searches ONLY IF the device record's org ID isn't in the list
# MAGIC     -- of org IDs found tied to the SAM number in the ordered serials data
# MAGIC     join clouddb.devices as devices on replace(
# MAGIC       lower(order_serials.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     ) = replace(
# MAGIC       lower(devices.serial),
# MAGIC       "-",
# MAGIC       ""
# MAGIC     )
# MAGIC     join last_day_distinct_org_ids as d_orgs on
# MAGIC       array_contains(d_orgs.org_id_set, devices.org_id)
# MAGIC     join (
# MAGIC       select serial, actual_org_id from raw_flagged_invalid_activations
# MAGIC         where device_updated_at >= DATEADD(
# MAGIC           DAY,
# MAGIC           -1,
# MAGIC           CAST(NOW() AS date)
# MAGIC         )
# MAGIC         and device_updated_at < CAST(
# MAGIC           NOW() AS DATE
# MAGIC         )
# MAGIC     ) as r_act on r_act.actual_org_id = devices.org_id and replace(
# MAGIC           lower(order_serials.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         ) = replace(
# MAGIC           lower(r_act.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         )
# MAGIC     left join (
# MAGIC       select serial, actual_org_id from invalid_activations_with_flagged_false_positives
# MAGIC         where
# MAGIC         likely_false_positive = false
# MAGIC         and device_updated_at >= DATEADD(
# MAGIC           DAY,
# MAGIC           -1,
# MAGIC           CAST(NOW() AS date)
# MAGIC         )
# MAGIC         and device_updated_at < CAST(
# MAGIC           NOW() AS DATE
# MAGIC         )
# MAGIC     ) as adj_act on adj_act.actual_org_id = devices.org_id and
# MAGIC         replace(
# MAGIC           lower(order_serials.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         ) = replace(
# MAGIC           lower(adj_act.serial),
# MAGIC           "-",
# MAGIC           ""
# MAGIC         )
# MAGIC         where devices.updated_at >= DATEADD(
# MAGIC             DAY,
# MAGIC             -1,
# MAGIC             CAST(NOW() AS date)
# MAGIC           )
# MAGIC           and devices.updated_at < CAST(
# MAGIC             NOW() AS DATE
# MAGIC           )
# MAGIC   group by devices.org_id
# MAGIC   order by adjusted_incorrect_activation_percentage asc, adjusted_incorrect_activation_delta asc, total_org_activation_count desc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### False-positive detection: All-time

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC  false_positives_count,
# MAGIC  true_positive_count,
# MAGIC  CAST(false_positives_count/(false_positives_count + true_positive_count) *100 as decimal(18,2)) as false_positive_percentage,
# MAGIC  CAST(true_positive_count/(false_positives_count + true_positive_count) *100 as decimal(18,2)) as true_positive_percentage from (
# MAGIC   select
# MAGIC     sum(case when likely_false_positive = true then 1 else 0 end) as false_positives_count,
# MAGIC     sum(case when likely_false_positive = false then 1 else 0 end) as true_positive_count
# MAGIC   from invalid_activations_with_flagged_false_positives
# MAGIC  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### False-positive detection: Last 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC  false_positives_count,
# MAGIC  true_positive_count,
# MAGIC  CAST(false_positives_count/(false_positives_count + true_positive_count) *100 as decimal(18,2)) as false_positive_percentage,
# MAGIC  CAST(true_positive_count/(false_positives_count + true_positive_count) *100 as decimal(18,2)) as true_positive_percentage from (
# MAGIC   select
# MAGIC     sum(case when likely_false_positive = true then 1 else 0 end) as false_positives_count,
# MAGIC     sum(case when likely_false_positive = false then 1 else 0 end) as true_positive_count
# MAGIC   from invalid_activations_with_flagged_false_positives where
# MAGIC     invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC         DAY,
# MAGIC         -30,
# MAGIC         CAST(NOW() AS date)
# MAGIC       )
# MAGIC       and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC         NOW() AS DATE
# MAGIC       )
# MAGIC  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### False-positive detection: Last day

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC  false_positives_count,
# MAGIC  true_positive_count,
# MAGIC  CAST(false_positives_count/(false_positives_count + true_positive_count) *100 as decimal(18,2)) as false_positive_percentage,
# MAGIC  CAST(true_positive_count/(false_positives_count + true_positive_count) *100 as decimal(18,2)) as true_positive_percentage from (
# MAGIC   select
# MAGIC     sum(case when likely_false_positive = true then 1 else 0 end) as false_positives_count,
# MAGIC     sum(case when likely_false_positive = false then 1 else 0 end) as true_positive_count
# MAGIC   from invalid_activations_with_flagged_false_positives where
# MAGIC     invalid_activations_with_flagged_false_positives.device_updated_at >= DATEADD(
# MAGIC         DAY,
# MAGIC         -1,
# MAGIC         CAST(NOW() AS date)
# MAGIC       )
# MAGIC       and invalid_activations_with_flagged_false_positives.device_updated_at < CAST(
# MAGIC         NOW() AS DATE
# MAGIC       )
# MAGIC  )
