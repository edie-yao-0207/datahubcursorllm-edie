-- Databricks notebook source
-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.functions import col, current_date, date_sub, from_unixtime, udf
-- MAGIC
-- MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
-- MAGIC spark.conf.set("spark.sql.broadcastTimeout", 72000)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Step 1: Construct Cell Carrier Connection Intervals from osdCellularInterfaceState

-- COMMAND ----------

-- Grab cell information. For simplicity, lumping Firstnet under ATT for now since it is an ATT service.
create or replace temp view cell_info as (
  select 
    cell.date,
    cell.time,
    cell.org_id,
    cell.object_id,
    cell.value.proto_value.interface_state.cellular.iccid,
    case 
      when product_type like "%VG5%" and COALESCE(cell.value.proto_value.interface_state.cellular.sim_slot, 0) = 1 then 'Vodafone'
      when product_type like "%VG5%" and COALESCE(cell.value.proto_value.interface_state.cellular.sim_slot, 0) = 2 then 'ATT'
      when product_type like "%VG34%" then 'ATT'
      when COALESCE(cell.value.proto_value.interface_state.cellular.sim_slot, 0) = 0 then 'Invalid'
    end as network,
    cell.value.proto_value.interface_state.cellular.sim_slot,
    cell.value.proto_value.interface_state.cellular.operator_s as operator
  from kinesisstats.osdcellularinterfacestate as cell
  join data_analytics.dataprep_vg3x_daily_health_metrics as dev on
    cell.date = dev.date
    and cell.org_id = dev.org_id
    and cell.object_id = dev.device_id
  where 
    --cell.date <= date_sub(current_date(),1)
    --and cell.date >= date(now() - interval 6 months) --added in to speed up scheduled run
    value.is_end = 'false'
    and value.is_databreak = 'false'
  );

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
create or replace temporary view cell_info_lag as (
  select
    object_id,
    org_id,
    COALESCE(lag(time) over (partition by org_id, object_id order by time), 0) as prev_time,
    COALESCE(lag(network) over (partition by org_id, object_id order by time), 0) as prev_network,
    COALESCE(lag(operator) over (partition by org_id, object_id order by time), 0) as prev_operator,
    time as cur_time,
    network as cur_network,
    operator as cur_operator
  from cell_info
);

-- Look ahead to grab the next state and time.
create or replace temporary view cell_info_lead as (
  select
    object_id,
    org_id,
    time as prev_time,
    network as prev_network,
    operator as prev_operator,
    COALESCE(lead(time) over (partition by org_id, object_id order by time), 0) as cur_time,
    COALESCE(lead(network) over (partition by org_id, object_id order by time), 0) as cur_network,
    COALESCE(lead(operator) over (partition by org_id, object_id order by time), 0) as cur_operator
  from cell_info
);

--Create table that is a union of the transitions between each state
create or replace temporary view cell_info_hist as (
  (select *
   from cell_info_lag lag
   where lag.prev_operator != lag.cur_operator
  )
  union
  (select *
   from cell_info_lead lead
   where lead.prev_operator != lead.cur_operator
  )
);

-- COMMAND ----------

create or replace temporary view network_states as (
  select
    object_id,
    org_id,
    cur_time as start_ms,
    cur_network as network,
    cur_operator as operator,
    lead(cur_time) over (partition by org_id, object_id order by cur_time) as end_ms
  from cell_info_hist
);

create or replace temporary view network_intervals as (
  select
    org_id,
    object_id,
    network,
    operator,
    start_ms,
    COALESCE(end_ms, unix_timestamp(now())*1000) as end_ms --fill open intervals
  from network_states
  where start_ms != 0
)

-- COMMAND ----------

create or replace temp view cell_network_intervals as (
  select
    org_id,
    object_id,
    network,
    operator,
    start_ms,
    end_ms
  from network_intervals
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Step 2: Explicitly handle intervals that span multiple days

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Now we need to handle intervals that are spanning multiple days. First we'll do this
-- MAGIC # by creating a dates temp table what will give us all of the dates since the first day
-- MAGIC # a modi device was attached. Additionally, we'll compute the interval start and end dates 
-- MAGIC # on the intervals table.
-- MAGIC
-- MAGIC dates_sdf = spark.sql("SELECT explode(sequence((select to_date(min(date)) from cell_info), current_date(), interval 1 day)) as interval_date")
-- MAGIC
-- MAGIC cell_network_intervals_sdf = spark.sql('''
-- MAGIC   SELECT *,
-- MAGIC     from_unixtime(start_ms / 1000, "yyyy-MM-dd") as start_date,
-- MAGIC     from_unixtime(end_ms / 1000, "yyyy-MM-dd") as end_date
-- MAGIC   FROM cell_network_intervals 
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Filter out intervals that do not span multiple days
-- MAGIC cell_network_intervals_multi_day = cell_network_intervals_sdf.filter(
-- MAGIC     col("start_date") != col("end_date")
-- MAGIC )
-- MAGIC
-- MAGIC # Use the dates dataframe to get each date in between the start
-- MAGIC # and end date of multi day intervals
-- MAGIC # For example if we have an interval from 01/01 to 01/03 the table
-- MAGIC # originally had one row like this
-- MAGIC # | 01/01 (start_date) | 01/03 (end_date)
-- MAGIC # Now the table will have three rows like this
-- MAGIC # | 01/01 (start_date) | 01/03 (end_date) | 01/01 (interval_date)
-- MAGIC # | 01/01 (start_date) | 01/03 (end_date) | 01/02 (interval_date)
-- MAGIC # | 01/01 (start_date) | 01/03 (end_date) | 01/03 (interval_date)
-- MAGIC cell_network_intervals_multi_day = cell_network_intervals_multi_day.join(
-- MAGIC     dates_sdf,
-- MAGIC     dates_sdf.interval_date.between(
-- MAGIC         cell_network_intervals_multi_day.start_date, cell_network_intervals_multi_day.end_date
-- MAGIC     ),
-- MAGIC     "left",
-- MAGIC )
-- MAGIC
-- MAGIC # We now have a row for each date spanned by a multi date interval
-- MAGIC # What we want however is a start ms and end ms for each of those dates
-- MAGIC # So we take each date and calculate the beginning of the day as the start ms
-- MAGIC # For the end ms we add 24 hours and subtract 1 ms to get the end ms (11:59:59:999)
-- MAGIC cell_network_intervals_multi_day = cell_network_intervals_multi_day.withColumn(
-- MAGIC     "interval_date_start_ms", F.unix_timestamp("interval_date", "yyyy-MM-dd") * 1000
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day = cell_network_intervals_multi_day.withColumn(
-- MAGIC     "interval_date_end_ms",
-- MAGIC     F.unix_timestamp("interval_date", "yyyy-MM-dd") * 1000 + 24 * 60 * 60 * 1000 - 1,
-- MAGIC )
-- MAGIC
-- MAGIC # So now we have each multi date interval broken down into a separate row for each date
-- MAGIC # We have a start ms and end ms for each date
-- MAGIC # However for the actual start date and end date of the state we don't want to use
-- MAGIC # the beginning of the day & end of the day as the start&end ms, we want to use the interval's
-- MAGIC # actual start & end ms
-- MAGIC # So if the date equals the interval start date we use the interval start ms otherwise we use beginning of the day start ms
-- MAGIC # Similarly if the date is the interval end date we use the interval end ms otherwise we use the end of day end ms
-- MAGIC cell_network_intervals_multi_day = cell_network_intervals_multi_day.withColumn(
-- MAGIC     "interval_start_ms",
-- MAGIC     F.when(F.col("start_date") == F.col("interval_date"), F.col("start_ms")).otherwise(
-- MAGIC         F.col("interval_date_start_ms")
-- MAGIC     ),
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day = cell_network_intervals_multi_day.withColumn(
-- MAGIC     "interval_end_ms",
-- MAGIC     F.when(F.col("end_date") == F.col("interval_date"), F.col("end_ms")).otherwise(
-- MAGIC         F.col("interval_date_end_ms")
-- MAGIC     ),
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # We now do some column renaming. We get rid of the old start & end ms columns and replace them with our newly calculated ones
-- MAGIC # We also recalculate the duration ms using our new start & end ms
-- MAGIC cell_network_intervals_multi_day_multi_day_union = cell_network_intervals_multi_day.withColumn(
-- MAGIC     "interval_end_date", F.col("interval_date")
-- MAGIC ).drop(
-- MAGIC     "start_ms",
-- MAGIC     "end_ms",
-- MAGIC     "start_date",
-- MAGIC     "end_date",
-- MAGIC     "interval_date_start_ms",
-- MAGIC     "interval_date_end_ms",
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day_multi_day_union = cell_network_intervals_multi_day_multi_day_union.withColumnRenamed(
-- MAGIC     "interval_start_ms", "start_ms"
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day_multi_day_union = cell_network_intervals_multi_day_multi_day_union.withColumnRenamed(
-- MAGIC     "interval_end_ms", "end_ms"
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day_multi_day_union = cell_network_intervals_multi_day_multi_day_union.withColumn(
-- MAGIC     "duration_ms", F.col("end_ms") - F.col("start_ms")
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day_multi_day_union = cell_network_intervals_multi_day_multi_day_union.withColumnRenamed(
-- MAGIC     "interval_date", "start_date"
-- MAGIC )
-- MAGIC cell_network_intervals_multi_day_multi_day_union = cell_network_intervals_multi_day_multi_day_union.withColumnRenamed(
-- MAGIC     "interval_end_date", "end_date"
-- MAGIC )
-- MAGIC
-- MAGIC # We now get the intervals that did not span multiple dates
-- MAGIC # We union these with the now broken up multi date intervals
-- MAGIC # Now we have the set of all intervals of each device, but
-- MAGIC # no interval spans more than one day
-- MAGIC cell_network_intervals_day_break = cell_network_intervals_sdf.filter(
-- MAGIC      col("start_date") == col("end_date")
-- MAGIC  )
-- MAGIC cell_network_intervals_day_break = cell_network_intervals_day_break.withColumn(
-- MAGIC    "duration_ms", F.col("end_ms") - F.col("start_ms")
-- MAGIC  )
-- MAGIC
-- MAGIC cell_network_intervals_day_break = cell_network_intervals_day_break.unionByName(
-- MAGIC   cell_network_intervals_multi_day_multi_day_union
-- MAGIC  )
-- MAGIC
-- MAGIC cell_network_intervals_day_break = cell_network_intervals_day_break.drop("date", "end_date")
-- MAGIC cell_network_intervals_day_break = cell_network_intervals_day_break.withColumnRenamed(
-- MAGIC      "start_date", "date"
-- MAGIC  )
-- MAGIC
-- MAGIC cell_network_intervals_day_break.createOrReplaceTempView("cell_network_intervals_corrected")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 3: Aggregate Data
-- MAGIC Note: The objective is to calculate the percentage of time spent across both Vodaphone and ATT networks and their respective operators, but a per device per operator granularity blows up the dataset. Instead, I've opted to create two views, one at the network and the other at the network + operator level of granularity with the distinct count of device connections.

-- COMMAND ----------

create or replace temp view device_network_operator_time as (
  select
    dev.date,
    dev.org_id,
    dev.org_type,
    dev.org_name,
    dev.product_type,
    dev.latest_build_on_day,
    dev.product_version,
    dev.rollout_stage_id,
    dev.product_program_id,
    dev.product_program_id_type,
    dev.status,
    COALESCE(dev.can_bus_type, 'null') as can_bus_type,
    dev.has_modi,
    dev.has_baxter,
    cell.network,
    cell.operator,
    count(distinct device_id) as operator_device_count,
    sum(cell.duration_ms) as total_operator_time
  from data_analytics.dataprep_vg3x_daily_health_metrics as dev
  join cell_network_intervals_corrected as cell on
    dev.date = cell.date
    and dev.org_id = cell.org_id
    and dev.device_id = cell.object_id
  --where --dev.date <= date_sub(current_date(),1)
     --and dev.date >= date_sub(current_date(),65)  --added in to speed up scheduled run
  group by
    dev.date,
    dev.org_id,
    dev.org_type,
    dev.org_name,
    dev.product_type,
    dev.rollout_stage_id,
    dev.product_program_id,
    dev.product_program_id_type,
    dev.latest_build_on_day,
    dev.product_version,
    dev.status,
    dev.can_bus_type,
    dev.has_modi,
    dev.has_baxter,
    cell.network,
    cell.operator
)

-- COMMAND ----------

create or replace temp view network_device_count as (
  select
    dev.date,
    dev.org_id,
    dev.org_type,
    dev.org_name,
    dev.product_type,
    dev.latest_build_on_day,
    dev.product_version,
    dev.rollout_stage_id,
    dev.product_program_id,
    dev.product_program_id_type,
    dev.status,
    COALESCE(dev.can_bus_type, 'null') as can_bus_type,
    dev.has_modi,
    dev.has_baxter,
    cell.network,
    count(distinct device_id) as network_device_count
  from data_analytics.dataprep_vg3x_daily_health_metrics as dev
  join cell_network_intervals_corrected as cell on
    dev.date = cell.date
    and dev.org_id = cell.org_id
    and dev.device_id = cell.object_id
  --where --dev.date <= date_sub(current_date(),1) 
    --and dev.date >= date_sub(current_date(),65) --added in to speed up scheduled run
  group by
    dev.date,
    dev.org_id,
    dev.org_type,
    dev.org_name,
    dev.product_type,
    dev.latest_build_on_day,
    dev.product_version,
    dev.rollout_stage_id,
    dev.product_program_id,
    dev.product_program_id_type,
    dev.status,
    dev.can_bus_type,
    dev.has_modi,
    dev.has_baxter,
    cell.network
)

-- COMMAND ----------

create or replace temp view vg3x_operator_network_joined as (
  select a.*,
    b.network_device_count
  from device_network_operator_time as a
  left join network_device_count as b on
  a.date = b.date
  and a.org_id = b.org_id
  and a.org_type = b.org_type
  and a.org_name = b.org_name
  and a.product_type = b.product_type
  and a.latest_build_on_day = b.latest_build_on_day
  and a.status = b.status
  and a.can_bus_type = b.can_bus_type
  and a.has_modi = b.has_modi
  and a.network = b.network
)

-- COMMAND ----------

create table if not exists data_analytics.vg3x_network_operator_time using delta
partitioned by (date)
select * from vg3x_operator_network_joined

-- COMMAND ----------

-- Update calculated fields for the last seven days using a delete and merge clause.
-- Upsert method was causing dupes
create or replace temp view vg3x_operator_network_joined_updates as (
  select *
  from vg3x_operator_network_joined
  where date >= date_sub(current_date(),10)
);

delete from data_analytics.vg3x_network_operator_time
where date >= date_sub(current_date(), 10);
  
merge into data_analytics.vg3x_network_operator_time as target 
using vg3x_operator_network_joined_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *
