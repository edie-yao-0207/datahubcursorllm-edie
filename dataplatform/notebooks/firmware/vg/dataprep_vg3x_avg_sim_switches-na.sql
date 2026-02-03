-- Databricks notebook source
-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.functions import col, current_date, date_sub, from_unixtime, udf
-- MAGIC
-- MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
-- MAGIC spark.conf.set("spark.sql.broadcastTimeout", 72000)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Step 1: Grab Cellular Network Connection Data

-- COMMAND ----------

-- Grab cell information. For simplicity, lumping Firstnet under ATT for now since it is an ATT service.
create or replace temp view cell_info as (
  select 
    cell.date,
    cell.time,
    cell.org_id,
    cell.object_id,
    case 
      when product_type like "%VG54%" and COALESCE(cell.value.proto_value.interface_state.cellular.sim_slot, 0) = 1 then 'Vodafone'
      when product_type like "%VG54%" and COALESCE(cell.value.proto_value.interface_state.cellular.sim_slot, 0) = 2 then 'ATT'
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
    cell.date <= date_sub(current_date(),1)
    and cell.date >= '2020-08-01' --dual sim slots didn't start coming online until 2020-09-17
    and value.is_end = 'false'
    and value.is_databreak = 'false'
    and dev.org_type not like 'Internal'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 2: Calculate total network switches

-- COMMAND ----------

-- -- In order to get the total times a device switches network on a given day,
-- -- we'll need to use a window function to look ahead 1 row to see the prev recorded network
-- -- and only grab where the network changes. 
create or replace temp view network_lag as (
    select
      date,
      org_id,
      object_id,
      network as cur_network,
      lag(network) over (partition by org_id, object_id order by time) as prev_network
    from cell_info
    where sim_slot is not null
  );
  
create or replace temp view network_switches as (
  select
    date,
    org_id,
    object_id,
    count(*) as switch_count
  from network_lag
  where prev_network != cur_network
    and prev_network is not null
  group by
  date,
  org_id,
  object_id
  
);

-- COMMAND ----------

create or replace temp view daily_device_count as (
  select
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    latest_build_on_day,
    status,
    COALESCE(can_bus_type, 'null') as can_bus_type,
    has_modi,
    has_baxter,
   count(distinct device_id) as total_device_count
  from data_analytics.dataprep_vg3x_daily_health_metrics
  where
    date <= date_sub(current_date(),1)
    and date >= '2020-08-01'
  group by
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    latest_build_on_day,
    status,
    can_bus_type,
    has_modi,
    has_baxter
);

-- COMMAND ----------

create or replace temp view network_switch_count as (
  select
    dev.date,
    dev.org_id,
    dev.org_type,
    dev.org_name,
    dev.product_type,
    dev.product_version,
    dev.rollout_stage_id,
    dev.product_program_id,
    dev.product_program_id_type,
    dev.latest_build_on_day,
    dev.status,
    COALESCE(dev.can_bus_type, 'null') as can_bus_type,
    dev.has_modi,
    dev.has_baxter,
    count(distinct cell.object_id) as switch_device_count,
    sum(cell.switch_count) as total_sim_switch_count,
    avg(cell.switch_count) as avg_sim_switch_count,
    percentile_approx(cell.switch_count, 0.50) as median_sim_switch_count,
    percentile_approx(cell.switch_count, 0.99) as p99_sim_switch_count
  from data_analytics.dataprep_vg3x_daily_health_metrics as dev
  join network_switches as cell on
    dev.date = cell.date
    and dev.org_id = cell.org_id
    and dev.device_id = cell.object_id
  group by
    dev.date,
    dev.org_id,
    dev.org_type,
    dev.org_name,
    dev.product_type,
    dev.product_version,
    dev.rollout_stage_id,
    dev.product_program_id,
    dev.product_program_id_type,
    dev.latest_build_on_day,
    dev.status,
    dev.can_bus_type,
    dev.has_modi,
    dev.has_baxter
)

-- COMMAND ----------

create or replace temp view switch_count_joined as (
  select
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.product_type,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    a.latest_build_on_day,
    a.status,
    COALESCE(a.can_bus_type, 'null') as can_bus_type,
    a.has_modi,
    a.has_baxter,
    a.total_device_count,
    b.switch_device_count,
    b.total_sim_switch_count,
    b.avg_sim_switch_count,
    b.median_sim_switch_count,
    b.p99_sim_switch_count
  from daily_device_count as a
  left join network_switch_count as b on
   a.date = b.date
   and a.org_id = b.org_id
   and a.org_type = b.org_type
   and a.org_name = b.org_name
   and a.product_type = b.product_type
   and a.product_version = b.product_version
   and a.latest_build_on_day = b.latest_build_on_day
   and a.status = b.status
   and a.can_bus_type = b.can_bus_type
   and a.has_modi = b.has_modi
   and a.rollout_stage_id = b.rollout_stage_id
   and a.product_program_id = b.product_program_id
   and a.product_program_id_type = b.product_program_id_type
)

-- COMMAND ----------

create table if not exists data_analytics.dataprep_vg3x_avg_sim_switches using delta
partitioned by (date)
select * from switch_count_joined

-- COMMAND ----------

-- Update calculated fields for the last seven days using a delete and merge clause.
-- Upsert method was causing dupes
create or replace temp view switch_count_joined_updates as (
  select *
  from switch_count_joined
  where date >= date_sub(current_date(),10)
);

delete from data_analytics.dataprep_vg3x_avg_sim_switches
where date >= date_sub(current_date(), 10);
  
merge into data_analytics.dataprep_vg3x_avg_sim_switches as target 
using switch_count_joined_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *
