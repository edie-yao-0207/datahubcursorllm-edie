-- Databricks notebook source
-- MAGIC %md
-- MAGIC Look at two cases;
-- MAGIC  - device had moved out of org 0 and then re-appeared in org 0 (i.e. was reset)
-- MAGIC  - did the above and then landed outside of org 0 again (recovered)

-- COMMAND ----------

--Select only Vg3x's from heartbeats table
create or replace temp view device_list as 
(
  select 
    hb.date,
    hb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.org_id,
    hb.time,
    hb.object_id as device_id,
    hb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.build,
    b.product_type,
    b.can_bus_type,
    b.status,
    b.gw_first_heartbeat_date,
    b.gw_last_heartbeat_date,
    b.org_name,
    b.has_modi,
    b.has_baxter,
    b.product_version,
    b.rollout_stage_id,
    b.product_program_id,
    b.product_program_id_type
  from kinesisstats.osdhubserverdeviceheartbeat as hb
  inner join data_analytics.dataprep_vg3x_daily_health_metrics as b on
   hb.date = b.date
   and hb.object_id = b.device_id
  where hb.date >= date_sub(current_date(),10)
    and b.date >= date_sub(current_date(),10)
    and hb.value.is_databreak = 'false'
    and hb.value.is_end = 'false'
    and hb.org_id not in (1,18103)
);
-- 24 = VG34
-- 35 = VG34-EU (US CLOUD)
-- 53 = 'VG54 (THOR)'

-- COMMAND ----------

create or replace temp view device_heartbeat_lag as 
(
  select
    date,
    org_id,
    device_id,
    build,
    product_type,
    can_bus_type,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    status,
    gw_first_heartbeat_date,
    gw_last_heartbeat_date,
    org_name,
    has_modi,
    has_baxter,
    lag(org_id) OVER (partition by device_id ORDER BY time) as prev_org_id,
    lag(time) OVER (partition by device_id ORDER BY time) as prev_time,
    case --catches edge case where first heartbeart is in org 0
      when lag(org_id,2) OVER (partition by device_id ORDER BY time) is null and org_id > 0 then 'true'
      else 'false' end as first_hb_correction
  from device_list
);

-- COMMAND ----------

--Add column to denote when heart beat is after a flash reset or flash recovery
create or replace temp view dev_list_lag as 
(
 select *,
   case
     when org_id = 0 and prev_org_id > 0 then 'reset'
     when org_id > 0 and prev_org_id = 0 and first_hb_correction = 'false' then 'recovered'
    else 'org_change' end as flash_status
 from device_heartbeat_lag
 where
   org_id != prev_org_id
);

--uncache table dev_list_lag

-- COMMAND ----------

--Next Steps: Split into two tables, flash_type reset and flash_type recovered, and then add in the org types. (Doing it this because of the lag and lead functions--need to know previous and then next org type and will want to filter them as one column in Tableau).

-- COMMAND ----------

--Get org type for flash reset heartbeats

create or replace temp view flash_reset_devices as
(
  select a.*,
    COALESCE(org.internal_type,'0') as org_type
    --org.name as org_name
  from dev_list_lag as a
  left join ( select id, internal_type, name from clouddb.organizations) as org on 
    a.prev_org_id = org.id
  where a.flash_status = 'reset'
);

-- COMMAND ----------

--Get org type for flash recovered devices
create or replace temp view flash_recovered_devices as
(
  select a.*,
    COALESCE(org.internal_type,'0') as org_type
    --org.name as org_name
  from dev_list_lag as a
  left join ( select id, internal_type, name from clouddb.organizations) as org on 
    a.org_id = org.id
  where a.flash_status = 'recovered'
);

-- COMMAND ----------

--Filter out unnecessary columns and union flash reset and flash recovered device tables.
create or replace temp view flash_dev_list as
(
  select 
    date,
    prev_org_id as org_id,
    device_id,
    build,
    product_type,
    can_bus_type,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    product_version,
    status,
    gw_first_heartbeat_date,
    gw_last_heartbeat_date,
    flash_status,
    org_type,
    org_name,
    has_modi,
    has_baxter
  from flash_reset_devices
  UNION
  select 
    date,
    org_id,
    device_id,
    build,
    product_type,
    product_version,
    can_bus_type,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    status,
    gw_first_heartbeat_date,
    gw_last_heartbeat_date,
    flash_status,
    org_type,
    org_name,
    has_modi,
    has_baxter
  from flash_recovered_devices
  order by date
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg_flash_reset_devices AS (
  select
    date,
    case
      when org_type = 1 then 'Internal'
      else 'Customer' end as org_type,
    org_name,
    has_modi,
    has_baxter,
    org_id,
    product_type,
    device_id,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    build as latest_build_on_day,
    case
          when can_bus_type = 0 then "Invalid"
          when can_bus_type = 1 then "J1939_250"
          when can_bus_type = 2 then "Passenger"
          when can_bus_type = 3 then "J1850_PWM"
          when can_bus_type = 4 then "J1850_VPW"
          when can_bus_type = 5 then "9141"
          when can_bus_type = 6 then "14230_5"
          when can_bus_type = 7 then "14230_FAST"
          when can_bus_type = 8 then "14230_11_500"
          when can_bus_type = 9 then "14230_29_500"
          when can_bus_type = 10 then "14230_11_250"
          when can_bus_type = 11 then "14230_29_250"
          when can_bus_type = 12 then "J1939_500"
          when can_bus_type = 13 then "J1708"
          when can_bus_type = 14 then "Thermoking"
          when can_bus_type = 15 then "Carrier"
          when can_bus_type = 16 then "Passenger 2nd"
          when can_bus_type = 17 then "Thermoking Ibox"
          when can_bus_type = 18 then "Passenger Passive"
          when can_bus_type = 19 then "Carrier Comms"
          when can_bus_type = 20 then "Primary Passenger 15765"
          when can_bus_type = 21 then "Secondary Passenger 15765"
          when can_bus_type = 22 then "Tertiary Passenger 15765"
          when can_bus_type = 23 then "SW Passenger 15765"
          else "Unknown" end as can_bus_type,
    status,
    
    flash_status,
    gw_first_heartbeat_date,
    gw_last_heartbeat_date
  from flash_dev_list
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.fleetfwhealth_vg34_flash_devices USING DELTA
PARTITIONED BY (date)
AS
SELECT * FROM vg_flash_reset_devices

-- COMMAND ----------

-- Update calculated fields for the last seven days using a delete and merge clause.
-- Upsert method was causing dupes
CREATE OR REPLACE TEMP VIEW vg_flash_reset_devices_updates as (
  select *
  from vg_flash_reset_devices
  where date >= date_sub(current_date(),10)
);

delete from data_analytics.fleetfwhealth_vg34_flash_devices
where date >= date_sub(current_date(), 10);
  
merge into data_analytics.fleetfwhealth_vg34_flash_devices as target 
using vg_flash_reset_devices_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *
