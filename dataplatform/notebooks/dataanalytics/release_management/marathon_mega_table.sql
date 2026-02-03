-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### READ ME
-- MAGIC This notebook runs daily and updates the `firmware.ag5x_customer_gateways` table.
-- MAGIC
-- MAGIC This table is an enhanced version of the `dataprep.device_heartbeats_extended` table with the differences being:
-- MAGIC 1. It is based on gateways instead of devices
-- MAGIC 1. It includes first FW build in addition to last FW build
-- MAGIC 1. It includes first boot count in addition to last boot count
-- MAGIC 1. It includes first and last battery voltages and temperature
-- MAGIC 1. It includes first and last org ID
-- MAGIC 1. It includes last cell info and quality
-- MAGIC
-- MAGIC For each gateway, this table contains:
-- MAGIC 1. serial
-- MAGIC 1. gateway_id
-- MAGIC 1. product_id
-- MAGIC 1. survival_days
-- MAGIC 1. last_org_id
-- MAGIC 1. last_org_name
-- MAGIC 1. last_heartbeat_date
-- MAGIC 1. last_heartbeat_time
-- MAGIC 1. last_fw_build
-- MAGIC 1. last_device_id
-- MAGIC 1. last_boot_count
-- MAGIC 1. last_batt1_mv
-- MAGIC 1. last_batt2_mv
-- MAGIC 1. last_batt3_mv
-- MAGIC 1. last_temperature_mc
-- MAGIC 1. last_mcc
-- MAGIC 1. last_mnc
-- MAGIC 1. last_access_technology
-- MAGIC 1. last_earfcn
-- MAGIC 1. last_cell_id
-- MAGIC 1. last_physical_cell_id
-- MAGIC 1. last_tac
-- MAGIC 1. last_periodic_tau_s
-- MAGIC 1. last_rsrp_dbm
-- MAGIC 1. last_rsrq_db
-- MAGIC 1. last_snr_db
-- MAGIC 1. last_energy_estimate
-- MAGIC 1. last_modem_fw_version
-- MAGIC 1. first_org_id
-- MAGIC 1. first_org_name
-- MAGIC 1. first_heartbeat_date
-- MAGIC 1. first_heartbeat_time
-- MAGIC 1. first_fw_build
-- MAGIC 1. first_device_id
-- MAGIC 1. first_boot_count
-- MAGIC 1. first_batt1_mv
-- MAGIC 1. first_batt2_mv
-- MAGIC 1. first_batt3_mv
-- MAGIC 1. first_temperature_mc

-- COMMAND ----------

-- DBTITLE 1,Get heartbeats from AG5x gateways in all customer orgs, US and EU product ID's
create or replace temporary view table_osdhubserverdeviceheartbeat_filtered as (
  select
    hb.date,
    hb.time,
    hb.org_id,
    org.name as org_name,
    hb.object_id as device_id,
    hb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id,
    gw.product_id,
    gw.serial,
    hb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.build as fw_build,
    hb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count
  from kinesisstats.osdhubserverdeviceheartbeat hb
  left join clouddb.organizations org
    ON hb.org_id = org.id
  left join clouddb.gateways gw
    ON hb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id = gw.id
  where gw.product_id in (124, 125, 142, 143, 140, 144)
    and hb.date >= DATE_SUB(CURRENT_DATE(),30)
    and org.internal_type = 0
);

-- COMMAND ----------

-- DBTITLE 1,Make a table with the first heartbeat for each gateway
create or replace temporary view table_first_heartbeats as (
  select
    gateway_id,
    product_id,
    serial,
    min_by(org_id, time) as first_org_id,
    min_by(org_name, time) as first_org_name,
    min_by(device_id, time) as first_device_id,
    min_by(fw_build, time) as first_fw_build,
    min_by(boot_count, time) as first_boot_count,
    min_by(date, time) as first_heartbeat_date,
    min(time) as first_heartbeat_time
  from table_osdhubserverdeviceheartbeat_filtered t1
  group by 1,2,3
);

-- COMMAND ----------

-- DBTITLE 1,Make a table with the last heartbeat for each gateway
create or replace temporary view table_last_heartbeats as (
  select
    gateway_id,
    product_id,
    serial,
    max_by(org_id, time) as last_org_id,
    max_by(org_name, time) as last_org_name,
    max_by(device_id, time) as last_device_id,
    max_by(fw_build, time) as last_fw_build,
    max_by(boot_count, time) as last_boot_count,
    max_by(date, time) as last_heartbeat_date,
    max(time) as last_heartbeat_time
  from table_osdhubserverdeviceheartbeat_filtered t1
  group by 1,2,3
);

-- COMMAND ----------

-- DBTITLE 1,Get the battery voltages and temperature for gateways in customer orgs
create or replace temporary view table_osdbatteryinfo_filtered as (
  select
    batt.date,
    batt.time,
    batt.org_id,
    batt.object_id as device_id,
    get(batt.value.proto_value.battery_info.cell, 0).mv as batt1_mv,
    get(batt.value.proto_value.battery_info.cell, 1).mv as batt2_mv,
    get(batt.value.proto_value.battery_info.cell, 2).mv as batt3_mv,
    batt.value.proto_value.battery_info.temperature_mc
  from kinesisstats.osdbatteryinfo batt
  left join clouddb.organizations org
    on batt.org_id = org.id
  where batt.date >= DATE_SUB(CURRENT_DATE(),30)
    and org.internal_type = 0
);

-- COMMAND ----------

-- DBTITLE 1,Make a table with the first voltages/temperature after first heartbeat for each gateway
create or replace temporary view table_first_batt_info_after_first_heartbeat as (
  WITH first_ts AS (
    SELECT
      device_id,
      MIN(tof.time) AS min_time
    FROM table_osdbatteryinfo_filtered tof
    JOIN table_first_heartbeats tfh
      ON tof.device_id = tfh.first_device_id
        AND tof.time >= tfh.first_heartbeat_time
    GROUP BY 1
  )

  select
    batt_t1.org_id,
    hb.gateway_id,
    hb.product_id,
    batt_t1.batt1_mv as first_batt1_mv,
    batt_t1.batt2_mv as first_batt2_mv,
    batt_t1.batt3_mv as first_batt3_mv,
    batt_t1.temperature_mc as first_temperature_mc,
    batt_t1.date as first_batt_info_date,
    batt_t1.time as first_batt_info_time
  from table_osdbatteryinfo_filtered batt_t1
  left join table_first_heartbeats hb
    on batt_t1.device_id = hb.first_device_id
  WHERE (batt_t1.device_id, batt_t1.time) IN (SELECT device_id, min_time FROM first_ts)
);

-- COMMAND ----------

-- DBTITLE 1,Make a table with the last voltages/temperature before last heartbeat for each gateway
create or replace temporary view table_last_batt_info_before_last_heartbeat as (
  WITH last_ts AS (
    SELECT
      device_id,
      MAX(tof.time) AS max_time
    FROM table_osdbatteryinfo_filtered tof
    JOIN table_last_heartbeats tfh
      ON tof.device_id = tfh.last_device_id
        AND tof.time <= tfh.last_heartbeat_time
    GROUP BY 1
  )

  select
    batt_t1.org_id,
    hb.gateway_id,
    hb.product_id,
    batt_t1.batt1_mv as last_batt1_mv,
    batt_t1.batt2_mv as last_batt2_mv,
    batt_t1.batt3_mv as last_batt3_mv,
    batt_t1.temperature_mc as last_temperature_mc,
    batt_t1.date as last_batt_info_date,
    batt_t1.time as last_batt_info_time
  from table_osdbatteryinfo_filtered batt_t1
  left join table_last_heartbeats hb
    on batt_t1.device_id = hb.last_device_id
  WHERE (batt_t1.device_id, batt_t1.time) IN (SELECT device_id, max_time FROM last_ts)
);

-- COMMAND ----------

-- DBTITLE 1,Make a table with the device_id and all cell info for devices in customer orgs
create or replace temporary view table_osdnordicltedebug_filtered as (
  select
    lte.date,
    lte.time,
    lte.org_id,
    lte.object_id as device_id,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.mcc,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.mnc,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.access_technology,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.earfcn,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.cell_id,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.physical_cell_id,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.tac,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.periodic_tau_s,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.rsrp_dbm,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.rsrq_db,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.signal_to_noise_ratio_db as snr_db,
    lte.value.proto_value.nordic_lte_debug.lte_cell_info.energy_estimate,
    lte.value.proto_value.nordic_lte_debug.modem_fw_version
  from kinesisstats.osdnordicltedebug lte
  left join clouddb.organizations org
    on lte.org_id = org.id
  where lte.date >= DATE_SUB(CURRENT_DATE(),30)
    and org.internal_type = 0
);

-- COMMAND ----------

-- DBTITLE 1,Update the cell info table with the corresponding gateway_id at the time the stat was reported
create or replace temporary view table_osdnordicltedebug_filtered_with_gateway_id as (
  select
    t.*,
    h.gateway_id
  from table_osdnordicltedebug_filtered t
  join hardware.gateways_heartbeat h on h.object_id = t.device_id and h.org_id = t.org_id
  where t.time between h.first_heartbeat_time and h.last_heartbeat_time
);

-- COMMAND ----------

-- DBTITLE 1,Make a table with the last cell info for each gateway
create or replace temporary view table_last_cell_info as (
  select
    gateway_id,
    max(date) as date,
    max(time) as time,
    max_by(mcc, time) as last_mcc,
    max_by(mnc, time) as last_mnc,
    max_by(access_technology, time) as last_access_technology,
    max_by(earfcn, time) as last_earfcn,
    max_by(cell_id, time) as last_cell_id,
    max_by(physical_cell_id, time) as last_physical_cell_id,
    max_by(tac, time) as last_tac,
    max_by(periodic_tau_s, time) as last_periodic_tau_s,
    max_by(rsrp_dbm, time) as last_rsrp_dbm,
    max_by(rsrq_db, time) as last_rsrq_db,
    max_by(snr_db, time) as last_snr_db,
    max_by(energy_estimate, time) as last_energy_estimate,
    max_by(modem_fw_version, time) as last_modem_fw_version
  from table_osdnordicltedebug_filtered_with_gateway_id
  where rsrp_dbm != 0 -- add this to filter out empty osdnordicltedebug stats
  group by 1
);

-- COMMAND ----------

-- DBTITLE 1,Combine everything into a single table
create or replace temporary view ag5x_customer_gateways as (
  select
    lhb.serial,
    lhb.gateway_id,
    lhb.product_id,
    datediff(day, first_heartbeat_date, last_heartbeat_date) as survival_days,
    last_org_id,
    last_org_name,
    last_heartbeat_date,
    last_heartbeat_time,
    last_fw_build,
    last_device_id,
    last_boot_count,
    last_batt1_mv,
    last_batt2_mv,
    last_batt3_mv,
    last_temperature_mc,
    last_mcc,
    last_mnc,
    last_access_technology,
    last_earfcn,
    last_cell_id,
    last_physical_cell_id,
    last_tac,
    last_periodic_tau_s,
    last_rsrp_dbm,
    last_rsrq_db,
    last_snr_db,
    last_energy_estimate,
    last_modem_fw_version,
    first_org_id,
    first_org_name,
    first_heartbeat_date,
    first_heartbeat_time,
    first_fw_build,
    first_device_id,
    first_boot_count,
    first_batt1_mv,
    first_batt2_mv,
    first_batt3_mv,
    first_temperature_mc
  from table_first_heartbeats fhb
  left join table_last_heartbeats lhb
    on fhb.gateway_id = lhb.gateway_id
  left join table_first_batt_info_after_first_heartbeat fbi
    on fhb.gateway_id = fbi.gateway_id
  left join table_last_batt_info_before_last_heartbeat lbi
    on fhb.gateway_id = lbi.gateway_id
  left join table_last_cell_info lci
    on fhb.gateway_id = lci.gateway_id
);

-- COMMAND ----------

create table if not exists firmware.ag5x_customer_gateways using delta partitioned by (last_heartbeat_date) AS (
  select *
  from ag5x_customer_gateways
)

-- COMMAND ----------

-- DBTITLE 1,Create updates temp view using only last 2 days of heartbeat data
create or replace temp view ag5x_customer_gateways_updates as (
  select * from ag5x_customer_gateways where last_heartbeat_date >= DATE_SUB(CURRENT_DATE(),2)
)

-- COMMAND ----------

-- Note: We only update last_* columns for existing gateways to preserve
-- the original first_* values (first_heartbeat_date, first_fw_build, etc.)
MERGE INTO firmware.ag5x_customer_gateways AS target
USING ag5x_customer_gateways_updates AS updates
ON target.gateway_id = updates.gateway_id
  AND target.product_id = updates.product_id
  AND target.serial = updates.serial
WHEN MATCHED THEN UPDATE SET
  target.survival_days = DATEDIFF(day, target.first_heartbeat_date, updates.last_heartbeat_date),
  target.last_org_id = updates.last_org_id,
  target.last_org_name = updates.last_org_name,
  target.last_heartbeat_date = updates.last_heartbeat_date,
  target.last_heartbeat_time = updates.last_heartbeat_time,
  target.last_fw_build = updates.last_fw_build,
  target.last_device_id = updates.last_device_id,
  target.last_boot_count = updates.last_boot_count,
  target.last_batt1_mv = updates.last_batt1_mv,
  target.last_batt2_mv = updates.last_batt2_mv,
  target.last_batt3_mv = updates.last_batt3_mv,
  target.last_temperature_mc = updates.last_temperature_mc,
  target.last_mcc = updates.last_mcc,
  target.last_mnc = updates.last_mnc,
  target.last_access_technology = updates.last_access_technology,
  target.last_earfcn = updates.last_earfcn,
  target.last_cell_id = updates.last_cell_id,
  target.last_physical_cell_id = updates.last_physical_cell_id,
  target.last_tac = updates.last_tac,
  target.last_periodic_tau_s = updates.last_periodic_tau_s,
  target.last_rsrp_dbm = updates.last_rsrp_dbm,
  target.last_rsrq_db = updates.last_rsrq_db,
  target.last_snr_db = updates.last_snr_db,
  target.last_energy_estimate = updates.last_energy_estimate,
  target.last_modem_fw_version = updates.last_modem_fw_version
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- -- Delete gateways from the table that were activated in orgs that were originally marked customer but then switched to internal
-- DELETE FROM
--   firmware.ag5x_customer_gateways
-- WHERE
--   last_org_id in (
--     select
--       distinct last_org_id
--     from
--       firmware.ag5x_customer_gateways gw,
--       clouddb.organizations o
--     where
--       o.internal_type != 0
--       and gw.last_org_id = o.id
--   )

-- COMMAND ----------

-- UPDATE METADATA --------
ALTER TABLE firmware.ag5x_customer_gateways SET TBLPROPERTIES ('comment' = 'This table is an enhanced version of the dataprep.device_heartbeats_extended table for AG5x. The main differences are that is based on gateways instead of devices and includes first and last reported FW build, org ID, boot count, battery voltage(s) and temperature.');
ALTER TABLE firmware.ag5x_customer_gateways CHANGE serial COMMENT 'Gateway serial number';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE gateway_id COMMENT 'Gateway ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE product_id COMMENT 'Product ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE survival_days COMMENT 'Days between first and last heartbeats';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_org_id COMMENT 'Last org ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_org_name COMMENT 'Last org name';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_heartbeat_date COMMENT 'Last heartbeat date';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_heartbeat_time COMMENT 'Last heartbeat time';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_fw_build COMMENT 'Last FW build';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_device_id COMMENT 'Last device ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_boot_count COMMENT 'Last boot count';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_batt1_mv COMMENT 'Last battery 1 voltage';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_batt2_mv COMMENT 'Last battery 2 voltage (if applicable)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_batt3_mv COMMENT 'Last battery 3 voltage (if applicable)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_temperature_mc COMMENT 'Last temperature';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_mcc COMMENT 'Last MCC';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_mnc COMMENT 'Last MNC';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_access_technology COMMENT 'Last access technology';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_earfcn COMMENT 'Last EARFCN';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_cell_id COMMENT 'Last cell ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_physical_cell_id COMMENT 'Last physical cell ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_tac COMMENT 'Last TAC';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_periodic_tau_s COMMENT 'Last periodic TAU (seconds)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_rsrp_dbm COMMENT 'Last RSRP (dBm)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_rsrq_db COMMENT 'Last RSRQ (dB)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_snr_db COMMENT 'Last SNR (dB)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_energy_estimate COMMENT 'Last energy estimate';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE last_modem_fw_version COMMENT 'Last modem FW version';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_org_id COMMENT 'First org ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_org_name COMMENT 'First org name';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_heartbeat_date COMMENT 'First heartbeat date';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_heartbeat_time COMMENT 'First heartbeat time';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_fw_build COMMENT 'First FW build';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_device_id COMMENT 'First device ID';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_boot_count COMMENT 'First boot count';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_batt1_mv COMMENT 'First battery 1 voltage';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_batt2_mv COMMENT 'First battery 2 voltage (if applicable)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_batt3_mv COMMENT 'First battery 3 voltage (if applicable)';
ALTER TABLE firmware.ag5x_customer_gateways CHANGE first_temperature_mc COMMENT 'First temperature';

-- COMMAND ----------

describe table extended firmware.ag5x_customer_gateways
