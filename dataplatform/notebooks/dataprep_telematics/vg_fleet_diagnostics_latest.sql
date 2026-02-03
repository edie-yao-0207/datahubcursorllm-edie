-- Databricks notebook source

-- COMMAND ----------

-- For backfill support
SET spark.databricks.queryWatchdog.maxQueryTasks=500000;

-- COMMAND ----------

-- DBTITLE 1,Dedup device_vin_metadata
-- there may be multiple rows in vindb table if device moves across org. Pick latest
create or replace temporary view device_vin_metadata_deduped as (
  select
    device_id,
    max_by(trim(engine_model), _timestamp) as engine_model,
    max_by(primary_fuel_type, _timestamp) as primary_fuel_type,
    max_by(engine_manufacturer, _timestamp) as engine_manufacturer
  from
    vindb_shards.device_vin_metadata
  group by
    device_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull All Customer VGs
create or replace temporary view vg_34_devices as (
  select
    a.device_id as object_id,
    a.org_id,
    a.product_id,
    a.variant_id,
    c.name as org_name,
    b.name as device_name,
    b.Serial as serial,
    trim(b.make) as make,
    trim(b.model) as model,
    b.year,
    b.config_override_json,
    b.manual_odometer_meters,
    b.vin,
    b.manual_engine_hours,
    b.manual_odometer_updated_at,
    b.manual_engine_hours_updated_at,
    c.internal_type,
    d.engine_model,
    d.primary_fuel_type,
    d.engine_manufacturer
  from
    productsdb.gateways as a
    join productsdb.devices as b on a.device_id = b.id
    join clouddb.organizations as c on a.org_id = c.id
    left join device_vin_metadata_deduped as d on b.id = d.device_id
  where
    a.product_id in (24, 53, 35, 89, 178)
);

-- COMMAND ----------

-- DBTITLE 1,Pull VG Heartbeats
create or replace temporary view vg_heartbeats as
(
  select object_id,
    min(date) as first_heartbeat,
    max(date) as last_heartbeat,
    max_by(value.proto_value.hub_server_device_heartbeat.connection.device_hello.build, time)  as last_reported_build
  from kinesisstats.osdhubserverdeviceheartbeat
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Cable Type
create or replace temporary view vg_cable_id as
(
  select
    object_id,
    max_by(value.int_value,time) as latest_cable_id
  from
    kinesisstats.osdobdcableid
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull CAN Type
create or replace temporary view vg_can_id as
(
  select
    object_id,
    max_by(value.int_value,time) as latest_can_id
  from
    kinesisstats.osdcanbustype
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull CAN Connection State
create or replace temporary view vg_can_connection_state as (
  select object_id,
    min(date) as first_can_connection,
    max(date) as last_can_connection
  from
    kinesisstats.osdcanconnected
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull VIN
create or replace temporary view vg_vin as
(
  select
    object_id,
    min(date) as first_vin_date,
    max(date) as last_vin_date,
    max_by(value.proto_value.vin_event.vin,time) as last_vin_value,
    COUNT(*) > 0 AS has_ecm_vin
  from kinesisstats.osdvin
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Odometer
create or replace temporary view vg_odometer as
(
  select
    object_id,
    min(date) as first_odometer_date,
    max(date) as last_odometer_date,
    COUNT(*) > 0 as has_ecm_odo,
    round(min_by(value.int_value,time) / 1609.344) as first_odometer_value,
    round(max_by(value.int_value,time) / 1609.344) as last_odometer_value
  from kinesisstats.osdOdometer
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Engine Hours
create or replace temporary view vg_engine_hours as
(
  select
    object_id,
    min(date) as first_engine_hour_date,
    max(date) as last_engine_hour_date,
    round(min_by(value.int_value,time) / 3600) as first_engine_hour_value,
    round(max_by(value.int_value,time) / 3600) as last_engine_hour_value,
     COUNT(*) > 0 AS has_ecm_enginehours
  from kinesisstats.osdengineseconds
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Fuel Level
create or replace temporary view vg_fuel_level as
(
  select
    object_id,
    min(date) as first_fuel_level_date,
    max(date) as last_fuel_level_date,
    max_by(value.proto_value.engine_gauge_event.fuel_level_percent,time) as last_fuel_level
    from kinesisstats.osdenginegauge
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Fuel Consumption
create or replace temporary view vg_fuel_consumption as
(
  select
    object_id,
    min(date) as first_fuel_cons_date,
    max(date) as last_fuel_cons_date,
    round(max_by(value.int_value,time)) as last_fuel_cons_value
  from kinesisstats.osdderivedfuelconsumed
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Seatbelt
create or replace temporary view vg_seatbelt as
(
  select
    object_id,
    min(date) as first_belt_date,
    max(date) as last_belt_date,
    round(max_by(value.int_value,time)) as last_seatbelt_value
  from kinesisstats.osdseatbeltdriver
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Vehicle Battery Voltage
create or replace temporary view vehicle_battery_voltage as (
  select
    object_id,
    max_by(value.int_value,time) as latest_battery_voltage
  from
    kinesisstats.osdcablevoltage
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull WiFi Status
create or replace temporary view vg_wifi_usage as (
  select
    object_id,
    max_by(value.int_value,time) as wifi_usage_value
  from
    kinesisstats.osdwifiapbytes
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Health Metrics
create or replace temporary view vg_boot_count as (
  select
    object_id,
    max_by(value.proto_value.anomaly_event.boot_count,time) as boot_count
  from
    kinesisstats.osdanomalyevent
  where value.is_databreak = 'false' and
    value.is_end = 'false' and
    object_id in (select object_id from vg_34_devices)
    and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  group by object_id
);

-- COMMAND ----------

-- DBTITLE 1,Pull Trip Date
create or replace temporary view vg_trip as
(
  select device_id,
    min(date) as first_trip_date,
    max(date) as last_trip_date
  from trips2db_shards.trips
  where device_id in (select object_id from vg_34_devices)
  and date between date_sub(current_date(), coalesce(cast(getArgument("LOOKBACK_DAYS") as int), 7)) and date_add(current_date(), 1)
  and version = 101
  group by device_id
);

-- COMMAND ----------

-- DBTITLE 1,Join All Tables
create or replace temporary view updated_fleet_diagnostics as (
  select
    lower(vg_34_devices.org_name) as org_name,
    vg_34_devices.org_id,
    vg_34_devices.object_id,
    vg_34_devices.product_id,
    vg_34_devices.variant_id,
    lower(vg_34_devices.device_name) as device_name,
    vg_34_devices.serial,
    lower(vg_34_devices.make) as make,
    lower(vg_34_devices.model) as model,
    vg_34_devices.year,
    lower(vg_34_devices.engine_model) as engine_model,
    lower(vg_34_devices.primary_fuel_type) as primary_fuel_type,
    lower(vg_34_devices.engine_manufacturer) as engine_manufacturer,

    vg_cable_id.latest_cable_id as cable_id,
    vg_can_id.latest_can_id as can_id,

    --Cable ID

    case
      when vg_cable_id.latest_cable_id is null then null
      when vg_cable_id.latest_cable_id = 0 then "CBL-VG-BPC or Power Only or Cable Disconnected"
      when vg_cable_id.latest_cable_id = 1 then "CBL-VG-BJ1939-Y0 / VG34 J1939 Universal or CBL-VG-BIZU / VG34 Isuzu"
      when vg_cable_id.latest_cable_id = 2 then "ACC-BJ1939-Y1 / VG34 Old J1939 Type 1 cable or CBL-VG-BFMS / VG34-EU J1939 FMS"
      when vg_cable_id.latest_cable_id = 3 then "CBL-VG-BJ1708 6-pin or 9-pin / VG34 J1708 with USB"
      when vg_cable_id.latest_cable_id = 4 then "CBL-VG-BOBDII / VG34 OBD Passenger"
      when vg_cable_id.latest_cable_id = 5 then "CBL-VG-BJ1939-VM / VG34 J1939 Type 1 - Volvo Mack"
      when vg_cable_id.latest_cable_id = 6 then "CBL-VG-B1226 / VG34 RP1226"
      when vg_cable_id.latest_cable_id = 7 then "CBL-VG-BHGV / VG34-EU J1939 Tachograph"
      when vg_cable_id.latest_cable_id = 8 then "CBL-VG-CPC / VG54 Power Only / Cable Disconnected"
      when vg_cable_id.latest_cable_id = 9 then "CBL-VG-CJ1708 / VG54/Modi J1708-6P"
      when vg_cable_id.latest_cable_id = 10 then "CBL-VG-CJ1939 / VG54/Modi J1939/J1708P"
      when vg_cable_id.latest_cable_id = 11 then "CBL-VG-CJ1939-VM / VG54/Modi J1939 Type 1 - Volvo Mack"
      when vg_cable_id.latest_cable_id = 12 then "CBL-VG-CRP1226 / VG54/Modi RP1226"
      when vg_cable_id.latest_cable_id = 13 then "CBL-VG-COBDII / VG54/Modi OBD Passenger"
      when vg_cable_id.latest_cable_id = 14 then "CBL-VG-CHGV / VG54-EU/Modi J1939 Tachograph"
      when vg_cable_id.latest_cable_id = 15 then "CBL-VG-CFMS / VG54-EU/Modi FMS"
      when vg_cable_id.latest_cable_id = 16 then "CBL-VG-COBDII-Y2 / VG54/Modi OBD Passenger"
      when vg_cable_id.latest_cable_id = 17 then "CBL-VG-CIZU / VG54/Modi Isuzu"
      when vg_cable_id.latest_cable_id = 18 then "CBL-VG-COBDII-Y0 / VG54/Modi OBD Passenger"
      when vg_cable_id.latest_cable_id = 19 then "CBL-VG-COBDII-Y3 / VG54/Modi OBD Passenger"
      when vg_cable_id.latest_cable_id = 20 then "CBL-VG-COBDII-Y1S / VG54/Modi OBD Passenger"
      when vg_cable_id.latest_cable_id = 21 then "C-class Power Only Cable V2"
      when vg_cable_id.latest_cable_id = 22 then "CBL-VG-CTSLA3Y-19 / VG54 Tesla 3/Y"
      when vg_cable_id.latest_cable_id = 23 then "CBL-VG-COBDII-Y0S / VG54/Modi OBD Passenger"
      when vg_cable_id.latest_cable_id = 24 then "CBL-VG-CTSLAXS-18 / VG54 Tesla X/S"
      when vg_cable_id.latest_cable_id = 26 then "CBL-VG-HJ1939 / VG54H Paccar J1939/J1708P"
      else "other"
    end as cable_name,

    --CAN BUS Types
    case
      when vg_can_id.latest_can_id is null then null
      when vg_can_id.latest_can_id = 0 then "CAN Disconnected"
      when vg_can_id.latest_can_id = 1 then "J1939 Type 1 (250)"
      when vg_can_id.latest_can_id = 2 then "OBD"
      when vg_can_id.latest_can_id = 3 then "OBD - J1850 PWM"
      when vg_can_id.latest_can_id = 4 then "OBD - J1850 VPW"
      when vg_can_id.latest_can_id = 5 then "ISO9141"
      when vg_can_id.latest_can_id = 6 then "KWP - 5 baud init"
      when vg_can_id.latest_can_id = 7 then "KWP - fast init"
      when vg_can_id.latest_can_id = 8 then "CAN 15765 11/500"
      when vg_can_id.latest_can_id = 9 then "CAN 15765 29/500"
      when vg_can_id.latest_can_id = 10 then "CAN 15765 11/250"
      when vg_can_id.latest_can_id = 11 then "CAN 15765 29/250"
      when vg_can_id.latest_can_id = 12 then "J1939 Type 2 (500)"
      when vg_can_id.latest_can_id = 13 then "J1708"
      when vg_can_id.latest_can_id = 14 then "THERMOKING"
      when vg_can_id.latest_can_id = 15 then "CARRIER"
      when vg_can_id.latest_can_id = 16 then "PASSENGER_SECONDARY_PASSIVE"
      when vg_can_id.latest_can_id = 17 then "THERMOKING_IBOX"
      when vg_can_id.latest_can_id = 18 then "PASSENGER_PASSIVE"
      when vg_can_id.latest_can_id = 19 then "CARRIER_COMMS"
      when vg_can_id.latest_can_id = 20 then "PRIMARY_PASSENGER_15765"
      when vg_can_id.latest_can_id = 21 then "SECONDARY_PASSENGER_15765"
      when vg_can_id.latest_can_id = 22 then "TERTIARY_PASSENGER_15765"
      when vg_can_id.latest_can_id = 23 then "SW_PASSENGER_15765"
      when vg_can_id.latest_can_id = 24 then "EV_PASSENGER_15765"
      when vg_can_id.latest_can_id = 25 then "KLINE"
      when vg_can_id.latest_can_id = 26 then "CAN1_J1939_9PIN_PINS_C_D"
      when vg_can_id.latest_can_id = 27 then "CAN2_J1939_9PIN_PINS_H_J"
      when vg_can_id.latest_can_id = 28 then "CAN3_J1939_9PIN_PINS_F_G"
      when vg_can_id.latest_can_id = 29 then "J1708_9PIN_PINS_F_G"
      when vg_can_id.latest_can_id = 30 then "J1708_6PIN_PINS_A_B"
      when vg_can_id.latest_can_id = 31 then "CAN1_J1939_VM_PINS_3_11"
      when vg_can_id.latest_can_id = 32 then "CAN1_J1939_RP1226_PINS_2_9"
      when vg_can_id.latest_can_id = 33 then "CAN2_J1939_RP1226_PINS_4_11"
      when vg_can_id.latest_can_id = 34 then "J1708_RP1226_PINS_6_13"
      when vg_can_id.latest_can_id = 35 then "CAN1_J1939_IZU_PINS_6_15"
      when vg_can_id.latest_can_id = 36 then "CAN2_J1939_IZU_PINS_9_18"
      when vg_can_id.latest_can_id = 37 then "CAN1_J1939_FMS_PINS_6_9"
      when vg_can_id.latest_can_id = 38 then "CAN2_J1939_FMS_PINS_5_8"
      when vg_can_id.latest_can_id = 39 then "CAN1_J1939_HGV_PINS_A4_A8"
      when vg_can_id.latest_can_id = 40 then "CAN2_J1939_HGV_PINS_C5_C7"
      when vg_can_id.latest_can_id = 41 then "J1850"
      when vg_can_id.latest_can_id = 42 then "CAN1_J1939_MULTIPLE_CABLE_IDS"
      when vg_can_id.latest_can_id = 43 then "CAN1_J1939_TYPE_1_CABLES"
      when vg_can_id.latest_can_id = 44 then "J1708_J1587_MULTIPLE_CABLE_IDS"
      when vg_can_id.latest_can_id = 45 then "CAN1_J1939_250K_MANUAL_OVERRIDE"
      when vg_can_id.latest_can_id = 46 then "CAN1_J1939_500K_MANUAL_OVERRIDE"
      when vg_can_id.latest_can_id = 47 then "CAN1_J1939_PACCAR_PINS_F_G"
      else "other"
    end as can_name,


    vg_vin.last_vin_value as ecm_vin_value,
    vg_vin.first_vin_date as first_vin_date,
    vg_vin.last_vin_date as last_vin_date,
    vg_odometer.last_odometer_value as ecm_odometer_value,
    vg_engine_hours.last_engine_hour_value as ecm_engine_hour_value,
    vg_fuel_level.last_fuel_level as fuel_level_value,
    vg_fuel_consumption.last_fuel_cons_value as fuel_consumption_value,
    vg_fuel_consumption.first_fuel_cons_date as first_fuel_consumption_date,
    vg_fuel_consumption.last_fuel_cons_date as last_fuel_consumption_date,
    vg_seatbelt.last_seatbelt_value as seatbelt_value,
    vehicle_battery_voltage.latest_battery_voltage as battery_voltage_value,

    vg_odometer.first_odometer_date,
    vg_odometer.last_odometer_date,
    vg_engine_hours.first_engine_hour_date,
    vg_engine_hours.last_engine_hour_date,
    vg_seatbelt.last_belt_date,
    vg_fuel_level.last_fuel_level_date,

    vg_34_devices.manual_odometer_meters,
    vg_34_devices.manual_engine_hours,
    vg_34_devices.manual_odometer_updated_at,
    vg_34_devices.manual_engine_hours_updated_at,
    vg_34_devices.vin as vin_value,

    vg_heartbeats.last_reported_build,
    vg_heartbeats.first_heartbeat,
    vg_heartbeats.last_heartbeat,
    vg_can_connection_state.first_can_connection,
    vg_can_connection_state.last_can_connection,

    if(vg_vin.has_ecm_vin is null or vg_vin.has_ecm_vin = false, false, true) as has_ecm_vin,
    if(vg_odometer.has_ecm_odo is null or vg_odometer.has_ecm_odo = false, false, true) as has_ecm_odo,
    if(vg_engine_hours.has_ecm_enginehours is null or vg_engine_hours.has_ecm_enginehours = false, false, true) as has_ecm_enginehours,

    if(vg_can_connection_state.last_can_connection is null, false, true) as can_connected,
    if(vg_vin.last_vin_value is null, false, true) as ecm_vin_detected,
    if(vg_odometer.last_odometer_value is null, false, true) as ecm_odometer_detected,
    if(vg_engine_hours.last_engine_hour_value is null, false, true) as ecm_engine_hours_detected,
    if(vg_fuel_level.last_fuel_level is null, false, true) as fuel_level_detected,
    if(vg_fuel_consumption.last_fuel_cons_value is null, false, true) as fuel_consumption_detected,
    if(vg_seatbelt.last_seatbelt_value is null, false, true) as seatbelt_detected,
    1 as total,
    case when vg_34_devices.internal_type = 0 then "Customer" else "Internal" end as org_type,

    case
      when length(org_id) <= 10
      then concat("https://cloud.samsara.com/o/", cast(vg_34_devices.org_id as string), "/devices/", cast(vg_34_devices.object_id as string), "/vehicle")
      else concat("https://cloud.eu.samsara.com/o/", cast(vg_34_devices.org_id as string), "/devices/", cast(vg_34_devices.object_id as string), "/vehicle")
    end as link,

    vg_trip.first_trip_date,
    vg_trip.last_trip_date,
    lower(vg_34_devices.config_override_json) as config_override_json

  from
    vg_34_devices
    left join vg_heartbeats on vg_34_devices.object_id = vg_heartbeats.object_id
    left join vg_cable_id on vg_34_devices.object_id = vg_cable_id.object_id
    left join vg_can_id on vg_34_devices.object_id = vg_can_id.object_id
    left join vg_vin on vg_34_devices.object_id = vg_vin.object_id
    left join vg_odometer on vg_34_devices.object_id = vg_odometer.object_id
    left join vg_engine_hours on vg_34_devices.object_id = vg_engine_hours.object_id
    left join vg_fuel_level on vg_34_devices.object_id = vg_fuel_level.object_id
    left join vg_fuel_consumption on vg_34_devices.object_id = vg_fuel_consumption.object_id
    left join vg_seatbelt on vg_34_devices.object_id = vg_seatbelt.object_id
    left join vehicle_battery_voltage on vg_34_devices.object_id = vehicle_battery_voltage.object_id
    left join vg_can_connection_state on vg_34_devices.object_id = vg_can_connection_state.object_id
    left join vg_trip on vg_34_devices.object_id = vg_trip.device_id
);

-- COMMAND ----------

-- DBTITLE 1,create table
create table if not exists dataprep_telematics.vg_fleet_diagnostics_latest(
  org_name string,
  org_id bigint,
  object_id bigint,
  product_id bigint,
  variant_id bigint,
  serial string,
  device_name string,
  make string,
  model string,
  year bigint,
  engine_model string,
  primary_fuel_type string,
  engine_manufacturer string,
  has_ecm_vin boolean,
  has_ecm_odo boolean,
  has_ecm_enginehours boolean,

  cable_id bigint,
  can_id bigint,

  cable_name string,
  can_name string,

  ecm_vin_value string,
  first_vin_date string,
  last_vin_date string,
  ecm_odometer_value bigint,
  ecm_engine_hour_value bigint,
  fuel_level_value bigint,
  fuel_consumption_value bigint,
  first_fuel_consumption_date string,
  last_fuel_consumption_date string,
  seatbelt_value bigint,
  battery_voltage_value bigint,

  first_odometer_date string,
  last_odometer_date string,
  first_engine_hour_date string,
  last_engine_hour_date string,
  last_belt_date string,
  last_fuel_level_date string,

  manual_odometer_meters bigint,
  manual_engine_hours bigint,
  manual_odometer_updated_at timestamp,
  manual_engine_hours_updated_at timestamp,
  vin_value string,

  first_heartbeat string,
  last_heartbeat string,
  last_reported_build string,
  first_can_connection string,
  last_can_connection string,

  can_connected boolean,
  ecm_vin_detected boolean,
  ecm_odometer_detected boolean,
  ecm_engine_hours_detected boolean,
  fuel_level_detected boolean,
  fuel_consumption_detected boolean,
  seatbelt_detected boolean,
  total bigint,
  org_type string,
  link string,
  first_trip_date string,
  last_trip_date string,
  config_override_json string
) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Compare incremental data w existing dataset
-- final dataset prior to the merge
-- retain non-null data for most fields unless incremental data is non-null
-- choose earliest min dates and latest last dates
-- for boolean fields, retain trues (ever had a value)
create or replace temporary view full_fleet_diagnostics as
(
select
    ifnull(today.org_name,yest.org_name) as org_name,
    ifnull(today.org_id,yest.org_id) as org_id,
    today.object_id as object_id,
    ifnull(today.product_id,yest.product_id) as product_id,
    ifnull(today.variant_id,yest.variant_id) as variant_id,
    ifnull(today.serial,yest.serial) as serial,
    ifnull(today.device_name,yest.device_name) as device_name,
    ifnull(today.make,yest.make) as make,
    ifnull(today.model,yest.model) as model,
    ifnull(today.year,yest.year) as year,
    ifnull(today.engine_model,yest.engine_model) as engine_model,
    ifnull(today.primary_fuel_type,yest.primary_fuel_type) as primary_fuel_type,
    ifnull(today.engine_manufacturer,yest.engine_manufacturer) as engine_manufacturer,
    today.has_ecm_vin or yest.has_ecm_vin as has_ecm_vin,
    today.has_ecm_odo or yest.has_ecm_odo as has_ecm_odo,
    today.has_ecm_enginehours or yest.has_ecm_enginehours as has_ecm_enginehours,

    ifnull(today.cable_id,yest.cable_id) as cable_id,
    ifnull(today.can_id,yest.can_id) as can_id,

    ifnull(today.cable_name,yest.cable_name) as cable_name,
    ifnull(today.can_name,yest.can_name) as can_name,

    ifnull(today.ecm_vin_value,yest.ecm_vin_value) as ecm_vin_value,

    -- for min/first timestamps, choose earliest dates
    case when ifnull(yest.first_vin_date,'9999-12-31') < ifnull(today.first_vin_date,'9999-12-31') then yest.first_vin_date else today.first_vin_date end as first_vin_date,

    -- invert the logic for latest/last timestamps
    case when ifnull(yest.last_vin_date,'0101-01-01') > ifnull(today.last_vin_date,'0101-01-01') then yest.last_vin_date else today.last_vin_date end as last_vin_date,
    ifnull(today.ecm_odometer_value,yest.ecm_odometer_value) as ecm_odometer_value,
    ifnull(today.ecm_engine_hour_value,yest.ecm_engine_hour_value) as ecm_engine_hour_value,
    ifnull(today.fuel_level_value,yest.fuel_level_value) as fuel_level_value,
    ifnull(today.fuel_consumption_value,yest.fuel_consumption_value) as fuel_consumption_value,
    case when ifnull(yest.first_fuel_consumption_date,'9999-12-31') < ifnull(today.first_fuel_consumption_date,'9999-12-31') then yest.first_fuel_consumption_date else today.first_fuel_consumption_date end as first_fuel_consumption_date,
    case when ifnull(yest.last_fuel_consumption_date,'0101-01-01') > ifnull(today.last_fuel_consumption_date,'0101-01-01') then yest.last_fuel_consumption_date else today.last_fuel_consumption_date end as last_fuel_consumption_date,
    ifnull(today.seatbelt_value,yest.seatbelt_value) as seatbelt_value,
    ifnull(today.battery_voltage_value,yest.battery_voltage_value) as battery_voltage_value,

    case when ifnull(yest.first_odometer_date,'9999-12-31') < ifnull(today.first_odometer_date,'9999-12-31') then yest.first_odometer_date else today.first_odometer_date end as first_odometer_date,
    case when ifnull(yest.last_odometer_date,'0101-01-01') > ifnull(today.last_odometer_date,'0101-01-01') then yest.last_odometer_date else today.last_odometer_date end as last_odometer_date,
    case when ifnull(yest.first_engine_hour_date,'9999-12-31') < ifnull(today.first_engine_hour_date,'9999-12-31') then yest.first_engine_hour_date else today.first_engine_hour_date end as first_engine_hour_date,
    case when ifnull(yest.last_engine_hour_date,'0101-01-01') > ifnull(today.last_engine_hour_date,'0101-01-01') then yest.last_engine_hour_date else today.last_engine_hour_date end as last_engine_hour_date,
    case when ifnull(yest.last_belt_date,'0101-01-01') > ifnull(today.last_belt_date,'0101-01-01') then yest.last_belt_date else today.last_belt_date end as last_belt_date,
    case when ifnull(yest.last_fuel_level_date,'0101-01-01') > ifnull(today.last_fuel_level_date,'0101-01-01') then yest.last_fuel_level_date else today.last_fuel_level_date end as last_fuel_level_date,

    ifnull(today.manual_odometer_meters,yest.manual_odometer_meters) as manual_odometer_meters,
    ifnull(today.manual_engine_hours,yest.manual_engine_hours) as manual_engine_hours,
    case when ifnull(yest.manual_odometer_updated_at,'0101-01-01') > ifnull(today.manual_odometer_updated_at,'0101-01-01') then yest.manual_odometer_updated_at else today.manual_odometer_updated_at end as manual_odometer_updated_at,
    case when ifnull(yest.manual_engine_hours_updated_at,'0101-01-01') > ifnull(today.manual_engine_hours_updated_at,'0101-01-01') then yest.manual_engine_hours_updated_at else today.manual_engine_hours_updated_at end as manual_engine_hours_updated_at,
    ifnull(today.vin_value,yest.vin_value) as vin_value,

    case when ifnull(yest.first_heartbeat,'9999-12-31') < ifnull(today.first_heartbeat,'9999-12-31') then yest.first_heartbeat else today.first_heartbeat end as first_heartbeat,
    case when ifnull(yest.last_heartbeat,'0101-01-01') > ifnull(today.last_heartbeat,'0101-01-01') then yest.last_heartbeat else today.last_heartbeat end as last_heartbeat,
    ifnull(today.last_reported_build,yest.last_reported_build) as last_reported_build,
    case when ifnull(yest.first_can_connection,'9999-12-31') < ifnull(today.first_can_connection,'9999-12-31') then yest.first_can_connection else today.first_can_connection end as first_can_connection,
    case when ifnull(yest.last_can_connection,'0101-01-01') > ifnull(today.last_can_connection,'0101-01-01') then yest.last_can_connection else today.last_can_connection end as last_can_connection,

    today.can_connected or yest.can_connected as can_connected,
    today.ecm_vin_detected or yest.ecm_vin_detected as ecm_vin_detected,
    today.ecm_odometer_detected or yest.ecm_odometer_detected as ecm_odometer_detected,
    today.ecm_engine_hours_detected or yest.ecm_engine_hours_detected as ecm_engine_hours_detected,
    today.fuel_level_detected or yest.fuel_level_detected as fuel_level_detected,
    today.fuel_consumption_detected or yest.fuel_consumption_detected as fuel_consumption_detected,
    today.seatbelt_detected or yest.seatbelt_detected as seatbelt_detected,
    ifnull(today.total,yest.total) as total,
    ifnull(today.org_type,yest.org_type) as org_type,
    ifnull(today.link,yest.link) as link,
    case when ifnull(yest.first_trip_date,'9999-12-31') < ifnull(today.first_trip_date,'9999-12-31') then yest.first_trip_date else today.first_trip_date end as first_trip_date,
    case when ifnull(yest.last_trip_date,'0101-01-01') > ifnull(today.last_trip_date,'0101-01-01') then yest.last_trip_date else today.last_trip_date end as last_trip_date,
    today.config_override_json as config_override_json

from
  updated_fleet_diagnostics today
left join dataprep_telematics.vg_fleet_diagnostics_latest yest on today.object_id = yest.object_id
);

-- COMMAND ----------

-- DBTITLE 1,Overwrite final dataset
INSERT OVERWRITE TABLE dataprep_telematics.vg_fleet_diagnostics_latest (
  SELECT *
  FROM full_fleet_diagnostics
);
