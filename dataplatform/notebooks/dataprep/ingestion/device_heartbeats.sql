-- Databricks notebook source
create or replace temporary view device_heartbeats as
(
  select
    CAST(DATE(FROM_UNIXTIME(value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms/1000, 'yyyy-MM-dd HH:mm:ss')) AS STRING) AS date_last_heartbeat
    -- The field `time` should not be used because it actually represents 
    -- the device time the osdhubserverdeviceheartbeat signal was sent.
    -- Instead we should use the `last_heartbeat_at_ms` field, which represents 
    -- the last time the device connected to the backend.
    -- Not only do these materially differ, but the `last_heartbeat_at_ms` is retroactively updated
    -- by the ingestion process to be the actual latest heartbeat.
    , value.proto_value.hub_server_device_heartbeat.last_heartbeat_at_ms
    , value
    , org_id
    , object_id as device_id
 from kinesisstats.osdhubserverdeviceheartbeat 
 where value.is_databreak = 'false' and value.is_end = 'false'
);

-- COMMAND ----------

-- Create initial state of table, if it does not exists.
create table if not exists dataprep.device_heartbeats using delta as
(
  select
    org_id,
    device_id,
    min(date_last_heartbeat) as first_heartbeat_date,
    min(last_heartbeat_at_ms) as first_heartbeat_ms,
    max(date_last_heartbeat) as last_heartbeat_date,
    max(last_heartbeat_at_ms) as last_heartbeat_ms,
    max((last_heartbeat_at_ms, value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count)).boot_count as last_reported_bootcount,
    max((last_heartbeat_at_ms, value.proto_value.hub_server_device_heartbeat.connection.device_hello.build)).build as last_reported_build,
    max((last_heartbeat_at_ms, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id)).gateway_id as last_reported_gateway_id
  from
    device_heartbeats
  group by
    org_id,
    device_id
);

-- COMMAND ----------

-- Incremental updates
create or replace temporary view device_heartbeats_updates as
(
  select
    org_id,
    device_id,
    min(date_last_heartbeat) as first_heartbeat_date,
    min(last_heartbeat_at_ms) as first_heartbeat_ms,
    max(date_last_heartbeat) as last_heartbeat_date,
    max(last_heartbeat_at_ms) as last_heartbeat_ms,
    max((last_heartbeat_at_ms, value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count)).boot_count as last_reported_bootcount,
    max((last_heartbeat_at_ms, value.proto_value.hub_server_device_heartbeat.connection.device_hello.build)).build as last_reported_build,
    max((last_heartbeat_at_ms, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id)).gateway_id as last_reported_gateway_id
  from
    device_heartbeats
  where 
    date_last_heartbeat between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
  group by
    org_id,
    device_id
);

-- COMMAND ----------

--Merge new updates
merge into dataprep.device_heartbeats as target
using device_heartbeats_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
when matched then update set
  last_heartbeat_date = updates.last_heartbeat_date,
  last_heartbeat_ms = updates.last_heartbeat_ms,
  last_reported_bootcount = updates.last_reported_bootcount,
  last_reported_build = updates.last_reported_build,
  last_reported_gateway_id = updates.last_reported_gateway_id
when not matched then insert * ;
