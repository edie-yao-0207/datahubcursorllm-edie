create or replace temp view device_builds as
(
  select
    date,
    org_id,
    object_id as device_id,
    max((time, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id as gateway_id)).gateway_id as latest_gateway_id,
    count(distinct(value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id)) as active_gateways_on_day,
    max((time, value.proto_value.hub_server_device_heartbeat.connection.device_hello.build as build)).build as latest_build_on_day,
    count(distinct(value.proto_value.hub_server_device_heartbeat.connection.device_hello.build)) as active_builds_on_day,
    max((time, value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count as bootcount)).bootcount - min((time, value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count as bootcount)).bootcount as boot_counts_on_day
  from kinesisstats.osdhubserverdeviceheartbeat as hb
  where
    value.is_databreak = 'false'
    and value.is_end = 'false'
    and date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
  group by
    date,
    org_id,
    device_id
);

create table if not exists dataprep.device_builds using delta partitioned by (date) as (
  select *
  from device_builds
);

create or replace temp view device_builds_updates as (
  select *
  from device_builds
);

merge into dataprep.device_builds as target
using device_builds_updates as updates
on target.date = updates.date
and target.device_id = updates.device_id
and target.org_id = updates.org_id
when matched then update set *
when not matched then insert *;
