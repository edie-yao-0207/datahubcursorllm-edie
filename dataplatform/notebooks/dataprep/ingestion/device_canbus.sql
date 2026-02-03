create or replace temp view device_canbus as
(
  select
    date,
    object_id as device_id,
    org_id,
    max((time,value.int_value as can_bus_type)).can_bus_type as can_bus_type
  from kinesisstats.osdcanbustype
  where
    value.is_databreak = false and
    value.is_end = false and
    value.int_value is not null
  group by
    date,
    object_id,
    org_id
);

create table if not exists dataprep.device_canbus using delta partitioned by (date) as (
  select *
  from device_canbus
);

create or replace temp view device_canbus_updates as (
  select *
  from device_canbus
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep.device_canbus as target
using device_canbus_updates as updates
on target.date = updates.date
and target.device_id = updates.device_id
and target.org_id = updates.org_id
when matched then update set *
when not matched then insert *;
