create or replace temp view device_cables as
(
  select
    date,
    object_id as device_id,
    org_id,
    max((time,value.int_value as cable_type)).cable_type as cable_type
  from kinesisstats.osdobdcableid
  where
    value.is_databreak = false and
    value.is_end = false and
    value.int_value is not null
  group by
    date,
    object_id,
    org_id
);

create table if not exists dataprep.device_cables using delta partitioned by (date) as (
  select *
  from device_cables
);

create or replace temp view device_cables_updates as (
  select *
  from device_cables
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep.device_cables as target
using device_cables_updates as updates
on target.date = updates.date
and target.device_id = updates.device_id
and target.org_id = updates.org_id
when matched then update set *
when not matched then insert *;
