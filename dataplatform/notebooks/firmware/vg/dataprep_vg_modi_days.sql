-- Databricks notebook source
-- Construct the date intervals for each day a device has has a heartbeat
create or replace temp view device_date_time as (
  with days as (
    select db.date,
      db.org_id,
      db.device_id,
      lead(db.date) over (partition by db.org_id, db.device_id order by db.date) as next_date
    from dataprep.device_builds as db
    left join clouddb.gateways gw on db.latest_gateway_id = gw.id and db.org_id = gw.org_id
    where gw.product_id in (24, 35, 53, 89, 178)
    )
    
    select
      date,
      org_id,
      device_id,
      to_unix_timestamp(date,'yyyy-MM-dd')*1000 as current_time,
      to_unix_timestamp(next_date,'yyyy-MM-dd')*1000 as next_time
    from days
)

-- COMMAND ----------

-- Attached intervals are constructed using the osdattachedusbdevices objectStat, 
-- we can't trust that the USB hub is always functioning properly. So we'll assume that if the modi was attached 
-- to the VG for any time at all on a given day then it is supposed to be attached for the entire day. We wouldn't
-- expect drivers to physically detached the device like they may a camera..
create or replace temp view attached_to_modi as (
  select a.date,
    a.org_id,
    a.device_id,
    case 
      when 
      sum
      (
        case 
            when b.start_ms <= a.current_time and b.end_ms >= a.next_time then a.next_time - a.current_time
            when b.start_ms >= a.current_time and b.end_ms <= a.next_time then b.end_ms - b.start_ms
            when b.start_ms <= a.current_time and b.end_ms <= a.next_time then b.end_ms - a.current_time
            when b.start_ms >= a.current_time and b.end_ms >= a.next_time then a.next_time - b.start_ms
            else 0
        end
      ) > 0 then true
      else false
    end as has_modi
  from device_date_time as a
  left join data_analytics.vg_modi_attached_ranges as b on 
    a.org_id = b.org_id and
    a.device_id = b.device_id and
    not (b.start_ms >= a.next_time or b.end_ms <= a.current_time)
  where attached_to_modi = 1
  group by 
    a.date,
    a.device_id,
    a.org_id
  )

-- COMMAND ----------

create table if not exists data_analytics.dataprep_vg_modi_days
using delta
partitioned by (date)
as
select * from attached_to_modi

-- COMMAND ----------

create or replace temp view vg_modi_days_updates as (
  select *
  from attached_to_modi
  where date >= date_sub(current_date(),5)
);


merge into data_analytics.dataprep_vg_modi_days as target
using vg_modi_days_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;
