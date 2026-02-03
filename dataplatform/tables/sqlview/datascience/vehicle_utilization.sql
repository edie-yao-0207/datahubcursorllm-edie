with trips_start_end_same_day as (
  select
    trips.date,
    trips.org_id,
    trips.device_id,
    hour(timestamp(proto.start.time / 1000)) as start_hour,
    hour(timestamp(proto.end.time / 1000)
) as end_hour
from
  trips2db_shards.trips trips
  left join productsdb.gateways gw on gw.org_id = trips.org_id
  and gw.device_id = trips.device_id
where
  date >= add_months(current_date(), -12)
  and (proto.end.time - proto.start.time) <(24 * 60 * 60 * 1000)
  and proto.start.time != proto.end.time
  and day(timestamp(proto.start.time / 1000)) = day(timestamp(proto.end.time / 1000))
  and gw.product_id in (7, 24, 17, 35)
  and trips.version = 101
),
trips_start_end_diff_day as (
  select -- Get all trips that span a day boundary and truncate the end to the 23rd hour
    date,
    org_id,
    device_id,
    hour(timestamp(proto.start.time / 1000)) as start_hour,
    23 as end_hour
  from
    trips2db_shards.trips trips
  where
    date >= add_months(current_date(), -12)
    and (proto.end.time - proto.start.time) <(24 * 60 * 60 * 1000)
    and proto.start.time != proto.end.time
    and day(timestamp(proto.start.time / 1000)) != day(timestamp(proto.end.time / 1000))
    and trips.version = 101
union all
  select -- Get all trips that span a day boundary and set the start to be the beginning of the day that the trip ends on
    date_add(date, 1) as date,
    org_id,
    device_id,
    0 as start_hour,
    hour(timestamp(proto.end.time / 1000)) as end_hour
  from
    trips2db_shards.trips trips
  where
    date >= add_months(current_date(), -12)
    and (proto.end.time - proto.start.time) <(24 * 60 * 60 * 1000)
    and proto.start.time != proto.end.time
    and day(timestamp(proto.start.time / 1000)) != day(timestamp(proto.end.time / 1000))
    and trips.version = 101
),
trips_start_end_hours as (
  select
    *
  from
    trips_start_end_same_day
  union all
  select
    *
  from
    trips_start_end_diff_day
),
trip_hours as (
  select
    date,
    org_id,
    device_id,
    explode(sequence(start_hour, end_hour)) as hour
  from
    trips_start_end_hours
),
daily_utilization_by_device as (
  select
    date,
    org_id,
    device_id,
    -- We use the count of hours instead of the total sum of each trip duration
    -- because that's how the existing vehicle utilization report works.
    count(distinct hour) as driving_hours,
    24 as total_hours
  from
    trip_hours
  group by
    date,
    org_id,
    device_id
),
driving_hours_by_org_by_day as (
  select
    date,
    org_id,
    sum(driving_hours) as driving_hours,
    -- We use sum of total hours per device instead of 24 here because if there
    -- were 5 devices driving in one day then the total available hours for
    -- driving is 5 * 24.
    sum(total_hours) as total_hours
  from
    daily_utilization_by_device
  group by
    date,
    org_id
)
select
  o.id as org_id,
  c.date,
  coalesce(driving_hours, 0) as driving_hours,
  coalesce(total_hours, 24) as total_hours
from
  clouddb.organizations as o
  join definitions.445_calendar as c
  left join driving_hours_by_org_by_day as m on m.org_id = o.id
  and m.date = c.date
where
  c.date between add_months(current_date(), -12)
  and current_date()
