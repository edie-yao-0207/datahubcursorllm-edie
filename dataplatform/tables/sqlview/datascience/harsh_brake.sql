with harsh_brakes_by_org_by_day as (
  select
    s.org_id,
    s.date,
    count(*) as total_harsh_brake
  from
    dataprep.vg_safety_events as s
  where
    accel_type = 2
  group by
    s.org_id,
    s.date
)
select
  o.id as org_id,
  c.date,
  coalesce(total_harsh_brake, 0) as total_harsh_brake
from
  clouddb.organizations as o
  join definitions.445_calendar as c
  left join harsh_brakes_by_org_by_day as m on m.org_id = o.id
  and m.date = c.date
where
  c.date between add_months(current_date(), -12)
  and current_date()
