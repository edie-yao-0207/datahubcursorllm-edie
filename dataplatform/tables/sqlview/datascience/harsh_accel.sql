with harsh_accels_by_org_by_day as (
  select
    a.org_id,
    a.date as date,
    count(*) as total_harsh_accel
  from
    dataprep.vg_harsh_accel_events as a
  group by
    a.org_id,
    a.date
)
select
  o.id as org_id,
  c.date,
  coalesce(total_harsh_accel, 0) as total_harsh_accel
from
  clouddb.organizations as o
  join definitions.445_calendar as c
  left join harsh_accels_by_org_by_day as m on m.org_id = o.id
  and m.date = c.date
where
  c.date between add_months(current_date(), -12)
  and current_date()
