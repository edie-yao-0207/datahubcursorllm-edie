with missing_dates as (
  SELECT
    org_id,
    date,
    double(SUM(proto.trip_distance.distance_meters) * 0.000621371) AS total_distance_mi
  FROM
    trips2db_shards.trips as t
  WHERE
    date between add_months(current_date(), -12)
    and current_date()
    and t.version = 101
  GROUP BY
    org_id,
    date
)
select
  o.id as org_id,
  c.date,
  coalesce(total_distance_mi, 0) as total_distance_mi
from
  clouddb.organizations as o
  join definitions.445_calendar as c
  left join missing_dates as m on m.org_id = o.id
  and m.date = c.date
where
  c.date between add_months(current_date(), -12)
  and current_date()
