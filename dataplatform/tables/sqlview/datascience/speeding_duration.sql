with speeding_durations_by_org_by_day as (
  SELECT
    org_id,
    date,
    COALESCE(SUM(proto.trip_speeding_mph.light_speeding_ms), 0) AS light_speeding_ms,
    COALESCE(SUM(proto.trip_speeding_mph.moderate_speeding_ms), 0) AS moderate_speeding_ms,
    COALESCE(SUM(proto.trip_speeding_mph.heavy_speeding_ms), 0) AS heavy_speeding_ms,
    COALESCE(SUM(proto.trip_speeding_mph.severe_speeding_ms), 0) AS severe_speeding_ms,
    COALESCE(SUM(proto.trip_speeding_mph.light_speeding_ms), 0) +
    COALESCE(SUM(proto.trip_speeding_mph.moderate_speeding_ms), 0) +
    COALESCE(SUM(proto.trip_speeding_mph.heavy_speeding_ms), 0)  +
    COALESCE(SUM(proto.trip_speeding_mph.severe_speeding_ms), 0) as all_speeding_ms,
    COALESCE(SUM(proto.end.time - proto.start.time),0) AS total_trip_duration_ms
  FROM
    trips2db_shards.trips as a
  WHERE
    a.date >= add_months(current_date(), -12)
    AND a.version = 101
  GROUP BY
    org_id,
    date
)
select
  o.id as org_id,
  c.date,
  coalesce(light_speeding_ms, 0) as light_speeding_ms,
  coalesce(moderate_speeding_ms, 0) as moderate_speeding_ms,
  coalesce(heavy_speeding_ms, 0) as heavy_speeding_ms,
  coalesce(severe_speeding_ms, 0) as severe_speeding_ms,
  coalesce(all_speeding_ms, 0) as all_speeding_ms,
  coalesce(total_trip_duration_ms, 0) as total_trip_duration_ms
from
  clouddb.organizations as o
  join definitions.445_calendar as c
  left join speeding_durations_by_org_by_day as m on m.org_id = o.id
  and m.date = c.date
where
  c.date between add_months(current_date(), -12)
  and current_date()
