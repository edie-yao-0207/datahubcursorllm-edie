-- Aggegate driving pattern data for past 2 months
-- Total trip distance and number of trips per org as proxies for driving pattern
select
  org_id,
  sum(proto.trip_distance.distance_meters) * 0.000621371 as total_trip_distance_miles,
  count(trips.device_id) as num_trips
from
  trips2db_shards.trips trips
where
  date >= add_months(current_date(), -2)
  and (proto.end.time - proto.start.time
) <(24 * 60 * 60 * 1000)
/*Filter out very long trips*/
and proto.start.time != proto.
end.time
/* legit trips only */
and trips.version = 101
group by
  org_id
