  with device_trips as (
    select
      org_id,
      COALESCE(trips.proto.trip_distance.distance_meters, 0) * 0.000621371 as distance_miles,
      case
        when cities.name is not null then 1
        else 0
      end as start_end_in_city -- if the started or ended in a top 40 city
    from
      trips2db_shards.trips trips
      left join dataproducts.cities as cities on upper(trips.proto.start.place.city) = cities.name
      or upper(trips.proto.end.place.city) = cities.name
    where
      trips.date >= add_months(current_date(), -2)
      and (trips.proto.end.time - trips.proto.start.time) <= 24 * 60 * 60 * 1000 -- filter out long trips
      and trips.version = 101
)
select
  org_id,
  count(1) as total_num_trips,
  sum(start_end_in_city) as num_trips_start_end_in_city,
  sum(start_end_in_city) / count(1) * 100 as percent_trips_start_end_city
from
  device_trips
group by
  org_id
