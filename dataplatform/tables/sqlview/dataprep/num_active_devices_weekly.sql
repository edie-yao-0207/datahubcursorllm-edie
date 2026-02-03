select
  date_sub(date,dayofweek(date)+1) as week,
  product_id,
  sum(cast(active_heartbeat AS bigint)) as total_devices_heartbeats,
  sum(cast(trip_count > 0 AS bigint)) as total_devices_trips
from dataprep.active_devices ad
left join clouddb.organizations o on
  o.id = ad.org_id
where o.internal_type <> 1
group by
  date_sub(date,dayofweek(date)+1),
  product_id
