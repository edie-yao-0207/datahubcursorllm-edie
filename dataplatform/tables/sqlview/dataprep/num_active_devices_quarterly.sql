select
  cal.fiscal_year,
  cal.quarter,
  product_id,
  count(distinct case when active_heartbeat = true then ad.device_id end) as total_devices_heartbeats,
  count(distinct case when trip_count is not null and trip_count <> 0 then ad.device_id end) as total_devices_trips
from dataprep.active_devices ad
left join clouddb.organizations o on
  o.id = ad.org_id
left join definitions.445_calendar cal on
  cal.date = ad.date
where o.internal_type <> 1
group by
  cal.fiscal_year,
  cal.quarter,
  product_id
