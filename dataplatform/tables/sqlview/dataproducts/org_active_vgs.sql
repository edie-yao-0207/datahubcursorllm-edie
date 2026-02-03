-- Get org # active devices (VGs) past 2 month
-- Used for calculating fleet size
select
  ad.org_id,
  count(
    distinct case
      when trip_count is not null
      and trip_count <> 0 then ad.device_id
    end
  ) as unique_active_all_vehicles,
  count(
    distinct case
      when trip_count is not null
      and trip_count <> 0
      and vc.cable_id = 4 then ad.device_id
    end
  ) as unique_active_passenger_vehicles
from
  dataprep.active_devices as ad
  left join productsdb.gateways as gw on gw.org_id = ad.org_id
  and gw.device_id = ad.device_id
  left join clouddb.organizations as o on o.id = ad.org_id
  left join dataproducts.vehicle_cable_data as vc on vc.org_id = ad.org_id
  and vc.device_id = ad.device_id
where
  o.internal_type != 1
  and o.quarantine_enabled != 1
  and gw.product_id in (7, 24, 17, 35)
  and ad.date >= add_months(current_date(), -2)
group by
  ad.org_id
