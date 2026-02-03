-- Fleet composition data to get num passenger vehicles per org
with vehicle_cable_data_pre as (
  select
    ad.org_id,
    ad.device_id,
    max((c.time, c.value.int_value)).int_value as cable_id
  from
    dataprep.active_devices as ad
    left join kinesisstats.osdobdcableid as c on c.org_id = ad.org_id
    and c.object_id = ad.device_id
    and c.date = ad.date
    and c.value.is_databreak = 'false'
    and c.value.is_end = 'false'
    and c.date >= add_months(current_date(), -2)
  where
    ad.date >= add_months(current_date(), -2)
  group by
    ad.org_id,
    ad.device_id
),
non_passenger_devices as (
  select
    id
  from
    productsdb.devices
  where
    LOWER(make) like '%isuzu%'
    or LOWER(make) like '%hino%'
    or LOWER(make) like '%byd%'
) -- join with non_passenger_devices to alter cable_id value to non passenger identifier
select
  org_id,
  device_id,
  IF(np.id is null, cable_id, -1) as cable_id
from
  vehicle_cable_data_pre as cd
  left join non_passenger_devices as np on cd.device_id = np.id
