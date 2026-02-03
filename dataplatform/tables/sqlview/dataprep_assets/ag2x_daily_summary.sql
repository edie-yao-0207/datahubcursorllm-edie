select
  db.date,
  db.org_id,
  o.internal_type as org_type,
  cm.name as org_name,
  db.device_id,
  gw.product_id,
  gw.serial as ag_serial_clean,
  db.active_builds_on_day,
  db.latest_build_on_day,
  ad.trip_count,
  CAST(ad.total_distance AS BIGINT) AS total_distance,
  ca.can_bus_type,
  cb.cable_type
from dataprep.device_builds db
left join dataprep.device_cables cb on db.device_id = cb.device_id and db.org_id = cb.org_id and db.date = cb.date
left join dataprep.device_canbus ca on db.device_id = ca.device_id and db.org_id = ca.org_id and db.date = ca.date
left join dataprep.active_devices ad on db.device_id = ad.device_id and db.org_id = ad.org_id and db.date = ad.date
left join productsdb.gateways gw on db.latest_gateway_id = gw.id and db.org_id = gw.org_id
left join clouddb.organizations o on db.org_id = o.id
left join dataprep.customer_metadata cm on db.org_id = cm.org_id
where gw.product_id in (27,36,68,83,84,85)