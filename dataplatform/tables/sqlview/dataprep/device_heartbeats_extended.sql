SELECT
  hb.org_id,
  hb.device_id,
  hb.first_heartbeat_date,
  hb.first_heartbeat_ms,
  hb.last_heartbeat_date,
  hb.last_heartbeat_ms,
  hb.last_reported_bootcount,
  hb.last_reported_build,
  hb.last_reported_gateway_id,
  gw.serial,
  org.name as org_name,
  gw.product_id,
  case when org.internal_type = 1 or org.id = 0 then True else False end as internal_org
FROM dataprep.device_heartbeats hb
LEFT JOIN productsdb.gateways gw ON gw.id = hb.last_reported_gateway_id and gw.org_id = hb.org_id and hb.device_id = gw.device_id
LEFT JOIN clouddb.organizations org ON org.id = hb.org_id
