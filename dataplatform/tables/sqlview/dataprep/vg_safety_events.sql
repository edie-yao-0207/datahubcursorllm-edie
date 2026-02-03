select
  s.org_id,
  s.date,
  s.created_at,
  s.device_id,
  s.event_ms,
  double(1.15078 * detail_proto.start.speed_milliknots / 1000) as speed_mph,
  detail_proto.start.latitude,
  detail_proto.start.longitude,
  detail_proto.accel_type
from
  safetydb_shards.safety_events as s
  left join clouddb.organizations as o on s.org_id = o.id
  left join productsdb.gateways as g on g.org_id = s.org_id
  and g.device_id = s.device_id
where
  detail_proto.start.speed_milliknots >= 4344.8812095
  and detail_proto.stop.speed_milliknots >= 4344.8812095
  and s.date between add_months(current_date(), -12)
  and current_date()
  and o.internal_type != 1
  and o.quarantine_enabled != 1
  and g.product_id in (7, 24, 17, 35)
