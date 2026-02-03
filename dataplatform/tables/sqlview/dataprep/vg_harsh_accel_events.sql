select
  a.org_id,
  a.date,
  a.time,
  double( 1.15078 * a.value.proto_value.accelerometer_event.last_gps.speed / 1000 ) as speed_mph,
  a.value.proto_value.accelerometer_event.last_gps.latitude,
  a.value.proto_value.accelerometer_event.last_gps.longitude,
  a.value.proto_value.accelerometer_event.max_accel_gs,
  a.value.proto_value.accelerometer_event.event_duration_ms
from
  kinesisstats.osdAccelerometer as a
  left join clouddb.organizations as b on a.org_id = b.id
  left join productsdb.gateways as c on a.org_id = c.org_id
  and a.object_id = c.device_id
where
  value.proto_value.accelerometer_event.harsh_accel_type = 1
  and value.proto_value.accelerometer_event.last_gps.speed >= 4344.8812095
  and value.is_databreak = 'false'
  and value.is_end = 'false'
  and a.date between add_months(current_date(), -12)
  and current_date()
  and b.internal_type != 1
  and b.quarantine_enabled != 1
  and c.product_id in (7, 24, 17, 35)
