WITH devices AS
(
  SELECT
    g.organization_id AS org_id,
    d.id AS device_id,
    d.serial,
    d.product_id,
    CASE WHEN vg.id is not null THEN vg.id ELSE d.id END AS vg_device_id
  FROM productsdb.devices AS d
  LEFT JOIN clouddb.groups AS g
    ON
    d.group_id = g.id
  LEFT JOIN productsdb.devices AS vg
    ON
    upper(replace(vg.camera_serial,"-","")) = upper(replace(d.serial,"-",""))
),

all_devices AS
(
  SELECT *
  FROM devices

  UNION ALL

  SELECT
    g.organization_id AS org_id,
    cast(concat(string(d.camera_product_id),string(d.id)) AS long) AS device_id,
    d.camera_serial AS serial,
    d.camera_product_id AS product_id,
    d.id AS vg_device_id
  FROM productsdb.devices AS d
  LEFT JOIN clouddb.groups AS g ON d.group_id = g.id
  WHERE d.camera_product_id IN (30,31)
),

trips AS
(
  SELECT
    org_id,
    device_id,
    date AS trip_date,
    count(1) AS trip_count,
    sum(proto.trip_distance.distance_meters) AS total_distance
  FROM trips2db_shards.trips trips
  WHERE
    date >= DATE_SUB(${end_date}, 183)  -- we restate last 6 months to capture late arriving data from offline devices
    AND date < ${end_date}
    AND version = 101
  GROUP BY
    org_id,
    device_id,
    date
),

device_trips AS
(
  SELECT
    d.org_id,
    d.device_id,
    d.product_id,
    t.trip_date,
    sum(t.trip_count) AS trip_count,
    sum(t.total_distance) AS total_distance
  FROM all_devices d
  LEFT JOIN trips t ON
    t.org_id = d.org_id AND
    t.device_id = d.vg_device_id
  WHERE t.trip_date IS NOT NULL
  GROUP BY
    d.org_id,
    d.device_id,
    d.product_id,
    t.trip_date
),

device_heartbeats_union AS
(
  SELECT
    hb.org_id,
    hb.object_id AS device_id,
    d.product_id,
    hb.date
  FROM kinesisstats.osdhubserverdeviceheartbeat hb
  LEFT JOIN productsdb.devices d ON hb.object_id = d.id AND hb.org_id = d.org_id
  WHERE
    hb.date >= DATE_SUB(${end_date}, 183)  -- we restate last 6 months to capture late arriving data from offline devices
    AND hb.date < ${end_date}
  GROUP BY
    hb.org_id,
    hb.object_id,
    d.product_id,
    hb.date

  UNION ALL

  SELECT
    hb.org_id,
    d.device_id,
    d.product_id,
    hb.date
  FROM kinesisstats.osdhubserverdeviceheartbeat hb
  LEFT JOIN all_devices d ON hb.object_id = d.vg_device_id AND hb.org_id = d.org_id
  WHERE
    d.product_id IN (30,31)
    AND hb.date >= DATE_SUB(${end_date}, 183)  -- we restate last 6 months to capture late arriving data from offline devices
    AND hb.date < ${end_date}
  GROUP BY
    hb.org_id,
    d.device_id,
    d.product_id,
    hb.date

  UNION ALL

  SELECT
    o.org_id,
    o.device_id,
    d.product_id,
    cal.date
  FROM oemdb_shards.oem_sources o
  LEFT JOIN productsdb.devices d ON o.device_id = d.id AND o.org_id = d.org_id
  LEFT JOIN definitions.445_calendar cal ON cal.date >= TO_DATE(o.created_at) AND cal.date <= current_date()
  WHERE
    is_activated = 1
    AND cal.date >= DATE_SUB(${end_date}, 183)  -- we restate last 6 months to capture late arriving data from offline devices
    AND cal.date < ${end_date}
  GROUP BY
    o.org_id,
    o.device_id,
    d.product_id,
    cal.date
),

device_heartbeats AS (
  SELECT
    org_id,
    device_id,
    product_id,
    date
  FROM device_heartbeats_union
  GROUP BY
    org_id,
    device_id,
    product_id,
    date
),

active_devices AS
(
  SELECT
    coalesce(hb.date,t.trip_date,"") AS date,
    coalesce(hb.device_id,t.device_id,-1) AS device_id,
    ifnull(hb.product_id,t.product_id) AS product_id,
    coalesce(hb.org_id,t.org_id,-1) AS org_id,
    hb.device_id is not null AS active_heartbeat,
    t.trip_count,
    t.total_distance
  FROM device_heartbeats hb
  FULL JOIN device_trips t ON
    hb.date = t.trip_date AND
    hb.device_id = t.device_id AND
    hb.org_id = t.org_id
)

SELECT
  date,
  device_id,
  product_id,
  org_id,
  active_heartbeat,
  trip_count,
  total_distance
FROM active_devices
