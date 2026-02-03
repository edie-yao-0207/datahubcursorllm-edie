WITH orgs AS (
  SELECT
    o.id AS org_id,
    cm.sam_number
  FROM clouddb.organizations o
  LEFT JOIN dataprep.customer_metadata cm ON
    o.id = cm.org_id
),

-- CM12s do not have device_ids
-- reverse engineer CM12 (30,31) logic from active_devices so their counts are captured correctly

activated_devices AS (

  SELECT * FROM (
    SELECT
      hb.org_id,
      substr(p.name, 0, 2) AS product_type,
      IF(hb.product_id IN (30,31), d.camera_serial, d.serial) as serial
    FROM dataprep.active_devices hb
    LEFT JOIN productsdb.devices d ON hb.org_id = d.org_id AND IF(hb.product_id IN (30,31), RIGHT(hb.device_id, LENGTH(hb.device_id)-2), hb.device_id) = d.id
    LEFT JOIN definitions.products p ON hb.product_id = p.product_id
    LEFT JOIN clouddb.organizations org ON hb.org_id = org.id
    WHERE org.internal_type <> 1 -- filter out test devices
    AND substr(p.name, 0, 2) IN ('AG', 'VG', 'CM', 'IG','SG','SC','EM')
    GROUP BY
      hb.org_id,
      substr(p.name, 0, 2),
      IF(hb.product_id IN (30,31), d.camera_serial, d.serial)
  )
  PIVOT (
    COUNT(DISTINCT serial)
    FOR product_type IN ('AG', 'VG', 'CM', 'IG','SG','SC','EM') -- only select AG, VG, and CM devices
  )

),

active_devices AS (

  SELECT * FROM (
    SELECT
      hb.org_id,
      substr(p.name, 0, 2) AS product_type,
      IF(hb.product_id IN (30,31), d.camera_serial, d.serial) as serial
    FROM dataprep.active_devices hb
    LEFT JOIN productsdb.devices d on hb.org_id = d.org_id AND IF(hb.product_id IN (30,31), RIGHT(hb.device_id, LENGTH(hb.device_id)-2), hb.device_id) = d.id
    LEFT JOIN definitions.products p ON hb.product_id = p.product_id
    LEFT JOIN clouddb.organizations org ON hb.org_id = org.id
    WHERE
      hb.date >= date_sub(current_date(), 30) AND -- only get devices that have sent a heartbeat in the last 30 days
      org.internal_type <> 1 -- filter out test devices
      AND substr(p.name, 0, 2) IN ('AG', 'VG', 'CM', 'IG','SG','SC','EM')
    GROUP BY
      hb.org_id,
      substr(p.name, 0, 2),
      IF(hb.product_id IN (30,31), d.camera_serial, d.serial)
  )
  PIVOT (
    COUNT(DISTINCT serial)
    FOR product_type IN ('AG', 'VG', 'CM', 'IG','SG','SC','EM') -- only select AG, VG, CM, IG devices
  )

),

device_cables AS (
    SELECT * FROM (
        SELECT
            c.org_id,
            c.device_id,
            CASE WHEN cable_type = 4 THEN 'Passenger' ELSE 'Non' END AS vehicle_type
        FROM dataprep.device_cables c
    )
    PIVOT (
        COUNT(DISTINCT device_id)
        FOR vehicle_type IN ('Passenger','Non')
    )
)

SELECT
    c.org_id,
    c.sam_number,
    act.VG AS num_vg_activated,
    act.CM AS num_cm_activated,
    act.AG AS num_ag_activated,
    act.IG AS num_ig_activated,
    ad.VG AS num_vg_active,
    ad.CM AS num_cm_active,
    ad.AG AS num_ag_active,
    ad.IG AS num_ig_active,
    dc.Passenger AS num_passenger_vehicles,
    dc.Non AS num_non_passenger_vehicles,
    act.SG AS num_sg_activated,
    act.SC AS num_sc_activated,
    act.EM AS num_em_activated,
    ad.SG AS num_sg_active,
    ad.SC AS num_sc_active,
    ad.EM AS num_em_active
FROM orgs c
LEFT JOIN activated_devices act ON c.org_id = act.org_id
LEFT JOIN active_devices ad ON c.org_id = ad.org_id
LEFT JOIN device_cables dc ON c.org_id = dc.org_id
WHERE c.org_id IS NOT NULL
