WITH osdrearcameraconnected AS ( 
  SELECT
    date,
    org_id,
    object_id AS vg_device_id,
    time AS timestamp,
    value.int_value AS connected
  FROM kinesisstats.osdrearcameraconnected
  WHERE
    value.is_databreak = false AND
    value.is_end = false AND
    date >= DATE_SUB(${end_date}, 14)
),

osdrearcameraframestate AS ( 
  SELECT DISTINCT
    rcfs.date,
    rcfs.org_id,
    clv.vg_device_id
  FROM kinesisstats.osdrearcameraframestate AS rcfs
  LEFT JOIN dataprep_safety.cm_linked_vgs AS clv
  ON rcfs.org_id = clv.org_id
    AND rcfs.object_id = clv.linked_cm_id
  WHERE rcfs.value.int_value IN (1,2)
    AND rcfs.value.is_databreak = false
    AND rcfs.value.is_end = false
    AND rcfs.date >= DATE_SUB(${end_date}, 14)
),

baxter_vg_devices AS ( 
  SELECT
    rcc.date,
    rcc.org_id,
    rcc.vg_device_id
  FROM (SELECT DISTINCT date, org_id, vg_device_id FROM osdrearcameraconnected WHERE connected = 1) AS rcc -- Device must have sent up a "connected" status over the past 10 days
  JOIN osdrearcameraframestate AS rcfs
  ON rcc.date = rcfs.date
    AND rcc.org_id = rcfs.org_id
    AND rcc.vg_device_id = rcfs.vg_device_id
),

avas_cm_linked_vgs AS (
  SELECT 
    a.org_id,
    a.id AS vg_device_id,
    a.serial AS vg_serial,
    a.product_id AS vg_product_id,
    b.id AS camera_device_id,
    REPLACE(a.camera_serial,'-','') AS camera_serial,
    a.camera_product_id  
  FROM productsdb.devices AS a
  INNER JOIN productsdb.devices AS b
  ON REPLACE(a.camera_serial,'-','') = b.serial
  WHERE b.id IS NOT NULL
),

first_connected AS (
  SELECT
    org_id,
    object_id,
    MIN(date) AS date_baxter_first_connected
  FROM kinesisstats.osdrearcameraconnected
  GROUP BY org_id, object_id
),

attributes AS (
  SELECT sub.*
  FROM (
    SELECT
      ace.org_id,
      ace.entity_id AS device_id,
      ace.created_at AS attribute_created_at_ts,
      av.string_value AS attribute_type,
      ROW_NUMBER() OVER(PARTITION BY ace.org_id, ace.entity_id ORDER BY ace.created_at DESC) AS rnk
    FROM attributedb_shards.attribute_cloud_entities AS ace
    LEFT JOIN attributedb_shards.attribute_values AS av 
    ON ace.attribute_value_id = av.uuid
    WHERE av.string_value IN ('Ignition On', 'Reverse Only')
  ) AS sub
  WHERE sub.rnk = 1
),

baxter_devices AS (
  SELECT
    bvd.date,
    aclv.org_id,
    aclv.vg_device_id,
    aclv.camera_device_id as cm_device_id,
    fc.date_baxter_first_connected,
    att.attribute_created_at_ts,
    att.attribute_type
  FROM baxter_vg_devices AS bvd
  JOIN avas_cm_linked_vgs AS aclv
    ON aclv.org_id = bvd.org_id
    AND aclv.vg_device_id = bvd.vg_device_id
  LEFT JOIN first_connected AS fc
    ON aclv.org_id = fc.org_id
    AND aclv.vg_device_id = fc.object_id
  LEFT JOIN attributes AS att
    ON aclv.org_id = att.org_id
    AND aclv.vg_device_id = att.device_id
)

SELECT DISTINCT
  date,
  org_id,
  vg_device_id,
  cm_device_id,
  date_baxter_first_connected,
  attribute_created_at_ts,
  attribute_type
FROM baxter_devices
WHERE date >= ${start_date}
  AND date < ${end_date}
