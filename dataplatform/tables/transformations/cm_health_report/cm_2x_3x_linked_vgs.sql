WITH gateways_with_upper_serial AS (
  SELECT
    org_id,
    device_id,
    product_id,
    UPPER(REPLACE(gateways.serial, '-', '')) AS upper_serial
  FROM
    productsdb.gateways
  -- filter out deactivated gateways
  WHERE group_id != 1
),
devices_with_upper_camera_serial AS (
  SELECT
    id,
    product_id,
    org_id,
    camera_product_id,
    UPPER(REPLACE(devices.camera_serial, '-', '')) AS upper_camera_serial,
    camera_first_connected_at_ms
  FROM
    productsdb.devices
  WHERE
    camera_serial IS NOT NULL
    -- filter out deactivated gateways
    AND group_id != 1
),
cm_linked_vgs AS (
  -- Join gateways and devices together on camera serial for CM3X's
  SELECT
    gateways.org_id,
    devices.id AS vg_device_id,
    devices.product_id AS product_id,
    gateways.device_id AS linked_cm_id,
    gateways.product_id AS cm_product_id,
    devices.upper_camera_serial,
    devices.camera_first_connected_at_ms
  FROM
    gateways_with_upper_serial gateways
    JOIN devices_with_upper_camera_serial AS devices ON upper_serial = upper_camera_serial
    AND devices.org_id = gateways.org_id
    JOIN clouddb.organizations AS orgs ON gateways.org_id = orgs.id
  WHERE
    orgs.quarantine_enabled != 1
),
cm_3x_linked_vgs AS (
  -- Filter down to CM3X product IDs
  SELECT
    *
  FROM
    cm_linked_vgs
  WHERE
    -- 43 is CM31
    -- 44 is CM32
    -- 167 is CM33
    -- 155 is CM34
    cm_product_id IN (43, 44, 167, 155)
),
cm_2x_linked_vgs AS (
  -- Get gateways connected to a CM2X
  SELECT
    org_id,
    id AS vg_device_id,
    product_id,
    -- spark cannot infer the type if all rows are null
    CAST(NULL AS BIGINT) AS linked_cm_id,
    camera_product_id AS cm_product_id,
    upper_camera_serial,
    camera_first_connected_at_ms
  FROM
    devices_with_upper_camera_serial
  WHERE
    camera_product_id IN (30, 31)
)
SELECT
  *
FROM
  cm_2x_linked_vgs
UNION
SELECT
  *
FROM
  cm_3x_linked_vgs
