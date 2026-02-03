WITH mdm_devices AS (
  SELECT
    COUNT(id) as Num_Devices,
    org_id
  FROM mdmdb_shards.mdm_devices
  -- Filter out non-active devices
  WHERE deleted_with_behavior IS NULL
  GROUP BY org_id
)

SELECT
  o.name AS org_name,
  oc.org_id AS org_id,
  CASE o.internal_type
    WHEN 0 THEN "Customer"
    WHEN 1 THEN "Internal"
    WHEN 2 THEN "Lab"
    WHEN 3 THEN "Developer Portal Test Org"
    ELSE "Developer Portal Dev Org"
    END AS org_type,
  d.Num_Devices AS num_devices,
  oc.cell_id AS cell,
  FORMAT_STRING("https://cloud.samsara.com/o/%d/fleet/mdm", oc.org_id) AS dashboard_link
FROM clouddb.org_cells oc
JOIN clouddb.organizations o
ON oc.org_id = o.id
JOIN mdm_devices d
ON oc.org_id = d.org_id
WHERE oc.org_id IN (SELECT DISTINCT(org_id) FROM mdmdb_shards.enterprise)
ORDER BY oc.cell_id, Num_Devices DESC
