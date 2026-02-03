SELECT
  org_id,
  COUNT(*) AS device_count
FROM
  productsdb.devices
WHERE
  org_id IN (
    SELECT
      id
    FROM
      clouddb.organizations
    WHERE
      internal_type != 1
  )
GROUP BY
  org_id
ORDER BY
  device_count DESC
