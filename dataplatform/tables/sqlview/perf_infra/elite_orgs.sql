WITH orgs_with_2k_devices AS (
  SELECT
    org_id,
    COUNT(*) AS count
  FROM
    productsdb.devices
  GROUP BY
    org_id
  HAVING
    count >= 2000
)
SELECT
  id AS org_id,
  name
FROM
  clouddb.organizations
WHERE
  id IN (
    SELECT
      org_id AS id
    FROM
      orgs_with_2k_devices
  )
