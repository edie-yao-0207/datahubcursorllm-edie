SELECT id, org_id
FROM clouddb.drivers
WHERE
  deleted_at = timestamp('0101-01-01T00:00:00.000+00:00')  -- filter for not deleted
  AND id IS NOT NULL
  AND org_id IS NOT NULL
