SELECT
  CAST(s.orgid AS BIGINT) AS org_id,
  s.date AS date,
  s.polymorphicuserid AS user_id
FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/worker-safety-sos-signals` s
LEFT OUTER JOIN users_eu i
  ON SPLIT_PART(s.polymorphicuserid, '-', 2) = i.user_id
  AND SPLIT_PART(s.polymorphicuserid, '-', 1) = 'user'
WHERE
  s.orgid IS NOT NULL
  AND s.sourceType IN (4, 5, 7, 8) -- Wearable SOS events
  AND i.user_id IS NULL -- Filter out internal users
