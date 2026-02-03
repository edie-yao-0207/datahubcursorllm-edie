SELECT
  CAST(s.orgid AS BIGINT) AS org_id,
  s.date AS date,
  s.polymorphicuserid AS user_id
FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/worker-safety-sos-signals` s
WHERE
  s.orgid IS NOT NULL
  AND s.sourceType IN (1, 2, 3) -- Mobile SOS events

UNION ALL

SELECT
  CAST(t.orgid AS BIGINT) AS org_id,
  t.date AS date,
  t.recipientpolymorphicuserid AS user_id
FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/worker-safety-timers` t
WHERE
  t.orgid IS NOT NULL
