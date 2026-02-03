SELECT
  OrgId AS org_id,
  DATE(FROM_UNIXTIME(CAST(InvokedAt AS BIGINT) / 1000)) AS date
FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/functions-invocation-event-log`
