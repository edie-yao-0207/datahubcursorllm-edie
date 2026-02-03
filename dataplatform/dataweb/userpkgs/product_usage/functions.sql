SELECT
  OrgId AS org_id,
  DATE(FROM_UNIXTIME(CAST(InvokedAt AS BIGINT) / 1000)) AS date
FROM dynamodb.functions_invocation_event_log
