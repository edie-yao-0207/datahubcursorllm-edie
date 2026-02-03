SELECT
  CAST(gc.OrgId AS BIGINT) AS org_id,
  DATE(gc.CreatedAt) AS date,
  gc.CreatedBy AS user_id
FROM
  delta.`s3://samsara-eu-dynamodb-delta-lake/table/group-coaching` gc
WHERE
  gc.ItemType = 'GROUP_COACHING_REPORT'
