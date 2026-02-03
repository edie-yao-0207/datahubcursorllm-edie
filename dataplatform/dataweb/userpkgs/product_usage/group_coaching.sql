SELECT
  CAST(gc.OrgId AS BIGINT) AS org_id,
  DATE(gc.CreatedAt) AS date,
  gc.CreatedBy AS user_id
FROM
  dynamodb.group_coaching gc
WHERE
  gc.ItemType = 'GROUP_COACHING_REPORT'
