SELECT
  CAST(wo.orgid AS BIGINT) AS org_id,
  wo.CreatedByUserId AS user_id,
  DATE(from_unixtime(wo.createdAtMs / 1000)) AS date,
  wo.workOrderId
FROM
  delta.`s3://samsara-eu-dynamodb-delta-lake/table/work-orders` wo
WHERE
  wo.orgid IS NOT NULL
  AND wo.createdAtMs IS NOT NULL
  AND CompositeKey LIKE '%#work_order'
