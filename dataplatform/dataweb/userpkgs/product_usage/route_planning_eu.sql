SELECT
  a.org_id,
  a.date,
  a.principal_id AS user_id
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-auditsdb/auditsdb/gql_audits_v0` a
WHERE
  a.query_name IN ("StartPlannerSessionOptimization")
  AND a.principal_type = 1

UNION ALL

SELECT
  a.org_id,
  a.date,
  a.principal_id AS user_id
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-audits-shard-1db/auditsdb/gql_audits_v0` a
WHERE
  a.query_name IN ("StartPlannerSessionOptimization")
  AND a.principal_type = 1
