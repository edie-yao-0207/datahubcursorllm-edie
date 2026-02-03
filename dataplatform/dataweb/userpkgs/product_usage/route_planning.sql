SELECT
  a.org_id,
  a.date,
  a.principal_id AS user_id
FROM
  auditsdb_shards.gql_audits a
WHERE
  a.query_name IN ("StartPlannerSessionOptimization")
  AND a.principal_type = 1
