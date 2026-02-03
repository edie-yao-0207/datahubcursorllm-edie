WITH combine_tokens AS (
  SELECT
    *, 
    CASE
      WHEN (access_token_id = "" OR access_token_id IS NULL) THEN oauth_token_app_uuid + " " + org_id
      ELSE access_token_id
    END AS token_used
  FROM dataprep.api_performance
),

active_deployments_in_each_month AS (
  SELECT
    DATE_ADD(LAST_DAY(ADD_MONTHS(date, -1)),1) AS month,
    token_used
  FROM combine_tokens
  GROUP BY 
    DATE_ADD(LAST_DAY(ADD_MONTHS(date, -1)),1),
    token_used
  HAVING SUM(num_api_requests_daily) > 50
),

api_performance_only_monthly_active_deployments AS (
  SELECT 
    DATE_ADD(LAST_DAY(ADD_MONTHS(date, -1)),1) AS month,
    *
  FROM combine_tokens t1
  WHERE 
    t1.token_used IN (
      SELECT t2.token_used 
      FROM active_deployments_in_each_month t2 
      WHERE
        MONTH(t1.date) = MONTH(t2.month) 
        AND YEAR(t1.date) = YEAR(t2.month)
    ) 
)

SELECT * FROM api_performance_only_monthly_active_deployments
