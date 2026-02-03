WITH combine_tokens AS (
  SELECT
    *, 
    CASE
      WHEN (access_token_id = "" OR access_token_id IS NULL) THEN oauth_token_app_uuid + " " + org_id
      ELSE access_token_id
    END AS token_used
  FROM dataprep.api_performance
),

active_users_in_each_month AS (
  SELECT
    DATE_ADD(LAST_DAY(ADD_MONTHS(date, -1)),1) AS month,
    org_id
  FROM combine_tokens
  GROUP BY 
    DATE_ADD(LAST_DAY(ADD_MONTHS(date, -1)),1),
    org_id
  HAVING SUM(num_api_requests_daily) > 50
),

api_performance_only_monthly_active_users AS (
  SELECT 
    DATE_ADD(LAST_DAY(ADD_MONTHS(date, -1)),1) AS month,
    *
  FROM dataprep.api_performance t1
  WHERE 
    t1.org_id IN (
      SELECT t2.org_id 
      FROM active_users_in_each_month t2 
      WHERE
        MONTH(t1.date) = MONTH(t2.month) 
        AND YEAR(t1.date) = YEAR(t2.month)
    ) 
)

SELECT * FROM api_performance_only_monthly_active_users
