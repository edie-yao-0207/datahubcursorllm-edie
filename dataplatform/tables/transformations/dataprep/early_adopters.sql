WITH sophistication_score AS (
  SELECT
    sam_number,
    csm_tier,
    customer_sophistication_score,
    CASE WHEN csm_tier = 'Elite' THEN
            CASE WHEN customer_sophistication_score >= 91 THEN 'Excellent'
                  WHEN customer_sophistication_score >= 85 AND customer_sophistication_score < 91 THEN 'Good'
                  WHEN customer_sophistication_score >= 63 AND customer_sophistication_score < 84 THEN 'Moderate'
                  ELSE 'Poor'
            END
        WHEN csm_tier = 'Premier' THEN
            CASE WHEN customer_sophistication_score >= 85 THEN 'Excellent'
                  WHEN customer_sophistication_score >= 78 AND customer_sophistication_score < 85 THEN 'Good'
                  WHEN customer_sophistication_score >= 53 AND customer_sophistication_score < 78 THEN 'Moderate'
                  ELSE 'Poor'
            END
        WHEN csm_tier = 'Plus' THEN
            CASE WHEN customer_sophistication_score >= 72 THEN 'Excellent'
                  WHEN customer_sophistication_score >= 61 AND customer_sophistication_score < 72 THEN 'Good'
                  WHEN customer_sophistication_score >= 39 AND customer_sophistication_score < 61 THEN 'Moderate'
                  ELSE 'Poor'
            END
        WHEN csm_tier = 'Starter' THEN
            CASE WHEN customer_sophistication_score >= 54 THEN 'Excellent'
                  WHEN customer_sophistication_score >= 46 AND customer_sophistication_score < 54 THEN 'Good'
                  WHEN customer_sophistication_score >= 26 AND customer_sophistication_score < 46 THEN 'Moderate'
                  ELSE 'Poor'
            END
    END AS score_classification
  FROM customer360.customer_sophistication_scores
),

active_devices AS (
  SELECT
    org_id,
    COUNT(DISTINCT device_id) AS num_active_devices
  FROM dataprep.active_devices
  WHERE date >= DATE_SUB(CURRENT_DATE(), 30)
  GROUP BY org_id
),

cloud_users AS (
  SELECT
    organization_id AS org_id,
    COUNT(DISTINCT id) AS num_users
  FROM clouddb.users_organizations
  GROUP BY org_id
)

SELECT
  cm.name AS account_name,
  cm.sam_number AS sam_number,
  o.name AS org_name,
  cm.org_id AS org_id,
  CASE
    WHEN o.release_type_enum = 2 THEN 'Current'
    ELSE 'Prospective'
  END AS release_type,
  o.release_type_enum,
  cm.segment,
  cm.org_size AS organization_size,
  u.num_users,
  ss.customer_sophistication_score,
  ad.num_active_devices,
  cm.industry,
  cm.sub_industry,
  cm.billingcity,
  cm.billingstatecode,
  cm.billingcountry,
  cm.region,
  cm.csm_rating,
  cm.csm_trend,
  o.locale,
  CAST(cm.customer_arr AS BIGINT),
  cm.implementation_consultant_name,
  cm.csm_name,
  cm.billing_first_name,
  cm.billing_last_name,
  cm.billing_email
FROM dataprep.customer_metadata cm
LEFT JOIN clouddb.organizations o ON
  cm.org_id = o.id
LEFT JOIN active_devices ad ON
  cm.org_id = ad.org_id
LEFT JOIN cloud_users u ON
  cm.org_id = u.org_id
LEFT JOIN sophistication_score ss ON
  cm.sam_number = ss.sam_number
WHERE
  cm.lifetime_acv > 500 AND
  cm.type = 'End Customer' AND
  ad.num_active_devices > 5 AND
  score_classification IN ('Excellent', 'Good') AND
  cm.has_active_contract = True AND
  cm.csm_rating = "Green" AND
  cm.support_escalation != True AND
  cm.current_escalated != True AND
  cm.high_churn_risk != True AND
  cm.delinquent != True AND
  cm.csm_trend != "Declining" AND
  o.internal_type <> 1 AND
  o.release_type_enum NOT IN (1)
