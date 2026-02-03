WITH risk_factors_base AS (
select date,
       org_id,
       driver_id,
       risk_classification,
       risk_score,
       get_json_object(explanation, '$.llm_insights.overall_risk_level') AS overall_risk_level,
       explode(from_json(get_json_object(explanation, '$.llm_insights.risk_factors'),
         'ARRAY<STRUCT<severity:STRING, factor_type:BIGINT, contribution:DOUBLE>>')) AS risk_factor
from safetyriskdb_shards.driver_risk_v2)

SELECT CAST(risk_factors_base.date AS string) AS date,
       risk_factors_base.org_id,
       risk_factors_base.driver_id,
       CAST(risk_factors_base.risk_classification AS integer) AS risk_classification,
       risk_factors_base.risk_score,
       risk_factors_base.overall_risk_level,
       factor_map.name AS risk_factor_name,
       CAST(risk_factor.factor_type AS integer) AS risk_factor_type_id,
       risk_factor.severity AS risk_factor_severity,
       risk_factor.contribution AS risk_factor_contribution,
       ROW_NUMBER() OVER (PARTITION BY risk_factors_base.driver_id,
                                       risk_factors_base.org_id,
                                       risk_factors_base.date
                          ORDER BY risk_factor.contribution DESC)
       AS factor_rank_overall,
       CASE WHEN risk_factor.severity != 'Protective'
            THEN  ROW_NUMBER() OVER
            (PARTITION BY risk_factors_base.driver_id,
                         risk_factors_base.org_id,
                         risk_factors_base.date,
                         CASE WHEN risk_factor.severity != 'Protective'
                              THEN 'Non-Protective' ELSE NULL END
             ORDER BY risk_factor.contribution DESC)
            ELSE NULL END AS risk_factor_rank,
       CASE WHEN risk_factor.severity = 'Protective'
            THEN  ROW_NUMBER() OVER
            (PARTITION BY risk_factors_base.driver_id,
                         risk_factors_base.org_id,
                         risk_factors_base.date,
                          CASE WHEN risk_factor.severity = 'Protective'
                               THEN 'Protective' ELSE NULL END
             ORDER BY risk_factor.contribution DESC)
            ELSE NULL END AS protective_factor_rank
FROM risk_factors_base
LEFT JOIN definitions.driver_risk_factor_map factor_map
       ON factor_map.id = risk_factor.factor_type
