WITH statuses AS (
  SELECT
    org_id,
    device_id,
    gateway_id,
    serial,
    product_id,
    recommended_actions AS recommended_action,
    rtn_date,
    rtn_number,
    last_heartbeat_date,
    last_batt_mv,
    last_cable_id,
    last_powered_date
  FROM
    technical_support.AG2x_health_labels
  UNION ALL
  SELECT
    org_id,
    device_id,
    gateway_id,
    serial,
    product_id,
    recommended_action,
    rtn_date,
    rtn_number,
    last_heartbeat_date,
    last_batt_mv,
    NULL AS last_cable_id,
    NULL AS last_powered_date
  FROM
    technical_support.AG4x_health_statuses
  UNION
  SELECT
    org_id,
    device_id,
    gateway_id,
    serial,
    product_id,
    recommended_action,
    rtn_date,
    rtn_number,
    last_heartbeat_date,
    last_batt_mv,
    last_cable_id,
    last_powered_date
  FROM
    technical_support.AG5x_health_statuses
)
SELECT
  -- External facing
  ag.org_id,
  orgs.name AS org_name,
  CONCAT('https://cloud.samsara.com/devices/', ag.device_id, '/show') AS url,
  ag.device_id,
  ag.gateway_id,
  ag.serial,
  pd.name AS product_name,
  ag.recommended_action AS identified_issue,
  rtn_date,
  rtn_number,
  last_heartbeat_date,
  last_batt_mv,
  last_cable_id,
  last_powered_date,
  rad.tam_health_status AS health_status,
  -- Internal helper columns
  rac.canonical_label AS __canonical_label,
  rac.issue_category AS __category,
  rac.next_step AS __next_step,
  rad.id AS __definition_id,
  rad.action_text AS __matched_text
FROM
  statuses ag
    LEFT JOIN definitions.products pd
      ON pd.product_id = ag.product_id
    LEFT JOIN productsdb.devices dev
      ON dev.id = ag.device_id
    LEFT JOIN clouddb.organizations orgs
      ON orgs.id = dev.org_id
    LEFT JOIN support_dev.ag_recommended_action_definitions rad
      ON ag.recommended_action = rad.action_text
    LEFT JOIN support_dev.ag_recommended_action_canonicals rac
      ON rad.canonical_id = rac.id