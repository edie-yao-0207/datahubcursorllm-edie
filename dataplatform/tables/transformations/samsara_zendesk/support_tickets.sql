WITH ticket_field AS (
  SELECT
    ticket_id,
    MAX((updated, field_name)).field_name AS field_name,
    MAX((updated, value)).value AS value
  FROM samsara_zendesk.ticket_field_history
  GROUP BY
    ticket_id
),

custom_fields AS (
  SELECT
    tf.ticket_id AS id,
    tf.value AS field_value,
    cf.title AS raw_title
  FROM samsara_zendesk.ticket_custom_field cf
  LEFT JOIN ticket_field tf ON 
    cf.id = tf.field_name
),

ticket_form AS (
  SELECT 
    id,
    MAX((updated_at, name)).name AS name
  FROM samsara_zendesk.ticket_form_history
  GROUP BY
    id
),

tag AS (
  SELECT
    ticket_id,
    collect_list(tag) AS tags
  FROM samsara_zendesk.ticket_tag
  GROUP BY 
    ticket_id
),

support_tickets AS (
  SELECT
    t.id AS ticket_id,
    DATE(from_utc_timestamp(t.created_at, 'America/Los_Angeles')) AS date_ticket_created,
    CASE WHEN t.status = 'closed' THEN DATE(from_utc_timestamp(t.updated_at, 'America/Los_Angeles')) ELSE NULL END AS date_ticket_solved,
    t.status AS ticket_status,
    tf.name AS ticket_form,
    case_reason.field_value AS case_reason,
    admin_app.field_value AS admin_app_area,
    driver_app.field_value AS driver_app_area,
    hardware.field_value AS hardware_area_new,
    dashboard.field_value AS dashboard_area,
    return_reason.field_value AS reason_for_return,
    product_team.field_value AS product_team,
    t.custom_product_group AS product_group,
    product.field_value AS product_new,
    g.name AS ticket_group,
    u.name AS ticket_assignee,
    o.custom_segment AS segment,
    customer_type.field_value AS customer_type,
    o.custom_global_geography_org_ AS global_geography,
    CONCAT(EXTRACT(YEAR FROM t.created_at),CONCAT("-",EXTRACT(MONTH FROM t.created_at))) AS calendar_month_created,
    CONCAT(cal.fiscal_year,CONCAT("_",CAST(DATE(cal.date) AS int))) AS fiscal_month_created,
    CONCAT(cal.fiscal_year,CONCAT("'",cal.quarter)) AS fiscal_quarter_created,
    CONCAT("Wk of ", CAST(date_trunc(t.created_at, 'WEEK') AS string)) AS week_display,
    CASE WHEN 
      tf.name = "Non Support Related [NEW]" OR
      tf.name = "Non Support Related (Deactivated)" OR
      tf.name = "Industrial" OR 
      tf.name = "Sales/CSM Escalation" OR
      tf.name = "NPS Follow up (Product Only) [NEW]" OR 
      tf.name = "NPS Follow up (Product Only) (Deactivated)"
    THEN false ELSE true END AS is_product_related,
    CASE WHEN admin_app.field_value IS NOT NULL THEN admin_app.field_value
        WHEN driver_app.field_value IS NOT NULL THEN driver_app.field_value
        WHEN return_reason.field_value IS NOT NULL THEN return_reason.field_value 
        WHEN hardware.field_value IS NOT NULL THEN hardware.field_value 
        WHEN dashboard.field_value IS NOT NULL THEN dashboard.field_value END AS issue_area,
    CONCAT(case_reason.field_value,CONCAT("||",
      CASE WHEN admin_app.field_value IS NOT NULL THEN admin_app.field_value
          WHEN driver_app.field_value IS NOT NULL THEN driver_app.field_value
          WHEN return_reason.field_value IS NOT NULL THEN return_reason.field_value 
          WHEN hardware.field_value IS NOT NULL THEN hardware.field_value 
          WHEN dashboard.field_value IS NOT NULL THEN dashboard.field_value END)) AS case_reason_issue_area,
    CASE WHEN o.custom_segment = 'Connected Worker' OR o.custom_segment = 'Industrial' THEN False ELSE True END is_fleet_ticket,
    CASE WHEN array_contains(tag.tags, 'closed_by_merge') THEN True ELSE False END AS is_merged_ticket,
    REPLACE(o.custom_sam_number_organization_, "-", "") AS sam_number,
    DATE(from_utc_timestamp(t.updated_at, 'America/Los_Angeles')) AS date_ticket_updated,
    t.custom_is_child_ticket_ AS is_child_ticket
  FROM samsara_zendesk.ticket t
  LEFT JOIN ticket_form tf ON
    t.ticket_form_id = tf.id
  LEFT JOIN custom_fields case_reason ON 
    case_reason.id = t.id AND 
    case_reason.raw_title = 'Case Reason'
  LEFT JOIN custom_fields admin_app ON 
    admin_app.id = t.id AND 
    admin_app.raw_title = 'Admin App Area'
  LEFT JOIN custom_fields driver_app ON 
    driver_app.id = t.id AND 
    driver_app.raw_title = 'Driver App Area '
  LEFT JOIN custom_fields hardware ON 
    hardware.id = t.id AND 
    hardware.raw_title = 'Hardware Area [New]'
  LEFT JOIN custom_fields dashboard ON 
    dashboard.id = t.id AND 
    dashboard.raw_title = 'Dashboard Area '
  LEFT JOIN custom_fields return_reason ON 
    return_reason.id = t.id AND 
    return_reason.raw_title = 'Reason for Return '
  LEFT JOIN custom_fields product_team ON 
    product_team.id = t.id AND 
    product_team.raw_title LIKE '%Product Team%'
  LEFT JOIN custom_fields product ON 
    product.id = t.id AND 
    product.raw_title = 'Product [New]'
  LEFT JOIN custom_fields customer_type ON 
    customer_type.id = t.id AND 
    customer_type.raw_title = 'Customer Type'
  LEFT JOIN samsara_zendesk.group g ON 
    g.id = t.group_id
  LEFT JOIN samsara_zendesk.user u ON 
    u.id = t.assignee_id
  LEFT JOIN samsara_zendesk.organization o ON 
    o.id = t.organization_id
  LEFT JOIN tag ON
    tag.ticket_id = t.id
  LEFT JOIN definitions.445_calendar cal ON 
    DATE(from_utc_timestamp(t.created_at, 'America/Los_Angeles')) = cal.date
  WHERE 
    t.status <> 'deleted' AND
    DATE(from_utc_timestamp(t.created_at, 'America/Los_Angeles')) > DATE('2020-05-01')
),

historical_support_tickets AS (
  SELECT 
    ht.*,
    REPLACE(o.custom_sam_number_organization_, "-", "") as sam_number,
    ht.date_ticket_created AS date_ticket_updated,
    t.custom_is_child_ticket_ AS is_child_ticket
  FROM samsara_zendesk.historical_support_tickets ht
  LEFT JOIN samsara_zendesk.ticket t ON
    ht.ticket_id = t.id
  LEFT JOIN samsara_zendesk.organization o ON
    t.organization_id = o.id
  WHERE 
    ht.ticket_id IS NOT NULL
)

SELECT 
  *
FROM support_tickets
WHERE
    date_ticket_updated >= ${start_date} AND
    date_ticket_updated < ${end_date} AND
    ticket_id IS NOT NULL

UNION ALL

SELECT 
  *
FROM historical_support_tickets
WHERE
    date_ticket_updated >= ${start_date} AND
    date_ticket_updated < ${end_date} AND 
    ticket_id IS NOT NULL
