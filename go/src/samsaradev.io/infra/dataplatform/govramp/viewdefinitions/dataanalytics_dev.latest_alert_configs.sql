(
  WITH alert_configs AS (
  SELECT
    `date`
    , org_id
    , alert_config_uuid
    , is_disabled
    , is_admin
    , created_ts_utc
    , updated_ts_utc
    , deleted_ts_utc
    , has_time_range_settings
    , primary_trigger_type
    , secondary_trigger_type
    , alert_action_types
    , alert_recipient_types
    , alert_notification_types
    , alert_recipient_user_ids
    , alert_recipient_contact_ids
    FROM datamodel_platform.dim_alert_configs
  ),
  max_date_alert AS (
    SELECT MAX(`date`) AS max_date_alert FROM datamodel_platform.dim_alert_configs
  ),
  max_date_org AS (
    SELECT MAX(`date`) AS max_date_org FROM datamodel_core.dim_organizations
  ),
  date_upper_bound AS (
  SELECT LEAST(a.max_date_alert, o.max_date_org) date_upper_bound
  FROM max_date_alert a
  CROSS JOIN max_date_org o
  )
  SELECT
    `date`
    , org_id
    , org_name
    ---------------------------------
    -- Customer organization features
    ---------------------------------
    , TO_DATE(created_at, 'yyyy-MM-dd') AS created_at
    , DATEDIFF(date, created_at) AS org_tenure  -- organization tenure at the present, measured in days
    , account_cs_tier
    , account_size_segment_name
    , is_paid_safety_customer
    , is_paid_stce_customer
    , is_paid_telematics_customer
    , account_arr_segment
    , account_industry
    , locale
    ---------------------------
    -- Alert configuration data
    ---------------------------
    , COALESCE(alert_config_uuid, -99) AS alert_config_uuid
    , DATEDIFF(created_ts_utc, created_at) AS days_to_creation
    , DATEDIFF(deleted_ts_utc, created_ts_utc) AS days_to_deletion
    , is_disabled
    , CASE WHEN deleted_ts_utc IS NULL THEN FALSE
          ELSE TRUE
          END AS is_deleted
    , CASE WHEN is_disabled = 0 AND deleted_ts_utc IS NULL THEN 1
          ELSE 0
          END AS is_active
    , is_admin
    , created_ts_utc
    , updated_ts_utc
    , deleted_ts_utc
    , has_time_range_settings
    , primary_trigger_type
    , secondary_trigger_type
    -- Add alert_action indicator variables
    , ARRAY_SIZE(alert_action_types) AS actions_n
    , CASE WHEN ARRAY_CONTAINS(alert_action_types, "Notification") THEN 1
          ELSE 0
          END AS alert_action_notification
    , CASE WHEN ARRAY_CONTAINS(alert_action_types, "DashboardNotification") THEN 1
          ELSE 0
          END AS alert_action_dashboard
    , CASE WHEN ARRAY_CONTAINS(alert_action_types, "DriverAppNotification") THEN 1
          ELSE 0
          END AS alert_action_driver_app
    , CASE WHEN ARRAY_CONTAINS(alert_action_types, "Webhook") THEN 1
          ELSE 0
          END AS alert_action_webhook
    , CASE WHEN ARRAY_CONTAINS(alert_action_types, "SlackWebhook") THEN 1
          ELSE 0
          END AS alert_action_slack
    , CASE WHEN ARRAY_CONTAINS(alert_action_types, "AssignForm") THEN 1
          ELSE 0
          END AS alert_action_assign_form
    -- Add recipient_type indicator variables:
    , CASE WHEN ARRAY_CONTAINS(alert_recipient_types, "contact") THEN 1
          ELSE 0
          END AS alert_recipient_contact
    , CASE WHEN ARRAY_CONTAINS(alert_recipient_types, "user") THEN 1
          ELSE 0
          END AS alert_recipient_user
    -- Add alert_notification indicator variables:
    , ARRAY_SIZE(alert_notification_types) AS alert_notifications_n
    , CASE WHEN ARRAY_CONTAINS(alert_notification_types, "email") THEN 1
          ELSE 0
          END AS alert_notification_email
    , CASE WHEN ARRAY_CONTAINS(alert_notification_types, "sms") THEN 1
          ELSE 0
          END AS alert_notification_sms
    , CASE WHEN ARRAY_CONTAINS(alert_notification_types, "push") THEN 1
          ELSE 0
          END AS alert_notification_push
    -- Number of users to contact
    , ARRAY_SIZE(alert_recipient_user_ids) AS alert_recipient_users_n
    , ARRAY_SIZE(alert_recipient_contact_ids) AS alert_recipient_contact_n
    , customer_metadata.csm_name
  FROM datamodel_core.dim_organizations
  LEFT JOIN alert_configs USING(org_id, `date`)
  LEFT JOIN dataprep.customer_metadata USING(org_id)
  WHERE 1=1
        AND `date` >= '2023-06-01'
        AND (is_paid_customer)   -- Only current customers
)
