from dataweb.userpkgs.constants import ColumnDescription

METRICS_V1 = {
    "vg_licenses": "SUM(CASE WHEN device_type = 'VG' THEN net_quantity END)",
    "cm_licenses": "SUM(CASE WHEN device_type IN ('CM-S', 'CM-D', 'CM-M') THEN net_quantity END)",
    "cm_m_licenses": "SUM(CASE WHEN device_type IN ('CM-M') THEN net_quantity END)",
    "cm_s_licenses": "SUM(CASE WHEN device_type IN ('CM-S') THEN net_quantity END)",
    "cm_d_licenses": "SUM(CASE WHEN device_type IN ('CM-D') THEN net_quantity END)",
    "ag_licenses": "SUM(CASE WHEN device_type LIKE 'AG%' THEN net_quantity END)",
    "powered_ag_licenses": "SUM(CASE WHEN device_type = 'AG Powered' THEN net_quantity END)",
    "unpowered_ag_licenses": "SUM(CASE WHEN device_type = 'AG Unpowered' THEN net_quantity END)",
    "at_licenses": "SUM(CASE WHEN device_type = 'AT' THEN net_quantity END)",
    "forms_licenses": "SUM(CASE WHEN sku LIKE 'LIC-FORMS%' AND is_core_license THEN quantity END)",
    "commercial_navigation_licenses": "SUM(CASE WHEN sku LIKE 'LIC-COMM-NAV%' AND is_core_license THEN quantity END)",
    "qualifications_licenses": "SUM(CASE WHEN sku LIKE 'LIC-QUALS%' AND is_core_license THEN quantity END)",
    "maintenance_licenses": "SUM(CASE WHEN sku LIKE 'LIC-MAINT%' AND is_core_license THEN quantity END)",
    "training_licenses": "SUM(CASE WHEN sku LIKE 'LIC-TRAINING%' AND is_core_license THEN quantity END)",
    "fleet_apps_licenses": "SUM(CASE WHEN sku = 'LIC-FL-APPS' AND is_core_license THEN quantity END)",
    "essential_safety_licenses": "SUM(CASE WHEN device_type in ('CM-S', 'CM-D', 'CM-M') AND is_core_license AND tier = 'Essential' AND is_legacy_license = FALSE THEN quantity END)",
    "premier_safety_licenses": "SUM(CASE WHEN device_type in ('CM-S', 'CM-D', 'CM-M') AND is_core_license AND tier = 'Premier' AND is_legacy_license = FALSE THEN quantity END)",
    "enterprise_safety_licenses": "SUM(CASE WHEN device_type in ('CM-S', 'CM-D', 'CM-M') AND is_core_license AND tier = 'Enterprise' AND is_legacy_license = FALSE THEN quantity END)",
    "legacy_safety_licenses": "SUM(CASE WHEN device_type in ('CM-S', 'CM-D', 'CM-M') AND is_core_license AND is_legacy_license THEN quantity END)",
    "essential_telematics_licenses": "SUM(CASE WHEN device_type = 'VG' AND is_core_license AND tier = 'Essential' AND sub_product_line LIKE '%Essential' AND is_legacy_license = FALSE THEN quantity END)",
    "essential_plus_telematics_licenses": "SUM(CASE WHEN device_type = 'VG' AND is_core_license AND tier = 'Essential' AND sub_product_line LIKE '%Essential Plus' AND is_legacy_license = FALSE THEN quantity END)",
    "premier_telematics_licenses": "SUM(CASE WHEN device_type = 'VG' AND is_core_license AND tier = 'Premier' AND is_legacy_license = FALSE THEN quantity END)",
    "enterprise_telematics_licenses": "SUM(CASE WHEN device_type = 'VG' AND is_core_license AND tier = 'Enterprise' AND is_legacy_license = FALSE THEN quantity END)",
    "legacy_telematics_licenses": "SUM(CASE WHEN device_type = 'VG' AND is_core_license AND (is_legacy_license OR sku = 'LIC-VG-PS') THEN quantity END)",
    "essential_ag_licenses": "SUM(CASE WHEN device_type IN ('AG Powered', 'AG Unpowered') AND is_core_license AND sku NOT LIKE 'LIC-TRLR%' AND sku NOT IN ('LIC-AG2-ENT', 'LIC-AG2T-EXPRESS', 'LIC-AG4-ENT', 'LIC-AG4P-ENT', 'LIC-AG-UNPWR', 'LIC-AG-PWR-REEFER-KIT', 'LIC-AG-PWR-REEFER', 'LIC-AG-PWR-PLUS', 'LIC-AG-PWR-BASIC') AND sub_product_line LIKE '%Basic' AND is_legacy_license = FALSE THEN quantity END)",
    "premier_ag_licenses": "SUM(CASE WHEN device_type IN ('AG Powered', 'AG Unpowered') AND is_core_license AND sku NOT LIKE 'LIC-TRLR%' AND sku NOT IN ('LIC-AG2-ENT', 'LIC-AG2T-EXPRESS', 'LIC-AG4-ENT', 'LIC-AG4P-ENT', 'LIC-AG-UNPWR', 'LIC-AG-PWR-REEFER-KIT', 'LIC-AG-PWR-REEFER', 'LIC-AG-PWR-PLUS', 'LIC-AG-PWR-BASIC', 'LIC-AG2-ENT-PLTFM-ESS', 'LIC-AG2-ENT-PLTFM-PREM') AND sub_product_line LIKE '%Plus' AND is_legacy_license = FALSE THEN quantity END)",
    "enterprise_ag_licenses": "SUM(CASE WHEN device_type IN ('AG Powered', 'AG Unpowered') AND is_core_license AND sku IN ('LIC-AG2-ENT-PLTFM-ESS', 'LIC-AG2-ENT-PLTFM-PREM') AND is_legacy_license = FALSE THEN quantity END)",
    "legacy_ag_licenses": "SUM(CASE WHEN device_type IN ('AG Powered', 'AG Unpowered') AND is_core_license AND (sku LIKE 'LIC-TRLR%' OR sku IN ('LIC-AG2-ENT', 'LIC-AG2T-EXPRESS', 'LIC-AG4-ENT', 'LIC-AG4P-ENT', 'LIC-AG-UNPWR', 'LIC-AG-PWR-REEFER-KIT', 'LIC-AG-PWR-REEFER', 'LIC-AG-PWR-PLUS', 'LIC-AG-PWR-BASIC')) THEN quantity END)",
    "forms_assigned_licenses": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-FORMS%' THEN dla.uuid END)",
    "training_assigned_licenses": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-TRAINING%' THEN dla.uuid END)",
    "commercial_navigation_assigned_licenses": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-COMM-NAV%' THEN dla.uuid END)",
    "qualifications_assigned_licenses": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-QUALS%' THEN dla.uuid END)",
    "maintenance_assigned_licenses": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-MAINT%' THEN dla.uuid END)",
    "fleet_apps_assigned_licenses": "COUNT(DISTINCT CASE WHEN dla.sku = 'LIC-FL-APPS' THEN dla.uuid END)",
    "forms_assigned_licenses_sam": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-FORMS%' THEN dla.uuid END)",
    "training_assigned_licenses_sam": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-TRAINING%' THEN dla.uuid END)",
    "commercial_navigation_assigned_licenses_sam": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-COMM-NAV%' THEN dla.uuid END)",
    "qualifications_assigned_licenses_sam": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-QUALS%' THEN dla.uuid END)",
    "maintenance_assigned_licenses_sam": "COUNT(DISTINCT CASE WHEN dla.sku LIKE 'LIC-MAINT%' THEN dla.uuid END)",
    "fleet_apps_assigned_licenses_sam": "COUNT(DISTINCT CASE WHEN dla.sku = 'LIC-FL-APPS' THEN dla.uuid END)",
    "installed_vgs_sam": "SUM(CASE WHEN device_type = 'VG' THEN devices_installed END)",
    "installed_ags_sam": "SUM(CASE WHEN device_type IN ('AG Unpowered', 'AG Powered') THEN devices_installed END)",
    "installed_unpowered_ags_sam": "SUM(CASE WHEN device_type = 'AG Unpowered' THEN devices_installed END)",
    "installed_powered_ags_sam": "SUM(CASE WHEN device_type = 'AG Powered' THEN devices_installed END)",
    "installed_cms_sam": "SUM(CASE WHEN device_type IN ('CM-D', 'CM-S', 'CM-M') THEN devices_installed END)",
    "installed_cm_m_sam": "SUM(CASE WHEN device_type = 'CM-M' THEN devices_installed END)",
    "installed_cm_s_sam": "SUM(CASE WHEN device_type = 'CM-S' THEN devices_installed END)",
    "installed_cm_d_sam": "SUM(CASE WHEN device_type = 'CM-D' THEN devices_installed END)",
    "installed_ats_sam": "SUM(CASE WHEN device_type = 'AT' THEN devices_installed END)",
    "installed_vgs": "COUNT(DISTINCT CASE WHEN a.device_type = 'VG' THEN dd.device_id END)",
    "installed_ags": "COUNT(DISTINCT CASE WHEN a.device_type IN ('AG Unpowered', 'AG Powered') THEN dd.device_id END)",
    "installed_unpowered_ags": "COUNT(DISTINCT CASE WHEN a.device_type = 'AG Unpowered' THEN dd.device_id END)",
    "installed_powered_ags": "COUNT(DISTINCT CASE WHEN a.device_type = 'AG Powered' THEN dd.device_id END)",
    "installed_cms": "COUNT(DISTINCT CASE WHEN a.device_type IN ('CM-D', 'CM-S', 'CM-M') THEN dd.device_id END)",
    "installed_cm_m": "COUNT(DISTINCT CASE WHEN a.device_type = 'CM-M' THEN dd.device_id END)",
    "installed_cm_s": "COUNT(DISTINCT CASE WHEN a.device_type = 'CM-S' THEN dd.device_id END)",
    "installed_cm_d": "COUNT(DISTINCT CASE WHEN a.device_type = 'CM-D' THEN dd.device_id END)",
    "installed_ats": "COUNT(DISTINCT CASE WHEN a.device_type = 'AT' THEN dd.device_id END)",
    "active_vgs_sam": "SUM(CASE WHEN device_type = 'VG' THEN devices_inuse END)",
    "active_ags_sam": "SUM(CASE WHEN device_type IN ('AG Unpowered', 'AG Powered') THEN devices_inuse END)",
    "active_unpowered_ags_sam": "SUM(CASE WHEN device_type = 'AG Unpowered' THEN devices_inuse END)",
    "active_powered_ags_sam": "SUM(CASE WHEN device_type = 'AG Powered' THEN devices_inuse END)",
    "active_cms_sam": "SUM(CASE WHEN device_type IN ('CM-D', 'CM-S', 'CM-M') THEN devices_inuse END)",
    "active_cm_m_sam": "SUM(CASE WHEN device_type = 'CM-M' THEN devices_inuse END)",
    "active_cm_s_sam": "SUM(CASE WHEN device_type = 'CM-S' THEN devices_inuse END)",
    "active_cm_d_sam": "SUM(CASE WHEN device_type = 'CM-D' THEN devices_inuse END)",
    "active_ats_sam": "SUM(CASE WHEN device_type = 'AT' THEN devices_inuse END)",
    "active_vgs": "COUNT(DISTINCT CASE WHEN a.device_type = 'VG' THEN dd.device_id END)",
    "active_ags": "COUNT(DISTINCT CASE WHEN a.device_type IN ('AG Unpowered', 'AG Powered') THEN dd.device_id END)",
    "active_unpowered_ags": "COUNT(DISTINCT CASE WHEN a.device_type = 'AG Unpowered' THEN dd.device_id END)",
    "active_powered_ags": "COUNT(DISTINCT CASE WHEN a.device_type = 'AG Powered' THEN dd.device_id END)",
    "active_cms": "COUNT(DISTINCT CASE WHEN a.device_type IN ('CM-D', 'CM-S', 'CM-M') THEN dd.device_id END)",
    "active_cm_m": "COUNT(DISTINCT CASE WHEN a.device_type = 'CM-M' THEN dd.device_id END)",
    "active_cm_s": "COUNT(DISTINCT CASE WHEN a.device_type = 'CM-S' THEN dd.device_id END)",
    "active_cm_d": "COUNT(DISTINCT CASE WHEN a.device_type = 'CM-D' THEN dd.device_id END)",
    "active_ats": "COUNT(DISTINCT CASE WHEN a.device_type = 'AT' THEN dd.device_id END)",
    "total_drivers": "COUNT(DISTINCT driver_id)",
    "total_drivers_previous_week": "COUNT(DISTINCT driver_id)",
    "driver_app_users": "COUNT(DISTINCT CASE WHEN dd_current.last_mobile_login_date IS NOT NULL THEN CONCAT(dd_current.org_id, '_', dd_current.driver_id) END)",
    "driver_app_users_previous_week": "COUNT(DISTINCT CASE WHEN dd_current.last_mobile_login_date IS NOT NULL THEN CONCAT(dd_current.org_id, '_', dd_current.driver_id) END)",
    "active_driver_app_users": "COUNT(DISTINCT CASE WHEN DATEDIFF('{PARTITION_END}', CAST(dd_current.last_mobile_login_date AS date)) <= 7 THEN CONCAT(dd_current.org_id, '_', dd_current.driver_id) END)",
    "active_driver_app_users_previous_week": "COUNT(DISTINCT CASE WHEN DATEDIFF(DATE_SUB('{PARTITION_END}', 7), CAST(dd_current.last_mobile_login_date AS date)) <= 7 THEN CONCAT(dd_current.org_id, '_', dd_current.driver_id) END)",
    "monthly_active_driver_app_users": "COUNT(DISTINCT CASE WHEN DATEDIFF('{PARTITION_END}', CAST(dd_current.last_mobile_login_date AS date)) <= 30 THEN CONCAT(dd_current.org_id, '_', dd_current.driver_id) END)",
    "admin_app_users": "COUNT(DISTINCT CASE WHEN aa_current.last_fleet_app_usage_date IS NOT NULL THEN aa_current.user_id END)",
    "admin_app_users_previous_week": "COUNT(DISTINCT CASE WHEN aa_current.last_fleet_app_usage_date IS NOT NULL THEN aa_current.user_id END)",
    "active_admin_app_users": "COUNT(DISTINCT CASE WHEN DATEDIFF('{PARTITION_END}', CAST(aa_current.last_fleet_app_usage_date AS DATE)) <= 7 THEN aa_current.user_id END)",
    "active_admin_app_users_previous_week": "COUNT(DISTINCT CASE WHEN DATEDIFF(DATE_SUB('{PARTITION_END}', 7), CAST(aa_current.last_fleet_app_usage_date AS DATE)) <= 7 THEN aa_current.user_id END)",
    "monthly_active_admin_app_users": "COUNT(DISTINCT CASE WHEN DATEDIFF('{PARTITION_END}', CAST(aa_current.last_fleet_app_usage_date AS DATE)) <= 30 THEN aa_current.user_id END)",
    "web_dashboard_users": "COUNT(DISTINCT CASE WHEN aa_current.last_web_usage_date IS NOT NULL THEN aa_current.user_id END)",
    "web_dashboard_users_previous_week": "COUNT(DISTINCT CASE WHEN aa_current.last_web_usage_date IS NOT NULL THEN aa_current.user_id END)",
    "active_web_dashboard_users": "COUNT(DISTINCT CASE WHEN DATEDIFF('{PARTITION_END}', CAST(aa_current.last_web_usage_date AS DATE)) <= 7 THEN aa_current.user_id END)",
    "active_web_dashboard_users_previous_week": "COUNT(DISTINCT CASE WHEN DATEDIFF(DATE_SUB('{PARTITION_END}', 7), CAST(aa_current.last_web_usage_date AS DATE)) <= 7 THEN aa_current.user_id END)",
    "monthly_active_web_dashboard_users": "COUNT(DISTINCT CASE WHEN DATEDIFF('{PARTITION_END}', CAST(aa_current.last_web_usage_date AS DATE)) <= 30 THEN aa_current.user_id END)",
    "marketplace_integrations": "COUNT(DISTINCT da.name)",
    "marketplace_integrations_previous_week": "COUNT(DISTINCT CASE WHEN DATE(dai.installed_at) < DATE_SUB('{PARTITION_END}', 6) THEN da.name END)",
    "marketplace_integrations_list": "COLLECT_SET(DISTINCT da.name)",
    "api_tokens": "COUNT(DISTINCT CASE WHEN DATE(created_at) <= '{PARTITION_END}' THEN id END)",
    "api_tokens_previous_week": "COUNT(DISTINCT CASE WHEN DATE(created_at) < DATE_SUB('{PARTITION_END}', 6) THEN id END)",
    "api_requests": "SUM(CASE WHEN date = '{PARTITION_END}' THEN num_requests END)",
    "weekly_api_requests": "SUM(CASE WHEN date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN num_requests END)",
    "weekly_api_requests_previous_week": "SUM(CASE WHEN date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN num_requests END)",
    "total_alerts_created": "COUNT(DISTINCT alert_config_uuid)",
    "total_alerts_created_previous_week": "COUNT(DISTINCT CASE WHEN DATE(created_ts_utc) < DATE_SUB('{PARTITION_END}', 6) THEN alert_config_uuid END)",
    "alerts_created": "COUNT(DISTINCT CASE WHEN DATE(created_ts_utc) = '{PARTITION_END}' THEN alert_config_uuid END)",
    "weekly_alerts_created": "COUNT(DISTINCT CASE WHEN DATE(created_ts_utc) BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN alert_config_uuid END)",
    "weekly_alerts_created_previous_week": "COUNT(DISTINCT CASE WHEN DATE(created_ts_utc) BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN alert_config_uuid END)",
    "alert_trigger_types": "COUNT(DISTINCT primary_trigger_type_id)",
    "alert_trigger_types_previous_week": "COUNT(DISTINCT CASE WHEN DATE(created_ts_utc) < DATE_SUB('{PARTITION_END}', 6) THEN primary_trigger_type_id END)",
    "alerts_triggered": "COUNT(DISTINCT CASE WHEN i.date = '{PARTITION_END}' THEN CONCAT(i.alert_config_id, '_', i.alert_occurred_at_ms, '_', i.object_id) END)",
    "weekly_alerts_triggered": "COUNT(DISTINCT CASE WHEN i.date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN CONCAT(i.alert_config_id, '_', i.alert_occurred_at_ms, '_', i.object_id) END)",
    "weekly_alerts_triggered_previous_week": "COUNT(DISTINCT CASE WHEN i.date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN CONCAT(i.alert_config_id, '_', i.alert_occurred_at_ms, '_', i.object_id) END)",
    "dual_dashcam_vehicles": "COUNT(DISTINCT CASE WHEN ad.latest_date IS NOT NULL AND p.product_id IN (31, 43, 155) THEN ad.device_id END)",
    "outward_dashcam_vehicles": "COUNT(DISTINCT CASE WHEN ad.latest_date IS NOT NULL AND p.product_id IN (25, 30, 44, 167) THEN ad.device_id END)",
    "camera_connector_vehicles": "COUNT(DISTINCT CASE WHEN ad.latest_date IS NOT NULL AND p.product_id IN (126) THEN ad.device_id END)",
    "ai_multicam_vehicles": "COUNT(DISTINCT CASE WHEN ad.latest_date IS NOT NULL AND p.product_id IN (213) THEN ad.device_id END)",
    "safety_events_captured": "COUNT(DISTINCT t.uuid)",
    "safety_events_viewed": "COUNT(DISTINCT CASE WHEN t.was_event_viewed THEN t.uuid END)",
    "safety_events_coached": "COUNT(DISTINCT ci.event_key)",
    "safety_events_coached_on_time": "COUNT(DISTINCT CASE WHEN DATE(ci.completed_date) <= DATE(ci.due_date) THEN ci.event_key END)",
    "safety_events_with_ser": "COUNT(DISTINCT CASE WHEN serj.job_uuid IS NOT NULL THEN t.uuid END)",
    "devices_with_diagnostics_coverage": "COUNT(DISTINCT CASE WHEN is_covered THEN device_id END)",
    "drivers_with_hos_logs": "COUNT(DISTINCT CONCAT(org_id, '_', driver_id))",
    "fuel_energy_hub_viewers": "COUNT(DISTINCT CASE WHEN scr.date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' AND scr.routename IN ('fleet_fuel_and_energy_summary', 'fleet_fuel_and_energy_benchmarks', 'fleet_fuel_and_energy_performance') THEN {USER_ID_COLUMN} END)",
    "fuel_energy_hub_viewers_previous_week": "COUNT(DISTINCT CASE WHEN scr.date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) AND scr.routename IN ('fleet_fuel_and_energy_summary', 'fleet_fuel_and_energy_benchmarks', 'fleet_fuel_and_energy_performance') THEN {USER_ID_COLUMN} END)",
    "maintenance_status_viewers": "COUNT(DISTINCT CASE WHEN scr.date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' AND scr.routename = 'fleet_maintenance_status' THEN {USER_ID_COLUMN} END)",
    "maintenance_status_viewers_previous_week": "COUNT(DISTINCT CASE WHEN scr.date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) AND scr.routename = 'fleet_maintenance_status' THEN {USER_ID_COLUMN} END)",
    "dvirs": "COUNT(DISTINCT driver_id)",
    "dvir_defects": "COUNT(DISTINCT dvir_id)",
    "resolved_dvirs": "COUNT(DISTINCT CASE WHEN dvir_status = 'resolved' THEN dvir_id END)",
    "vehicles_with_assigned_routes": "COUNT(DISTINCT device_id)",
    "vehicles_with_preventative_maintenance_schedule": "COUNT(DISTINCT device_id)",
    "completed_service_logs": "COUNT(DISTINCT CASE WHEN DATE(vml.serviced_at) = '{PARTITION_END}' THEN vml.id END)",
    "total_unassigned_driving_hours": "SUM(unassigned_driver_time_hours)",
    "electric_vehicles": "COUNT(DISTINCT CASE WHEN ad.latest_date IS NOT NULL AND dft.fuel_group = 4 AND dd.device_type = 'VG - Vehicle Gateway' THEN ad.device_id END)",
    "total_users_with_completed_workflow": "COUNT(DISTINCT CASE WHEN DATE(server_created_at) <= '{PARTITION_END}' THEN COALESCE(submitted_by_polymorphic, assigned_to_polymorphic, created_by_polymorphic) END)",
    "users_with_completed_workflow": "COUNT(DISTINCT CASE WHEN DATE(server_created_at) = '{PARTITION_END}' THEN COALESCE(submitted_by_polymorphic, assigned_to_polymorphic, created_by_polymorphic) END)",
    "total_workflow_templates": "COUNT(DISTINCT CASE WHEN DATE(server_created_at) <= '{PARTITION_END}' THEN uuid END)",
    "workflow_templates": "COUNT(DISTINCT CASE WHEN DATE(server_created_at) = '{PARTITION_END}' THEN uuid END)",
    "total_users_with_completed_training": "COUNT(DISTINCT CASE WHEN DATE(completed_at) <= '{PARTITION_END}' AND product_type = 3 THEN assigned_to_polymorphic END)",
    "users_with_completed_training": "COUNT(DISTINCT CASE WHEN DATE(completed_at) = '{PARTITION_END}' AND product_type = 3 THEN assigned_to_polymorphic END)",
    "total_created_custom_courses": "COUNT(DISTINCT CASE WHEN DATE(FROM_UNIXTIME(c.created_at_ms / 1000)) <= '{PARTITION_END}' THEN c.uuid END)",
    "created_custom_courses": "COUNT(DISTINCT CASE WHEN DATE(FROM_UNIXTIME(c.created_at_ms / 1000)) = '{PARTITION_END}' THEN c.uuid END)",
    "total_geofences": "COUNT(DISTINCT CASE WHEN DATE(a.created_at) <= '{PARTITION_END}' THEN a.id END)",
    "total_geofences_previous_week": "COUNT(DISTINCT CASE WHEN DATE(a.created_at) < DATE_SUB('{PARTITION_END}', 6) THEN a.id END)",
    "geofences": "COUNT(DISTINCT CASE WHEN DATE(a.created_at) = '{PARTITION_END}' THEN a.id END)",
    "weekly_geofences": "COUNT(DISTINCT CASE WHEN DATE(a.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN a.id END)",
    "weekly_geofences_previous_week": "COUNT(DISTINCT CASE WHEN DATE(a.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN a.id END)",
    "weekly_trips": "COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN CONCAT(date, '_', device_id, '_', org_id, '_', start_time_ms, '_', trip_type) END)",
    "weekly_trips_previous_week": "COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN CONCAT(date, '_', device_id, '_', org_id, '_', start_time_ms, '_', trip_type) END)",
    "trips": "COUNT(DISTINCT CASE WHEN date = '{PARTITION_END}' THEN CONCAT(date, '_', device_id, '_', org_id, '_', start_time_ms, '_', trip_type) END)",
    "weekly_trips_with_driver": "COUNT(DISTINCT CASE WHEN driver_id IS NOT NULL AND driver_id != 0 AND date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN CONCAT(date, '_', device_id, '_', org_id, '_', start_time_ms, '_', trip_type) END)",
    "weekly_trips_with_driver_previous_week": "COUNT(DISTINCT CASE WHEN driver_id IS NOT NULL AND driver_id != 0 AND date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN CONCAT(date, '_', device_id, '_', org_id, '_', start_time_ms, '_', trip_type) END)",
    "trips_with_driver": "COUNT(DISTINCT CASE WHEN driver_id IS NOT NULL AND driver_id != 0 AND date = '{PARTITION_END}' THEN CONCAT(date, '_', device_id, '_', org_id, '_', start_time_ms, '_', trip_type) END)",
    "weekly_drivers_assigned_trips": "COUNT(DISTINCT CASE WHEN driver_id IS NOT NULL AND driver_id != 0 AND date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN driver_id END)",
    "weekly_drivers_assigned_trips_previous_week": "COUNT(DISTINCT CASE WHEN driver_id IS NOT NULL AND driver_id != 0 AND date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN driver_id END)",
    "drivers_assigned_trips": "COUNT(DISTINCT CASE WHEN driver_id IS NOT NULL AND driver_id != 0 AND date = '{PARTITION_END}' THEN driver_id END)",
    "total_ai_chat_users": "COUNT(DISTINCT COALESCE(mix.mp_user_id, mix.mp_device_id))",
    "total_ai_chat_users_previous_week": "COUNT(DISTINCT CASE WHEN mix.mp_date < DATE_SUB('{PARTITION_END}', 6) THEN COALESCE(mix.mp_user_id, mix.mp_device_id) END)",
    "weekly_ai_chat_users": "COUNT(DISTINCT CASE WHEN mix.mp_date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN COALESCE(mix.mp_user_id, mix.mp_device_id) END)",
    "weekly_ai_chat_users_previous_week": "COUNT(DISTINCT CASE WHEN mix.mp_date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN COALESCE(mix.mp_user_id, mix.mp_device_id) END)",
    "ai_chat_messages": "COUNT(CASE WHEN mix.mp_date = '{PARTITION_END}' THEN 1 END)",
    "weekly_ai_chat_messages": "COUNT(CASE WHEN mix.mp_date BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN 1 END)",
    "weekly_ai_chat_messages_previous_week": "COUNT(CASE WHEN mix.mp_date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN 1 END)",
    "total_custom_reports": "COUNT(DISTINCT crc.ReportId)",
    "total_custom_reports_previous_week": "COUNT(DISTINCT CASE WHEN DATE(FROM_UNIXTIME(crc.CreatedAt / 1000)) < DATE_SUB('{PARTITION_END}', 6) THEN crc.ReportId END)",
    "custom_reports": "COUNT(DISTINCT CASE WHEN DATE(FROM_UNIXTIME(crc.CreatedAt / 1000)) = '{PARTITION_END}' THEN crc.ReportId END)",
    "weekly_custom_reports": "COUNT(DISTINCT CASE WHEN DATE(FROM_UNIXTIME(crc.CreatedAt / 1000)) BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN crc.ReportId END)",
    "weekly_custom_reports_previous_week": "COUNT(DISTINCT CASE WHEN DATE(FROM_UNIXTIME(crc.CreatedAt / 1000)) BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN crc.ReportId END)",
    "weekly_custom_report_runs": "COUNT(CASE WHEN DATE(rr.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN 1 END)",
    "weekly_custom_report_runs_previous_week": "COUNT(CASE WHEN DATE(rr.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN 1 END)",
    "custom_report_runs": "COUNT(CASE WHEN DATE(rr.created_at) = '{PARTITION_END}' THEN 1 END)",
    "weekly_custom_report_users": "COUNT(DISTINCT CASE WHEN DATE(rr.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 6) AND '{PARTITION_END}' THEN rr.user_id END)",
    "weekly_custom_report_users_previous_week": "COUNT(DISTINCT CASE WHEN DATE(rr.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 13) AND DATE_SUB('{PARTITION_END}', 7) THEN rr.user_id END)",
    "infringements": "COUNT(DISTINCT uuid)",
    "total_asset_utilized_hours": "SUM(metric_numerator)",
    "total_asset_available_hours": "SUM(metric_denominator)",
    "hos_violation_duration_hours": "SUM(metric_numerator / (1000 * 60 * 60))",
    "possible_driver_hours": "SUM(metric_denominator / (1000 * 60 * 60))",
    "mobile_usage_count": "CAST(SUM(metric_numerator) AS BIGINT)",
    "mobile_usage_miles": "CAST(SUM(metric_denominator) * 1000 AS DECIMAL(38,13))",
    "driver_privacy_mode_cameras": "COUNT(DISTINCT CASE WHEN camera_recording_mode = 'Driver' THEN cm_device_id END)",
    "complete_privacy_mode_cameras": "COUNT(DISTINCT CASE WHEN camera_recording_mode = 'Complete' THEN cm_device_id END)",
    "severe_speeding_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.severe_speeding_enabled THEN vg_device_id END)",
    "policy_violations_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.policy_violations_enabled THEN cm_device_id END)",
    "mobile_usage_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.mobile_usage_enabled THEN cm_device_id END)",
    "drowsiness_detection_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.drowsiness_detection_enabled THEN cm_device_id END)",
    "inattentive_driving_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.inattentive_driving_enabled THEN cm_device_id END)",
    "lane_departure_warning_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.lane_departure_warning_enabled THEN cm_device_id END)",
    "forward_collision_warning_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.forward_collision_warning_enabled THEN cm_device_id END)",
    "following_distance_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.following_distance_enabled THEN cm_device_id END)",
    "seatbelt_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.seatbelt_enabled THEN cm_device_id END)",
    "inward_obstruction_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.inward_obstruction_enabled THEN cm_device_id END)",
    "rolling_stop_enabled_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.rolling_stop_enabled THEN cm_device_id END)",
    "severe_speeding_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.severe_speeding_in_cab_audio_alerts_enabled THEN vg_device_id END)",
    "policy_violations_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.policy_violations_audio_alerts_enabled THEN cm_device_id END)",
    "mobile_usage_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.mobile_usage_audio_alerts_enabled THEN cm_device_id END)",
    "drowsiness_detection_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.drowsiness_detection_audio_alerts_enabled THEN cm_device_id END)",
    "inattentive_driving_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.inattentive_driving_audio_alerts_enabled THEN cm_device_id END)",
    "lane_departure_warning_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.lane_departure_warning_audio_alerts_enabled THEN cm_device_id END)",
    "forward_collision_warning_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.forward_collision_warning_audio_alerts_enabled THEN cm_device_id END)",
    "following_distance_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.following_distance_audio_alerts_enabled THEN cm_device_id END)",
    "seatbelt_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.seatbelt_audio_alerts_enabled THEN cm_device_id END)",
    "inward_obstruction_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type = 'inward' AND ddss.inward_obstruction_audio_alerts_enabled THEN cm_device_id END)",
    "rolling_stop_enabled_incab_count": "COUNT(DISTINCT CASE WHEN ddss.cm_type IS NOT NULL AND ddss.rolling_stop_audio_alerts_enabled THEN cm_device_id END)",
}

SCHEMA_V1 = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the organization",
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ARR segment for the account in which the organization is based",
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.SAM_NUMBER,
        },
    },
    {
        "name": "org_id_list",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "All relevant orgs for a given SAM"
        },
    },
    {
        "name": "account_industry",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Industry classification of the account.",
        },
    },
    {
        "name": "number_of_vehicles",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of vehicles in the org (queried from EDW)",
        },
    },
    {
        "name": "severe_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has severe speeding alerts enabled or not"
        },
    },
    {
        "name": "speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has in-cab audio alerts enabled or not for speeding violations"
        },
    },
    {
        "name": "distracted_driving_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has any type of distracted driving alerts (inattentive driving, mobile usage, drowsiness detection) enabled or not"
        },
    },
    {
        "name": "inattentive_driving_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has inattentive driving alerts enabled or not"
        },
    },
    {
        "name": "inattentive_driving_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for inattentive driving violations or not"
        },
    },
    {
        "name": "policy_violations_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has policy violations enabled or not"
        },
    },
    {
        "name": "policy_violations_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled or not for policy violations"
        },
    },
    {
        "name": "mobile_usage_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has mobile usage alerts enabled or not"
        },
    },
    {
        "name": "mobile_usage_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for mobile usage violations or not"
        },
    },
    {
        "name": "seatbelt_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether seatbelt violations are enabled or not for the org"
        },
    },
    {
        "name": "seatbelt_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for seatbelt violations or not"
        },
    },
    {
        "name": "rolling_stop_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether rolling stop violations are enabled or not for the org"
        },
    },
    {
        "name": "in_cab_stop_sign_violation_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab stop sign violations are enabled for the org or not"
        },
    },
    {
        "name": "following_distance_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance violations are enabled for the org or not"
        },
    },
    {
        "name": "following_distance_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for following distance violations or not"
        },
    },
    {
        "name": "forward_collision_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether forward collision warning alerts are enabled or not"
        },
    },
    {
        "name": "forward_collision_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for forward collision violations or not"
        },
    },
    {
        "name": "lane_departure_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warning alerts are enabled or not"
        },
    },
    {
        "name": "lane_departure_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for lane departure warning violations or not"
        },
    },
    {
        "name": "inward_obstruction_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inward obstruction alerts are enabled for the org or not"
        },
    },
    {
        "name": "inward_obstruction_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for inward obstruction violations or not"
        },
    },
    {
        "name": "drowsiness_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether drowsiness detection alerts are enabled for the org or not"
        },
    },
    {
        "name": "drowsiness_detection_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for drowsiness detection violations or not"
        },
    },
    {
        "name": "severe_speeding_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for severe speeding alerts"
        },
    },
    {
        "name": "policy_violations_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for policy violation alerts"
        },
    },
    {
        "name": "mobile_usage_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for mobile usage alerts"
        },
    },
    {
        "name": "drowsiness_detection_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for drowsiness detection alerts"
        },
    },
    {
        "name": "inattentive_driving_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for inattentive driving alerts"
        },
    },
    {
        "name": "lane_departure_warning_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for lane departure warning alerts"
        },
    },
    {
        "name": "forward_collision_warning_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for forward collision warning alerts"
        },
    },
    {
        "name": "following_distance_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for following distance alerts"
        },
    },
    {
        "name": "seatbelt_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for no seatbelt alerts"
        },
    },
    {
        "name": "inward_obstruction_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for inward obstruction alerts"
        },
    },
    {
        "name": "rolling_stop_enabled_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for rolling stop alerts"
        },
    },
    {
        "name": "severe_speeding_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for severe speeding audio alerts"
        },
    },
    {
        "name": "policy_violations_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for policy violation audio alerts"
        },
    },
    {
        "name": "mobile_usage_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for mobile usage audio alerts"
        },
    },
    {
        "name": "drowsiness_detection_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for drowsiness detection audio alerts"
        },
    },
    {
        "name": "inattentive_driving_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for inattentive driving audio alerts"
        },
    },
    {
        "name": "lane_departure_warning_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for lane departure warning audio alerts"
        },
    },
    {
        "name": "forward_collision_warning_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for forward collision warning audio alerts"
        },
    },
    {
        "name": "following_distance_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for following distance audio alerts"
        },
    },
    {
        "name": "seatbelt_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for no seatbelt audio alerts"
        },
    },
    {
        "name": "inward_obstruction_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for inward obstruction audio alerts"
        },
    },
    {
        "name": "rolling_stop_enabled_incab_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of devices who are enabled for rolling stop audio alerts"
        },
    },
    {
        "name": "vg_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a VG SKU (non-additive)"
        },
    },
    {
        "name": "vg_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a VG SKU from last month (non-additive)"
        },
    },
    {
        "name": "cm_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM SKU (non-additive)"
        },
    },
    {
        "name": "cm_m_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM-M SKU (non-additive)"
        },
    },
    {
        "name": "cm_d_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM-D SKU (non-additive)"
        },
    },
    {
        "name": "cm_s_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM-S SKU (non-additive)"
        },
    },
    {
        "name": "cm_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM SKU from last month (non-additive)"
        },
    },
    {
        "name": "cm_m_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM-M SKU from last month (non-additive)"
        },
    },
    {
        "name": "cm_d_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM-D SKU from last month (non-additive)"
        },
    },
    {
        "name": "cm_s_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a CM-S SKU from last month (non-additive)"
        },
    },
    {
        "name": "ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AG SKU (non-additive)"
        },
    },
    {
        "name": "ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AG SKU from last month (non-additive)"
        },
    },
    {
        "name": "powered_ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AG Powered SKU (non-additive)"
        },
    },
    {
        "name": "powered_ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AG Powered SKU from last month (non-additive)"
        },
    },
    {
        "name": "unpowered_ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AG Unpowered SKU (non-additive)"
        },
    },
    {
        "name": "unpowered_ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AG Unpowered SKU from last month (non-additive)"
        },
    },
    {
        "name": "at_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AT SKU (non-additive)"
        },
    },
    {
        "name": "at_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a AT SKU from last month (non-additive)"
        },
    },
    {
        "name": "essential_safety_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Essential tier (non-additive)"
        },
    },
    {
        "name": "essential_safety_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Essential tier from last month (non-additive)"
        },
    },
    {
        "name": "premier_safety_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Premier tier (non-additive)"
        },
    },
    {
        "name": "premier_safety_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Premier tier from last month (non-additive)"
        },
    },
    {
        "name": "enterprise_safety_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Enterprise tier (non-additive)"
        },
    },
     {
        "name": "enterprise_safety_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Enterprise tier from last month (non-additive)"
        },
    },
    {
        "name": "legacy_safety_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Legacy tier (all CM licenses not in the above tiers) (non-additive)"
        },
    },
    {
        "name": "legacy_safety_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Safety Legacy tier from last month (all CM licenses not in the above tiers) (non-additive)"
        },
    },
    {
        "name": "essential_telematics_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Essential tier (non-additive)"
        },
    },
    {
        "name": "essential_telematics_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Essential tier from last month (non-additive)"
        },
    },
    {
        "name": "essential_plus_telematics_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Essential Plus tier (non-additive)"
        },
    },
    {
        "name": "essential_plus_telematics_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Essential Plus tier from last month (non-additive)"
        },
    },
    {
        "name": "premier_telematics_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Premier tier (non-additive)"
        },
    },
    {
        "name": "premier_telematics_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Premier tier from last month (non-additive)"
        },
    },
    {
        "name": "enterprise_telematics_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Enterprise tier (non-additive)"
        },
    },
    {
        "name": "enterprise_telematics_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Enterprise tier from last month (non-additive)"
        },
    },
    {
        "name": "legacy_telematics_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Legacy tier (all VG licenses not in the above tiers) (non-additive)"
        },
    },
    {
        "name": "legacy_telematics_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the Telematics Legacy tier from last month (all VG licenses not in the above tiers) (non-additive)"
        },
    },
    {
        "name": "essential_ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Essential tier (non-additive)"
        },
    },
    {
        "name": "essential_ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Essential tier from last month (non-additive)"
        },
    },
    {
        "name": "premier_ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Premier tier (non-additive)"
        },
    },
    {
        "name": "premier_ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Premier tier from last month (non-additive)"
        },
    },
    {
        "name": "enterprise_ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Enterprise tier (non-additive)"
        },
    },
    {
        "name": "enterprise_ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Enterprise tier from last month (non-additive)"
        },
    },
    {
        "name": "legacy_ag_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Legacy tier (all AG licenses not in the above tiers) (non-additive)"
        },
    },
    {
        "name": "legacy_ag_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses in the AG Legacy tier from last month (all AG licenses not in the above tiers) (non-additive)"
        },
    },
    {
        "name": "forms_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-FORMS SKU (non-additive)"
        },
    },
    {
        "name": "forms_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-FORMS SKU from last month (non-additive)"
        },
    },
    {
        "name": "training_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-TRAINING SKU (non-additive)"
        },
    },
    {
        "name": "training_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-TRAINING SKU from last month (non-additive)"
        },
    },
    {
        "name": "commercial_navigation_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-COMM-NAV SKU (non-additive)"
        },
    },
    {
        "name": "commercial_navigation_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-COMM-NAV SKU from last month (non-additive)"
        },
    },
    {
        "name": "qualifications_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-QUALS SKU (non-additive)"
        },
    },
    {
        "name": "qualifications_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-QUALS SKU from last month (non-additive)"
        },
    },
    {
        "name": "maintenance_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-MAINT SKU (non-additive)"
        },
    },
    {
        "name": "maintenance_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-MAINT SKU from last month (non-additive)"
        },
    },
    {
        "name": "fleet_apps_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-FL-APPS SKU (non-additive)"
        },
    },
    {
        "name": "fleet_apps_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of licenses with a LIC-FL-APPS SKU from the past month (non-additive)"
        },
    },
    {
        "name": "forms_assigned_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FORMS SKU (non-additive)"
        },
    },
    {
        "name": "forms_assigned_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FORMS SKU from last month (non-additive)"
        },
    },
    {
        "name": "forms_assigned_licenses_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FORMS SKU at the SAM level (non-additive)"
        },
    },
    {
        "name": "forms_assigned_licenses_sam_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FORMS SKU at the SAM level from last month (non-additive)"
        },
    },
    {
        "name": "training_assigned_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-TRAINING SKU (non-additive)"
        },
    },
    {
        "name": "training_assigned_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-TRAINING SKU from last month (non-additive)"
        },
    },
    {
        "name": "training_assigned_licenses_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-TRAINING SKU at the SAM level (non-additive)"
        },
    },
    {
        "name": "training_assigned_licenses_sam_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-TRAINING SKU at the SAM level from last month (non-additive)"
        },
    },
    {
        "name": "commercial_navigation_assigned_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-COMM-NAV SKU (non-additive)"
        },
    },
    {
        "name": "commercial_navigation_assigned_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-COMM-NAV SKU from last month (non-additive)"
        },
    },
    {
        "name": "commercial_navigation_assigned_licenses_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-COMM-NAV SKU at the SAM level (non-additive)"
        },
    },
    {
        "name": "commercial_navigation_assigned_licenses_sam_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-COMM-NAV SKU at the SAM level from last month (non-additive)"
        },
    },
    {
        "name": "qualifications_assigned_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-QUALS SKU (non-additive)"
        },
    },
    {
        "name": "qualifications_assigned_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-QUALS SKU from last month (non-additive)"
        },
    },
    {
        "name": "qualifications_assigned_licenses_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-QUALS SKU at the SAM level (non-additive)"
        },
    },
    {
        "name": "qualifications_assigned_licenses_sam_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-QUALS SKU at the SAM level from last month (non-additive)"
        },
    },
    {
        "name": "maintenance_assigned_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-MAINT SKU (non-additive)"
        },
    },
    {
        "name": "maintenance_assigned_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-MAINT SKU from last month (non-additive)"
        },
    },
    {
        "name": "maintenance_assigned_licenses_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-MAINT SKU at the SAM level (non-additive)"
        },
    },
    {
        "name": "maintenance_assigned_licenses_sam_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-MAINT SKU at the SAM level from last month (non-additive)"
        },
    },
    {
        "name": "fleet_apps_assigned_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FL-APPS SKU (non-additive)"
        },
    },
    {
        "name": "fleet_apps_assigned_licenses_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FL-APPS SKU from last month (non-additive)"
        },
    },
    {
        "name": "fleet_apps_assigned_licenses_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FL-APPS SKU at the SAM level (non-additive)"
        },
    },
    {
        "name": "fleet_apps_assigned_licenses_sam_previous_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of assigned licenses with a LIC-FL-APPS SKU at the SAM level from last month (non-additive)"
        },
    },
    {
        "name": "installed_vgs_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed VG devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_ags_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed AG devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_powered_ags_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed powered AG devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_unpowered_ags_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed unpowered AG devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_cms_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_cm_m_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM-M devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_cm_d_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM-D devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_cm_s_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM-S devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_ats_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed AT devices (SAM grouping) (non-additive)"
        },
    },
    {
        "name": "installed_vgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed VG devices (non-additive)"
        },
    },
    {
        "name": "installed_ags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed AG devices (non-additive)"
        },
    },
    {
        "name": "installed_powered_ags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed powered AG devices (non-additive)"
        },
    },
    {
        "name": "installed_unpowered_ags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed unpowered AG devices (non-additive)"
        },
    },
    {
        "name": "installed_cms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM devices (non-additive)"
        },
    },
    {
        "name": "installed_cm_m",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM-M devices (non-additive)"
        },
    },
    {
        "name": "installed_cm_d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM-D devices (non-additive)"
        },
    },
    {
        "name": "installed_cm_s",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed CM-S devices (non-additive)"
        },
    },
    {
        "name": "installed_ats",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed AT devices (non-additive)"
        },
    },
    {
        "name": "active_vgs_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active VG devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_ags_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active AG devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_powered_ags_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active powered AG devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_unpowered_ags_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active unpowered AG devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cms_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cm_m_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM-M devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cm_s_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM-S devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cm_d_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM-D devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_ats_sam",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active AT devices (SAM grouping) within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_vgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active VG devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_ags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active AG devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_powered_ags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active powered AG devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_unpowered_ags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active unpowered AG devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cm_m",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM-M devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cm_s",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM-S devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_cm_d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active CM-D devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "active_ats",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active AT devices within the last 30 days (non-additive)"
        },
    },
    {
        "name": "total_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of total drivers (non-additive)"
        },
    },
    {
        "name": "total_drivers_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of total drivers as of last week (non-additive)"
        },
    },
    {
        "name": "driver_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of total Driver App users (non-additive)"
        },
    },
    {
        "name": "driver_app_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of total Driver App users as of last week (non-additive)"
        },
    },
    {
        "name": "active_driver_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of Driver App users who were active within the last week (non-additive)"
        },
    },
    {
        "name": "active_driver_app_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of Driver App users who were active within the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "monthly_active_driver_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of Driver App users who were active within the last month (non-additive)"
        },
    },
    {
        "name": "admin_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of Admin App users (non-additive)"
        },
    },
    {
        "name": "admin_app_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of Admin App users as of last week (non-additive)"
        },
    },
    {
        "name": "active_admin_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active Admin App users within the last week (non-additive)"
        },
    },
    {
        "name": "active_admin_app_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active Admin App users within the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "monthly_active_admin_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active Admin App users within the last month (non-additive)"
        },
    },
    {
        "name": "web_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of Web Dashboard users (non-additive)"
        },
    },
    {
        "name": "web_dashboard_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of Web Dashboard users as of last week (non-additive)"
        },
    },
    {
        "name": "active_web_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active Web Dashboard users within the last week (non-additive)"
        },
    },
    {
        "name": "active_web_dashboard_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active Web Dashboard users within the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "monthly_active_web_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of active Web Dashboard users within the last month (non-additive)"
        },
    },
    {
        "name": "marketplace_integrations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed apps by an org (non-additive)"
        },
    },
    {
        "name": "marketplace_integrations_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The number of installed apps by an org as of a week ago (non-additive)"
        },
    },
    {
        "name": "marketplace_integrations_list",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "The set of installed marketplace integrations for the org"
        },
    },
    {
        "name": "api_tokens",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of API tokens for the org (non-additive)"
        },
    },
    {
        "name": "api_tokens_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of API tokens for the org as of a week ago (non-additive)"
        },
    },
    {
        "name": "api_requests",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of API requests made (additive)"
        },
    },
    {
        "name": "weekly_api_requests",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of API requests made in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_api_requests_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of API requests made in the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "total_alerts_created",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of alert configs that have been created (non-additive)"
        },
    },
    {
        "name": "total_alerts_created_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of alert configs that have been created as of last week (non-additive)"
        },
    },
    {
        "name": "alerts_created",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of alert configs that were created on the day (additive)"
        },
    },
    {
        "name": "weekly_alerts_created",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of alert configs that were created in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_alerts_created_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of alert configs that were created in the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "alert_trigger_types",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of primary trigger types associated with created alerts (non-additive)"
        },
    },
    {
        "name": "alert_trigger_types_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of primary trigger types associated with created alerts as of last week (non-additive)"
        },
    },
    {
        "name": "alerts_triggered",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of alerts that were fired on the day (additive)"
        },
    },
    {
        "name": "weekly_alerts_triggered",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of alerts that were fired in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_alerts_triggered_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of alerts that were fired in the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "dual_dashcam_vehicles",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed CMs that are CM22/CM32/CM34 (non-additive)"
        },
    },
    {
        "name": "outward_dashcam_vehicles",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed CMs with an outward dashcam product ID (non-additive)"
        },
    },
    {
        "name": "ai_multicam_vehicles",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed AHD4 devices (non-additive)"
        },
    },
    {
        "name": "camera_connector_vehicles",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed AHD1 devices (non-additive)"
        },
    },
    {
        "name": "safety_events_captured",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of safety events (additive)"
        },
    },
    {
        "name": "safety_events_viewed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of viewed safety events (additive)"
        },
    },
    {
        "name": "safety_events_coached",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of coached safety events (additive)"
        },
    },
    {
        "name": "safety_events_coached_on_time",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of coached safety events within 14 days of the event (additive)"
        },
    },
    {
        "name": "safety_events_with_ser",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of safety events with SER (additive)"
        },
    },
    {
        "name": "drivers_with_hos_logs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers with an HoS log (non-additive)"
        },
    },
    {
        "name": "devices_with_diagnostics_coverage",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of devices that are covered by at least one P0 diagnostic stat (non-additive)"
        },
    },
    {
        "name": "fuel_energy_hub_viewers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique viewers on Fuel & Energy Hub-related pages within the last week (non-additive)"
        },
    },
    {
        "name": "fuel_energy_hub_viewers_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique viewers on Fuel & Energy Hub-related pages within the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "maintenance_status_viewers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique viewers on Maintenance Status-related pages within the last week (non-additive)"
        },
    },
    {
        "name": "maintenance_status_viewers_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique viewers on Maintenance Status-related pages within the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "dvirs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique drivers who submitted a DVIR (non-additive)"
        },
    },
    {
        "name": "dvir_defects",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of DVIRs (additive)"
        },
    },
    {
        "name": "resolved_dvirs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of resolved DVIRs (additive)"
        },
    },
    {
        "name": "vehicles_with_assigned_routes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of device IDs in the routes table (non-additive)"
        },
    },
    {
        "name": "vehicles_with_preventative_maintenance_schedule",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Devices with a preventative maintenance schedules for the org (this is all-time as there's no current way to track when it was created)"
        },
    },
    {
        "name": "electric_vehicles",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Active VGs within the last 90 days with an EV fuel category (non-additive)"
        },
    },
    {
        "name": "total_users_with_completed_workflow",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Users with a completed form submission all-time (non-additive)"
        },
    },
    {
        "name": "users_with_completed_workflow",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Users with a completed form submission on the day (non-additive)"
        },
    },
    {
        "name": "total_workflow_templates",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Created workflow templates for the org all-time (additive)"
        },
    },
    {
        "name": "workflow_templates",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Created workflow templates for the org on the day (additive)"
        },
    },
    {
        "name": "total_users_with_completed_training",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Users that have completed a training all-time (non-additive)"
        },
    },
    {
        "name": "users_with_completed_training",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Users that have completed a training on the day (non-additive)"
        },
    },
    {
        "name": "total_created_custom_courses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Custom courses that are created all-time (additive)"
        },
    },
    {
        "name": "created_custom_courses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Custom courses that are created on the day (additive)"
        },
    },
    {
        "name": "vg_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether VGs in the SAM are under utilized or not"
        },
    },
    {
        "name": "vg_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether VGs in the SAM are over utilized or not"
        },
    },
    {
        "name": "vg_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether VGs in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "vg_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether VGs in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "cm_m_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ms in the SAM are under utilized or not"
        },
    },
    {
        "name": "cm_m_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ms in the SAM are over utilized or not"
        },
    },
    {
        "name": "cm_m_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ms in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "cm_m_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ms in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "cm_s_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ss in the SAM are under utilized or not"
        },
    },
    {
        "name": "cm_s_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ss in the SAM are over utilized or not"
        },
    },
    {
        "name": "cm_s_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ss in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "cm_s_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ss in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "cm_d_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ds in the SAM are under utilized or not"
        },
    },
    {
        "name": "cm_d_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ds in the SAM are over utilized or not"
        },
    },
    {
        "name": "cm_d_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ds in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "cm_d_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether CM-Ds in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "at_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether ATs in the SAM are under utilized or not"
        },
    },
    {
        "name": "at_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether ATs in the SAM are over utilized or not"
        },
    },
    {
        "name": "at_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether ATs in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "at_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether ATs in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "ag_powered_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Powered in the SAM are under utilized or not"
        },
    },
    {
        "name": "ag_powered_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Powered in the SAM are over utilized or not"
        },
    },
    {
        "name": "ag_powered_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Powered in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "ag_powered_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Powered in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "ag_unpowered_under_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Unpowered in the SAM are under utilized or not"
        },
    },
    {
        "name": "ag_unpowered_over_utilized",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Unpowered in the SAM are over utilized or not"
        },
    },
    {
        "name": "ag_unpowered_under_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Unpowered in the SAM were under utilized in the previous month"
        },
    },
    {
        "name": "ag_unpowered_over_utilized_previous_month",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether AG-Unpowered in the SAM were over utilized in the previous month"
        },
    },
    {
        "name": "total_geofences",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total geofences that are created all-time (non-additive)"
        },
    },
    {
        "name": "total_geofences_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total geofences that were created as of a week ago (non-additive)"
        },
    },
    {
        "name": "geofences",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total geofences that are created on the day (additive)"
        },
    },
    {
        "name": "weekly_geofences",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total geofences that were created this week (non-additive)"
        },
    },
    {
        "name": "weekly_geofences_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total geofences that were created in the previous week (non-additive)"
        },
    },
    {
        "name": "weekly_trips",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total trips that are taken in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_trips_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total trips that are taken in the previous week (non-additive)"
        },
    },
    {
        "name": "trips",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total trips that are taken on the day (additive)"
        },
    },
    {
        "name": "weekly_trips_with_driver",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total trips that are taken in the last week with an assigned driver (non-additive)"
        },
    },
    {
        "name": "weekly_trips_with_driver_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total trips that are taken in the previous week with an assigned driver (non-additive)"
        },
    },
    {
        "name": "trips_with_driver",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total trips that are taken on the day with an assigned driver (additive)"
        },
    },
    {
        "name": "weekly_drivers_assigned_trips",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers assigned to a trip in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_drivers_assigned_trips_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers assigned to a trip in the previous week (non-additive)"
        },
    },
    {
        "name": "drivers_assigned_trips",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers that are assigned to a trip on the day (non-additive)"
        },
    },
    {
        "name": "total_ai_chat_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat users all-time (non-additive)"
        },
    },
    {
        "name": "total_ai_chat_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat users all-time as of a week ago (non-additive)"
        },
    },
    {
        "name": "weekly_ai_chat_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat users in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_ai_chat_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat users in the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "ai_chat_messages",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat messages on the day (additive)"
        },
    },
    {
        "name": "weekly_ai_chat_messages",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat messages in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_ai_chat_messages_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of AI chat messages in the last week as of a week ago (non-additive)"
        },
    },
    {
        "name": "total_custom_reports",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports created (non-additive)"
        },
    },
    {
        "name": "total_custom_reports_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports created as of a week ago (non-additive)"
        },
    },
    {
        "name": "custom_reports",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports created on the day (additive)"
        },
    },
    {
        "name": "weekly_custom_reports",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports created within the last week (additive)"
        },
    },
    {
        "name": "weekly_custom_reports_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports created within the week prior to the last week (additive)"
        },
    },
    {
        "name": "weekly_custom_report_runs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports run in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_custom_report_runs_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports run in the previous week (non-additive)"
        },
    },
    {
        "name": "custom_report_runs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom reports run on the day (additive)"
        },
    },
    {
        "name": "weekly_custom_report_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom report users in the last week (non-additive)"
        },
    },
    {
        "name": "weekly_custom_report_users_previous_week",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom report users in the week prior to the last week (non-additive)"
        },
    },
    {
        "name": "completed_service_logs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Service logs that are completed on the day (additive)"
        },
    },
    {
        "name": "total_unassigned_driving_hours",
        "type": "decimal(38,7)",
        "nullable": False,
        "metadata": {
            "comment": "Total unassigned driving hours for the day (additive)"
        },
    },
    {
        "name": "infringements",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of driver infringements for the day (additive)"
        },
    },
    {
        "name": "driver_privacy_mode_cameras",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of driver privacy mode cameras (non-additive)"
        },
    },
    {
        "name": "complete_privacy_mode_cameras",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of complete privacy mode cameras (non-additive)"
        },
    },
    {
        "name": "total_asset_utilized_hours",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total asset utilization hours for the day (additive)"
        },
    },
    {
        "name": "total_asset_available_hours",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total asset available hours for the day (additive)"
        },
    },
    {
        "name": "hos_violation_duration_hours",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total HoS violation hours for the day (additive)"
        },
    },
    {
        "name": "possible_driver_hours",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total possible driver hours for the day (additive)"
        },
    },
    {
        "name": "mobile_usage_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total mobile usage violation count for the day (additive)"
        },
    },
    {
        "name": "mobile_usage_miles",
        "type": "decimal(38,13)",
        "nullable": False,
        "metadata": {
            "comment": "Total miles covered by vehicles enabled for mobile usage for the day (additive)"
        },
    },
]

QUERY_V1 = """
WITH global_orgs AS (
    -- Get orgs for base sample
    SELECT
        org.org_id,
        org.org_name,
        org.sam_number,
        org.salesforce_sam_number,
        org.account_arr_segment,
        org.account_industry,
        org.date,
        GREATEST(CAST(COALESCE(account.number_of_vehicles, 0.0) AS BIGINT), 0) AS number_of_vehicles
    FROM datamodel_core.dim_organizations org
    LEFT OUTER JOIN {EDW_DATA_SOURCE}.silver.dim_customer account
        ON account.account_id = org.account_id
    WHERE org.date = '{PARTITION_END}'
      AND internal_type = 0
),
org_list AS (
    -- Get orgs for base sample
    SELECT
        sam_number,
        org_ids AS org_id_list
    FROM {SAM_ACCOUNT_ORG_XREF}
    WHERE
        is_valid = TRUE
        AND is_deleted = FALSE
        AND _run_dt = (SELECT MAX(_run_dt) FROM {SAM_ACCOUNT_ORG_XREF})
),
sam_to_org AS (
    SELECT
        sam_number,
        org_id
    FROM {SAM_ACCOUNT_ORG_XREF}
    LATERAL VIEW EXPLODE(org_ids) AS org_id
    WHERE
        is_valid = TRUE
        AND is_deleted = FALSE
        AND _run_dt = (SELECT MAX(_run_dt) FROM {SAM_ACCOUNT_ORG_XREF})
),
all_hardwares AS (
  SELECT DISTINCT
    device_type,
    COLLECT_SET(hardware_sku) AS all_hardwares
  FROM {LICENSE_HW_MAPPING_TABLE}
  LATERAL VIEW EXPLODE(hardware_skus) AS hardware_sku
  WHERE is_core_license
  AND device_type IN ('AT', 'AG Powered', 'AG Unpowered', 'VG', 'CM-D', 'CM-M', 'CM-S')
  GROUP BY 1
),
installed_counts_org AS (
  SELECT
    '{PARTITION_END}' AS date,
    dd.org_id,
    {INSTALLED_VGS},
    {INSTALLED_AGS},
    {INSTALLED_CMS},
    {INSTALLED_CM_D},
    {INSTALLED_CM_S},
    {INSTALLED_CM_M},
    {INSTALLED_ATS},
    {INSTALLED_UNPOWERED_AGS},
    {INSTALLED_POWERED_AGS}
  FROM datamodel_core.dim_devices dd
  JOIN datamodel_core.dim_product_variants dpv
    ON dd.variant_id = dpv.variant_id
    AND dd.product_id = dpv.product_id
  JOIN datamodel_core.lifetime_device_online o
    ON dd.device_id = o.device_id
    AND dd.org_id = o.org_id
    AND dd.date = o.date
  JOIN all_hardwares a
    ON array_contains(a.all_hardwares, dpv.hw_variant_name)
  WHERE o.date = '{PARTITION_END}'
  GROUP BY 1, 2
),
active_counts_org AS (
  SELECT
    '{PARTITION_END}' AS date,
    dd.org_id,
    {ACTIVE_VGS},
    {ACTIVE_AGS},
    {ACTIVE_CMS},
    {ACTIVE_CM_D},
    {ACTIVE_CM_S},
    {ACTIVE_CM_M},
    {ACTIVE_ATS},
    {ACTIVE_UNPOWERED_AGS},
    {ACTIVE_POWERED_AGS}
  FROM datamodel_core.dim_devices dd
  JOIN datamodel_core.dim_product_variants dpv
    ON dd.variant_id = dpv.variant_id
    AND dd.product_id = dpv.product_id
  JOIN datamodel_core.lifetime_device_activity o
    ON dd.device_id = o.device_id
    AND dd.org_id = o.org_id
    AND dd.date = o.date
  JOIN all_hardwares a
    ON array_contains(a.all_hardwares, dpv.hw_variant_name)
  WHERE
    o.date = '{PARTITION_END}'
    AND o.latest_date BETWEEN DATE_SUB('{PARTITION_END}', 29) AND '{PARTITION_END}'
  GROUP BY 1, 2
),
kpi_base AS (
    SELECT
        dt._run_dt,
        dt.sam_number,
        c.org_id,
        dt.device_type,
        dt.net_quantity,
        dt.devices_installed,
        dt.devices_inuse,
        dt.is_underutilized,
        dt.is_overutilized
    FROM {KPI_TABLE} dt
    JOIN sam_to_org c
        ON dt.sam_number = c.sam_number
    WHERE dt._run_dt IN ('{PARTITION_END}', DATE_SUB('{PARTITION_END}', 30))
),
installed_devices_sam AS (
    SELECT
        '{PARTITION_END}' AS date,
        sam_number,
        {INSTALLED_VGS_SAM},
        {INSTALLED_AGS_SAM},
        {INSTALLED_CMS_SAM},
        {INSTALLED_CM_D_SAM},
        {INSTALLED_CM_S_SAM},
        {INSTALLED_CM_M_SAM},
        {INSTALLED_ATS_SAM},
        {INSTALLED_UNPOWERED_AGS_SAM},
        {INSTALLED_POWERED_AGS_SAM}
    FROM {KPI_TABLE}
    WHERE _run_dt = '{PARTITION_END}'
    GROUP BY 1, 2
),
active_devices_sam AS (
    SELECT
        '{PARTITION_END}' AS date,
        sam_number,
        {ACTIVE_VGS_SAM},
        {ACTIVE_AGS_SAM},
        {ACTIVE_POWERED_AGS_SAM},
        {ACTIVE_CMS_SAM},
        {ACTIVE_CM_M_SAM},
        {ACTIVE_CM_S_SAM},
        {ACTIVE_CM_D_SAM},
        {ACTIVE_ATS_SAM},
        {ACTIVE_UNPOWERED_AGS_SAM}
    FROM {KPI_TABLE}
    WHERE _run_dt = '{PARTITION_END}'
    GROUP BY 1, 2
),
licenses_deduped AS (
    SELECT
        a._run_dt AS date,
        c.org_id,
        a.sam_number,
        a.product_sku AS sku,
        b.sub_product_line,
        b.device_type,
        b.hardware_skus,
        b.is_core_license,
        b.is_legacy_license,
        b.tier,
        ANY_VALUE(a.net_quantity) AS quantity
    FROM {LICENSE_TABLE} a
    JOIN {LICENSE_HW_MAPPING_TABLE} b
        ON a.product_sku = b.license_sku
    JOIN sam_to_org c
        ON a.sam_number = c.sam_number
    WHERE
        a._run_dt = '{PARTITION_END}'
        AND a.net_quantity > 0 -- at least one license
    GROUP BY ALL
),
licenses_deduped_previous_month AS (
    SELECT
        a._run_dt AS date,
        c.org_id,
        a.sam_number,
        a.product_sku AS sku,
        b.sub_product_line,
        b.device_type,
        b.hardware_skus,
        b.is_core_license,
        b.is_legacy_license,
        b.tier,
        ANY_VALUE(a.net_quantity) AS quantity
    FROM {LICENSE_TABLE} a
    JOIN {LICENSE_HW_MAPPING_TABLE} b
        ON a.product_sku = b.license_sku
    JOIN sam_to_org c
        ON a.sam_number = c.sam_number
    WHERE
        a._run_dt = DATE_SUB('{PARTITION_END}', 30)
        AND a.net_quantity > 0 -- at least one license
    GROUP BY ALL
),
device_licenses AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {VG_LICENSES},
        {CM_LICENSES},
        {CM_M_LICENSES},
        {CM_D_LICENSES},
        {CM_S_LICENSES},
        {AG_LICENSES},
        {POWERED_AG_LICENSES},
        {UNPOWERED_AG_LICENSES},
        {AT_LICENSES}
    FROM kpi_base
    WHERE _run_dt = '{PARTITION_END}'
    GROUP BY 1, 2
),
under_over_utilized AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        MAX(COALESCE(CASE WHEN device_type = 'VG' THEN is_underutilized END, FALSE)) AS vg_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'VG' THEN is_overutilized END, FALSE)) AS vg_over_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'CM-M' THEN is_underutilized END, FALSE)) AS cm_m_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'CM-M' THEN is_overutilized END, FALSE)) AS cm_m_over_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'CM-S' THEN is_underutilized END, FALSE)) AS cm_s_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'CM-S' THEN is_overutilized END, FALSE)) AS cm_s_over_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'CM-D' THEN is_underutilized END, FALSE)) AS cm_d_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'CM-D' THEN is_overutilized END, FALSE)) AS cm_d_over_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'AT' THEN is_underutilized END, FALSE)) AS at_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'AT' THEN is_overutilized END, FALSE)) AS at_over_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'AG Powered' THEN is_underutilized END, FALSE)) AS ag_powered_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'AG Powered' THEN is_overutilized END, FALSE)) AS ag_powered_over_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'AG Unpowered' THEN is_underutilized END, FALSE)) AS ag_unpowered_under_utilized,
        MAX(COALESCE(CASE WHEN device_type = 'AG Unpowered' THEN is_overutilized END, FALSE)) AS ag_unpowered_over_utilized
    FROM kpi_base
    WHERE _run_dt = '{PARTITION_END}'
    GROUP BY 1, 2
),
device_licenses_previous_month AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {VG_LICENSES},
        {CM_LICENSES},
        {CM_M_LICENSES},
        {CM_D_LICENSES},
        {CM_S_LICENSES},
        {AG_LICENSES},
        {POWERED_AG_LICENSES},
        {UNPOWERED_AG_LICENSES},
        {AT_LICENSES}
    FROM kpi_base
    WHERE _run_dt = DATE_SUB('{PARTITION_END}', 30)
    GROUP BY 1, 2
),
device_licenses_previous_month_renamed AS (
    SELECT
        date,
        org_id,
        vg_licenses AS vg_licenses_previous_month,
        cm_licenses AS cm_licenses_previous_month,
        cm_m_licenses AS cm_m_licenses_previous_month,
        cm_d_licenses AS cm_d_licenses_previous_month,
        cm_s_licenses AS cm_s_licenses_previous_month,
        ag_licenses AS ag_licenses_previous_month,
        powered_ag_licenses AS powered_ag_licenses_previous_month,
        unpowered_ag_licenses AS unpowered_ag_licenses_previous_month,
        at_licenses AS at_licenses_previous_month
    FROM device_licenses_previous_month
),
under_over_utilized_previous_month AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        MAX(COALESCE(CASE WHEN device_type = 'VG' THEN is_underutilized END, FALSE)) AS vg_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'VG' THEN is_overutilized END, FALSE)) AS vg_over_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'CM-M' THEN is_underutilized END, FALSE)) AS cm_m_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'CM-M' THEN is_overutilized END, FALSE)) AS cm_m_over_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'CM-S' THEN is_underutilized END, FALSE)) AS cm_s_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'CM-S' THEN is_overutilized END, FALSE)) AS cm_s_over_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'CM-D' THEN is_underutilized END, FALSE)) AS cm_d_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'CM-D' THEN is_overutilized END, FALSE)) AS cm_d_over_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'AT' THEN is_underutilized END, FALSE)) AS at_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'AT' THEN is_overutilized END, FALSE)) AS at_over_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'AG Powered' THEN is_underutilized END, FALSE)) AS ag_powered_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'AG Powered' THEN is_overutilized END, FALSE)) AS ag_powered_over_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'AG Unpowered' THEN is_underutilized END, FALSE)) AS ag_unpowered_under_utilized_previous_month,
        MAX(COALESCE(CASE WHEN device_type = 'AG Unpowered' THEN is_overutilized END, FALSE)) AS ag_unpowered_over_utilized_previous_month
    FROM kpi_base
    WHERE _run_dt = DATE_SUB('{PARTITION_END}', 30)
    GROUP BY 1, 2
),
licenses AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {FORMS_LICENSES},
        {TRAINING_LICENSES},
        {COMMERCIAL_NAVIGATION_LICENSES},
        {QUALIFICATIONS_LICENSES},
        {MAINTENANCE_LICENSES},
        {FLEET_APPS_LICENSES},
        {ESSENTIAL_SAFETY_LICENSES},
        {PREMIER_SAFETY_LICENSES},
        {ENTERPRISE_SAFETY_LICENSES},
        {LEGACY_SAFETY_LICENSES},
        {ESSENTIAL_TELEMATICS_LICENSES},
        {ESSENTIAL_PLUS_TELEMATICS_LICENSES},
        {PREMIER_TELEMATICS_LICENSES},
        {ENTERPRISE_TELEMATICS_LICENSES},
        {LEGACY_TELEMATICS_LICENSES},
        {ESSENTIAL_AG_LICENSES},
        {PREMIER_AG_LICENSES},
        {ENTERPRISE_AG_LICENSES},
        {LEGACY_AG_LICENSES}
    FROM licenses_deduped
    GROUP BY 1, 2
),
licenses_previous_month AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {FORMS_LICENSES},
        {TRAINING_LICENSES},
        {COMMERCIAL_NAVIGATION_LICENSES},
        {QUALIFICATIONS_LICENSES},
        {MAINTENANCE_LICENSES},
        {FLEET_APPS_LICENSES},
        {ESSENTIAL_SAFETY_LICENSES},
        {PREMIER_SAFETY_LICENSES},
        {ENTERPRISE_SAFETY_LICENSES},
        {LEGACY_SAFETY_LICENSES},
        {ESSENTIAL_TELEMATICS_LICENSES},
        {ESSENTIAL_PLUS_TELEMATICS_LICENSES},
        {PREMIER_TELEMATICS_LICENSES},
        {ENTERPRISE_TELEMATICS_LICENSES},
        {LEGACY_TELEMATICS_LICENSES},
        {ESSENTIAL_AG_LICENSES},
        {PREMIER_AG_LICENSES},
        {ENTERPRISE_AG_LICENSES},
        {LEGACY_AG_LICENSES}
    FROM licenses_deduped_previous_month
    GROUP BY 1, 2
),
licenses_previous_month_renamed AS (
    SELECT
        date,
        org_id,
        forms_licenses AS forms_licenses_previous_month,
        training_licenses AS training_licenses_previous_month,
        commercial_navigation_licenses AS commercial_navigation_licenses_previous_month,
        qualifications_licenses AS qualifications_licenses_previous_month,
        fleet_apps_licenses AS fleet_apps_licenses_previous_month,
        maintenance_licenses AS maintenance_licenses_previous_month,
        essential_safety_licenses AS essential_safety_licenses_previous_month,
        premier_safety_licenses AS premier_safety_licenses_previous_month,
        enterprise_safety_licenses AS enterprise_safety_licenses_previous_month,
        legacy_safety_licenses AS legacy_safety_licenses_previous_month,
        essential_telematics_licenses AS essential_telematics_licenses_previous_month,
        essential_plus_telematics_licenses AS essential_plus_telematics_licenses_previous_month,
        premier_telematics_licenses AS premier_telematics_licenses_previous_month,
        enterprise_telematics_licenses AS enterprise_telematics_licenses_previous_month,
        legacy_telematics_licenses AS legacy_telematics_licenses_previous_month,
        essential_ag_licenses AS essential_ag_licenses_previous_month,
        premier_ag_licenses AS premier_ag_licenses_previous_month,
        enterprise_ag_licenses AS enterprise_ag_licenses_previous_month,
        legacy_ag_licenses AS legacy_ag_licenses_previous_month
    FROM licenses_previous_month
),
license_assignment_base AS (
    SELECT
        dla.date AS dla_date,
        dla.org_id,
        dla.sam_number AS dla_sam_number,
        dla.sku,
        dla.uuid
    FROM datamodel_platform.dim_license_assignment dla
    WHERE
        dla.date IN ('{PARTITION_END}', DATE_SUB('{PARTITION_END}', 30))
        AND dla.is_expired = FALSE
        AND (
            (dla.date = '{PARTITION_END}' AND COALESCE(DATE(dla.end_time), '{PARTITION_END}') >= '{PARTITION_END}')
            OR (dla.date = DATE_SUB('{PARTITION_END}', 30) AND COALESCE(DATE(dla.end_time), DATE_SUB('{PARTITION_END}', 30)) >= DATE_SUB('{PARTITION_END}', 30))
        )
),
assigned_licenses AS (
    SELECT
        '{PARTITION_END}' AS date,
        c.org_id,
        {FORMS_ASSIGNED_LICENSES},
        {TRAINING_ASSIGNED_LICENSES},
        {COMMERCIAL_NAVIGATION_ASSIGNED_LICENSES},
        {QUALIFICATIONS_ASSIGNED_LICENSES},
        {MAINTENANCE_ASSIGNED_LICENSES},
        {FLEET_APPS_ASSIGNED_LICENSES}
    FROM license_assignment_base dla
    JOIN licenses_deduped ld
        ON dla.org_id = ld.org_id
        AND ld.sku = dla.sku
    JOIN sam_to_org c
        ON dla.dla_sam_number = c.sam_number
    WHERE
        dla.dla_date = '{PARTITION_END}'
    GROUP BY 1, 2
),
assigned_licenses_previous_month AS (
    SELECT
        '{PARTITION_END}' AS date,
        c.org_id,
        {FORMS_ASSIGNED_LICENSES},
        {TRAINING_ASSIGNED_LICENSES},
        {COMMERCIAL_NAVIGATION_ASSIGNED_LICENSES},
        {QUALIFICATIONS_ASSIGNED_LICENSES},
        {MAINTENANCE_ASSIGNED_LICENSES},
        {FLEET_APPS_ASSIGNED_LICENSES}
    FROM license_assignment_base dla
    JOIN licenses_deduped_previous_month ld
        ON dla.org_id = ld.org_id
        AND ld.sku = dla.sku
    JOIN sam_to_org c
        ON dla.dla_sam_number = c.sam_number
    WHERE
        dla.dla_date = DATE_SUB('{PARTITION_END}', 30)
    GROUP BY 1, 2
),
assigned_licenses_previous_month_renamed AS (
    SELECT
        date,
        org_id,
        forms_assigned_licenses AS forms_assigned_licenses_previous_month,
        training_assigned_licenses AS training_assigned_licenses_previous_month,
        commercial_navigation_assigned_licenses AS commercial_navigation_assigned_licenses_previous_month,
        qualifications_assigned_licenses AS qualifications_assigned_licenses_previous_month,
        maintenance_assigned_licenses AS maintenance_assigned_licenses_previous_month,
        fleet_apps_assigned_licenses AS fleet_apps_assigned_licenses_previous_month
    FROM assigned_licenses_previous_month
),
assigned_licenses_sam AS (
    SELECT
        '{PARTITION_END}' AS date,
        c.sam_number,
        {FORMS_ASSIGNED_LICENSES_SAM},
        {TRAINING_ASSIGNED_LICENSES_SAM},
        {COMMERCIAL_NAVIGATION_ASSIGNED_LICENSES_SAM},
        {QUALIFICATIONS_ASSIGNED_LICENSES_SAM},
        {MAINTENANCE_ASSIGNED_LICENSES_SAM},
        {FLEET_APPS_ASSIGNED_LICENSES_SAM}
    FROM license_assignment_base dla
    JOIN licenses_deduped ld
        ON dla.dla_sam_number = ld.sam_number
        AND ld.sku = dla.sku
    JOIN global_orgs g
        ON g.org_id = dla.org_id
    JOIN sam_to_org c
        ON COALESCE(g.salesforce_sam_number, g.sam_number) = c.sam_number
    WHERE
        dla.dla_date = '{PARTITION_END}'
    GROUP BY 1, 2
),
assigned_licenses_sam_previous_month AS (
    SELECT
        '{PARTITION_END}' AS date,
        c.sam_number,
        {FORMS_ASSIGNED_LICENSES_SAM},
        {TRAINING_ASSIGNED_LICENSES_SAM},
        {COMMERCIAL_NAVIGATION_ASSIGNED_LICENSES_SAM},
        {QUALIFICATIONS_ASSIGNED_LICENSES_SAM},
        {MAINTENANCE_ASSIGNED_LICENSES_SAM},
        {FLEET_APPS_ASSIGNED_LICENSES_SAM}
    FROM license_assignment_base dla
    JOIN licenses_deduped_previous_month ld
        ON dla.dla_sam_number = ld.sam_number
        AND ld.sku = dla.sku
    JOIN global_orgs g
        ON g.org_id = dla.org_id
    JOIN sam_to_org c
        ON COALESCE(g.salesforce_sam_number, g.sam_number) = c.sam_number
    WHERE
        dla.dla_date = DATE_SUB('{PARTITION_END}', 30)
    GROUP BY 1, 2
),
assigned_licenses_sam_previous_month_renamed AS (
    SELECT
        date,
        sam_number,
        forms_assigned_licenses_sam AS forms_assigned_licenses_sam_previous_month,
        training_assigned_licenses_sam AS training_assigned_licenses_sam_previous_month,
        commercial_navigation_assigned_licenses_sam AS commercial_navigation_assigned_licenses_sam_previous_month,
        qualifications_assigned_licenses_sam AS qualifications_assigned_licenses_sam_previous_month,
        maintenance_assigned_licenses_sam AS maintenance_assigned_licenses_sam_previous_month,
        fleet_apps_assigned_licenses_sam AS fleet_apps_assigned_licenses_sam_previous_month
    FROM assigned_licenses_sam_previous_month
),
vehicles AS (
    SELECT
        '{PARTITION_END}' AS date,
        ad.org_id,
        {DUAL_DASHCAM_VEHICLES},
        {OUTWARD_DASHCAM_VEHICLES},
        {AI_MULTICAM_VEHICLES},
        {CAMERA_CONNECTOR_VEHICLES},
        {ELECTRIC_VEHICLES}
    FROM datamodel_core.lifetime_device_online ad
    JOIN datamodel_core.dim_devices dd
        ON ad.org_id = dd.org_id
        AND ad.device_id = dd.device_id
    JOIN definitions.products p
        ON dd.product_id = p.product_id
    LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dft
        ON dft.device_id = dd.device_id
        AND dft.org_id = dd.org_id
        AND dft.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
    WHERE
        ad.date = '{PARTITION_END}'
        AND dd.date = '{PARTITION_END}'
    GROUP BY 1, 2
),
driver_activity AS (
    SELECT
        '{PARTITION_END}' AS date,
        dd_current.org_id,
        {TOTAL_DRIVERS},
        {DRIVER_APP_USERS},
        {ACTIVE_DRIVER_APP_USERS},
        {MONTHLY_ACTIVE_DRIVER_APP_USERS}
    FROM datamodel_platform.dim_drivers dd_current
    WHERE
        dd_current.date = GREATEST('{PARTITION_END}', '2024-05-01')
        AND is_deleted = FALSE
    GROUP BY 1, 2
),
driver_activity_previous_week AS (
    SELECT
        '{PARTITION_END}' AS date,
        dd_current.org_id,
        {TOTAL_DRIVERS_PREVIOUS_WEEK},
        {DRIVER_APP_USERS_PREVIOUS_WEEK},
        {ACTIVE_DRIVER_APP_USERS_PREVIOUS_WEEK}
    FROM datamodel_platform.dim_drivers dd_current
    WHERE
        dd_current.date = GREATEST(CAST(DATE_SUB('{PARTITION_END}', 7) AS STRING), '2024-05-01')
        AND is_deleted = FALSE
    GROUP BY 1, 2
),
internal_users AS (
    SELECT
        '{PARTITION_END}' AS date,
        aa_current.user_id,
        aa_current.email
    FROM datamodel_platform.dim_users aa_current
    WHERE
        aa_current.date = GREATEST('{PARTITION_END}', '2024-05-01')
        AND (
                aa_current.is_samsara_email = TRUE
                OR aa_current.email LIKE '%samsara.canary%'
                OR aa_current.email LIKE '%samsara.forms.canary%'
                OR aa_current.email LIKE '%samsaracanarydevcontractor%'
                OR aa_current.email LIKE '%samsaratest%'
                OR aa_current.email LIKE '%@samsara%'
                OR aa_current.email LIKE '%@samsara-service-account.com'
        )
),
internal_users_previous_week AS (
    SELECT
        DATE_SUB('{PARTITION_END}', 7) AS date,
        aa_current.user_id,
        aa_current.email
    FROM datamodel_platform.dim_users aa_current
    WHERE
        aa_current.date = GREATEST(CAST(DATE_SUB('{PARTITION_END}', 7) AS STRING), '2024-05-01')
        AND (
                aa_current.is_samsara_email = TRUE
                OR aa_current.email LIKE '%samsara.canary%'
                OR aa_current.email LIKE '%samsara.forms.canary%'
                OR aa_current.email LIKE '%samsaracanarydevcontractor%'
                OR aa_current.email LIKE '%samsaratest%'
                OR aa_current.email LIKE '%@samsara%'
                OR aa_current.email LIKE '%@samsara-service-account.com'
        )
),
admin_activity AS (
    SELECT
        '{PARTITION_END}' AS date,
        aa_current.org_id,
        {ADMIN_APP_USERS},
        {ACTIVE_ADMIN_APP_USERS},
        {MONTHLY_ACTIVE_ADMIN_APP_USERS},
        {WEB_DASHBOARD_USERS},
        {ACTIVE_WEB_DASHBOARD_USERS},
        {MONTHLY_ACTIVE_WEB_DASHBOARD_USERS}
    FROM datamodel_platform.dim_users_organizations aa_current
    JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
        ON aa_current.org_id = rcuo.organization_id
        AND aa_current.user_id = rcuo.user_id
    LEFT OUTER JOIN internal_users iu
        ON aa_current.user_id = iu.user_id
    WHERE
        aa_current.date = GREATEST('{PARTITION_END}', '2024-05-01')
        AND rcuo.date = GREATEST('{PARTITION_END}', '2024-05-01')
        AND iu.user_id IS NULL -- Not an internal user
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{PARTITION_END}', 1)) >= '{PARTITION_END}'
    GROUP BY 1, 2
),
admin_activity_previous_week AS (
    SELECT
        '{PARTITION_END}' AS date,
        aa_current.org_id,
        {ADMIN_APP_USERS_PREVIOUS_WEEK},
        {ACTIVE_ADMIN_APP_USERS_PREVIOUS_WEEK},
        {WEB_DASHBOARD_USERS_PREVIOUS_WEEK},
        {ACTIVE_WEB_DASHBOARD_USERS_PREVIOUS_WEEK}
    FROM datamodel_platform.dim_users_organizations aa_current
    JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
        ON aa_current.org_id = rcuo.organization_id
        AND aa_current.user_id = rcuo.user_id
    LEFT OUTER JOIN internal_users_previous_week iu
        ON aa_current.user_id = iu.user_id
    WHERE
        aa_current.date = GREATEST(CAST(DATE_SUB('{PARTITION_END}', 7) AS STRING), '2024-05-01')
        AND rcuo.date = GREATEST(CAST(DATE_SUB('{PARTITION_END}', 7) AS STRING), '2024-05-01')
        AND iu.user_id IS NULL -- Not an internal user
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD(DATE_SUB('{PARTITION_END}', 7), 1)) >= DATE_SUB('{PARTITION_END}', 7)
    GROUP BY 1, 2
),
pages_viewed AS (
    SELECT
        '{PARTITION_END}' AS date,
        scr.org_id,
        {MAINTENANCE_STATUS_VIEWERS},
        {MAINTENANCE_STATUS_VIEWERS_PREVIOUS_WEEK},
        {FUEL_ENERGY_HUB_VIEWERS},
        {FUEL_ENERGY_HUB_VIEWERS_PREVIOUS_WEEK}
    FROM {MIXPANEL_DATA_SOURCE}.datamodel_platform_silver.stg_cloud_routes scr
    JOIN datamodel_platform.dim_users_organizations duo
        ON scr.org_id = duo.org_id
        AND {DIM_USERS_ORGANIZATIONS_JOIN_CONDITION}
    WHERE
        scr.date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND '{PARTITION_END}'
        AND duo.date = GREATEST('{PARTITION_END}', '2024-05-01')
        AND scr.is_customer_email = TRUE
        AND duo.email NOT LIKE '%samsara.canary%'
        AND duo.email NOT LIKE '%samsara.forms.canary%'
        AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
        AND duo.email NOT LIKE '%samsaratest%'
        AND duo.email NOT LIKE '%@samsara%'
        AND duo.email NOT LIKE '%@samsara-service-account.com'
    GROUP BY 1, 2
),
marketplace_integrations AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {MARKETPLACE_INTEGRATIONS},
        {MARKETPLACE_INTEGRATIONS_PREVIOUS_WEEK},
        {MARKETPLACE_INTEGRATIONS_LIST}
    FROM product_analytics.dim_app_installs dai
    JOIN clouddb.developer_apps da
        ON dai.app_uuid = da.uuid
    WHERE
        date = (SELECT MAX(date) FROM product_analytics.dim_app_installs)
        AND da.approval_state = 'Published'
        AND da.deactivated = 0
        AND DATE(dai.installed_at) <= '{PARTITION_END}'
    GROUP BY 1, 2
),
api_tokens AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {API_TOKENS},
        {API_TOKENS_PREVIOUS_WEEK}
    FROM clouddb.api_tokens api
    LEFT OUTER JOIN internal_users iu
        ON api.created_by = iu.user_id
    WHERE iu.user_id IS NULL -- Not an internal user
    GROUP BY 1, 2
),
api_requests AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {API_REQUESTS},
        {WEEKLY_API_REQUESTS},
        {WEEKLY_API_REQUESTS_PREVIOUS_WEEK}
    FROM product_analytics.fct_api_usage
    WHERE
        date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND '{PARTITION_END}'
        AND status_code = 200
    GROUP BY 1, 2
),
alert_configs AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {TOTAL_ALERTS_CREATED},
        {TOTAL_ALERTS_CREATED_PREVIOUS_WEEK},
        {ALERTS_CREATED},
        {WEEKLY_ALERTS_CREATED},
        {WEEKLY_ALERTS_CREATED_PREVIOUS_WEEK},
        {ALERT_TRIGGER_TYPES},
        {ALERT_TRIGGER_TYPES_PREVIOUS_WEEK}
    FROM datamodel_platform.dim_alert_configs
    WHERE
        date = (SELECT MAX(date) FROM datamodel_platform.dim_alert_configs)
        AND is_disabled = FALSE
        AND COALESCE(DATE(deleted_ts_utc), DATE_ADD('{PARTITION_END}', 1)) > '{PARTITION_END}'
        AND DATE(created_ts_utc) <= '{PARTITION_END}'
        AND is_admin = FALSE -- not created by superuser
    GROUP BY 1, 2
),
alerts_triggered AS (
    SELECT
        '{PARTITION_END}' AS date,
        i.org_id,
        {ALERTS_TRIGGERED},
        {WEEKLY_ALERTS_TRIGGERED},
        {WEEKLY_ALERTS_TRIGGERED_PREVIOUS_WEEK}
    FROM datamodel_platform.fct_alert_incidents i
    JOIN datamodel_platform.dim_alert_configs c
        ON i.org_id = c.org_id
        AND i.alert_config_uuid = c.alert_config_uuid
    WHERE
        i.date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND '{PARTITION_END}'
        AND c.date = (SELECT MAX(date) FROM datamodel_platform.dim_alert_configs)
        AND c.is_admin = FALSE -- not created by superuser
        AND c.is_disabled = FALSE -- not disabled
        AND COALESCE(DATE(c.deleted_ts_utc), DATE_ADD('{PARTITION_END}', 1)) > '{PARTITION_END}'
        AND DATE(c.created_ts_utc) <= '{PARTITION_END}'
    GROUP BY 1, 2
),
safety_event_review_jobs AS (
  SELECT * FROM (
        SELECT DISTINCT
            a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at
        FROM safetyeventreviewdb.review_request_metadata a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN safetyeventreviewdb.jobs b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
          ON a.job_uuid = b.uuid
        INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
          ON a.org_id = o.org_id
        WHERE
            b.date = '{PARTITION_END}'
            AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
            AND b.completed_at IS NOT NULL -- only completed reviews
            AND o.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
),
triage_events AS (
    SELECT
        '{PARTITION_END}' AS date,
        t.org_id,
        {SAFETY_EVENTS_CAPTURED},
        {SAFETY_EVENTS_VIEWED},
        {SAFETY_EVENTS_WITH_SER}
    FROM product_analytics.fct_safety_triage_events t
    LEFT JOIN safety_event_review_jobs serj
        ON t.org_id = serj.org_id
        AND t.device_id = serj.device_id
        AND t.start_ms = serj.event_ms
    WHERE
        t.date = '{PARTITION_END}'
        AND t.triage_state_name NOT IN ('ManualReview', 'ManualReviewDismissed', 'Unknown', 'InvalidTriageState')
        -- For InCoaching events, must have valid coaching state
        AND (
            t.triage_state_name != 'InCoaching'
            OR t.coaching_state_name IN ('ITEM_NEEDS_COACHING', 'ITEM_INCOMPLETE', 'ITEM_COACHED', 'ITEM_DISMISSED')
        )
    GROUP BY 1, 2
),
safety_events_coached AS (
    SELECT
        sem.org_id,
        CONCAT(sem.org_id, ',', sem.device_id, ',', sem.event_ms) AS event_key,
        ci.uuid AS coachable_item_uuid,
        ci.due_date,
        cs.completed_date
    FROM safetydb_shards.safety_event_metadata sem
    JOIN coachingdb_shards.coachable_item ci
        ON ci.source_id = CONCAT(sem.org_id, ',', sem.device_id, ',', sem.event_ms)
        AND ci.org_id = sem.org_id
        AND ci.item_type = 1  -- SAFETY_EVENT
    JOIN coachingdb_shards.coaching_sessions cs
        ON cs.uuid = ci.coaching_session_uuid
        AND cs.org_id = ci.org_id
    WHERE
        sem.coaching_state = 2  -- CoachingWorkflow_COACHED
        AND DATE(FROM_UNIXTIME(sem.event_ms / 1000)) = '{PARTITION_END}'
        AND ci.coaching_state = 100   -- ITEM_COACHED
),
triage_events_coached AS (
    SELECT
        te.org_id,
        te.uuid AS event_key,
        ci.uuid AS coachable_item_uuid,
        ci.due_date,
        cs.completed_date
    FROM product_analytics.fct_safety_triage_events te
    JOIN coachingdb_shards.coachable_item ci
        ON UPPER(REPLACE(ci.source_id, '-', '')) = te.uuid
        AND ci.org_id = te.org_id
        AND ci.item_type = 4  -- TRIAGE_EVENT
    JOIN coachingdb_shards.coaching_sessions cs
        ON cs.uuid = ci.coaching_session_uuid
        AND cs.org_id = ci.org_id
    WHERE
        te.date = '{PARTITION_END}'
        AND te.triage_state_enum = 3  -- InCoaching
        AND ci.coaching_state = 100   -- ITEM_COACHED
        AND te.triage_state_name NOT IN ('ManualReview', 'ManualReviewDismissed', 'Unknown', 'InvalidTriageState')
        -- For InCoaching events, must have valid coaching state
        AND (
            te.triage_state_name != 'InCoaching'
            OR te.coaching_state_name IN ('ITEM_NEEDS_COACHING', 'ITEM_INCOMPLETE', 'ITEM_COACHED', 'ITEM_DISMISSED')
        )
),
all_coached_events AS (
    SELECT * FROM safety_events_coached

    UNION ALL

    SELECT * FROM triage_events_coached
),
coached_events AS (
    SELECT
        '{PARTITION_END}' AS date,
        ci.org_id,
        {SAFETY_EVENTS_COACHED},
        {SAFETY_EVENTS_COACHED_ON_TIME}
    FROM all_coached_events ci
    GROUP BY 1, 2
),
diagnostics_coverage_deduped AS (
    SELECT
        dims.org_id,
        dims.device_id,
        data.type,
        data.is_covered
    FROM product_analytics.dim_device_dimensions AS dims
    JOIN product_analytics.agg_device_stats_secondary_coverage AS data
        ON data.date = dims.date
        AND data.org_id = dims.org_id
        AND data.device_id = dims.device_id
    JOIN datamodel_core.dim_devices AS dd
        ON dd.device_id = data.device_id
    WHERE
        dims.date BETWEEN DATE_SUB('{PARTITION_END}', 29) AND '{PARTITION_END}'
        AND type IN (
            'battery_milli_v',
            'ev_high_capacity_battery_state_of_health',
            'ev_charging_status',
            'delta_ev_energy_consumed',
            'delta_ev_distance_driven_on_electric_power',
            'diagnostic_fault_count',
            'engine_state',
            'delta_fuel_consumed',
            'fuel_level_percent',
            'odometer',
            'ev_usable_state_of_charge_milli_percent',
            'tell_tale_status',
            'engine_seconds',
            'engine_milli_knots',
            'unique_vin_count'
        )
        AND dd.date = '{PARTITION_END}'
        AND dims.diagnostics_capable
        AND dims.include_in_agg_metrics
        AND dd.device_type = 'VG - Vehicle Gateway'
),
diagnostics_coverage AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {DEVICES_WITH_DIAGNOSTICS_COVERAGE}
    FROM diagnostics_coverage_deduped
    GROUP BY 1, 2
),
hos_logs AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {DRIVERS_WITH_HOS_LOGS}
    FROM datamodel_telematics_silver.stg_hos_logs
    WHERE
        date = '{PARTITION_END}'
        AND driver_id != 0
        AND driver_id IS NOT NULL
    GROUP BY 1, 2
),
workflows AS (
    SELECT
        '{PARTITION_END}' AS date,
        fs.org_id,
        {TOTAL_USERS_WITH_COMPLETED_WORKFLOW},
        {USERS_WITH_COMPLETED_WORKFLOW},
        {TOTAL_USERS_WITH_COMPLETED_TRAINING},
        {USERS_WITH_COMPLETED_TRAINING}
    FROM formsdb_shards.form_submissions fs
    LEFT OUTER JOIN internal_users iu
        ON SPLIT_PART(COALESCE(fs.submitted_by_polymorphic, fs.assigned_to_polymorphic, fs.created_by_polymorphic), '-', 2) = iu.user_id
        AND SPLIT_PART(COALESCE(fs.submitted_by_polymorphic, fs.assigned_to_polymorphic, fs.created_by_polymorphic), '-', 1) = 'user'
    WHERE
        status = 1  -- completed
        AND COALESCE(DATE(server_deleted_at), DATE_ADD('{PARTITION_END}', 1)) >= '{PARTITION_END}'
        AND iu.user_id IS NULL -- not submitted by internal user
        AND fs.created_by_polymorphic != 'user-1' -- Exclude system user records
    GROUP BY 1, 2
),
workflow_templates AS (
    SELECT
        '{PARTITION_END}' AS date,
        ft.org_id,
        {TOTAL_WORKFLOW_TEMPLATES},
        {WORKFLOW_TEMPLATES}
    FROM formsdb_shards.form_templates ft
    LEFT OUTER JOIN internal_users iu
        ON ft.created_by = iu.user_id
    WHERE
        COALESCE(DATE(server_deleted_at), DATE_ADD('{PARTITION_END}', 1)) >= '{PARTITION_END}'
        AND iu.user_id IS NULL -- not created by internal user
    GROUP BY 1, 2
),
custom_courses AS (
    SELECT
        '{PARTITION_END}' AS date,
        c.org_id,
        {TOTAL_CREATED_CUSTOM_COURSES},
        {CREATED_CUSTOM_COURSES}
    FROM trainingdb_shards.courses c
    JOIN trainingdb_shards.course_revisions cr
        ON c.current_revision_uuid = cr.uuid
    LEFT OUTER JOIN internal_users iu
        ON c.created_by = iu.user_id
    WHERE
        c.global_course_uuid IS NULL
        AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{PARTITION_END}', 1)) >= '{PARTITION_END}'
        AND COALESCE(DATE(FROM_UNIXTIME(cr.deleted_at_ms / 1000)), DATE_ADD('{PARTITION_END}', 1)) >= '{PARTITION_END}'
        AND cr.content_import_status = 5 -- 5 represents AVAILABLE (i.e. not a draft)
        AND iu.user_id IS NULL -- not created by internal user
    GROUP BY 1, 2
),
created_routes AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {VEHICLES_WITH_ASSIGNED_ROUTES}
    FROM datamodel_telematics.fct_dispatch_routes
    WHERE date = '{PARTITION_END}'
    GROUP BY 1, 2
),
dvirs AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {DVIRS},
        {DVIR_DEFECTS},
        {RESOLVED_DVIRS}
    FROM datamodel_telematics.fct_dvirs
    WHERE
        date = '{PARTITION_END}'
        AND inspection_type in ('pretrip', 'posttrip')
    GROUP BY 1, 2
),
preventative_maintenance_base AS (
    SELECT
        g.organization_id AS org_id,
        date_interval_schedules.id AS device_id
    FROM clouddb.preventative_maintenance_schedules pms
    JOIN clouddb.groups g ON pms.group_id = g.id
    LATERAL VIEW explode(pms.marshaled_proto.interval.date_interval.vehicleSchedules) AS date_interval_schedules

    UNION ALL

    SELECT
        g.organization_id AS org_id,
        distance_interval_schedules.id AS device_id
    FROM clouddb.preventative_maintenance_schedules pms
    JOIN clouddb.groups g ON pms.group_id = g.id
    LATERAL VIEW explode(pms.marshaled_proto.interval.distance_interval.vehicleSchedules) AS distance_interval_schedules

    UNION ALL

    SELECT
        g.organization_id AS org_id,
        engine_hour_interval_schedules.id AS device_id
    FROM clouddb.preventative_maintenance_schedules pms
    JOIN clouddb.groups g ON pms.group_id = g.id
    LATERAL VIEW explode(pms.marshaled_proto.interval.engine_hour_interval.vehicleSchedules) AS engine_hour_interval_schedules
),
maintenance_schedules AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {VEHICLES_WITH_PREVENTATIVE_MAINTENANCE_SCHEDULE}
    FROM preventative_maintenance_base
    GROUP BY 1, 2
),
geofences AS (
    SELECT
        '{PARTITION_END}' AS date,
        a.org_id AS org_id,
        {TOTAL_GEOFENCES},
        {TOTAL_GEOFENCES_PREVIOUS_WEEK},
        {GEOFENCES},
        {WEEKLY_GEOFENCES},
        {WEEKLY_GEOFENCES_PREVIOUS_WEEK}
    FROM placedb_shards.public__place a
    LEFT OUTER JOIN internal_users iu
        ON a.created_by = iu.user_id
    WHERE
        (a.geometry_buffer_meters > 0 OR (a.geometry_blob IS NOT NULL AND LOWER(SUBSTRING(CAST(a.geometry_blob AS STRING), 3, 8)) IN ('00000003', '03000000'))) --Geofence: circle (buffer>0) or polygon (WKB type 3); geometry_blob hex string; no ST_* to avoid Photon
        AND iu.user_id IS NULL -- not created by internal user
        AND COALESCE(DATE(a.deleted_at), DATE_ADD('{PARTITION_END}', 1)) >= '{PARTITION_END}'
    GROUP BY 1, 2
),
trips AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {WEEKLY_TRIPS},
        {WEEKLY_TRIPS_PREVIOUS_WEEK},
        {TRIPS},
        {WEEKLY_TRIPS_WITH_DRIVER},
        {WEEKLY_TRIPS_WITH_DRIVER_PREVIOUS_WEEK},
        {TRIPS_WITH_DRIVER},
        {WEEKLY_DRIVERS_ASSIGNED_TRIPS},
        {WEEKLY_DRIVERS_ASSIGNED_TRIPS_PREVIOUS_WEEK},
        {DRIVERS_ASSIGNED_TRIPS}
    FROM datamodel_telematics.fct_trips
    WHERE date BETWEEN DATE_SUB('{PARTITION_END}', 13) AND '{PARTITION_END}'
    GROUP BY 1, 2
),
ai_chats AS (
    SELECT
        '{PARTITION_END}' AS date,
        orgid AS org_id,
        {TOTAL_AI_CHAT_USERS},
        {TOTAL_AI_CHAT_USERS_PREVIOUS_WEEK},
        {WEEKLY_AI_CHAT_USERS},
        {WEEKLY_AI_CHAT_USERS_PREVIOUS_WEEK},
        {AI_CHAT_MESSAGES},
        {WEEKLY_AI_CHAT_MESSAGES},
        {WEEKLY_AI_CHAT_MESSAGES_PREVIOUS_WEEK}
    FROM {AI_CHAT_SEND_TABLE} mix
    LEFT OUTER JOIN internal_users iu
        ON COALESCE(mix.mp_user_id, mix.mp_device_id) = iu.email
    WHERE iu.user_id IS NULL -- not an internal user
    GROUP BY 1, 2
),
custom_reports AS (
    SELECT
        '{PARTITION_END}' AS date,
        OrgId AS org_id,
        {TOTAL_CUSTOM_REPORTS},
        {TOTAL_CUSTOM_REPORTS_PREVIOUS_WEEK},
        {CUSTOM_REPORTS},
        {WEEKLY_CUSTOM_REPORTS},
        {WEEKLY_CUSTOM_REPORTS_PREVIOUS_WEEK}
    FROM dynamodb.custom_report_configurations crc
    LEFT OUTER JOIN internal_users iu
        ON crc.OwnerId = iu.user_id
    WHERE
        iu.user_id IS NULL -- not created by an internal user
        AND crc.IsSuperuserOnly = FALSE
        AND DATE(FROM_UNIXTIME(crc.CreatedAt / 1000)) <= '{PARTITION_END}'
    GROUP BY 1, 2
),
custom_report_runs AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {WEEKLY_CUSTOM_REPORT_RUNS},
        {WEEKLY_CUSTOM_REPORT_RUNS_PREVIOUS_WEEK},
        {CUSTOM_REPORT_RUNS},
        {WEEKLY_CUSTOM_REPORT_USERS},
        {WEEKLY_CUSTOM_REPORT_USERS_PREVIOUS_WEEK}
    FROM reportconfigdb_shards.report_runs rr
    LEFT OUTER JOIN internal_users iu
        ON rr.user_id = iu.user_id
    WHERE
        iu.user_id IS NULL -- not an internal user
        AND rr.report_run_metadata.report_config.is_superuser_only = FALSE
        AND rr.report_run_metadata.report_config IS NOT NULL
    AND DATE(rr.created_at) BETWEEN DATE_SUB('{PARTITION_END}', 13) AND '{PARTITION_END}'
    GROUP BY 1, 2
),
service_logs AS (
    SELECT
        '{PARTITION_END}' AS date,
        g.organization_id AS org_id,
        {COMPLETED_SERVICE_LOGS}
    FROM clouddb.vehicle_maintenance_logs vml
    JOIN clouddb.groups g
        ON vml.group_id = g.id
    WHERE vml.deleted = 0 -- Not deleted
    GROUP BY 1, 2
),
active_unassigned_logs AS (
    SELECT *
    FROM compliancedb_shards.driver_hos_logs
    WHERE
        driver_id = 0
        AND (log_proto['active_to_inactive_changed_at_ms'] IS NULL OR log_proto['active_to_inactive_changed_at_ms'] = 0)
        AND date = '{PARTITION_END}'
        -- Exclude annotated records (annotated segments have non-empty notes)
        AND (notes IS NULL OR TRIM(notes) = '')
        AND (log_type & 65536) != 0
),
active_unassigned_logs_and_end_time AS (
    SELECT
        *,
        log_at AS start_time,
        LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
    FROM active_unassigned_logs
),
active_unassigned_logs_and_duration AS (
    SELECT
        *,
        (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS unassigned_driver_time_hours
    FROM active_unassigned_logs_and_end_time
    WHERE status_code = 2
),
unassigned_hos AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {TOTAL_UNASSIGNED_DRIVING_HOURS}
    FROM active_unassigned_logs_and_duration
    GROUP BY 1, 2
),
infringements AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {INFRINGEMENTS}
    FROM eucompliancedb_shards.infringements
    WHERE date = '{PARTITION_END}'
    GROUP BY 1, 2
),
camera_privacy AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {DRIVER_PRIVACY_MODE_CAMERAS},
        {COMPLETE_PRIVACY_MODE_CAMERAS}
    FROM product_analytics.dim_devices_safety_settings
    WHERE date = (SELECT MAX(date) FROM product_analytics.dim_devices_safety_settings)
    GROUP BY 1, 2
),
asset_utilization AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {TOTAL_ASSET_AVAILABLE_HOURS},
        {TOTAL_ASSET_UTILIZED_HOURS}
    FROM product_analytics_staging.stg_organization_benchmark_metrics_daily
    WHERE
        date = '{PARTITION_END}'
        AND metric_type = 'VEHICLE_UTILIZATION'
    GROUP BY 1, 2
),
hos_violations AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {HOS_VIOLATION_DURATION_HOURS},
        {POSSIBLE_DRIVER_HOURS}
    FROM product_analytics_staging.stg_organization_benchmark_metrics_daily
    WHERE
        date = '{PARTITION_END}'
        AND metric_type = 'HOS_COMPLIANCE_TIME_PERCENT'
    GROUP BY 1, 2
),
mobile_usage AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        {MOBILE_USAGE_COUNT},
        {MOBILE_USAGE_MILES}
    FROM product_analytics_staging.stg_organization_benchmark_metrics_daily
    WHERE
        date = '{PARTITION_END}'
        AND metric_type = 'MOBILE_USAGE_EVENTS_PER_1000MI'
    GROUP BY 1, 2
),
safety_settings AS (
    SELECT
        '{PARTITION_END}' AS date,
        org_id,
        severe_speeding_enabled,
        severe_speeding_in_cab_audio_alerts_enabled,
        distracted_driving_detection_enabled,
        inattentive_driving_enabled,
        inattentive_driving_audio_alerts_enabled,
        policy_violations_enabled,
        policy_violations_audio_alerts_enabled,
        mobile_usage_enabled,
        mobile_usage_audio_alerts_enabled,
        seatbelt_enabled,
        seatbelt_audio_alerts_enabled,
        in_cab_stop_sign_violation_enabled AS rolling_stop_enabled,
        in_cab_stop_sign_violation_audio_alerts_enabled,
        following_distance_enabled,
        following_distance_audio_alerts_enabled,
        forward_collision_warning_enabled,
        forward_collision_warning_audio_alerts_enabled,
        lane_departure_warning_enabled,
        lane_departure_warning_audio_alerts_enabled,
        inward_obstruction_enabled,
        inward_obstruction_audio_alerts_enabled,
        drowsiness_detection_enabled,
        drowsiness_detection_audio_alerts_enabled
    FROM product_analytics.dim_organizations_safety_settings
    WHERE date = (SELECT MAX(date) FROM product_analytics.dim_organizations_safety_settings)
),
safety_settings_org_agg_cm AS (
    SELECT
        '{PARTITION_END}' AS date,
        ddss.org_id,
        {POLICY_VIOLATIONS_ENABLED_COUNT},
        {MOBILE_USAGE_ENABLED_COUNT},
        {DROWSINESS_DETECTION_ENABLED_COUNT},
        {INATTENTIVE_DRIVING_ENABLED_COUNT},
        {LANE_DEPARTURE_WARNING_ENABLED_COUNT},
        {FORWARD_COLLISION_WARNING_ENABLED_COUNT},
        {FOLLOWING_DISTANCE_ENABLED_COUNT},
        {SEATBELT_ENABLED_COUNT},
        {INWARD_OBSTRUCTION_ENABLED_COUNT},
        {ROLLING_STOP_ENABLED_COUNT},
        {POLICY_VIOLATIONS_ENABLED_INCAB_COUNT},
        {MOBILE_USAGE_ENABLED_INCAB_COUNT},
        {DROWSINESS_DETECTION_ENABLED_INCAB_COUNT},
        {INATTENTIVE_DRIVING_ENABLED_INCAB_COUNT},
        {LANE_DEPARTURE_WARNING_ENABLED_INCAB_COUNT},
        {FORWARD_COLLISION_WARNING_ENABLED_INCAB_COUNT},
        {FOLLOWING_DISTANCE_ENABLED_INCAB_COUNT},
        {SEATBELT_ENABLED_INCAB_COUNT},
        {INWARD_OBSTRUCTION_ENABLED_INCAB_COUNT},
        {ROLLING_STOP_ENABLED_INCAB_COUNT}
    FROM product_analytics.dim_devices_safety_settings ddss
    JOIN datamodel_core.dim_devices dd
        ON ddss.org_id = dd.org_id
        AND ddss.cm_device_id = dd.device_id
    JOIN datamodel_core.lifetime_device_activity ad
        ON ad.org_id = dd.org_id
        AND ad.device_id = dd.device_id
    WHERE
        ddss.date = (SELECT MAX(date) FROM product_analytics.dim_devices_safety_settings)
        AND dd.date = '{PARTITION_END}'
        AND ad.date = '{PARTITION_END}'
        AND ad.latest_date BETWEEN DATE_SUB('{PARTITION_END}', 29) AND '{PARTITION_END}'
    GROUP BY
        ddss.org_id
),
safety_settings_org_agg_vg AS (
    SELECT
        '{PARTITION_END}' AS date,
        ddss.org_id,
        {SEVERE_SPEEDING_ENABLED_COUNT},
        {SEVERE_SPEEDING_ENABLED_INCAB_COUNT}
    FROM product_analytics.dim_devices_safety_settings ddss
    JOIN datamodel_core.dim_devices dd
        ON ddss.org_id = dd.org_id
        AND ddss.vg_device_id = dd.device_id
    JOIN datamodel_core.lifetime_device_activity ad
        ON ad.org_id = dd.org_id
        AND ad.device_id = dd.device_id
    WHERE
        ddss.date = (SELECT MAX(date) FROM product_analytics.dim_devices_safety_settings)
        AND dd.date = '{PARTITION_END}'
        AND ad.date = '{PARTITION_END}'
        AND ad.latest_date BETWEEN DATE_SUB('{PARTITION_END}', 29) AND '{PARTITION_END}'
    GROUP BY
        ddss.org_id
),
final AS (
    -- Get row for each org
    SELECT
        go.date,
        go.org_id,
        go.org_name,
        go.account_arr_segment,
        COALESCE(ol.sam_number, go.sam_number) AS sam_number,
        ol.org_id_list,
        go.account_industry,
        go.number_of_vehicles,
        COALESCE(ss.severe_speeding_enabled, FALSE) AS severe_speeding_enabled,
        COALESCE(ss.severe_speeding_in_cab_audio_alerts_enabled, FALSE) AS speeding_in_cab_audio_alerts_enabled,
        COALESCE(ss.distracted_driving_detection_enabled, FALSE) AS distracted_driving_detection_enabled,
        COALESCE(ss.inattentive_driving_enabled, FALSE) AS inattentive_driving_enabled,
        COALESCE(ss.inattentive_driving_audio_alerts_enabled, FALSE) AS inattentive_driving_audio_alerts_enabled,
        COALESCE(ss.policy_violations_enabled, FALSE) AS policy_violations_enabled,
        COALESCE(ss.policy_violations_audio_alerts_enabled, FALSE) AS policy_violations_audio_alerts_enabled,
        COALESCE(ss.mobile_usage_enabled, FALSE) AS mobile_usage_enabled,
        COALESCE(ss.mobile_usage_audio_alerts_enabled, FALSE) AS mobile_usage_audio_alerts_enabled,
        COALESCE(ss.seatbelt_enabled, FALSE) AS seatbelt_enabled,
        COALESCE(ss.seatbelt_audio_alerts_enabled, FALSE) AS seatbelt_audio_alerts_enabled,
        COALESCE(ss.rolling_stop_enabled, FALSE) AS rolling_stop_enabled,
        COALESCE(ss.in_cab_stop_sign_violation_audio_alerts_enabled, FALSE) AS in_cab_stop_sign_violation_enabled,
        COALESCE(ss.following_distance_enabled, FALSE) AS following_distance_enabled,
        COALESCE(ss.following_distance_audio_alerts_enabled, FALSE) AS following_distance_audio_alerts_enabled,
        COALESCE(ss.forward_collision_warning_enabled, FALSE) AS forward_collision_warning_enabled,
        COALESCE(ss.forward_collision_warning_audio_alerts_enabled, FALSE) AS forward_collision_warning_audio_alerts_enabled,
        COALESCE(ss.lane_departure_warning_enabled, FALSE) AS lane_departure_warning_enabled,
        COALESCE(ss.lane_departure_warning_audio_alerts_enabled, FALSE) AS lane_departure_warning_audio_alerts_enabled,
        COALESCE(ss.inward_obstruction_enabled, FALSE) AS inward_obstruction_enabled,
        COALESCE(ss.inward_obstruction_audio_alerts_enabled, FALSE) AS inward_obstruction_audio_alerts_enabled,
        COALESCE(ss.drowsiness_detection_enabled, FALSE) AS drowsiness_detection_enabled,
        COALESCE(ss.drowsiness_detection_audio_alerts_enabled, FALSE) AS drowsiness_detection_audio_alerts_enabled,
        u.vg_under_utilized,
        u.vg_over_utilized,
        u.cm_m_under_utilized,
        u.cm_m_over_utilized,
        u.cm_s_under_utilized,
        u.cm_s_over_utilized,
        u.cm_d_under_utilized,
        u.cm_d_over_utilized,
        u.at_under_utilized,
        u.at_over_utilized,
        u.ag_powered_under_utilized,
        u.ag_powered_over_utilized,
        u.ag_unpowered_under_utilized,
        u.ag_unpowered_over_utilized,
        u_prev.vg_under_utilized_previous_month,
        u_prev.vg_over_utilized_previous_month,
        u_prev.cm_m_under_utilized_previous_month,
        u_prev.cm_m_over_utilized_previous_month,
        u_prev.cm_s_under_utilized_previous_month,
        u_prev.cm_s_over_utilized_previous_month,
        u_prev.cm_d_under_utilized_previous_month,
        u_prev.cm_d_over_utilized_previous_month,
        u_prev.at_under_utilized_previous_month,
        u_prev.at_over_utilized_previous_month,
        u_prev.ag_powered_under_utilized_previous_month,
        u_prev.ag_powered_over_utilized_previous_month,
        u_prev.ag_unpowered_under_utilized_previous_month,
        u_prev.ag_unpowered_over_utilized_previous_month,
        {COALESCE_METRICS}
    FROM global_orgs go
    LEFT JOIN safety_settings ss
        ON go.org_id = ss.org_id
        AND go.date = ss.date
    LEFT JOIN under_over_utilized u
        ON go.org_id = u.org_id
        AND go.date = u.date
    LEFT JOIN under_over_utilized_previous_month u_prev
        ON go.org_id = u_prev.org_id
    LEFT JOIN assigned_licenses_sam
        ON go.sam_number = assigned_licenses_sam.sam_number
        AND go.date = assigned_licenses_sam.date
    LEFT JOIN assigned_licenses_sam_previous_month_renamed
        ON go.sam_number = assigned_licenses_sam_previous_month_renamed.sam_number
    LEFT JOIN installed_devices_sam
        ON COALESCE(go.salesforce_sam_number, go.sam_number) = installed_devices_sam.sam_number
        AND go.date = installed_devices_sam.date
    LEFT JOIN active_devices_sam
        ON COALESCE(go.salesforce_sam_number, go.sam_number) = active_devices_sam.sam_number
        AND go.date = active_devices_sam.date
    LEFT JOIN org_list ol
        ON COALESCE(go.salesforce_sam_number, go.sam_number) = ol.sam_number
    {LEFT_JOIN_STATEMENTS}
)
SELECT *
FROM final
"""
