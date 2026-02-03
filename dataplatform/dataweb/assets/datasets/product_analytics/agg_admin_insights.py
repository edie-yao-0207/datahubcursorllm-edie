from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, SQLDQCheck, table, TrendDQCheck
from dataweb._core.dq_utils import Operator
from dataweb.userpkgs.constants import (
    AWSRegion,
    DATAENGINEERING,
    FRESHNESS_SLO_9PM_PST,
    ColumnType,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.admin_insights_constants import (
    SCHEMA_V1,
    METRICS_V1,
    QUERY_V1,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    generate_coalesce_statement,
    generate_left_join_statements,
    generate_metric_expression,
    get_formatted_metrics,
    partition_key_ranges_from_context,
    get_all_regions
)
from dataweb.userpkgs.query import create_run_config_overrides
from pyspark.sql import SparkSession


PRIMARY_KEYS = ["date", "org_id"]


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""This table contains aggregated usage metrics for different components of the Samsara platform for the Sales and CS teams""",
        row_meaning="""Each row represents various usage stats for an org over each day. Not all metrics are additive and have been denoted as such in the column descriptions where applicable.""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2, AWSRegion.EU_WEST_1],  # intentionarlly exclude CA
    owners=[DATAENGINEERING],
    schema=SCHEMA_V1,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=3,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_agg_admin_insights"),
        NonNullDQCheck(name="dq_non_null_agg_admin_insights", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_admin_insights", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_admin_insights", lookback_days=1, tolerance=0.1, block_before_write=False),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_users_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE monthly_active_web_dashboard_users < active_web_dashboard_users
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_safety_events_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE safety_events_captured < safety_events_viewed
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_safety_events_coached_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE safety_events_coached < safety_events_coached_on_time
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_dvir_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE dvir_defects < resolved_dvirs
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_active_devices_check",
            sql_query="""
                SELECT COUNT(DISTINCT CASE WHEN active_ats_sam = 0 AND active_cm_d_sam = 0 AND active_cm_m_sam = 0 AND active_cm_s_sam = 0 AND active_vgs_sam = 0 AND active_unpowered_ags_sam = 0 AND active_powered_ags_sam = 0 THEN org_id END) * 1.0 / COUNT(DISTINCT org_id) AS observed_value
                FROM df
            """,
            expected_value=0.75,
            operator=Operator.lte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_installed_devices_check",
            sql_query="""
                SELECT COUNT(DISTINCT CASE WHEN installed_ats_sam = 0 AND installed_cm_d_sam = 0 AND installed_cm_m_sam = 0 AND installed_cm_s_sam = 0 AND installed_vgs_sam = 0 AND installed_unpowered_ags_sam = 0 AND installed_powered_ags_sam = 0 THEN org_id END) * 1.0 / COUNT(DISTINCT org_id) AS observed_value
                FROM df
            """,
            expected_value=0.70,
            operator=Operator.lte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_trips_check",
            sql_query="""
                SELECT COUNT(DISTINCT CASE WHEN trips = 0 THEN org_id END) * 1.0 / COUNT(DISTINCT org_id) AS observed_value
                FROM df
            """,
            expected_value=0.95,
            operator=Operator.lte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_admin_insights_licenses_check",
            sql_query="""
                SELECT COUNT(DISTINCT CASE WHEN vg_licenses = 0 AND cm_m_licenses = 0 AND cm_d_licenses = 0 AND cm_s_licenses = 0 AND at_licenses = 0 AND ag_licenses = 0 THEN org_id END) * 1.0 / COUNT(DISTINCT org_id) AS observed_value
                FROM df
            """,
            expected_value=0.95,
            operator=Operator.lte,
            block_before_write=True,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "datamodel_platform_silver.stg_cloud_routes",
        "product_analytics.fct_api_usage",
        "datamodel_telematics_silver.stg_hos_logs",
        "datamodel_telematics.fct_dispatch_routes",
        "datamodel_core.dim_organizations",
        "formsdb_shards.form_submissions",
        "formsdb_shards.form_templates",
        "trainingdb_shards.courses",
        "trainingdb_shards.course_revisions",
        "datamodel_telematics.fct_dvirs",
        "datamodel_platform.dim_license_assignment",
        "datamodel_core.lifetime_device_activity",
        "datamodel_core.lifetime_device_online",
        "clouddb.preventative_maintenance_schedules",
        "clouddb.developer_apps",
        "datamodel_platform.fct_alert_incidents",
        "datamodel_safety.fct_safety_events",
        "datamodel_core.dim_devices",
        "safetyeventreviewdb.review_request_metadata",
        "safetyeventreviewdb.jobs",
        "datamodel_safety_silver.stg_activity_events",
        "placedb_shards.public__place",
        "clouddb.groups",
        "datamodel_telematics.fct_trips",
        "mixpanel_samsara.ai_chat_send",
        "reportconfigdb_shards.report_runs",
        "dynamodb.custom_report_configurations",
        "clouddb.vehicle_maintenance_logs",
        "compliancedb_shards.driver_hos_logs",
        "eucompliancedb_shards.infringements",
        "coachingdb_shards.coaching_sessions",
        "coachingdb_shards.coachable_item",
        "safetyeventreviewdb.review_request_metadata",
        "safetyeventreviewdb.jobs",
        "product_analytics.fct_safety_triage_events",
        "clouddb.api_tokens",
        "safetydb_shards.safety_event_metadata",
    ],
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=4,
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
    ),
)
def agg_admin_insights(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_admin_insights")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]

    region = context.asset_key.path[0]

    if region == AWSRegion.US_WEST_2:
        edw_data_source = "edw"
        mixpanel_data_source = "default"
        dim_users_organizations_join_condition = "scr.user_id = duo.user_id"
        user_id_column = "scr.user_id"
        license_hw_mapping_table = "edw.silver.license_hw_sku_xref"
        sam_account_org_xref = "edw.silver.sam_account_org_xref"
        region_filter = "us"
        license_table = "edw.silver.fct_license_orders_daily_snapshot"
        orders_table = "edw.silver.fct_license_orders"
        kpi_table = "edw.extracts_gold.agg_device_type_core_utilization_kpis"
        ai_chat_send_table = "mixpanel_samsara.ai_chat_send"
    else:
        edw_data_source = "edw_delta_share"
        mixpanel_data_source = "mixpanel_delta_share"
        dim_users_organizations_join_condition = "scr.useremail = duo.email"
        user_id_column = "scr.useremail"
        license_hw_mapping_table = "edw_delta_share.silver.license_hw_sku_xref"
        sam_account_org_xref = "edw_delta_share.silver.sam_account_org_xref"
        region_filter = "eu"
        license_table = "edw_delta_share.silver.fct_license_orders_daily_snapshot"
        orders_table = "edw_delta_share.silver.fct_license_orders"
        kpi_table = "biztech_edw_customer_success_gold.agg_device_type_core_utilization_kpis"
        ai_chat_send_table = "mixpanel_delta_share.mixpanel_samsara.ai_chat_send"

    metric_expressions = {
        metric_name: generate_metric_expression(METRICS_V1, metric_name).format(PARTITION_END=PARTITION_END, USER_ID_COLUMN=user_id_column)
        for metric_name in METRICS_V1.keys()
    }

    query = QUERY_V1.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
        EDW_DATA_SOURCE=edw_data_source,
        MIXPANEL_DATA_SOURCE=mixpanel_data_source,
        LICENSE_HW_MAPPING_TABLE=license_hw_mapping_table,
        SAM_ACCOUNT_ORG_XREF=sam_account_org_xref,
        REGION_FILTER=region_filter,
        LICENSE_TABLE=license_table,
        ORDERS_TABLE=orders_table,
        KPI_TABLE=kpi_table,
        DIM_USERS_ORGANIZATIONS_JOIN_CONDITION=dim_users_organizations_join_condition,
        AI_CHAT_SEND_TABLE=ai_chat_send_table,
        **get_formatted_metrics(metric_expressions),
        COALESCE_METRICS=generate_coalesce_statement(
            columns = [
                "device_licenses.vg_licenses",
                "device_licenses.cm_licenses",
                "device_licenses.cm_d_licenses",
                "device_licenses.cm_s_licenses",
                "device_licenses.cm_m_licenses",
                "device_licenses.ag_licenses",
                "device_licenses.powered_ag_licenses",
                "device_licenses.unpowered_ag_licenses",
                "device_licenses.at_licenses",
                "licenses.forms_licenses",
                "licenses.training_licenses",
                "licenses.commercial_navigation_licenses",
                "licenses.qualifications_licenses",
                "licenses.maintenance_licenses",
                "licenses.fleet_apps_licenses",
                "licenses.essential_safety_licenses",
                "licenses.premier_safety_licenses",
                "licenses.enterprise_safety_licenses",
                "licenses.legacy_safety_licenses",
                "licenses.essential_telematics_licenses",
                "licenses.essential_plus_telematics_licenses",
                "licenses.premier_telematics_licenses",
                "licenses.enterprise_telematics_licenses",
                "licenses.legacy_telematics_licenses",
                "licenses.essential_ag_licenses",
                "licenses.premier_ag_licenses",
                "licenses.enterprise_ag_licenses",
                "licenses.legacy_ag_licenses",
                "assigned_licenses.forms_assigned_licenses",
                "assigned_licenses.training_assigned_licenses",
                "assigned_licenses.commercial_navigation_assigned_licenses",
                "assigned_licenses.qualifications_assigned_licenses",
                "assigned_licenses.maintenance_assigned_licenses",
                "assigned_licenses.fleet_apps_assigned_licenses",
                "assigned_licenses_sam.forms_assigned_licenses_sam",
                "assigned_licenses_sam.training_assigned_licenses_sam",
                "assigned_licenses_sam.commercial_navigation_assigned_licenses_sam",
                "assigned_licenses_sam.qualifications_assigned_licenses_sam",
                "assigned_licenses_sam.maintenance_assigned_licenses_sam",
                "assigned_licenses_sam.fleet_apps_assigned_licenses_sam",
                "device_licenses_previous_month.vg_licenses_previous_month",
                "device_licenses_previous_month.cm_licenses_previous_month",
                "device_licenses_previous_month.cm_m_licenses_previous_month",
                "device_licenses_previous_month.cm_d_licenses_previous_month",
                "device_licenses_previous_month.cm_s_licenses_previous_month",
                "device_licenses_previous_month.ag_licenses_previous_month",
                "device_licenses_previous_month.powered_ag_licenses_previous_month",
                "device_licenses_previous_month.unpowered_ag_licenses_previous_month",
                "device_licenses_previous_month.at_licenses_previous_month",
                "licenses_previous_month.forms_licenses_previous_month",
                "licenses_previous_month.training_licenses_previous_month",
                "licenses_previous_month.commercial_navigation_licenses_previous_month",
                "licenses_previous_month.qualifications_licenses_previous_month",
                "licenses_previous_month.maintenance_licenses_previous_month",
                "licenses_previous_month.fleet_apps_licenses_previous_month",
                "licenses_previous_month.essential_safety_licenses_previous_month",
                "licenses_previous_month.premier_safety_licenses_previous_month",
                "licenses_previous_month.enterprise_safety_licenses_previous_month",
                "licenses_previous_month.legacy_safety_licenses_previous_month",
                "licenses_previous_month.essential_telematics_licenses_previous_month",
                "licenses_previous_month.essential_plus_telematics_licenses_previous_month",
                "licenses_previous_month.premier_telematics_licenses_previous_month",
                "licenses_previous_month.enterprise_telematics_licenses_previous_month",
                "licenses_previous_month.legacy_telematics_licenses_previous_month",
                "licenses_previous_month.essential_ag_licenses_previous_month",
                "licenses_previous_month.premier_ag_licenses_previous_month",
                "licenses_previous_month.enterprise_ag_licenses_previous_month",
                "licenses_previous_month.legacy_ag_licenses_previous_month",
                "assigned_licenses_previous_month.forms_assigned_licenses_previous_month",
                "assigned_licenses_previous_month.training_assigned_licenses_previous_month",
                "assigned_licenses_previous_month.commercial_navigation_assigned_licenses_previous_month",
                "assigned_licenses_previous_month.qualifications_assigned_licenses_previous_month",
                "assigned_licenses_previous_month.maintenance_assigned_licenses_previous_month",
                "assigned_licenses_previous_month.fleet_apps_assigned_licenses_previous_month",
                "assigned_licenses_sam_previous_month_renamed.forms_assigned_licenses_sam_previous_month",
                "assigned_licenses_sam_previous_month_renamed.training_assigned_licenses_sam_previous_month",
                "assigned_licenses_sam_previous_month_renamed.commercial_navigation_assigned_licenses_sam_previous_month",
                "assigned_licenses_sam_previous_month_renamed.qualifications_assigned_licenses_sam_previous_month",
                "assigned_licenses_sam_previous_month_renamed.maintenance_assigned_licenses_sam_previous_month",
                "assigned_licenses_sam_previous_month_renamed.fleet_apps_assigned_licenses_sam_previous_month",
                "installed_devices_sam.installed_vgs_sam",
                "installed_devices_sam.installed_ags_sam",
                "installed_devices_sam.installed_powered_ags_sam",
                "installed_devices_sam.installed_unpowered_ags_sam",
                "installed_devices_sam.installed_cms_sam",
                "installed_devices_sam.installed_cm_m_sam",
                "installed_devices_sam.installed_cm_s_sam",
                "installed_devices_sam.installed_cm_d_sam",
                "installed_devices_sam.installed_ats_sam",
                "installed_counts_org.installed_vgs",
                "installed_counts_org.installed_ags",
                "installed_counts_org.installed_powered_ags",
                "installed_counts_org.installed_unpowered_ags",
                "installed_counts_org.installed_cms",
                "installed_counts_org.installed_cm_m",
                "installed_counts_org.installed_cm_s",
                "installed_counts_org.installed_cm_d",
                "installed_counts_org.installed_ats",
                "active_devices_sam.active_vgs_sam",
                "active_devices_sam.active_ags_sam",
                "active_devices_sam.active_powered_ags_sam",
                "active_devices_sam.active_unpowered_ags_sam",
                "active_devices_sam.active_cms_sam",
                "active_devices_sam.active_cm_m_sam",
                "active_devices_sam.active_cm_s_sam",
                "active_devices_sam.active_cm_d_sam",
                "active_devices_sam.active_ats_sam",
                "active_counts_org.active_vgs",
                "active_counts_org.active_ags",
                "active_counts_org.active_powered_ags",
                "active_counts_org.active_unpowered_ags",
                "active_counts_org.active_cms",
                "active_counts_org.active_cm_m",
                "active_counts_org.active_cm_s",
                "active_counts_org.active_cm_d",
                "active_counts_org.active_ats",
                "driver_activity.total_drivers",
                "driver_activity.driver_app_users",
                "driver_activity.active_driver_app_users",
                "driver_activity.monthly_active_driver_app_users",
                "driver_activity_previous_week.total_drivers_previous_week",
                "driver_activity_previous_week.driver_app_users_previous_week",
                "driver_activity_previous_week.active_driver_app_users_previous_week",
                "admin_activity.admin_app_users",
                "admin_activity.active_admin_app_users",
                "admin_activity.monthly_active_admin_app_users",
                "admin_activity.web_dashboard_users",
                "admin_activity.active_web_dashboard_users",
                "admin_activity.monthly_active_web_dashboard_users",
                "admin_activity_previous_week.admin_app_users_previous_week",
                "admin_activity_previous_week.active_admin_app_users_previous_week",
                "admin_activity_previous_week.web_dashboard_users_previous_week",
                "admin_activity_previous_week.active_web_dashboard_users_previous_week",
                "marketplace_integrations.marketplace_integrations",
                "marketplace_integrations.marketplace_integrations_previous_week",
                "marketplace_integrations.marketplace_integrations_list",
                "api_tokens.api_tokens",
                "api_tokens.api_tokens_previous_week",
                "api_requests.api_requests",
                "api_requests.weekly_api_requests",
                "api_requests.weekly_api_requests_previous_week",
                "alert_configs.total_alerts_created",
                "alert_configs.total_alerts_created_previous_week",
                "alert_configs.alerts_created",
                "alert_configs.weekly_alerts_created",
                "alert_configs.weekly_alerts_created_previous_week",
                "alert_configs.alert_trigger_types",
                "alert_configs.alert_trigger_types_previous_week",
                "alerts_triggered.alerts_triggered",
                "alerts_triggered.weekly_alerts_triggered",
                "alerts_triggered.weekly_alerts_triggered_previous_week",
                "vehicles.dual_dashcam_vehicles",
                "vehicles.outward_dashcam_vehicles",
                "vehicles.ai_multicam_vehicles",
                "vehicles.camera_connector_vehicles",
                "triage_events.safety_events_captured",
                "triage_events.safety_events_viewed",
                "coached_events.safety_events_coached",
                "coached_events.safety_events_coached_on_time",
                "triage_events.safety_events_with_ser",
                "diagnostics.devices_with_diagnostics_coverage",
                "hos_logs.drivers_with_hos_logs",
                "pages_viewed.fuel_energy_hub_viewers",
                "pages_viewed.maintenance_status_viewers",
                "pages_viewed.fuel_energy_hub_viewers_previous_week",
                "pages_viewed.maintenance_status_viewers_previous_week",
                "dvirs.dvirs",
                "dvirs.dvir_defects",
                "dvirs.resolved_dvirs",
                "created_routes.vehicles_with_assigned_routes",
                "maintenance_schedules.vehicles_with_preventative_maintenance_schedule",
                "vehicles.electric_vehicles",
                "workflows.total_users_with_completed_workflow",
                "workflows.users_with_completed_workflow",
                "workflow_templates.total_workflow_templates",
                "workflow_templates.workflow_templates",
                "workflows.total_users_with_completed_training",
                "workflows.users_with_completed_training",
                "custom_courses.total_created_custom_courses",
                "custom_courses.created_custom_courses",
                "safety_settings_org_agg_vg.severe_speeding_enabled_count",
                "safety_settings_org_agg_vg.severe_speeding_enabled_incab_count",
                "safety_settings_org_agg_cm.policy_violations_enabled_count",
                "safety_settings_org_agg_cm.policy_violations_enabled_incab_count",
                "safety_settings_org_agg_cm.mobile_usage_enabled_count",
                "safety_settings_org_agg_cm.mobile_usage_enabled_incab_count",
                "safety_settings_org_agg_cm.drowsiness_detection_enabled_count",
                "safety_settings_org_agg_cm.drowsiness_detection_enabled_incab_count",
                "safety_settings_org_agg_cm.inattentive_driving_enabled_count",
                "safety_settings_org_agg_cm.inattentive_driving_enabled_incab_count",
                "safety_settings_org_agg_cm.lane_departure_warning_enabled_count",
                "safety_settings_org_agg_cm.lane_departure_warning_enabled_incab_count",
                "safety_settings_org_agg_cm.forward_collision_warning_enabled_count",
                "safety_settings_org_agg_cm.forward_collision_warning_enabled_incab_count",
                "safety_settings_org_agg_cm.following_distance_enabled_count",
                "safety_settings_org_agg_cm.following_distance_enabled_incab_count",
                "safety_settings_org_agg_cm.seatbelt_enabled_count",
                "safety_settings_org_agg_cm.seatbelt_enabled_incab_count",
                "safety_settings_org_agg_cm.inward_obstruction_enabled_count",
                "safety_settings_org_agg_cm.inward_obstruction_enabled_incab_count",
                "safety_settings_org_agg_cm.rolling_stop_enabled_count",
                "safety_settings_org_agg_cm.rolling_stop_enabled_incab_count",
                "geofences.total_geofences",
                "geofences.total_geofences_previous_week",
                "geofences.geofences",
                "geofences.weekly_geofences",
                "geofences.weekly_geofences_previous_week",
                "trips.weekly_trips",
                "trips.weekly_trips_previous_week",
                "trips.trips",
                "trips.weekly_trips_with_driver",
                "trips.weekly_trips_with_driver_previous_week",
                "trips.trips_with_driver",
                "trips.weekly_drivers_assigned_trips",
                "trips.weekly_drivers_assigned_trips_previous_week",
                "trips.drivers_assigned_trips",
                "ai_chats.total_ai_chat_users",
                "ai_chats.total_ai_chat_users_previous_week",
                "ai_chats.weekly_ai_chat_users",
                "ai_chats.weekly_ai_chat_users_previous_week",
                "ai_chats.ai_chat_messages",
                "ai_chats.weekly_ai_chat_messages",
                "ai_chats.weekly_ai_chat_messages_previous_week",
                "custom_reports.total_custom_reports",
                "custom_reports.total_custom_reports_previous_week",
                "custom_reports.custom_reports",
                "custom_reports.weekly_custom_reports",
                "custom_reports.weekly_custom_reports_previous_week",
                "custom_report_runs.weekly_custom_report_runs",
                "custom_report_runs.weekly_custom_report_runs_previous_week",
                "custom_report_runs.custom_report_runs",
                "custom_report_runs.weekly_custom_report_users",
                "custom_report_runs.weekly_custom_report_users_previous_week",
                "service_logs.completed_service_logs",
                "unassigned_hos.total_unassigned_driving_hours",
                "infringements.infringements",
                "camera_privacy.driver_privacy_mode_cameras",
                "camera_privacy.complete_privacy_mode_cameras",
                "mobile_usage.mobile_usage_count",
                "mobile_usage.mobile_usage_miles",
                "asset_utilization.total_asset_utilized_hours",
                "asset_utilization.total_asset_available_hours",
                "hos_violations.hos_violation_duration_hours",
                "hos_violations.possible_driver_hours",
            ],
            column_type=ColumnType.METRIC,
            default_value=0,
            value_overrides=["marketplace_integrations_list"],
        ),
        LEFT_JOIN_STATEMENTS=generate_left_join_statements(
            {
                "licenses": "licenses",
                "device_licenses": "device_licenses",
                "licenses_previous_month_renamed": "licenses_previous_month",
                "device_licenses_previous_month_renamed": "device_licenses_previous_month",
                "assigned_licenses": "assigned_licenses",
                "assigned_licenses_previous_month_renamed": "assigned_licenses_previous_month",
                "driver_activity": "driver_activity",
                "driver_activity_previous_week": "driver_activity_previous_week",
                "admin_activity": "admin_activity",
                "admin_activity_previous_week": "admin_activity_previous_week",
                "pages_viewed": "pages_viewed",
                "marketplace_integrations": "marketplace_integrations",
                "api_requests": "api_requests",
                "alert_configs": "alert_configs",
                "alerts_triggered": "alerts_triggered",
                "triage_events": "triage_events",
                "coached_events": "coached_events",
                "created_routes": "created_routes",
                "dvirs": "dvirs",
                "maintenance_schedules": "maintenance_schedules",
                "hos_logs": "hos_logs",
                "workflows": "workflows",
                "workflow_templates": "workflow_templates",
                "custom_courses": "custom_courses",
                "vehicles": "vehicles",
                "diagnostics_coverage": "diagnostics",
                "safety_settings_org_agg_cm": "safety_settings_org_agg_cm",
                "safety_settings_org_agg_vg": "safety_settings_org_agg_vg",
                "geofences": "geofences",
                "trips": "trips",
                "ai_chats": "ai_chats",
                "custom_reports": "custom_reports",
                "custom_report_runs": "custom_report_runs",
                "service_logs": "service_logs",
                "unassigned_hos": "unassigned_hos",
                "infringements": "infringements",
                "camera_privacy": "camera_privacy",
                "api_tokens": "api_tokens",
                "mobile_usage": "mobile_usage",
                "asset_utilization": "asset_utilization",
                "hos_violations": "hos_violations",
                "installed_counts_org": "installed_counts_org",
                "active_counts_org": "active_counts_org",
            },
            ["date", "org_id"]
        ),
    )
    context.log.info(f"{query}")
    return query
