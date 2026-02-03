from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.executive_scorecard_constants import PARTITIONING
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Day for which usage is being tracked",
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
            "comment": "Name of the organization"
        },
    },
    {
        "name": "org_category",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category of org, based on licenses and internal users in corresponding account"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """ARR from the previously publicly reporting quarter in the buckets:
                <0
                0 - 100k
                100k - 500K,
                500K - 1M
                1M+
                Note - ARR information is calculated at the account level, not the SAM Number level
                An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
                """
        },
    },
    {
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_billing_country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by billing country"
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Cloud region of the organization",
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.SAM_NUMBER
        },
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique ID of the account"
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the account"
        },
    },
    {
        "name": "enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org has access to the feature or not",
        },
    },
    {
        "name": "usage_weekly",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the last 7 days",
        },
    },
    {
        "name": "usage_monthly",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the last 28 days",
        },
    },
    {
        "name": "usage_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the 28 days prior to the last 28 days",
        },
    },
    {
        "name": "usage_weekly_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the 7 days prior to the last 28 days",
        },
    },
    {
        "name": "daily_active_user",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of active users for the day for the feature",
        },
    },
    {
        "name": "daily_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users who had access to use the feature on the day",
        },
    },
    {
        "name": "weekly_active_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users over last 7 days for the feature",
        },
    },
    {
        "name": "weekly_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users who had access to use the feature in the last 7 days",
        },
    },
    {
        "name": "monthly_active_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users over last 28 days for the feature",
        },
    },
    {
        "name": "monthly_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users who had access to use the feature in the last 28 days",
        },
    },
    {
        "name": "org_active_day",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature on the day or not",
        },
    },
    {
        "name": "org_active_week",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in the last week or not",
        },
    },
    {
        "name": "org_active_week_prior_month",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in the week 28 days ago or not",
        },
    },
    {
        "name": "org_active_month",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in the last 28 days or not",
        },
    },
    {
        "name": "org_active_prior_month",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in 28 days prior to the last 28 days or not",
        },
    },
    {
        "name": "feature",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of feature being tracked",
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp when the table was last run for a day",
        },
    },
]

UPSTREAMS = [
    "us-west-2:product_analytics_staging.agg_executive_scorecard_acr_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_advanced_idling_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_automations_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_assistant_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_assistant_main_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_assistant_open_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_fault_codes_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_fault_codes_entrypoint_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_report_builder_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ai_report_builder_entrypoint_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_api_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_asset_qualifications_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_coaching_automation_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_connected_asset_maintenance_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_connected_training_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_connected_workflows_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_custom_dashboards_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_detention_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_device_install_workflow_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_driver_app_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_driver_detail_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_driver_efficiency_report_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_dvir_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_dvir2_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_fleet_benchmarks_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_fuel_card_integrations_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_fuel_card_transactions_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_fuel_theft_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_functions_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_group_coaching_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_hos_coaching_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_hos_logs_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_idling_coaching_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_ifta_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_maintenance_status_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_manager_coaching_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_marketplace_integrations_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_multicam_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_navigation_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_incab_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_performance_overview_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_positive_recognition_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_roadside_parking_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_route_forms_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_route_planning_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_routing_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_safety_inbox_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_safety_incab_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_safety_settings_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_safety_training_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_spotlight_search_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_utilization_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_virtual_coaching_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_weather_intelligence_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_worker_qualifications_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_worker_safety_mobile_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_global",
    "us-west-2:product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_incidents_global",
]

QUERY = """
    WITH base_features AS (
        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Advanced Custom Reports' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_acr_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Advanced Idling Report' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_advanced_idling_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Assistant' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Fault Code Descriptions' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_fault_codes_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'API Usage' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_api_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Asset Qualifications' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_asset_qualifications_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Automations' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_automations_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Driver Efficiency Report' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_driver_efficiency_report_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            CONCAT(feature, ' Incab Alerts') AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            CAST(0 AS BIGINT) AS daily_active_user,
            CAST(0 AS BIGINT) AS daily_enabled_users,
            CAST(0 AS BIGINT) AS weekly_active_users,
            CAST(0 AS BIGINT) AS weekly_enabled_users,
            CAST(0 AS BIGINT) AS monthly_active_users,
            CAST(0 AS BIGINT) AS monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_safety_incab_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            CAST(0 AS BIGINT) AS daily_active_user,
            CAST(0 AS BIGINT) AS daily_enabled_users,
            CAST(0 AS BIGINT) AS weekly_active_users,
            CAST(0 AS BIGINT) AS weekly_enabled_users,
            CAST(0 AS BIGINT) AS monthly_active_users,
            CAST(0 AS BIGINT) AS monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_safety_settings_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'DVIR' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_dvir_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'DVIR 2.0' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_dvir2_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Driver App' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_driver_app_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Fleet Benchmarks' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_fleet_benchmarks_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Functions' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_functions_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Fuel Card Integrations' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_fuel_card_integrations_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Positive Recognition' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_positive_recognition_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Route Planning' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_route_planning_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Routing and Dispatch' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_routing_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Safety Inbox v2' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_safety_inbox_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Coaching Automation' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_coaching_automation_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Performance Overview' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_performance_overview_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Connected Workflows' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_connected_workflows_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Connected Training' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_connected_training_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Turn-by-Turn Navigation' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_navigation_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Connected Asset Maintenance' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_connected_asset_maintenance_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Fuel Card Transactions Investigation' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_fuel_card_transactions_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Fuel Theft' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_fuel_theft_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'HoS Coaching' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_hos_coaching_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'HoS Logs' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_hos_logs_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Idling Coaching' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_idling_coaching_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'IFTA Report' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ifta_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Worker Safety Mobile' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_worker_safety_mobile_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Worker Safety Wearable' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Worker Safety Wearable Incidents' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_worker_safety_wearable_incidents_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Worker Qualifications' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_worker_qualifications_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Asset Utilization v2' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_utilization_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Weather Intelligence' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_weather_intelligence_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Safety Training' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_safety_training_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Route Forms' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_route_forms_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Driver Event Review' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_virtual_coaching_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Manager-Led Coaching' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_manager_coaching_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Marketplace Integrations' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_marketplace_integrations_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Group Coaching' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_group_coaching_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Roadside Parking' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_roadside_parking_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Detention 2.0' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_detention_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Custom Dashboards' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_custom_dashboards_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Maintenance Status' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_maintenance_status_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Device Install Workflow' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_device_install_workflow_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Assistant (Main Flow)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_main_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Assistant (Open Chat)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_open_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Fault Codes (Ask Samsara)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_fault_codes_entrypoint_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Report Builder (Create Report)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Report Builder (Build With AI)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_entrypoint_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Pedestrian Collision Warning (Multicam)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Pedestrian Collision Warning (Brigid)' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Spotlight Search' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_spotlight_search_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Driver Detail v2' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_driver_detail_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Pedestrian Collision Warning (Brigid) Incab Alerts' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'Pedestrian Collision Warning (Multicam) Incab Alerts' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_multicam_incab_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'

        UNION ALL

        SELECT
            date,
            org_id,
            org_name,
            org_category,
            account_arr_segment,
            account_size_segment_name,
            account_billing_country,
            region,
            sam_number,
            account_id,
            account_name,
            'AI Multicam' AS feature,
            enabled,
            usage_weekly,
            usage_monthly,
            usage_prior_month,
            usage_weekly_prior_month,
            daily_active_user,
            daily_enabled_users,
            weekly_active_users,
            weekly_enabled_users,
            monthly_active_users,
            monthly_enabled_users,
            org_active_day,
            org_active_week,
            org_active_week_prior_month,
            org_active_month,
            org_active_prior_month
        FROM product_analytics_staging.agg_executive_scorecard_multicam_global
        WHERE org_category NOT IN ('Failed Trial', 'Internal Orgs')
        AND date = '{PARTITION_START}'
    )
    SELECT *,
    CURRENT_TIMESTAMP AS updated_at
    FROM base_features
    """

@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""A dataset of all unioned features from Product Usage""",
        row_meaning="""Each row represent an org's usage of a feature""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=UPSTREAMS,
    primary_keys=[
        "date",
        "org_id",
        "feature",
    ],
    partitioning=PARTITIONING,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_agg_product_usage_global"),
        NonNullDQCheck(
            name="dq_non_null_agg_product_usage_global",
            non_null_columns=["date", "org_id", "region", "feature"],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_product_usage_global",
            primary_keys=[
                "date",
                "org_id",
                "feature",
            ],
            block_before_write=True,
        ),
    ],
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def agg_product_usage_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_product_usage_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    context.log.info(f"{query}")
    return query
