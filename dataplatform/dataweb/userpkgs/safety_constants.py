from dataweb.userpkgs.constants import (
    ColumnDescription,
    DimensionConfig,
    DimensionDataType,
)

# Dimensions
SAFETY_DIMENSION_TEMPLATES = {
    "DIMENSIONS": {
        "US": {
            "alias": "cc",
            "region": "us-west-2",
        },
        "EU": {
            "alias": "cc",
            "region": "eu-west-1",
        },
        "CA": {
            "alias": "cc",
            "region": "ca-central-1",
        }
    },
}

SAFETY_DIMENSIONS = {
    "time_dimensions": [
        ("root", "'{DATEID}'"),
        ("period", "'{PERIOD}'"),
        ("period_start", "'{START_OF_PERIOD}'"),
        ("period_end", "'{END_OF_PERIOD}'"),
    ],
    "account_dimensions": [
        ("account_size_segment", "CASE WHEN {ALIAS}.account_size_segment IS NULL THEN 'Unknown' ELSE {ALIAS}.account_size_segment END"),
        ("account_industry", "CASE WHEN {ALIAS}.account_industry IS NULL THEN 'Unknown' ELSE {ALIAS}.account_industry END"),
        ("account_arr_segment", "CASE WHEN {ALIAS}.account_arr_segment IS NULL THEN 'Unknown' ELSE {ALIAS}.account_arr_segment END"),
        ("account_billing_country", "CASE WHEN {ALIAS}.account_billing_country IS NULL THEN 'Unknown' ELSE {ALIAS}.account_billing_country END"),
        ("account_csm_email", "CASE WHEN {ALIAS}.account_csm_email IS NULL THEN 'Unknown' ELSE {ALIAS}.account_csm_email END"),
        ("account_name", "CASE WHEN {ALIAS}.account_name IS NULL THEN 'Unknown' ELSE {ALIAS}.account_name END"),
        ("account_id", "CASE WHEN {ALIAS}.account_id IS NULL THEN 'Unknown' ELSE {ALIAS}.account_id END"),
        ("account_created_date", "CASE WHEN {ALIAS}.account_created_date IS NULL THEN 'Unknown' ELSE {ALIAS}.account_created_date END"),
        ("account_renewal_12mo", "{ALIAS}.account_renewal_12mo"),
    ],
    "product_dimensions": [
        ("is_paid_safety_customer", "CASE WHEN {ALIAS}.is_paid_safety_customer = 1 THEN TRUE ELSE FALSE END"),
        ("is_paid_telematics_customer", "CASE WHEN {ALIAS}.is_paid_telematics_customer = 1 THEN TRUE ELSE FALSE END"),
        ("is_paid_stce_customer", "CASE WHEN {ALIAS}.is_paid_stce_customer = 1 THEN TRUE ELSE FALSE END"),
    ],
    "sam_dimensions": [
        ("sam_number", "CASE WHEN {ALIAS}.sam_number IS NULL THEN 'Unknown' ELSE {ALIAS}.sam_number END"),
    ],
    "device_dimensions": [
        ("dual_facing_cam", "{ALIAS}.dual_facing_cam"),
    ],
    "event_type_dimensions": [
        ("event_type", "{ALIAS}.event_type"),
        ("dual_facing_event_type", "{ALIAS}.dual_facing_event_type"),
    ]
}

# Region mappings
SAFETY_REGION_MAPPINGS = {
    "us-west-2": {
        "cloud_region": "us-west-2",
        "region_alias": "US",
    },
    "eu-west-1": {
        "cloud_region": "eu-west-1",
        "region_alias": "EU"
    },
    "ca-central-1": {
        "cloud_region": "ca-central-1",
        "region_alias": "CA"
    }
}

# Join mappings
SAFETY_JOIN_MAPPINGS = {
    "us-west-2": {
        "join_statements": """
            JOIN us_customer_check cc
                ON {join_alias}.sam_number = cc.sam_number
                AND {join_alias}.account_id = cc.account_id
        """,
    },
    "eu-west-1": {
        "join_statements": """
            JOIN eu_customer_check cc
                ON {join_alias}.sam_number = cc.sam_number
                AND {join_alias}.account_id = cc.account_id
        """,
    },
    "ca-central-1": {
        "join_statements": """
            JOIN ca_customer_check cc
                ON {join_alias}.sam_number = cc.sam_number
                AND {join_alias}.account_id = cc.account_id
        """,
    }
}

EVENT_SAFETY_JOIN_MAPPINGS = {
    "us-west-2": {
        "join_statements": """
            JOIN us_customer_check cc
                ON {join_alias}.sam_number = cc.sam_number
                AND {join_alias}.account_id = cc.account_id
                AND {join_alias}.event_type = cc.event_type
        """,
    },
    "eu-west-1": {
        "join_statements": """
            JOIN eu_customer_check cc
                ON {join_alias}.sam_number = cc.sam_number
                AND {join_alias}.account_id = cc.account_id
                AND {join_alias}.event_type = cc.event_type
        """,
    },
    "ca-central-1": {
        "join_statements": """
            JOIN ca_customer_check cc
                ON {join_alias}.sam_number = cc.sam_number
                AND {join_alias}.account_id = cc.account_id
                AND {join_alias}.event_type = cc.event_type
        """,
    }
}

# Dimension configs
DIMENSION_CONFIGS = {
    "period_start": DimensionConfig(name="period_start", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "period_end": DimensionConfig(name="period_end", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "sam_number": DimensionConfig(name="sam_number", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_size_segment": DimensionConfig(name="account_size_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_arr_segment": DimensionConfig(name="account_arr_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_billing_country": DimensionConfig(name="account_billing_country", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_industry": DimensionConfig(name="account_industry", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "industry_vertical": DimensionConfig(name="industry_vertical", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "is_paid_safety_customer": DimensionConfig(name="is_paid_safety_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_telematics_customer": DimensionConfig(name="is_paid_telematics_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_stce_customer": DimensionConfig(name="is_paid_stce_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "account_csm_email": DimensionConfig(name="account_csm_email", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_name": DimensionConfig(name="account_name", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_id": DimensionConfig(name="account_id", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_created_date": DimensionConfig(name="account_created_date", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_renewal_12mo": DimensionConfig(name="account_renewal_12mo", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "dual_facing_cam": DimensionConfig(name="dual_facing_cam", data_type=DimensionDataType.BOOLEAN, default_value="false"),
}

# Dimension configs
EVENT_DIMENSION_CONFIGS = {
    "period_start": DimensionConfig(name="period_start", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "period_end": DimensionConfig(name="period_end", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "sam_number": DimensionConfig(name="sam_number", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_size_segment": DimensionConfig(name="account_size_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_arr_segment": DimensionConfig(name="account_arr_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_billing_country": DimensionConfig(name="account_billing_country", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_industry": DimensionConfig(name="account_industry", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "industry_vertical": DimensionConfig(name="industry_vertical", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "is_paid_safety_customer": DimensionConfig(name="is_paid_safety_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_telematics_customer": DimensionConfig(name="is_paid_telematics_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_stce_customer": DimensionConfig(name="is_paid_stce_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "account_csm_email": DimensionConfig(name="account_csm_email", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_name": DimensionConfig(name="account_name", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_id": DimensionConfig(name="account_id", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_created_date": DimensionConfig(name="account_created_date", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_renewal_12mo": DimensionConfig(name="account_renewal_12mo", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "dual_facing_cam": DimensionConfig(name="dual_facing_cam", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "event_type": DimensionConfig(name="event_type", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "dual_facing_event_type": DimensionConfig(name="dual_facing_event_type", data_type=DimensionDataType.BOOLEAN, default_value="false"),
}

# Metrics
SAFETY_METRICS = {
    "total_drivers": "COUNT(DISTINCT driver_id)",
    "total_drivers_using_app_month": "COUNT(DISTINCT CASE WHEN last_mobile_login_date >= '{START_OF_PERIOD}' THEN driver_id END)",
    "total_drivers_using_app": "COUNT(DISTINCT CASE WHEN last_mobile_login_date IS NOT NULL THEN driver_id END)",
    "assigned_trainings": "COUNT(DISTINCT uuid)",
    "total_drivers_assigned_training": "COUNT(DISTINCT CASE WHEN assigned_to_polymorphic IS NOT NULL THEN assigned_to_polymorphic END)",
    "total_drivers_completing_training_on_time": "COUNT(DISTINCT CASE WHEN completed_at < due_date AND assigned_to_polymorphic IS NOT NULL THEN assigned_to_polymorphic END)",
    "drivers_completing_manager_led_coaching": "COUNT(DISTINCT CASE WHEN DATE(completed_date) IS NOT NULL AND driver_id IS NOT NULL AND driver_id != 0 THEN driver_id END)",
    "drivers_completing_self_coaching": "COUNT(DISTINCT CASE WHEN DATE(completed_date) IS NOT NULL AND driver_id IS NOT NULL AND driver_id != 0 THEN driver_id END)",
    "drivers_completing_coaching": "COUNT(DISTINCT CASE WHEN DATE(completed_date) IS NOT NULL AND driver_id IS NOT NULL AND driver_id != 0 THEN driver_id END)",
    "miles": "SUM(distance_miles)",
}

EVENT_SAFETY_METRICS = {
    "event_type_enabled": "MAX(CASE WHEN event_type_enabled = TRUE THEN 1 ELSE 0 END)",
    "in_cab_alerts_enabled": "MAX(CASE WHEN incab_enabled = TRUE THEN 1 ELSE 0 END)",
    "send_to_safety_inbox_enabled": "MAX(CASE WHEN send_to_safety_inbox = TRUE THEN 1 ELSE 0 END)",
    "inbox_events": "COUNT(DISTINCT uuid)",
    "inbox_events_viewed": "COUNT(DISTINCT CASE WHEN was_event_viewed = TRUE THEN uuid END)",
    "inbox_events_dismissed": "COUNT(DISTINCT CASE WHEN triage_state = 4 THEN uuid END)",
    "inbox_events_viewed_dismissed": "COUNT(DISTINCT CASE WHEN was_event_viewed = TRUE AND triage_state = 4 THEN uuid END)",
    "inbox_events_viewed_not_useful": "COUNT(DISTINCT CASE WHEN was_event_viewed = TRUE AND event_was_useful = FALSE THEN uuid END)",
    "eligible_events": "COUNT(DISTINCT CASE WHEN triage_state != 4 THEN uuid END)",
    "eligible_events_with_driver": "COUNT(DISTINCT CASE WHEN triage_state != 4 AND driver_id != 0 THEN uuid END)",
    "in_cab_alerts": "COUNT(DISTINCT CASE WHEN in_cab_alert_played_at_ms IS NOT NULL THEN uuid END)",
    "events_without_alert": "COUNT(DISTINCT CASE WHEN in_cab_alert_played_at_ms IS NULL AND triage_org_id IS NOT NULL THEN triage_uuid END)",
    "detection_log_events": "COUNT(DISTINCT uuid)",
    "detection_log_events_caps": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason IN (1, 2, 3, 5, 6, 7) THEN uuid END)",
    "detection_log_events_threshold": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason = 16 THEN uuid END)",
    "detection_log_events_nudges": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason = 15 THEN uuid END)",
    "detection_log_events_speed": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason = 14 THEN uuid END)",
    "detection_log_events_confidence": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason = 12 THEN uuid END)",
    "detection_log_events_severity": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason IN (8, 17) THEN uuid END)",
    "detection_log_events_ser": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason = 26 THEN uuid END)",
    "detection_log_events_incab": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason = 36 THEN uuid END)",
    "detection_log_events_other": "COUNT(DISTINCT CASE WHEN (customer_facing_filter_reason NOT IN (1, 2, 3, 5, 6, 7, 8, 12, 14, 15, 16, 17, 26, 36) AND customer_facing_filter_reason IS NOT NULL) OR (filter_reason IS NOT NULL) THEN uuid END)",
    "detection_log_events_no_filter": "COUNT(DISTINCT CASE WHEN customer_facing_filter_reason IS NULL AND filter_reason IS NULL THEN uuid END)",
    "self_coaching_events": "COUNT(DISTINCT coachable_item_uuid)",
    "self_coaching_events_completed": "COUNT(DISTINCT CASE WHEN coaching_state = 100 THEN coachable_item_uuid END)",
    "total_drivers_assigned_self_coaching": "COUNT(DISTINCT CASE WHEN driver_id != 0 THEN driver_id END)",
    "total_drivers_completed_self_coaching": "COUNT(DISTINCT CASE WHEN coaching_state = 100 AND driver_id != 0 THEN driver_id END)",
    "manager_led_coaching_sessions": "COUNT(DISTINCT uuid)",
    "manager_led_coaching_events": "COUNT(DISTINCT coachable_item_uuid)",
    "manager_led_coaching_events_completed": "COUNT(DISTINCT CASE WHEN coaching_state = 100 THEN coachable_item_uuid END)",
    "manager_led_coaches": "COUNT(DISTINCT coach_id)",
    "manager_led_coaches_coaching_on_time": "COUNT(DISTINCT CASE WHEN completed_date <= due_date THEN coach_id END)",
    "completed_manager_led_coaching_sessions": "COUNT(DISTINCT CASE WHEN completed_date IS NOT NULL THEN uuid END)",
    "completed_manager_led_coaching_sessions_14d": "COUNT(DISTINCT CASE WHEN DATEDIFF(completed_date, date) <= 14 THEN uuid END)",
    "manager_led_coaching_drivers": "COUNT(DISTINCT CASE WHEN driver_id != 0 THEN driver_id END)",
    "manager_led_coaching_drivers_coached": "COUNT(DISTINCT CASE WHEN coaching_state = 100 AND driver_id != 0 THEN driver_id END)",
    "miles_event": "SUM(distance_miles)",
}

SAFETY_METRIC_CONFIGS_TEMPLATE = {
    "drivers_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "drivers_us",
        "eu_source": "drivers_eu",
        "ca_source": "drivers_ca",
        "table_alias": "dd",
        "join_alias": "dd",
        "metrics": [
            "total_drivers",
            "total_drivers_using_app_month",
            "total_drivers_using_app",
        ]
    },
    "trainings_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "training_us",
        "eu_source": "training_eu",
        "ca_source": "training_ca",
        "table_alias": "t",
        "join_alias": "t",
        "metrics": [
            "assigned_trainings",
            "total_drivers_assigned_training",
            "total_drivers_completing_training_on_time",
        ]
    },
    "self_coaching_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "self_coaching_us",
        "eu_source": "self_coaching_eu",
        "ca_source": "self_coaching_ca",
        "table_alias": "s",
        "join_alias": "s",
        "metrics": ["drivers_completing_self_coaching"],
    },
    "manager_led_coaching_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "manager_led_coaching_us",
        "eu_source": "manager_led_coaching_eu",
        "ca_source": "manager_led_coaching_ca",
        "table_alias": "m",
        "join_alias": "m",
        "metrics": ["drivers_completing_manager_led_coaching"],
    },
    "coaching_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "coaching_us",
        "eu_source": "coaching_eu",
        "ca_source": "coaching_ca",
        "table_alias": "c",
        "join_alias": "c",
        "metrics": ["drivers_completing_coaching"],
    },
    "trips_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "trips_us",
        "eu_source": "trips_eu",
        "ca_source": "trips_ca",
        "table_alias": "tr",
        "join_alias": "tr",
        "metrics": ["miles"],
    },
}

EVENT_SAFETY_METRIC_CONFIGS_TEMPLATE = {
    "enabled_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "enabled_us",
        "eu_source": "enabled_eu",
        "ca_source": "enabled_ca",
        "table_alias": "e",
        "join_alias": "e",
        "metrics": [
            "event_type_enabled",
            "in_cab_alerts_enabled",
            "send_to_safety_inbox_enabled",
        ]
    },
    "detection_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "detection_us",
        "eu_source": "detection_eu",
        "ca_source": "detection_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": [
            "detection_log_events",
            "in_cab_alerts",
            "events_without_alert",
            "detection_log_events_caps",
            "detection_log_events_threshold",
            "detection_log_events_nudges",
            "detection_log_events_speed",
            "detection_log_events_confidence",
            "detection_log_events_severity",
            "detection_log_events_ser",
            "detection_log_events_incab",
            "detection_log_events_other",
            "detection_log_events_no_filter",
        ]
    },
    "triage_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "triage_us",
        "eu_source": "triage_eu",
        "ca_source": "triage_ca",
        "table_alias": "t",
        "join_alias": "t",
        "metrics": [
            "inbox_events",
            "inbox_events_viewed",
            "inbox_events_dismissed",
            "inbox_events_viewed_dismissed",
            "inbox_events_viewed_not_useful",
            "eligible_events",
            "eligible_events_with_driver",
        ]
    },
    "self_coaching_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "self_coaching_us",
        "eu_source": "self_coaching_eu",
        "ca_source": "self_coaching_ca",
        "table_alias": "s",
        "join_alias": "s",
        "metrics": [
            "self_coaching_events",
            "self_coaching_events_completed",
            "total_drivers_assigned_self_coaching",
            "total_drivers_completed_self_coaching",
        ]
    },
    "manager_led_coaching_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "manager_led_coaching_us",
        "eu_source": "manager_led_coaching_eu",
        "ca_source": "manager_led_coaching_ca",
        "table_alias": "m",
        "join_alias": "m",
        "metrics": [
            "manager_led_coaching_sessions",
            "manager_led_coaching_events",
            "manager_led_coaching_events_completed",
            "manager_led_coaches",
            "manager_led_coaches_coaching_on_time",
            "completed_manager_led_coaching_sessions",
            "completed_manager_led_coaching_sessions_14d",
            "manager_led_coaching_drivers",
            "manager_led_coaching_drivers_coached",
        ]
    },
    "trips_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "trips_us",
        "eu_source": "trips_eu",
        "ca_source": "trips_ca",
        "table_alias": "t",
        "join_alias": "t",
        "metrics": [
            "miles_event",
        ]
    },
}

SAFETY_SCHEMA = [
    {
        "name": "date_placeholder",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The date of the run (shouldn't be used)"
        },
    },
    {
        "name": "period_start",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Start of period"
        },
    },
    {
        "name": "period_end",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "End of period"
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
        "name": "account_size_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """
            ARR from the previously publicly reporting quarter in the buckets:
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
        "name": "account_billing_country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Geographic information about the account"
        },
    },
    {
        "name": "account_industry",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Industry classification of the account."
        },
    },
    {
        "name": "industry_vertical",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Values include `Transportation, Wholesale & Retail Trade, Manufacturing`, `Field Services, Construction, Utilities & Energy`, `Government, Education, Healthcare`, `Unknown`, `All`"
        },
    },
    {
        "name": "is_paid_safety_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
            (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
            (2) at least one active license within safety product family
            (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
            (4) internal_type of org_id is equal to 0
            (5) org_id has a non null sam_number
            (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'"""
        },
    },
    {
        "name": "is_paid_telematics_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
            (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
            (2) at least one active license within telematics product family
            (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
            (4) internal_type of org_id is equal to 0
            (5) org_id has a non null sam_number
            (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'"""
        },
    },
    {
        "name": "is_paid_stce_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
            (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
            (2) at least one active license within stce product family
            (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
            (4) internal_type of org_id is equal to 0
            (5) org_id has a non null sam_number
            (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'"""
        },
    },
    {
        "name": "account_csm_email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email for the CSM of the org"
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
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ID of the account"
        },
    },
    {
        "name": "account_created_date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Creation date of the account"
        },
    },
    {
        "name": "account_renewal_12mo",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account is considered up for renewal in the next 12 months or not"
        },
    },
    {
        "name": "dual_facing_cam",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has any dual-facing camera devices or not"
        },
    },
    {
        "name": "total_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers in the account"
        },
    },
    {
        "name": "total_drivers_using_app_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers in the account who have logged into the Mobile App in the last month"
        },
    },
    {
        "name": "total_drivers_using_app",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers in the account who have logged into the Mobile App ever"
        },
    },
    {
        "name": "assigned_trainings",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of assigned trainings"
        },
    },
    {
        "name": "total_drivers_assigned_training",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers who were assigned training"
        },
    },
    {
        "name": "total_drivers_completing_training_on_time",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers who completed training on time"
        },
    },
    {
        "name": "drivers_completing_manager_led_coaching",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers who completed a coaching session with a manager-led event"
        },
    },
    {
        "name": "drivers_completing_self_coaching",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers who completed a coaching session with a self-coaching event"
        },
    },
    {
        "name": "drivers_completing_coaching",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers who completed a coaching session"
        },
    },
    {
        "name": "miles",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total miles covered by devices enabled for at least one safety event type in the account over the month"
        },
    },
]

EVENT_SAFETY_SCHEMA = [
    {
        "name": "date_placeholder",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The date of the run (shouldn't be used)"
        },
    },
    {
        "name": "period_start",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Start of period"
        },
    },
    {
        "name": "period_end",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "End of period"
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
        "name": "account_size_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """
            ARR from the previously publicly reporting quarter in the buckets:
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
        "name": "account_billing_country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Geographic information about the account"
        },
    },
    {
        "name": "account_industry",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.salesforce_sterling.account. Industry classification of the account."
        },
    },
    {
        "name": "industry_vertical",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Values include `Transportation, Wholesale & Retail Trade, Manufacturing`, `Field Services, Construction, Utilities & Energy`, `Government, Education, Healthcare`, `Unknown`, `All`"
        },
    },
    {
        "name": "is_paid_safety_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
            (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
            (2) at least one active license within safety product family
            (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
            (4) internal_type of org_id is equal to 0
            (5) org_id has a non null sam_number
            (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'"""
        },
    },
    {
        "name": "is_paid_telematics_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
            (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
            (2) at least one active license within telematics product family
            (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
            (4) internal_type of org_id is equal to 0
            (5) org_id has a non null sam_number
            (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'"""
        },
    },
    {
        "name": "is_paid_stce_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """TRUE if the following criteria are met:
            (1) org_id has a non-zero count of non-expired and non-trial licenses in edw.silver.fct_license_orders for corresponding date
            (2) at least one active license within stce product family
            (3) org_id not in internaldb.simulated_orgs (queried in stg_organizations)
            (4) internal_type of org_id is equal to 0
            (5) org_id has a non null sam_number
            (6) sam_number is not one of internally allocated accounts: 'C2N65AXKY7','CJEU6A8TFD','CR5MBC7RNY','C2NUBA3HFM','CNU9J2BYRW'"""
        },
    },
    {
        "name": "account_csm_email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email for the CSM of the org"
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
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ID of the account"
        },
    },
    {
        "name": "account_created_date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Creation date of the account"
        },
    },
    {
        "name": "account_renewal_12mo",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account is considered up for renewal in the next 12 months or not"
        },
    },
    {
        "name": "dual_facing_cam",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has any dual-facing camera devices or not"
        },
    },
    {
        "name": "event_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Type of the safety event"
        },
    },
    {
        "name": "dual_facing_event_type",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the event is a dual-facing event or not"
        },
    },
    {
        "name": "event_type_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has the event type enabled or not"
        },
    },
    {
        "name": "in_cab_alerts_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has in-cab alerts enabled or not"
        },
    },
    {
        "name": "send_to_safety_inbox_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has the send to safety inbox enabled or not"
        },
    },
    {
        "name": "inbox_events",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of triage events for the given event type"
        },
    },
    {
        "name": "inbox_events_viewed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of triage events for the given event type that were viewed by the customer"
        },
    },
    {
        "name": "inbox_events_dismissed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of triage events for the given event type that were dismissed by the customer"
        },
    },
    {
        "name": "inbox_events_viewed_dismissed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of triage events for the given event type that were viewed and dismissed by the customer"
        },
    },
    {
        "name": "inbox_events_viewed_not_useful",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of triage events for the given event type that were viewed and marked as not useful by the customer"
        },
    },
    {
        "name": "eligible_events",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of non-dismissed triage events for the given event type"
        },
    },
    {
        "name": "eligible_events_with_driver",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of non-dismissed triage events with a driver for the given event type"
        },
    },
    {
        "name": "detection_log_events",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events for the given event type"
        },
    },
    {
        "name": "in_cab_alerts",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of in-cab nudges for the given event type"
        },
    },
    {
        "name": "events_without_alert",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of triage events without in-cab alerts for the given event type"
        },
    },
    {
        "name": "total_detections",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Sum of triage events without in-cab alerts and in-cab alerts for the given event type"
        },
    },
    {
        "name": "detection_log_events_caps",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for a caps/rate limit purpose for the given event type"
        },
    },
    {
        "name": "detection_log_events_threshold",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for a threshold purpose for the given event type"
        },
    },
    {
        "name": "detection_log_events_nudges",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for low nudges for the given event type"
        },
    },
    {
        "name": "detection_log_events_speed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for low speed for the given event type"
        },
    },
    {
        "name": "detection_log_events_confidence",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for low confidence for the given event type"
        },
    },
    {
        "name": "detection_log_events_severity",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for low severity for the given event type"
        },
    },
    {
        "name": "detection_log_events_ser",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for SER for the given event type"
        },
    },
    {
        "name": "detection_log_events_incab",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for incab only for the given event type"
        },
    },
    {
        "name": "detection_log_events_other",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events filtered for other reasons for the given event type"
        },
    },
    {
        "name": "detection_log_events_no_filter",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of detection log events with no filter for the given event type"
        },
    },
    {
        "name": "self_coaching_events",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of coachable items from self-coaching for the given event type"
        },
    },
    {
        "name": "self_coaching_events_completed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of completed coachable items from self-coaching for the given event type"
        },
    },
    {
        "name": "total_drivers_assigned_self_coaching",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers in the account who have been assigned self-coaching for the given event type"
        },
    },
    {
        "name": "total_drivers_completed_self_coaching",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers in the account who have completed self-coaching for the given event type"
        },
    },
    {
        "name": "manager_led_coaching_sessions",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of manager-led coaching sessions for the given event type"
        },
    },
    {
        "name": "manager_led_coaching_events",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of coachable items from manager-led coaching for the given event type"
        },
    },
    {
        "name": "manager_led_coaching_events_completed",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of completed coachable items from manager-led coaching for the given event type"
        },
    },
    {
        "name": "manager_led_coaches",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of coaches involved in manager-led coaching for the given event type"
        },
    },
    {
        "name": "manager_led_coaches_coaching_on_time",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of coaches involved in manager-led coaching for the given event type that were able to get a session completed on time"
        },
    },
    {
        "name": "completed_manager_led_coaching_sessions",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of assigned manager-led coaching sessions for the given event type"
        },
    },
    {
        "name": "completed_manager_led_coaching_sessions_14d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of completed manager-led coaching sessions for the given event type within 14 days of being assigned"
        },
    },
    {
        "name": "manager_led_coaching_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers involved in manager-led coaching for the given event type"
        },
    },
    {
        "name": "manager_led_coaching_drivers_coached",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drivers who completed manager-led coaching sessions for the given event type"
        },
    },
    {
        "name": "miles_event",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total miles covered by enabled devices for the event types in the account over the month"
        },
    },
]

# Queries
SAFETY_QUERY = """
    WITH account_arr AS (
        -- Handles edge case where multiple accounts with different ARRs exist for a given SAM
        SELECT
            account_id,
            customer_arr,
            customer_arr_segment
        FROM product_analytics_staging.stg_customer_enriched
        WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
    ),
    account_arr_sam AS (
        -- Handles cases where there's no account_id match (take max for SAM instead)
        SELECT sam_number_undecorated AS sam_number,
        MAX_BY(customer_arr, customer_arr) AS customer_arr,
        MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
        FROM product_analytics_staging.stg_customer_enriched
        WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
        GROUP BY sam_number_undecorated
    ),
    pubsec_us AS (
        -- This identifies whether an organization is PubSec or not
        -- Note that we only care about this for US accounts
        SELECT
            sam_number,
            sled_account
        FROM finopsdb.customer_info
    ),
    pubsec_eu AS (
        -- This identifies whether an organization is PubSec or not
        -- Note that we only care about this for US accounts
        SELECT
            sam_number,
            sled_account
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
    ),
    pubsec_ca AS (
        -- This identifies whether an organization is PubSec or not
        -- Note that we only care about this for US accounts
        SELECT
            sam_number,
            sled_account
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
    ),
    total_cm_devices_us AS (
        -- This identifies how many dual-facing CMs an org has
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN associated_devices.camera_product_id IN (43, 155) THEN device_id END) AS dual_facing_cms
        FROM datamodel_core.dim_devices
        WHERE
            date = '{END_OF_PERIOD}'
            AND device_type = 'VG - Vehicle Gateway'
        GROUP BY 1
    ),
    total_cm_devices_eu AS (
        -- This identifies how many dual-facing CMs an org has
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN associated_devices.camera_product_id IN (43, 155) THEN device_id END) AS dual_facing_cms
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices`
        WHERE
            date = '{END_OF_PERIOD}'
            AND device_type = 'VG - Vehicle Gateway'
        GROUP BY 1
    ),
    total_cm_devices_ca AS (
        -- This identifies how many dual-facing CMs an org has
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN associated_devices.camera_product_id IN (43, 155) THEN device_id END) AS dual_facing_cms
        FROM data_tools_delta_share_ca.datamodel_core.dim_devices
        WHERE
            date = '{END_OF_PERIOD}'
            AND device_type = 'VG - Vehicle Gateway'
        GROUP BY 1
    ),
    latest_org_us AS (
        -- org data at end of period
        SELECT DISTINCT
            o.org_id,
            o.sam_number,
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
            CASE
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
                ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
            END AS account_arr_segment,
            COALESCE(o.account_size_segment, 'Unknown') AS account_size_segment,
            CASE
                WHEN o.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil and Gas'
                ELSE COALESCE(o.account_industry, 'Unknown')
            END AS account_industry,
            o.is_paid_safety_customer,
            o.is_paid_telematics_customer,
            o.is_paid_stce_customer,
            COALESCE(o.account_csm_email, 'Unknown') AS account_csm_email,
            COALESCE(o.account_name, 'Unknown') AS account_name,
            COALESCE(o.account_id, 'Unknown') AS account_id,
            COALESCE(o.account_created_date, 'Unknown') AS account_created_date,
            FALSE AS account_renewal_12mo,
            COALESCE(t.dual_facing_cms, 0) AS dual_facing_cms
        FROM datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_us p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN total_cm_devices_us t
            ON o.org_id = t.org_id
        WHERE o.date = '{END_OF_PERIOD}'
        AND o.is_paid_customer
        AND (
            o.is_paid_safety_customer = TRUE
        )
    ),
    us_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            '{START_OF_PERIOD}' AS date_month,
            '{START_OF_PERIOD}' AS period_start,
            '{END_OF_PERIOD}' AS period_end,
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_created_date,
            account_renewal_12mo,
            SUM(dual_facing_cms) > 0 AS dual_facing_cam,
            MAX_BY(account_size_segment, CASE WHEN account_size_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_size_segment,
            MAX_BY(account_arr_segment, CASE WHEN account_arr_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_arr_segment,
            MAX_BY(account_billing_country, CASE WHEN account_billing_country != 'Unknown' THEN 1 ELSE 0 END) AS account_billing_country,
            MAX_BY(account_industry, CASE WHEN account_industry != 'Unknown' THEN 1 ELSE 0 END) AS account_industry,
            MAX(CASE WHEN is_paid_safety_customer = true THEN 1 ELSE 0 END) AS is_paid_safety_customer,
            MAX(CASE WHEN is_paid_telematics_customer = true THEN 1 ELSE 0 END) AS is_paid_telematics_customer,
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer
        FROM latest_org_us
        GROUP BY ALL
    ),
    latest_org_eu AS (
        -- org data at end of period
        SELECT DISTINCT
            o.org_id,
            o.sam_number,
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
            CASE
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
                ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
            END AS account_arr_segment,
            COALESCE(o.account_size_segment, 'Unknown') AS account_size_segment,
            CASE
                WHEN o.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil and Gas'
                ELSE COALESCE(o.account_industry, 'Unknown')
            END AS account_industry,
            o.is_paid_safety_customer,
            o.is_paid_telematics_customer,
            o.is_paid_stce_customer,
            COALESCE(o.account_csm_email, 'Unknown') AS account_csm_email,
            COALESCE(o.account_name, 'Unknown') AS account_name,
            COALESCE(o.account_id, 'Unknown') AS account_id,
            COALESCE(o.account_created_date, 'Unknown') AS account_created_date,
            FALSE AS account_renewal_12mo,
            COALESCE(t.dual_facing_cms, 0) AS dual_facing_cms
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_eu p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN total_cm_devices_eu t
            ON o.org_id = t.org_id
        WHERE o.date = '{END_OF_PERIOD}'
        AND o.is_paid_customer
        AND (
            o.is_paid_safety_customer = TRUE
        )
    ),
    eu_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            '{START_OF_PERIOD}' AS date_month,
            '{START_OF_PERIOD}' AS period_start,
            '{END_OF_PERIOD}' AS period_end,
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_created_date,
            account_renewal_12mo,
            SUM(dual_facing_cms) > 0 AS dual_facing_cam,
            MAX_BY(account_size_segment, CASE WHEN account_size_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_size_segment,
            MAX_BY(account_arr_segment, CASE WHEN account_arr_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_arr_segment,
            MAX_BY(account_billing_country, CASE WHEN account_billing_country != 'Unknown' THEN 1 ELSE 0 END) AS account_billing_country,
            MAX_BY(account_industry, CASE WHEN account_industry != 'Unknown' THEN 1 ELSE 0 END) AS account_industry,
            MAX(CASE WHEN is_paid_safety_customer = true THEN 1 ELSE 0 END) AS is_paid_safety_customer,
            MAX(CASE WHEN is_paid_telematics_customer = true THEN 1 ELSE 0 END) AS is_paid_telematics_customer,
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer
        FROM latest_org_eu
        GROUP BY ALL
    ),
    latest_org_ca AS (
        -- org data at end of period
        SELECT DISTINCT
            o.org_id,
            o.sam_number,
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
            CASE
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
                ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
            END AS account_arr_segment,
            COALESCE(o.account_size_segment, 'Unknown') AS account_size_segment,
            CASE
                WHEN o.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil and Gas'
                ELSE COALESCE(o.account_industry, 'Unknown')
            END AS account_industry,
            o.is_paid_safety_customer,
            o.is_paid_telematics_customer,
            o.is_paid_stce_customer,
            COALESCE(o.account_csm_email, 'Unknown') AS account_csm_email,
            COALESCE(o.account_name, 'Unknown') AS account_name,
            COALESCE(o.account_id, 'Unknown') AS account_id,
            COALESCE(o.account_created_date, 'Unknown') AS account_created_date,
            FALSE AS account_renewal_12mo,
            COALESCE(t.dual_facing_cms, 0) AS dual_facing_cms
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_ca p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN total_cm_devices_ca t
            ON o.org_id = t.org_id
        WHERE o.date = '{END_OF_PERIOD}'
        AND o.is_paid_customer
        AND (
            o.is_paid_safety_customer = TRUE
        )
    ),
    ca_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            '{START_OF_PERIOD}' AS date_month,
            '{START_OF_PERIOD}' AS period_start,
            '{END_OF_PERIOD}' AS period_end,
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_created_date,
            account_renewal_12mo,
            SUM(dual_facing_cms) > 0 AS dual_facing_cam,
            MAX_BY(account_size_segment, CASE WHEN account_size_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_size_segment,
            MAX_BY(account_arr_segment, CASE WHEN account_arr_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_arr_segment,
            MAX_BY(account_billing_country, CASE WHEN account_billing_country != 'Unknown' THEN 1 ELSE 0 END) AS account_billing_country,
            MAX_BY(account_industry, CASE WHEN account_industry != 'Unknown' THEN 1 ELSE 0 END) AS account_industry,
            MAX(CASE WHEN is_paid_safety_customer = true THEN 1 ELSE 0 END) AS is_paid_safety_customer,
            MAX(CASE WHEN is_paid_telematics_customer = true THEN 1 ELSE 0 END) AS is_paid_telematics_customer,
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer
        FROM latest_org_ca
        GROUP BY ALL
    ),
    latest_org AS (
        SELECT * FROM us_customer_check

        UNION ALL

        SELECT * FROM eu_customer_check

        UNION ALL

        SELECT * FROM ca_customer_check
    ),
    drivers_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            dd.driver_id,
            dd.last_mobile_login_date
        FROM datamodel_platform.dim_drivers dd
        JOIN latest_org_us lo
            ON dd.org_id = lo.org_id
        WHERE
            dd.date = '{END_OF_PERIOD}'
            AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{START_OF_PERIOD}')
    ),
    drivers_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            dd.driver_id,
            dd.last_mobile_login_date
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
        JOIN latest_org_eu lo
            ON dd.org_id = lo.org_id
        WHERE
            dd.date = '{END_OF_PERIOD}'
            AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{START_OF_PERIOD}')
    ),
    drivers_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            dd.driver_id,
            dd.last_mobile_login_date
        FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers dd
        JOIN latest_org_ca lo
            ON dd.org_id = lo.org_id
        WHERE
            dd.date = '{END_OF_PERIOD}'
            AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{START_OF_PERIOD}')
    ),
    enabled_devices_us AS (
        -- Devices with at least one safety event type enabled
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            ds.vg_device_id AS device_id
        FROM product_analytics.dim_devices_safety_settings ds
        JOIN latest_org_us lo
            ON ds.org_id = lo.org_id
        WHERE
            ds.date = '{END_OF_PERIOD}'
            AND (
                ds.drowsiness_detection_enabled
                OR ds.inattentive_driving_enabled
                OR ds.rolling_stop_enabled
                OR ds.inward_obstruction_enabled
                OR ds.seatbelt_enabled
                OR ds.mobile_usage_enabled
                OR ds.lane_departure_warning_enabled
                OR ds.forward_collision_warning_enabled
                OR ds.following_distance_enabled
                OR ds.severe_speeding_enabled
                OR ds.light_speeding_enabled
                OR ds.moderate_speeding_enabled
                OR ds.heavy_speeding_enabled
                OR ds.max_speed_enabled
                OR ds.harsh_driving_enabled
            )
    ),
    enabled_devices_eu AS (
        -- Devices with at least one safety event type enabled
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            ds.vg_device_id AS device_id
        FROM data_tools_delta_share.product_analytics.dim_devices_safety_settings ds
        JOIN latest_org_eu lo
            ON ds.org_id = lo.org_id
        WHERE
            ds.date = '{END_OF_PERIOD}'
            AND (
                ds.drowsiness_detection_enabled
                OR ds.inattentive_driving_enabled
                OR ds.rolling_stop_enabled
                OR ds.inward_obstruction_enabled
                OR ds.seatbelt_enabled
                OR ds.mobile_usage_enabled
                OR ds.lane_departure_warning_enabled
                OR ds.forward_collision_warning_enabled
                OR ds.following_distance_enabled
                OR ds.severe_speeding_enabled
                OR ds.light_speeding_enabled
                OR ds.moderate_speeding_enabled
                OR ds.heavy_speeding_enabled
                OR ds.max_speed_enabled
                OR ds.harsh_driving_enabled
            )
    ),
    enabled_devices_ca AS (
        -- Devices with at least one safety event type enabled
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            ds.vg_device_id AS device_id
        FROM data_tools_delta_share_ca.product_analytics.dim_devices_safety_settings ds
        JOIN latest_org_ca lo
            ON ds.org_id = lo.org_id
        WHERE
            ds.date = '{END_OF_PERIOD}'
            AND (
                ds.drowsiness_detection_enabled
                OR ds.inattentive_driving_enabled
                OR ds.rolling_stop_enabled
                OR ds.inward_obstruction_enabled
                OR ds.seatbelt_enabled
                OR ds.mobile_usage_enabled
                OR ds.lane_departure_warning_enabled
                OR ds.forward_collision_warning_enabled
                OR ds.following_distance_enabled
                OR ds.severe_speeding_enabled
                OR ds.light_speeding_enabled
                OR ds.moderate_speeding_enabled
                OR ds.heavy_speeding_enabled
                OR ds.max_speed_enabled
                OR ds.harsh_driving_enabled
            )
    ),
    trips_us AS (
        SELECT
            ed.sam_number,
            ed.account_id,
            SUM(ts.distance_miles) AS distance_miles
        FROM datamodel_telematics.fct_trips ts
        JOIN latest_org_us lo
            ON ts.org_id = lo.org_id
        JOIN enabled_devices_us ed
            ON ts.device_id = ed.device_id
            AND lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
        WHERE
            ts.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY ed.sam_number, ed.account_id
    ),
    trips_eu AS (
        SELECT
            ed.sam_number,
            ed.account_id,
            SUM(ts.distance_miles) AS distance_miles
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_trips` ts
        JOIN latest_org_eu lo
            ON ts.org_id = lo.org_id
        JOIN enabled_devices_eu ed
            ON ts.device_id = ed.device_id
            AND lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
        WHERE
            ts.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY ed.sam_number, ed.account_id
    ),
    trips_ca AS (
        SELECT
            ed.sam_number,
            ed.account_id,
            SUM(ts.distance_miles) AS distance_miles
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_trips ts
        JOIN latest_org_ca lo
            ON ts.org_id = lo.org_id
        JOIN enabled_devices_ca ed
            ON ts.device_id = ed.device_id
            AND lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
        WHERE
            ts.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY ed.sam_number, ed.account_id
    ),
    training_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            t.assigned_to_polymorphic,
            t.uuid,
            t.completed_at,
            t.due_date
        FROM formsdb_shards.form_submissions t
        JOIN trainingdb_shards.courses c
            ON t.form_template_uuid = c.uuid
        JOIN trainingdb_shards.categories ca
            ON c.category_uuid = ca.uuid
        JOIN latest_org_us lo
            ON t.org_id = lo.org_id
        WHERE
            DATE(t.server_created_at) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.assigned_to_polymorphic LIKE 'driver%'
            AND t.product_type = 3 -- training
            AND COALESCE(DATE(t.server_deleted_at), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --record is still valid
            AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --valid records
            AND (
                TRIM(ca.label) IN (
                  'Driver Safety - Shorts'
                )
                AND c.global_course_uuid IS NOT NULL
            )
    ),
    training_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            t.assigned_to_polymorphic,
            t.uuid,
            t.completed_at,
            t.due_date
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0` t
        JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/courses_v0` c
            ON t.form_template_uuid = c.uuid
        JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/categories_v0` ca
            ON c.category_uuid = ca.uuid
        JOIN latest_org_eu lo
            ON t.org_id = lo.org_id
        WHERE
            DATE(t.server_created_at) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.assigned_to_polymorphic LIKE 'driver%'
            AND t.product_type = 3 -- training
            AND COALESCE(DATE(t.server_deleted_at), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --record is still valid
            AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --valid records
            AND (
                TRIM(ca.label) IN (
                  'Driver Safety - Shorts'
                )
                AND c.global_course_uuid IS NOT NULL
            )

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            t.assigned_to_polymorphic,
            t.uuid,
            t.completed_at,
            t.due_date
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_submissions_v0` t
        JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-training-shard-1db/trainingdb/courses_v0` c
            ON t.form_template_uuid = c.uuid
        JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-training-shard-1db/trainingdb/categories_v0` ca
            ON c.category_uuid = ca.uuid
        JOIN latest_org_eu lo
            ON t.org_id = lo.org_id
        WHERE
            DATE(t.server_created_at) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.assigned_to_polymorphic LIKE 'driver%'
            AND t.product_type = 3 -- training
            AND COALESCE(DATE(t.server_deleted_at), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --record is still valid
            AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --valid records
            AND (
                TRIM(ca.label) IN (
                  'Driver Safety - Shorts'
                )
                AND c.global_course_uuid IS NOT NULL
            )
    ),
    training_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            t.assigned_to_polymorphic,
            t.uuid,
            t.completed_at,
            t.due_date
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0` t
        JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/courses_v0` c
            ON t.form_template_uuid = c.uuid
        JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/categories_v0` ca
            ON c.category_uuid = ca.uuid
        JOIN latest_org_ca lo
            ON t.org_id = lo.org_id
        WHERE
            DATE(t.server_created_at) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.assigned_to_polymorphic LIKE 'driver%'
            AND t.product_type = 3 -- training
            AND COALESCE(DATE(t.server_deleted_at), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --record is still valid
            AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{START_OF_PERIOD}', 1)) > '{START_OF_PERIOD}' --valid records
            AND (
                TRIM(ca.label) IN (
                  'Driver Safety - Shorts'
                )
                AND c.global_course_uuid IS NOT NULL
            )
    ),
    self_coaching_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coachingdb_shards.coaching_sessions cs
        JOIN coachingdb_shards.coachable_item ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachingdb_shards.coachable_item_share cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_us lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 1 -- Self-Coaching
    ),
    coaching_sessions_eu AS (
        SELECT
            org_id,
            uuid,
            coach_id,
            driver_id,
            completed_date,
            date,
            due_date,
            session_type
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`

        UNION ALL

        SELECT
            org_id,
            uuid,
            coach_id,
            driver_id,
            completed_date,
            date,
            due_date,
            session_type
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coaching_sessions_v0`
    ),
    coachable_item_eu AS (
        SELECT
            org_id,
            coaching_session_uuid,
            driver_id,
            coaching_state,
            due_date,
            updated_at,
            uuid,
            coachable_item_type,
            date
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_v0`

        UNION ALL

        SELECT
            org_id,
            coaching_session_uuid,
            driver_id,
            coaching_state,
            due_date,
            updated_at,
            uuid,
            coachable_item_type,
            date
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_v0`
    ),
    coachable_item_share_eu AS (
        SELECT
            org_id,
            driver_id,
            shared_at,
            share_type,
            coachable_item_uuid
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_share_v0`

        UNION ALL

        SELECT
            org_id,
            driver_id,
            shared_at,
            share_type,
            coachable_item_uuid
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_share_v0`
    ),
    coaching_sessions_ca AS (
        SELECT
            org_id,
            uuid,
            coach_id,
            driver_id,
            completed_date,
            date,
            due_date,
            session_type
        FROM
            delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`
    ),
    coachable_item_ca AS (
        SELECT
            org_id,
            coaching_session_uuid,
            driver_id,
            coaching_state,
            due_date,
            updated_at,
            uuid,
            coachable_item_type,
            date
        FROM
            delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_v0`
    ),
    coachable_item_share_ca AS (
        SELECT
            org_id,
            driver_id,
            shared_at,
            share_type,
            coachable_item_uuid
        FROM
            delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_share_v0`
    ),
    self_coaching_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coaching_sessions_eu cs
        JOIN coachable_item_eu ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_eu cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_eu lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 1 -- Self-Coaching
    ),
    self_coaching_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coaching_sessions_ca cs
        JOIN coachable_item_ca ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_ca cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_ca lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 1 -- Self-Coaching
    ),
    manager_led_coaching_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coachingdb_shards.coaching_sessions cs
        JOIN coachingdb_shards.coachable_item ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachingdb_shards.coachable_item_share cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_us lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 2 -- Manager-Led Coaching
    ),
    manager_led_coaching_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coaching_sessions_eu cs
        JOIN coachable_item_eu ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_eu cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_eu lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 2 -- Manager-Led Coaching
    ),
    manager_led_coaching_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coaching_sessions_ca cs
        JOIN coachable_item_ca ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_ca cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_ca lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 2 -- Manager-Led Coaching
    ),
    coaching_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coachingdb_shards.coaching_sessions cs
        JOIN coachingdb_shards.coachable_item ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachingdb_shards.coachable_item_share cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_us lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    coaching_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coaching_sessions_eu cs
        JOIN coachable_item_eu ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_eu cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_eu lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    coaching_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            cs.driver_id,
            cs.completed_date
        FROM coaching_sessions_ca cs
        JOIN coachable_item_ca ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_ca cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_ca lo
            ON cs.org_id = lo.org_id
        WHERE
            cs.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    {METRIC_CTES},
    final AS (
        SELECT DISTINCT
            {COALESCE_STATEMENTS},
            {COALESCE_METRIC_STATEMENTS}
        FROM drivers_aggregated
        {FULL_OUTER_JOIN_STATEMENTS}
    )
    SELECT
        org.date_month,
        org.period_start,
        org.period_end,
        org.sam_number,
        org.account_size_segment,
        org.account_arr_segment,
        org.account_billing_country,
        org.account_industry,
        COALESCE(final.industry_vertical, 'Unknown') AS industry_vertical,
        CASE WHEN org.is_paid_safety_customer = 1 THEN TRUE ELSE FALSE END AS is_paid_safety_customer,
        CASE WHEN org.is_paid_telematics_customer = 1 THEN TRUE ELSE FALSE END AS is_paid_telematics_customer,
        CASE WHEN org.is_paid_stce_customer = 1 THEN TRUE ELSE FALSE END AS is_paid_stce_customer,
        org.account_csm_email,
        org.account_name,
        org.account_id,
        org.account_created_date,
        COALESCE(MAX(org.account_renewal_12mo), FALSE) AS account_renewal_12mo,
        COALESCE(MAX(org.dual_facing_cam), FALSE) AS dual_facing_cam,
        COALESCE(MAX(final.total_drivers), 0) AS total_drivers,
        COALESCE(MAX(final.total_drivers_using_app_month), 0) AS total_drivers_using_app_month,
        COALESCE(MAX(final.total_drivers_using_app), 0) AS total_drivers_using_app,
        COALESCE(MAX(final.assigned_trainings), 0) AS assigned_trainings,
        COALESCE(MAX(final.total_drivers_assigned_training), 0) AS total_drivers_assigned_training,
        COALESCE(MAX(final.total_drivers_completing_training_on_time), 0) AS total_drivers_completing_training_on_time,
        COALESCE(MAX(final.drivers_completing_manager_led_coaching), 0) AS drivers_completing_manager_led_coaching,
        COALESCE(MAX(final.drivers_completing_self_coaching), 0) AS drivers_completing_self_coaching,
        COALESCE(MAX(final.drivers_completing_coaching), 0) AS drivers_completing_coaching,
        COALESCE(MAX(final.miles), 0) AS miles
    FROM latest_org org
    LEFT OUTER JOIN final
        ON org.sam_number = final.sam_number
        AND org.account_id = final.account_id
    GROUP BY
        org.date_month,
        org.period_start,
        org.period_end,
        org.sam_number,
        org.account_size_segment,
        org.account_arr_segment,
        org.account_billing_country,
        org.account_industry,
        final.industry_vertical,
        org.is_paid_safety_customer,
        org.is_paid_telematics_customer,
        org.is_paid_stce_customer,
        org.account_csm_email,
        org.account_name,
        org.account_id,
        org.account_created_date
"""


EVENT_SAFETY_QUERY = """
    WITH account_arr AS (
        -- Handles edge case where multiple accounts with different ARRs exist for a given SAM
        SELECT
            account_id,
            customer_arr,
            customer_arr_segment
        FROM product_analytics_staging.stg_customer_enriched
        WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
    ),
    account_arr_sam AS (
        -- Handles cases where there's no account_id match (take max for SAM instead)
        SELECT sam_number_undecorated AS sam_number,
        MAX_BY(customer_arr, customer_arr) AS customer_arr,
        MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
        FROM product_analytics_staging.stg_customer_enriched
        WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
        GROUP BY sam_number_undecorated
    ),
    pubsec_us AS (
        -- This identifies whether an organization is PubSec or not
        -- Note that we only care about this for US accounts
        SELECT
            sam_number,
            sled_account
        FROM finopsdb.customer_info
    ),
    pubsec_eu AS (
        -- This identifies whether an organization is PubSec or not
        -- Note that we only care about this for US accounts
        SELECT
            sam_number,
            sled_account
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
    ),
    pubsec_ca AS (
        -- This identifies whether an organization is PubSec or not
        -- Note that we only care about this for US accounts
        SELECT
            sam_number,
            sled_account
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
    ),
    safety_event_types AS (
        SELECT 'Drowsy' AS trigger_label_name, true AS dual_facing_event_type, 'Drowsy' AS final_trigger_label_name

        UNION ALL

        SELECT 'GenericDistraction' AS trigger_label_name, true AS dual_facing_event_type, 'GenericDistraction' AS final_trigger_label_name

        UNION ALL

        SELECT 'ObstructedCamera' AS trigger_label_name, true AS dual_facing_event_type, 'ObstructedCamera' AS final_trigger_label_name

        UNION ALL

        SELECT 'NoSeatbelt' AS trigger_label_name, true AS dual_facing_event_type, 'NoSeatbelt' AS final_trigger_label_name

        UNION ALL

        SELECT 'MobileUsage' AS trigger_label_name, true AS dual_facing_event_type, 'MobileUsage' AS final_trigger_label_name

        UNION ALL

        SELECT 'LaneDeparture' AS trigger_label_name, false AS dual_facing_event_type, 'LaneDeparture' AS final_trigger_label_name

        UNION ALL

        SELECT 'ForwardCollisionWarning' AS trigger_label_name, false AS dual_facing_event_type, 'ForwardCollisionWarning' AS final_trigger_label_name

        UNION ALL

        SELECT 'FollowingDistance' AS trigger_label_name, false AS dual_facing_event_type, 'FollowingDistance' AS final_trigger_label_name

        UNION ALL

        SELECT 'SevereSpeeding' AS trigger_label_name, false AS dual_facing_event_type, 'OverLimitSpeeding' AS final_trigger_label_name

        UNION ALL

        SELECT 'LightSpeeding' AS trigger_label_name, false AS dual_facing_event_type, 'OverLimitSpeeding' AS final_trigger_label_name

        UNION ALL

        SELECT 'ModerateSpeeding' AS trigger_label_name, false AS dual_facing_event_type, 'OverLimitSpeeding' AS final_trigger_label_name

        UNION ALL

        SELECT 'HeavySpeeding' AS trigger_label_name, false AS dual_facing_event_type, 'OverLimitSpeeding' AS final_trigger_label_name

        UNION ALL

        SELECT 'Speeding' AS trigger_label_name, false AS dual_facing_event_type, 'OverLimitSpeeding' AS final_trigger_label_name

        UNION ALL

        SELECT 'MaxSpeed' AS trigger_label_name, false AS dual_facing_event_type, 'MaxSpeed' AS final_trigger_label_name

        UNION ALL

        SELECT 'RollingStop' AS trigger_label_name, false AS dual_facing_event_type, 'RollingStop' AS final_trigger_label_name

        UNION ALL

        SELECT 'Acceleration' AS trigger_label_name, false AS dual_facing_event_type, 'Acceleration' AS final_trigger_label_name

        UNION ALL

        SELECT 'Braking' AS trigger_label_name, false AS dual_facing_event_type, 'Braking' AS final_trigger_label_name

        UNION ALL

        SELECT 'HarshTurn' AS trigger_label_name, false AS dual_facing_event_type, 'HarshTurn' AS final_trigger_label_name

        UNION ALL

        SELECT 'Crash' AS trigger_label_name, false AS dual_facing_event_type, 'Crash' AS final_trigger_label_name
    ),
    total_cm_devices_us AS (
        -- This identifies how many dual-facing CMs an org has
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN associated_devices.camera_product_id IN (43, 155) THEN device_id END) AS dual_facing_cms
        FROM datamodel_core.dim_devices
        WHERE
            date = '{END_OF_PERIOD}'
            AND device_type = 'VG - Vehicle Gateway'
        GROUP BY 1
    ),
    total_cm_devices_eu AS (
        -- This identifies how many dual-facing CMs an org has
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN associated_devices.camera_product_id IN (43, 155) THEN device_id END) AS dual_facing_cms
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices`
        WHERE
            date = '{END_OF_PERIOD}'
            AND device_type = 'VG - Vehicle Gateway'
        GROUP BY 1
    ),
    total_cm_devices_ca AS (
        -- This identifies how many dual-facing CMs an org has
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN associated_devices.camera_product_id IN (43, 155) THEN device_id END) AS dual_facing_cms
        FROM data_tools_delta_share_ca.datamodel_core.dim_devices
        WHERE
            date = '{END_OF_PERIOD}'
            AND device_type = 'VG - Vehicle Gateway'
        GROUP BY 1
    ),
    latest_org_us AS (
        -- org data at end of period
        SELECT DISTINCT
            o.org_id,
            o.sam_number,
            o.salesforce_sam_number,
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
            CASE
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
                ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
            END AS account_arr_segment,
            COALESCE(o.account_size_segment, 'Unknown') AS account_size_segment,
            CASE
                WHEN o.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil and Gas'
                ELSE COALESCE(o.account_industry, 'Unknown')
            END AS account_industry,
            o.is_paid_safety_customer,
            o.is_paid_telematics_customer,
            o.is_paid_stce_customer,
            COALESCE(o.account_csm_email, 'Unknown') AS account_csm_email,
            COALESCE(o.account_name, 'Unknown') AS account_name,
            COALESCE(o.account_id, 'Unknown') AS account_id,
            COALESCE(o.account_created_date, 'Unknown') AS account_created_date,
            FALSE AS account_renewal_12mo,
            COALESCE(t.dual_facing_cms, 0) AS dual_facing_cms,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type
        FROM datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_us p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN total_cm_devices_us t
            ON o.org_id = t.org_id
        CROSS JOIN safety_event_types et
        WHERE
            o.date = '{END_OF_PERIOD}'
            AND o.is_paid_customer
            AND (
                o.is_paid_safety_customer = TRUE
            )
    ),
    us_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            '{START_OF_PERIOD}' AS date_month,
            '{START_OF_PERIOD}' AS period_start,
            '{END_OF_PERIOD}' AS period_end,
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_created_date,
            account_renewal_12mo,
            event_type,
            dual_facing_event_type,
            SUM(dual_facing_cms) > 0 AS dual_facing_cam,
            MAX_BY(account_size_segment, CASE WHEN account_size_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_size_segment,
            MAX_BY(account_arr_segment, CASE WHEN account_arr_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_arr_segment,
            MAX_BY(account_billing_country, CASE WHEN account_billing_country != 'Unknown' THEN 1 ELSE 0 END) AS account_billing_country,
            MAX_BY(account_industry, CASE WHEN account_industry != 'Unknown' THEN 1 ELSE 0 END) AS account_industry,
            MAX(CASE WHEN is_paid_safety_customer = true THEN 1 ELSE 0 END) AS is_paid_safety_customer,
            MAX(CASE WHEN is_paid_telematics_customer = true THEN 1 ELSE 0 END) AS is_paid_telematics_customer,
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer
        FROM latest_org_us
        GROUP BY ALL
    ),
    latest_org_eu AS (
        -- org data at end of period
        SELECT DISTINCT
            o.org_id,
            o.sam_number,
            o.salesforce_sam_number,
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
            CASE
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
                ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
            END AS account_arr_segment,
            COALESCE(o.account_size_segment, 'Unknown') AS account_size_segment,
            CASE
                WHEN o.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil and Gas'
                ELSE COALESCE(o.account_industry, 'Unknown')
            END AS account_industry,
            o.is_paid_safety_customer,
            o.is_paid_telematics_customer,
            o.is_paid_stce_customer,
            COALESCE(o.account_csm_email, 'Unknown') AS account_csm_email,
            COALESCE(o.account_name, 'Unknown') AS account_name,
            COALESCE(o.account_id, 'Unknown') AS account_id,
            COALESCE(o.account_created_date, 'Unknown') AS account_created_date,
            FALSE AS account_renewal_12mo,
            COALESCE(t.dual_facing_cms, 0) AS dual_facing_cms,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_eu p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN total_cm_devices_eu t
            ON o.org_id = t.org_id
        CROSS JOIN safety_event_types et
        WHERE
            o.date = '{END_OF_PERIOD}'
            AND o.is_paid_customer
            AND (
                o.is_paid_safety_customer = TRUE
            )
    ),
    eu_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            '{START_OF_PERIOD}' AS date_month,
            '{START_OF_PERIOD}' AS period_start,
            '{END_OF_PERIOD}' AS period_end,
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_created_date,
            account_renewal_12mo,
            event_type,
            dual_facing_event_type,
            SUM(dual_facing_cms) > 0 AS dual_facing_cam,
            MAX_BY(account_size_segment, CASE WHEN account_size_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_size_segment,
            MAX_BY(account_arr_segment, CASE WHEN account_arr_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_arr_segment,
            MAX_BY(account_billing_country, CASE WHEN account_billing_country != 'Unknown' THEN 1 ELSE 0 END) AS account_billing_country,
            MAX_BY(account_industry, CASE WHEN account_industry != 'Unknown' THEN 1 ELSE 0 END) AS account_industry,
            MAX(CASE WHEN is_paid_safety_customer = true THEN 1 ELSE 0 END) AS is_paid_safety_customer,
            MAX(CASE WHEN is_paid_telematics_customer = true THEN 1 ELSE 0 END) AS is_paid_telematics_customer,
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer
        FROM latest_org_eu
        GROUP BY ALL
    ),
    latest_org_ca AS (
        -- org data at end of period
        SELECT DISTINCT
            o.org_id,
            o.sam_number,
            o.salesforce_sam_number,
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
            CASE
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
                WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
                ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
            END AS account_arr_segment,
            COALESCE(o.account_size_segment, 'Unknown') AS account_size_segment,
            CASE
                WHEN o.account_industry IN ('Mining, Quarrying, Oil & Gas', 'Mining, Quarrying, Oil and Gas') THEN 'Mining, Quarrying, Oil and Gas'
                ELSE COALESCE(o.account_industry, 'Unknown')
            END AS account_industry,
            o.is_paid_safety_customer,
            o.is_paid_telematics_customer,
            o.is_paid_stce_customer,
            COALESCE(o.account_csm_email, 'Unknown') AS account_csm_email,
            COALESCE(o.account_name, 'Unknown') AS account_name,
            COALESCE(o.account_id, 'Unknown') AS account_id,
            COALESCE(o.account_created_date, 'Unknown') AS account_created_date,
            FALSE AS account_renewal_12mo,
            COALESCE(t.dual_facing_cms, 0) AS dual_facing_cms,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_ca p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN total_cm_devices_ca t
            ON o.org_id = t.org_id
        CROSS JOIN safety_event_types et
        WHERE
            o.date = '{END_OF_PERIOD}'
            AND o.is_paid_customer
            AND (
                o.is_paid_safety_customer = TRUE
            )
    ),
    ca_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            '{START_OF_PERIOD}' AS date_month,
            '{START_OF_PERIOD}' AS period_start,
            '{END_OF_PERIOD}' AS period_end,
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_created_date,
            account_renewal_12mo,
            event_type,
            dual_facing_event_type,
            SUM(dual_facing_cms) > 0 AS dual_facing_cam,
            MAX_BY(account_size_segment, CASE WHEN account_size_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_size_segment,
            MAX_BY(account_arr_segment, CASE WHEN account_arr_segment != 'Unknown' THEN 1 ELSE 0 END) AS account_arr_segment,
            MAX_BY(account_billing_country, CASE WHEN account_billing_country != 'Unknown' THEN 1 ELSE 0 END) AS account_billing_country,
            MAX_BY(account_industry, CASE WHEN account_industry != 'Unknown' THEN 1 ELSE 0 END) AS account_industry,
            MAX(CASE WHEN is_paid_safety_customer = true THEN 1 ELSE 0 END) AS is_paid_safety_customer,
            MAX(CASE WHEN is_paid_telematics_customer = true THEN 1 ELSE 0 END) AS is_paid_telematics_customer,
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer
        FROM latest_org_ca
        GROUP BY ALL
    ),
    latest_org AS (
        SELECT * FROM us_customer_check

        UNION ALL

        SELECT * FROM eu_customer_check

        UNION ALL

        SELECT * FROM ca_customer_check
    ),
    relevant_licenses AS (
        -- Look for the relevant CM licenses for safety events
        -- 1. PREMIER
        -- 2. ENTERPRISE
        -- 3. PREM
        -- 4. ENT
        -- 5. CM1-ENT/CM2-ENT
        -- 6. CM1-EXPRESS/CM2-EXPRESS (this is Essential, so need to handle a little differently)
        -- Anything with CM2 or CM-D is dual-facing, whereas CM1 or CM-S is single-facing
        SELECT DISTINCT o.account_id, o.sam_number
        FROM edw.silver.fct_license_orders_daily_snapshot fo
        JOIN latest_org_us o
            ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
        JOIN edw.silver.license_hw_sku_xref xref
            ON fo.product_sku = xref.license_sku
        WHERE
            DATE(fo._run_dt) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND fo.net_quantity > 0
            AND (
                (
                    fo.product_sku = 'LIC-CM-D-PREMIER'
                    OR fo.product_sku = 'LIC-CM-D-ENTERPRISE'
                    OR fo.product_sku = 'LIC-CM-S-PREMIER'
                    OR fo.product_sku = 'LIC-CM-S-ENTERPRISE'
                    OR fo.product_sku LIKE 'LIC-CM-D-PREM-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-S-PREM-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-D-ENT-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-S-ENT-PLTFM%'
                    OR fo.product_sku = 'LIC-CM1-ENT'
                    OR fo.product_sku = 'LIC-CM2-ENT'
                    OR fo.product_sku = 'LIC-CM1-EXPRESS'
                    OR fo.product_sku = 'LIC-CM2-EXPRESS'
                )
                AND xref.is_core_license
            )

        UNION ALL

        SELECT DISTINCT o.account_id, o.sam_number
        FROM edw.silver.fct_license_orders_daily_snapshot fo
        JOIN latest_org_eu o
            ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
        JOIN edw.silver.license_hw_sku_xref xref
            ON fo.product_sku = xref.license_sku
        WHERE
            DATE(fo._run_dt) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND fo.net_quantity > 0
            AND (
                (
                    fo.product_sku = 'LIC-CM-D-PREMIER'
                    OR fo.product_sku = 'LIC-CM-D-ENTERPRISE'
                    OR fo.product_sku = 'LIC-CM-S-PREMIER'
                    OR fo.product_sku = 'LIC-CM-S-ENTERPRISE'
                    OR fo.product_sku LIKE 'LIC-CM-D-PREM-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-S-PREM-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-D-ENT-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-S-ENT-PLTFM%'
                    OR fo.product_sku = 'LIC-CM1-ENT'
                    OR fo.product_sku = 'LIC-CM2-ENT'
                    OR fo.product_sku = 'LIC-CM1-EXPRESS'
                    OR fo.product_sku = 'LIC-CM2-EXPRESS'
                )
                AND xref.is_core_license
            )

        UNION ALL

        SELECT DISTINCT o.account_id, o.sam_number
        FROM edw.silver.fct_license_orders_daily_snapshot fo
        JOIN latest_org_ca o
            ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
        JOIN edw.silver.license_hw_sku_xref xref
            ON fo.product_sku = xref.license_sku
        WHERE
            DATE(fo._run_dt) BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND fo.net_quantity > 0
            AND (
                (
                    fo.product_sku = 'LIC-CM-D-PREMIER'
                    OR fo.product_sku = 'LIC-CM-D-ENTERPRISE'
                    OR fo.product_sku = 'LIC-CM-S-PREMIER'
                    OR fo.product_sku = 'LIC-CM-S-ENTERPRISE'
                    OR fo.product_sku LIKE 'LIC-CM-D-PREM-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-S-PREM-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-D-ENT-PLTFM%'
                    OR fo.product_sku LIKE 'LIC-CM-S-ENT-PLTFM%'
                    OR fo.product_sku = 'LIC-CM1-ENT'
                    OR fo.product_sku = 'LIC-CM2-ENT'
                    OR fo.product_sku = 'LIC-CM1-EXPRESS'
                    OR fo.product_sku = 'LIC-CM2-EXPRESS'
                )
                AND xref.is_core_license
            )
    ),
    device_settings_us AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            ds.cm_device_id,
            ds.vg_device_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            CASE
                WHEN et.trigger_label_name = 'Drowsy' THEN ds.drowsiness_detection_enabled
                WHEN et.trigger_label_name = 'GenericDistraction' THEN ds.inattentive_driving_enabled
                WHEN et.trigger_label_name = 'RollingStop' THEN ds.rolling_stop_enabled
                WHEN et.trigger_label_name = 'ObstructedCamera' THEN ds.inward_obstruction_enabled
                WHEN et.trigger_label_name = 'NoSeatbelt' THEN ds.seatbelt_enabled
                WHEN et.trigger_label_name = 'MobileUsage' THEN ds.mobile_usage_enabled
                WHEN et.trigger_label_name = 'LaneDeparture' THEN ds.lane_departure_warning_enabled
                WHEN et.trigger_label_name = 'ForwardCollisionWarning' THEN ds.forward_collision_warning_enabled
                WHEN et.trigger_label_name = 'FollowingDistance' THEN ds.following_distance_enabled
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_enabled
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_enabled
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_enabled
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_enabled
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_enabled
                WHEN et.trigger_label_name IN ('Acceleration', 'Braking', 'HarshTurn') THEN ds.harsh_driving_enabled
                WHEN et.trigger_label_name = 'Crash' THEN TRUE
            END AS event_type_enabled,
            CASE
                WHEN et.trigger_label_name = 'Drowsy' THEN ds.drowsiness_detection_audio_alerts_enabled
                WHEN et.trigger_label_name = 'GenericDistraction' THEN ds.inattentive_driving_audio_alerts_enabled
                WHEN et.trigger_label_name = 'RollingStop' THEN ds.rolling_stop_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ObstructedCamera' THEN ds.inward_obstruction_audio_alerts_enabled
                WHEN et.trigger_label_name = 'NoSeatbelt' THEN ds.seatbelt_audio_alerts_enabled
                WHEN et.trigger_label_name = 'MobileUsage' THEN ds.mobile_usage_audio_alerts_enabled
                WHEN et.trigger_label_name = 'LaneDeparture' THEN ds.lane_departure_warning_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ForwardCollisionWarning' THEN ds.forward_collision_warning_audio_alerts_enabled
                WHEN et.trigger_label_name = 'FollowingDistance' THEN ds.following_distance_audio_alerts_enabled
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name IN ('Acceleration', 'Braking', 'HarshTurn') THEN ds.harsh_driving_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'Crash' THEN ds.crash_in_cab_audio_alerts_enabled
            END AS incab_enabled,
            CASE
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_send_to_safety_inbox
            END AS send_to_safety_inbox
        FROM product_analytics.dim_devices_safety_settings ds
        JOIN latest_org_us lo
            ON ds.org_id = lo.org_id
        CROSS JOIN safety_event_types et
        WHERE ds.date = '{END_OF_PERIOD}'
    ),
    active_devices_base_us AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            dd.product_id,
            lda.device_id,
            lda.device_type
        FROM datamodel_core.lifetime_device_activity lda
        JOIN datamodel_core.dim_devices dd
            ON lda.device_id = dd.device_id
            AND lda.org_id = dd.org_id
            AND lda.date = dd.date
        JOIN latest_org_us lo
            ON lda.org_id = lo.org_id
        WHERE
            lda.date = '{END_OF_PERIOD}'
            AND lda.l28 > 0
            AND lda.device_type IN ('CM - AI Dash Cam', 'VG - Vehicle Gateway')
    ),
    active_devices_us AS (
        SELECT
            sam_number,
            account_id,
            product_id,
            device_id
        FROM active_devices_base_us
        WHERE device_type = 'CM - AI Dash Cam'
    ),
    active_vg_devices_us AS (
        SELECT
            sam_number,
            account_id,
            product_id,
            device_id
        FROM active_devices_base_us
        WHERE device_type = 'VG - Vehicle Gateway'
    ),
    enabled_device_settings_us AS (
        -- CM-based events (non-speeding/harsh)
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            COUNT(DISTINCT CASE WHEN (ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167)) THEN ds.cm_device_id END) AS total_devices,
            COUNT(DISTINCT CASE WHEN ((ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167))) AND ds.event_type_enabled THEN ds.cm_device_id END) AS enabled_devices,
            COUNT(DISTINCT CASE WHEN ((ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167))) AND ds.incab_enabled THEN ds.cm_device_id END) AS incab_enabled_devices,
            0 AS inbox_enabled_devices
        FROM device_settings_us ds
        JOIN relevant_licenses rl
            ON ds.sam_number = rl.sam_number
            AND ds.account_id = rl.account_id
        JOIN active_devices_us ad
            ON ds.cm_device_id = ad.device_id
            AND ds.sam_number = ad.sam_number
            AND ds.account_id = ad.account_id
        WHERE ds.event_type NOT IN ('OverLimitSpeeding', 'MaxSpeed', 'Acceleration', 'Braking', 'HarshTurn', 'Crash')
        GROUP BY ALL

        UNION ALL

        -- VG-based events (speeding, harsh)
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            COUNT(DISTINCT ds.vg_device_id) AS total_devices,
            COUNT(DISTINCT CASE WHEN ds.event_type_enabled THEN ds.vg_device_id END) AS enabled_devices,
            COUNT(DISTINCT CASE WHEN ds.incab_enabled THEN ds.vg_device_id END) AS incab_enabled_devices,
            COUNT(DISTINCT CASE WHEN ds.send_to_safety_inbox THEN ds.vg_device_id END) AS inbox_enabled_devices
        FROM device_settings_us ds
        JOIN relevant_licenses rl
            ON ds.sam_number = rl.sam_number
            AND ds.account_id = rl.account_id
        JOIN active_vg_devices_us ad
            ON ds.vg_device_id = ad.device_id
            AND ds.sam_number = ad.sam_number
            AND ds.account_id = ad.account_id
        WHERE ds.event_type IN ('OverLimitSpeeding', 'MaxSpeed', 'Acceleration', 'Braking', 'HarshTurn', 'Crash')
        GROUP BY ALL
    ),
    enabled_us AS (
        SELECT
            sam_number,
            account_id,
            event_type,
            dual_facing_event_type,
            CASE WHEN total_devices != 0 AND enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS event_type_enabled,
            CASE WHEN total_devices != 0 AND incab_enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS incab_enabled,
            CASE WHEN total_devices != 0 AND inbox_enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS send_to_safety_inbox
        FROM enabled_device_settings_us ds
    ),
    device_settings_eu AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            ds.cm_device_id,
            ds.vg_device_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            CASE
                WHEN et.trigger_label_name = 'Drowsy' THEN ds.drowsiness_detection_enabled
                WHEN et.trigger_label_name = 'GenericDistraction' THEN ds.inattentive_driving_enabled
                WHEN et.trigger_label_name = 'RollingStop' THEN ds.rolling_stop_enabled
                WHEN et.trigger_label_name = 'ObstructedCamera' THEN ds.inward_obstruction_enabled
                WHEN et.trigger_label_name = 'NoSeatbelt' THEN ds.seatbelt_enabled
                WHEN et.trigger_label_name = 'MobileUsage' THEN ds.mobile_usage_enabled
                WHEN et.trigger_label_name = 'LaneDeparture' THEN ds.lane_departure_warning_enabled
                WHEN et.trigger_label_name = 'ForwardCollisionWarning' THEN ds.forward_collision_warning_enabled
                WHEN et.trigger_label_name = 'FollowingDistance' THEN ds.following_distance_enabled
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_enabled
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_enabled
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_enabled
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_enabled
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_enabled
                WHEN et.trigger_label_name IN ('Acceleration', 'Braking', 'HarshTurn') THEN ds.harsh_driving_enabled
                WHEN et.trigger_label_name = 'Crash' THEN TRUE
            END AS event_type_enabled,
            CASE
                WHEN et.trigger_label_name = 'Drowsy' THEN ds.drowsiness_detection_audio_alerts_enabled
                WHEN et.trigger_label_name = 'GenericDistraction' THEN ds.inattentive_driving_audio_alerts_enabled
                WHEN et.trigger_label_name = 'RollingStop' THEN ds.rolling_stop_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ObstructedCamera' THEN ds.inward_obstruction_audio_alerts_enabled
                WHEN et.trigger_label_name = 'NoSeatbelt' THEN ds.seatbelt_audio_alerts_enabled
                WHEN et.trigger_label_name = 'MobileUsage' THEN ds.mobile_usage_audio_alerts_enabled
                WHEN et.trigger_label_name = 'LaneDeparture' THEN ds.lane_departure_warning_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ForwardCollisionWarning' THEN ds.forward_collision_warning_audio_alerts_enabled
                WHEN et.trigger_label_name = 'FollowingDistance' THEN ds.following_distance_audio_alerts_enabled
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name IN ('Acceleration', 'Braking', 'HarshTurn') THEN ds.harsh_driving_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'Crash' THEN ds.crash_in_cab_audio_alerts_enabled
            END AS incab_enabled,
            CASE
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_send_to_safety_inbox
            END AS send_to_safety_inbox
        FROM data_tools_delta_share.product_analytics.dim_devices_safety_settings ds
        JOIN latest_org_eu lo
            ON ds.org_id = lo.org_id
        CROSS JOIN safety_event_types et
        WHERE ds.date = '{END_OF_PERIOD}'
    ),
    active_devices_base_eu AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            dd.product_id,
            lda.device_id,
            lda.device_type
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` lda
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
            ON lda.device_id = dd.device_id
            AND lda.org_id = dd.org_id
            AND lda.date = dd.date
        JOIN latest_org_eu lo
            ON lda.org_id = lo.org_id
        WHERE
            lda.date = '{END_OF_PERIOD}'
            AND lda.l28 > 0
            AND lda.device_type IN ('CM - AI Dash Cam', 'VG - Vehicle Gateway')
    ),
    active_devices_eu AS (
        SELECT
            sam_number,
            account_id,
            product_id,
            device_id
        FROM active_devices_base_eu
        WHERE device_type = 'CM - AI Dash Cam'
    ),
    active_vg_devices_eu AS (
        SELECT
            sam_number,
            account_id,
            product_id,
            device_id
        FROM active_devices_base_eu
        WHERE device_type = 'VG - Vehicle Gateway'
    ),
    enabled_device_settings_eu AS (
        -- CM-based events (non-speeding/harsh)
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            COUNT(DISTINCT CASE WHEN (ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167)) THEN ds.cm_device_id END) AS total_devices,
            COUNT(DISTINCT CASE WHEN ((ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167))) AND ds.event_type_enabled THEN ds.cm_device_id END) AS enabled_devices,
            COUNT(DISTINCT CASE WHEN ((ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167))) AND ds.incab_enabled THEN ds.cm_device_id END) AS incab_enabled_devices,
            0 AS inbox_enabled_devices
        FROM device_settings_eu ds
        JOIN relevant_licenses rl
            ON ds.sam_number = rl.sam_number
            AND ds.account_id = rl.account_id
        JOIN active_devices_eu ad
            ON ds.cm_device_id = ad.device_id
            AND ds.sam_number = ad.sam_number
            AND ds.account_id = ad.account_id
        WHERE ds.event_type NOT IN ('OverLimitSpeeding', 'MaxSpeed', 'Acceleration', 'Braking', 'HarshTurn', 'Crash')
        GROUP BY ALL

        UNION ALL

        -- VG-based events (speeding, harsh)
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            COUNT(DISTINCT ds.vg_device_id) AS total_devices,
            COUNT(DISTINCT CASE WHEN ds.event_type_enabled THEN ds.vg_device_id END) AS enabled_devices,
            COUNT(DISTINCT CASE WHEN ds.incab_enabled THEN ds.vg_device_id END) AS incab_enabled_devices,
            COUNT(DISTINCT CASE WHEN ds.send_to_safety_inbox THEN ds.vg_device_id END) AS inbox_enabled_devices
        FROM device_settings_eu ds
        JOIN relevant_licenses rl
            ON ds.sam_number = rl.sam_number
            AND ds.account_id = rl.account_id
        JOIN active_vg_devices_eu ad
            ON ds.vg_device_id = ad.device_id
            AND ds.sam_number = ad.sam_number
            AND ds.account_id = ad.account_id
        WHERE ds.event_type IN ('OverLimitSpeeding', 'MaxSpeed', 'Acceleration', 'Braking', 'HarshTurn', 'Crash')
        GROUP BY ALL
    ),
    enabled_eu AS (
        SELECT
            sam_number,
            account_id,
            event_type,
            dual_facing_event_type,
            CASE WHEN total_devices != 0 AND enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS event_type_enabled,
            CASE WHEN total_devices != 0 AND incab_enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS incab_enabled,
            CASE WHEN total_devices != 0 AND inbox_enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS send_to_safety_inbox
        FROM enabled_device_settings_eu ds
    ),
    device_settings_ca AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            ds.cm_device_id,
            ds.vg_device_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            CASE
                WHEN et.trigger_label_name = 'Drowsy' THEN ds.drowsiness_detection_enabled
                WHEN et.trigger_label_name = 'GenericDistraction' THEN ds.inattentive_driving_enabled
                WHEN et.trigger_label_name = 'RollingStop' THEN ds.rolling_stop_enabled
                WHEN et.trigger_label_name = 'ObstructedCamera' THEN ds.inward_obstruction_enabled
                WHEN et.trigger_label_name = 'NoSeatbelt' THEN ds.seatbelt_enabled
                WHEN et.trigger_label_name = 'MobileUsage' THEN ds.mobile_usage_enabled
                WHEN et.trigger_label_name = 'LaneDeparture' THEN ds.lane_departure_warning_enabled
                WHEN et.trigger_label_name = 'ForwardCollisionWarning' THEN ds.forward_collision_warning_enabled
                WHEN et.trigger_label_name = 'FollowingDistance' THEN ds.following_distance_enabled
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_enabled
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_enabled
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_enabled
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_enabled
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_enabled
                WHEN et.trigger_label_name IN ('Acceleration', 'Braking', 'HarshTurn') THEN ds.harsh_driving_enabled
                WHEN et.trigger_label_name = 'Crash' THEN TRUE
            END AS event_type_enabled,
            CASE
                WHEN et.trigger_label_name = 'Drowsy' THEN ds.drowsiness_detection_audio_alerts_enabled
                WHEN et.trigger_label_name = 'GenericDistraction' THEN ds.inattentive_driving_audio_alerts_enabled
                WHEN et.trigger_label_name = 'RollingStop' THEN ds.rolling_stop_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ObstructedCamera' THEN ds.inward_obstruction_audio_alerts_enabled
                WHEN et.trigger_label_name = 'NoSeatbelt' THEN ds.seatbelt_audio_alerts_enabled
                WHEN et.trigger_label_name = 'MobileUsage' THEN ds.mobile_usage_audio_alerts_enabled
                WHEN et.trigger_label_name = 'LaneDeparture' THEN ds.lane_departure_warning_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ForwardCollisionWarning' THEN ds.forward_collision_warning_audio_alerts_enabled
                WHEN et.trigger_label_name = 'FollowingDistance' THEN ds.following_distance_audio_alerts_enabled
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name IN ('Acceleration', 'Braking', 'HarshTurn') THEN ds.harsh_driving_in_cab_audio_alerts_enabled
                WHEN et.trigger_label_name = 'Crash' THEN ds.crash_in_cab_audio_alerts_enabled
            END AS incab_enabled,
            CASE
                WHEN et.trigger_label_name = 'SevereSpeeding' THEN ds.severe_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'LightSpeeding' THEN ds.light_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'ModerateSpeeding' THEN ds.moderate_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'HeavySpeeding' THEN ds.heavy_speeding_send_to_safety_inbox
                WHEN et.trigger_label_name = 'MaxSpeed' THEN ds.max_speed_send_to_safety_inbox
            END AS send_to_safety_inbox
        FROM data_tools_delta_share_ca.product_analytics.dim_devices_safety_settings ds
        JOIN latest_org_ca lo
            ON ds.org_id = lo.org_id
        CROSS JOIN safety_event_types et
        WHERE ds.date = '{END_OF_PERIOD}'
    ),
    active_devices_ca AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            dd.product_id,
            lda.device_id
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_activity lda
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dd
            ON lda.device_id = dd.device_id
            AND lda.org_id = dd.org_id
            AND lda.date = dd.date
        JOIN latest_org_ca lo
            ON lda.org_id = lo.org_id
        WHERE
            lda.date = '{END_OF_PERIOD}'
            AND lda.l28 > 0
            AND lda.device_type = 'CM - AI Dash Cam'
    ),
    active_vg_devices_ca AS (
        SELECT DISTINCT
            lo.sam_number,
            lo.account_id,
            dd.product_id,
            lda.device_id
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_activity lda
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dd
            ON lda.device_id = dd.device_id
            AND lda.org_id = dd.org_id
            AND lda.date = dd.date
        JOIN latest_org_ca lo
            ON lda.org_id = lo.org_id
        WHERE
            lda.date = '{END_OF_PERIOD}'
            AND lda.l28 > 0
            AND lda.device_type = 'VG - Vehicle Gateway'
    ),
    enabled_device_settings_ca AS (
        -- CM-based events (non-speeding/harsh)
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            COUNT(DISTINCT CASE WHEN (ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167)) THEN ds.cm_device_id END) AS total_devices,
            COUNT(DISTINCT CASE WHEN ((ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167))) AND ds.event_type_enabled THEN ds.cm_device_id END) AS enabled_devices,
            COUNT(DISTINCT CASE WHEN ((ds.dual_facing_event_type AND ad.product_id IN (43, 155)) OR (ds.dual_facing_event_type = FALSE AND ad.product_id IN (43, 44, 155, 167))) AND ds.incab_enabled THEN ds.cm_device_id END) AS incab_enabled_devices,
            0 AS inbox_enabled_devices
        FROM device_settings_ca ds
        JOIN relevant_licenses rl
            ON ds.sam_number = rl.sam_number
            AND ds.account_id = rl.account_id
        JOIN active_devices_ca ad
            ON ds.cm_device_id = ad.device_id
            AND ds.sam_number = ad.sam_number
            AND ds.account_id = ad.account_id
        WHERE ds.event_type NOT IN ('OverLimitSpeeding', 'MaxSpeed', 'Acceleration', 'Braking', 'HarshTurn', 'Crash')
        GROUP BY ALL

        UNION ALL

        -- VG-based events (speeding, harsh)
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            COUNT(DISTINCT ds.vg_device_id) AS total_devices,
            COUNT(DISTINCT CASE WHEN ds.event_type_enabled THEN ds.vg_device_id END) AS enabled_devices,
            COUNT(DISTINCT CASE WHEN ds.incab_enabled THEN ds.vg_device_id END) AS incab_enabled_devices,
            COUNT(DISTINCT CASE WHEN ds.send_to_safety_inbox THEN ds.vg_device_id END) AS inbox_enabled_devices
        FROM device_settings_ca ds
        JOIN relevant_licenses rl
            ON ds.sam_number = rl.sam_number
            AND ds.account_id = rl.account_id
        JOIN active_vg_devices_ca ad
            ON ds.vg_device_id = ad.device_id
            AND ds.sam_number = ad.sam_number
            AND ds.account_id = ad.account_id
        WHERE ds.event_type IN ('OverLimitSpeeding', 'MaxSpeed', 'Acceleration', 'Braking', 'HarshTurn', 'Crash')
        GROUP BY ALL
    ),
    enabled_ca AS (
        SELECT
            sam_number,
            account_id,
            event_type,
            dual_facing_event_type,
            CASE WHEN total_devices != 0 AND enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS event_type_enabled,
            CASE WHEN total_devices != 0 AND incab_enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS incab_enabled,
            CASE WHEN total_devices != 0 AND inbox_enabled_devices * 1.0 / total_devices >= 0.5 THEN TRUE ELSE FALSE END AS send_to_safety_inbox
        FROM enabled_device_settings_ca ds
    ),
    detection_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            d.uuid,
            FROM_JSON(d.metadata, 'struct<customer_facing_filter_reason:integer>').customer_facing_filter_reason AS customer_facing_filter_reason,
            FROM_JSON(d.metadata, 'struct<filter_reason:integer>').filter_reason AS filter_reason,
            d.in_cab_alert_played_at_ms,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            t.org_id AS triage_org_id,
            t.uuid AS triage_uuid
        FROM safetyeventtriagedb_shards.detections d
        JOIN latest_org_us lo
            ON d.org_id = lo.org_id
        JOIN definitions.behavior_label_to_detection_type bdt
            ON d.detection_type = bdt.detection_type
        JOIN safety_event_types st
            ON bdt.behavior_label_type = st.trigger_label_name
        JOIN device_settings_us ed
            ON lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
            AND d.device_id = ed.vg_device_id
            AND st.final_trigger_label_name = ed.event_type
            AND ed.event_type_enabled = TRUE
        LEFT JOIN safetyeventtriagedb_shards.triage_events_v2 t
            ON d.org_id = t.org_id
            AND d.triage_event_uuid = t.uuid
            AND d.device_id = t.device_id
        WHERE d.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    detection_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            d.uuid,
            FROM_JSON(d.metadata, 'struct<customer_facing_filter_reason:integer>').customer_facing_filter_reason AS customer_facing_filter_reason,
            FROM_JSON(d.metadata, 'struct<filter_reason:integer>').filter_reason AS filter_reason,
            d.in_cab_alert_played_at_ms,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            t.org_id AS triage_org_id,
            t.uuid AS triage_uuid
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventtriage-shard-1db/safetyeventtriagedb/detections_v0` d
        JOIN latest_org_eu lo
            ON d.org_id = lo.org_id
        JOIN definitions.behavior_label_to_detection_type bdt
            ON d.detection_type = bdt.detection_type
        JOIN safety_event_types st
            ON bdt.behavior_label_type = st.trigger_label_name
        JOIN device_settings_eu ed
            ON lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
            AND d.device_id = ed.vg_device_id
            AND st.final_trigger_label_name = ed.event_type
            AND ed.event_type_enabled = TRUE
        LEFT JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventtriage-shard-1db/safetyeventtriagedb/triage_events_v2_v0` t
            ON d.org_id = t.org_id
            AND d.triage_event_uuid = t.uuid
            AND d.device_id = t.device_id
        WHERE d.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            d.uuid,
            FROM_JSON(d.metadata, 'struct<customer_facing_filter_reason:integer>').customer_facing_filter_reason AS customer_facing_filter_reason,
            FROM_JSON(d.metadata, 'struct<filter_reason:integer>').filter_reason AS filter_reason,
            d.in_cab_alert_played_at_ms,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            t.org_id AS triage_org_id,
            t.uuid AS triage_uuid
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventtriagedb/safetyeventtriagedb/detections_v0` d
        JOIN latest_org_eu lo
            ON d.org_id = lo.org_id
        JOIN definitions.behavior_label_to_detection_type bdt
            ON d.detection_type = bdt.detection_type
        JOIN safety_event_types st
            ON bdt.behavior_label_type = st.trigger_label_name
        JOIN device_settings_eu ed
            ON lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
            AND d.device_id = ed.vg_device_id
            AND st.final_trigger_label_name = ed.event_type
            AND ed.event_type_enabled = TRUE
        LEFT JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventtriagedb/safetyeventtriagedb/triage_events_v2_v0` t
            ON d.org_id = t.org_id
            AND d.triage_event_uuid = t.uuid
            AND d.device_id = t.device_id
        WHERE d.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    detection_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            d.uuid,
            FROM_JSON(d.metadata, 'struct<customer_facing_filter_reason:integer>').customer_facing_filter_reason AS customer_facing_filter_reason,
            FROM_JSON(d.metadata, 'struct<filter_reason:integer>').filter_reason AS filter_reason,
            d.in_cab_alert_played_at_ms,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            t.org_id AS triage_org_id,
            t.uuid AS triage_uuid
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventtriagedb/safetyeventtriagedb/detections_v0` d
        JOIN latest_org_ca lo
            ON d.org_id = lo.org_id
        JOIN definitions.behavior_label_to_detection_type bdt
            ON d.detection_type = bdt.detection_type
        JOIN safety_event_types st
            ON bdt.behavior_label_type = st.trigger_label_name
        JOIN device_settings_ca ed
            ON lo.sam_number = ed.sam_number
            AND lo.account_id = ed.account_id
            AND d.device_id = ed.vg_device_id
            AND st.final_trigger_label_name = ed.event_type
            AND ed.event_type_enabled = TRUE
        LEFT JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventtriagedb/safetyeventtriagedb/triage_events_v2_v0` t
            ON d.org_id = t.org_id
            AND d.triage_event_uuid = t.uuid
            AND d.device_id = t.device_id
        WHERE d.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    activity_events_us AS (
        SELECT
            CAST(date AS string) AS date,
            ae.org_id,
            ae.device_id,
            ae.event_ms,
            ae.created_at,
            ae.activity_type AS activity_enum,
            es.activity_type AS activity_name
        FROM safetydb_shards.activity_events ae
        LEFT OUTER JOIN definitions.safety_activity_type_enums es
            ON ae.activity_type = es.enum
        WHERE ae.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    activity_events_grouped_us AS (
        SELECT
            date,
            org_id,
            device_id,
            event_ms,
            COLLECT_SET(CAST(activity_enum AS INT)) AS activity_enums_array,
            COLLECT_SET(activity_name) AS activity_names_array
        FROM activity_events_us
        GROUP BY 1, 2, 3, 4
    ),
    org_for_trips_us AS (
        -- Simplified org mapping without event type cross join for trips aggregation
        SELECT DISTINCT
            org_id,
            sam_number,
            account_id
        FROM latest_org_us
    ),
    trips_by_device_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ts.device_id,
            SUM(ts.distance_miles) AS distance_miles
        FROM datamodel_telematics.fct_trips ts
        JOIN org_for_trips_us lo
            ON ts.org_id = lo.org_id
        WHERE
            ts.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY lo.sam_number, lo.account_id, ts.device_id
    ),
    trips_us AS (
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            td.distance_miles
        FROM trips_by_device_us td
        JOIN device_settings_us ds
            ON td.device_id = ds.vg_device_id
            AND td.sam_number = ds.sam_number
            AND td.account_id = ds.account_id
        WHERE ds.event_type_enabled = TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY td.device_id, ds.sam_number, ds.account_id, ds.event_type ORDER BY ds.event_type) = 1
    ),
    org_for_trips_eu AS (
        -- Simplified org mapping without event type cross join for trips aggregation
        SELECT DISTINCT
            org_id,
            sam_number,
            account_id
        FROM latest_org_eu
    ),
    trips_by_device_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ts.device_id,
            SUM(ts.distance_miles) AS distance_miles
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_trips` ts
        JOIN org_for_trips_eu lo
            ON ts.org_id = lo.org_id
        WHERE
            ts.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY lo.sam_number, lo.account_id, ts.device_id
    ),
    trips_eu AS (
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            td.distance_miles
        FROM trips_by_device_eu td
        JOIN device_settings_eu ds
            ON td.device_id = ds.vg_device_id
            AND td.sam_number = ds.sam_number
            AND td.account_id = ds.account_id
        WHERE ds.event_type_enabled = TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY td.device_id, ds.sam_number, ds.account_id, ds.event_type ORDER BY ds.event_type) = 1
    ),
    org_for_trips_ca AS (
        -- Simplified org mapping without event type cross join for trips aggregation
        SELECT DISTINCT
            org_id,
            sam_number,
            account_id
        FROM latest_org_ca
    ),
    trips_by_device_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ts.device_id,
            SUM(ts.distance_miles) AS distance_miles
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_trips ts
        JOIN org_for_trips_ca lo
            ON ts.org_id = lo.org_id
        WHERE
            ts.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY lo.sam_number, lo.account_id, ts.device_id
    ),
    trips_ca AS (
        SELECT
            ds.sam_number,
            ds.account_id,
            ds.event_type,
            ds.dual_facing_event_type,
            td.distance_miles
        FROM trips_by_device_ca td
        JOIN device_settings_ca ds
            ON td.device_id = ds.vg_device_id
            AND td.sam_number = ds.sam_number
            AND td.account_id = ds.account_id
        WHERE ds.event_type_enabled = TRUE
        QUALIFY ROW_NUMBER() OVER(PARTITION BY td.device_id, ds.sam_number, ds.account_id, ds.event_type ORDER BY ds.event_type) = 1
    ),
    surveys_us AS (
        SELECT
            org_id,
            device_id,
            date,
            event_ms,
            detail_proto.is_useful
        FROM safetydb_shards.harsh_event_surveys
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY created_at DESC) = 1 -- most recent survey
    ),
    triage_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            t.uuid,
            t.triage_state,
            t.driver_id,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            s.is_useful AS event_was_useful,
            COALESCE(ARRAY_CONTAINS(aeg.activity_names_array, 'ViewedByActivityType'), FALSE) AS was_event_viewed
        FROM safetyeventtriagedb_shards.triage_events_v2 t
        JOIN latest_org_us lo
            ON t.org_id = lo.org_id
        LEFT OUTER JOIN activity_events_grouped_us aeg
            ON t.org_id = aeg.org_id
            AND t.device_id = aeg.device_id
            AND t.date = aeg.date
            AND t.start_ms = aeg.event_ms
        LEFT OUTER JOIN surveys_us s
            ON t.org_id = s.org_id
            AND t.device_id = s.device_id
            AND t.start_ms = s.event_ms
            AND t.date = s.date
        JOIN definitions.behavior_label_type_enums bl
            ON t.trigger_label = bl.enum
        JOIN safety_event_types st
            ON bl.behavior_label_type = st.trigger_label_name
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    activity_events_eu AS (
        SELECT
            CAST(date AS string) AS date,
            ae.org_id,
            ae.device_id,
            ae.event_ms,
            ae.created_at,
            ae.activity_type AS activity_enum,
            es.activity_type AS activity_name
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safety-shard-1db/safetydb/activity_events_v0` ae
        LEFT OUTER JOIN definitions.safety_activity_type_enums es
            ON ae.activity_type = es.enum
        WHERE ae.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'

        UNION ALL

        SELECT
            CAST(date AS string) AS date,
            ae.org_id,
            ae.device_id,
            ae.event_ms,
            ae.created_at,
            ae.activity_type AS activity_enum,
            es.activity_type AS activity_name
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetydb/safetydb/activity_events_v0` ae
        LEFT OUTER JOIN definitions.safety_activity_type_enums es
            ON ae.activity_type = es.enum
        WHERE ae.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    activity_events_grouped_eu AS (
        SELECT
            date,
            org_id,
            device_id,
            event_ms,
            COLLECT_SET(CAST(activity_enum AS INT)) AS activity_enums_array,
            COLLECT_SET(activity_name) AS activity_names_array
        FROM activity_events_eu
        GROUP BY 1, 2, 3, 4
    ),
    surveys_eu AS (
        SELECT
            org_id,
            device_id,
            date,
            event_ms,
            detail_proto.is_useful
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetydb/safetydb/harsh_event_surveys_v2`
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY created_at DESC) = 1 -- most recent survey

        UNION ALL

        SELECT
            org_id,
            device_id,
            date,
            event_ms,
            detail_proto.is_useful
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safety-shard-1db/safetydb/harsh_event_surveys_v2`
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY created_at DESC) = 1 -- most recent survey
    ),
    triage_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            t.uuid,
            t.triage_state,
            t.driver_id,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            s.is_useful AS event_was_useful,
            COALESCE(ARRAY_CONTAINS(aeg.activity_names_array, 'ViewedByActivityType'), FALSE) AS was_event_viewed
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventtriage-shard-1db/safetyeventtriagedb/triage_events_v2_v0` t
        JOIN latest_org_eu lo
            ON t.org_id = lo.org_id
        LEFT OUTER JOIN activity_events_grouped_eu aeg
            ON t.org_id = aeg.org_id
            AND t.device_id = aeg.device_id
            AND t.date = aeg.date
            AND t.start_ms = aeg.event_ms
        LEFT OUTER JOIN surveys_eu s
            ON t.org_id = s.org_id
            AND t.device_id = s.device_id
            AND t.start_ms = s.event_ms
            AND t.date = s.date
        JOIN definitions.behavior_label_type_enums bl
            ON t.trigger_label = bl.enum
        JOIN safety_event_types st
            ON bl.behavior_label_type = st.trigger_label_name
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            t.uuid,
            t.triage_state,
            t.driver_id,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            s.is_useful AS event_was_useful,
            COALESCE(ARRAY_CONTAINS(aeg.activity_names_array, 'ViewedByActivityType'), FALSE) AS was_event_viewed
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventtriagedb/safetyeventtriagedb/triage_events_v2_v0` t
        JOIN latest_org_eu lo
            ON t.org_id = lo.org_id
        LEFT OUTER JOIN activity_events_grouped_eu aeg
            ON t.org_id = aeg.org_id
            AND t.device_id = aeg.device_id
            AND t.date = aeg.date
            AND t.start_ms = aeg.event_ms
        LEFT OUTER JOIN surveys_eu s
            ON t.org_id = s.org_id
            AND t.device_id = s.device_id
            AND t.start_ms = s.event_ms
            AND t.date = s.date
        JOIN definitions.behavior_label_type_enums bl
            ON t.trigger_label = bl.enum
        JOIN safety_event_types st
            ON bl.behavior_label_type = st.trigger_label_name
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    activity_events_ca AS (
        SELECT
            CAST(date AS string) AS date,
            ae.org_id,
            ae.device_id,
            ae.event_ms,
            ae.created_at,
            ae.activity_type AS activity_enum,
            es.activity_type AS activity_name
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetydb/safetydb/activity_events_v0` ae
        LEFT OUTER JOIN definitions.safety_activity_type_enums es
            ON ae.activity_type = es.enum
        WHERE ae.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    activity_events_grouped_ca AS (
        SELECT
            date,
            org_id,
            device_id,
            event_ms,
            COLLECT_SET(CAST(activity_enum AS INT)) AS activity_enums_array,
            COLLECT_SET(activity_name) AS activity_names_array
        FROM activity_events_ca
        GROUP BY 1, 2, 3, 4
    ),
    surveys_ca AS (
        SELECT
            org_id,
            device_id,
            date,
            event_ms,
            detail_proto.is_useful
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetydb/safetydb/harsh_event_surveys_v2`
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY created_at DESC) = 1 -- most recent survey
    ),
    triage_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            t.uuid,
            t.triage_state,
            t.driver_id,
            st.final_trigger_label_name AS event_type,
            st.dual_facing_event_type,
            s.is_useful AS event_was_useful,
            COALESCE(ARRAY_CONTAINS(aeg.activity_names_array, 'ViewedByActivityType'), FALSE) AS was_event_viewed
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventtriagedb/safetyeventtriagedb/triage_events_v2_v0` t
        JOIN latest_org_ca lo
            ON t.org_id = lo.org_id
        LEFT OUTER JOIN activity_events_grouped_ca aeg
            ON t.org_id = aeg.org_id
            AND t.device_id = aeg.device_id
            AND t.date = aeg.date
            AND t.start_ms = aeg.event_ms
        LEFT OUTER JOIN surveys_ca s
            ON t.org_id = s.org_id
            AND t.device_id = s.device_id
            AND t.start_ms = s.event_ms
            AND t.date = s.date
        JOIN definitions.behavior_label_type_enums bl
            ON t.trigger_label = bl.enum
        JOIN safety_event_types st
            ON bl.behavior_label_type = st.trigger_label_name
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    self_coaching_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            ci.driver_id,
            cs.coach_id,
            ci.coaching_state,
            ci.due_date,
            ci.updated_at,
            ci.uuid AS coachable_item_uuid
        FROM coachingdb_shards.coaching_sessions cs
        JOIN coachingdb_shards.coachable_item ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachingdb_shards.coachable_item_share cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_us lo
            ON cs.org_id = lo.org_id
        JOIN definitions.behavior_label_to_coachable_item_type cit
            ON ci.coachable_item_type = cit.coachable_item_id
        JOIN definitions.behavior_label_type_enums blt
            ON cit.behavior_label = blt.enum
        JOIN safety_event_types et
            ON blt.behavior_label_type = et.trigger_label_name
        WHERE
            ci.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 1 -- Self-Coaching
    ),
    coaching_sessions_eu AS (
        SELECT
            org_id,
            uuid,
            coach_id,
            driver_id,
            completed_date,
            date,
            due_date,
            session_type
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`

        UNION ALL

        SELECT
            org_id,
            uuid,
            coach_id,
            driver_id,
            completed_date,
            date,
            due_date,
            session_type
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coaching_sessions_v0`
    ),
    coachable_item_eu AS (
        SELECT
            org_id,
            coaching_session_uuid,
            driver_id,
            coaching_state,
            due_date,
            updated_at,
            uuid,
            coachable_item_type,
            date
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_v0`

        UNION ALL

        SELECT
            org_id,
            coaching_session_uuid,
            driver_id,
            coaching_state,
            due_date,
            updated_at,
            uuid,
            coachable_item_type,
            date
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_v0`
    ),
    coachable_item_share_eu AS (
        SELECT
            org_id,
            driver_id,
            shared_at,
            share_type,
            coachable_item_uuid
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_share_v0`

        UNION ALL

        SELECT
            org_id,
            driver_id,
            shared_at,
            share_type,
            coachable_item_uuid
        FROM
            delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_share_v0`
    ),
    coaching_sessions_ca AS (
        SELECT
            org_id,
            uuid,
            coach_id,
            driver_id,
            completed_date,
            date,
            due_date,
            session_type
        FROM
            delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`
    ),
    coachable_item_ca AS (
        SELECT
            org_id,
            coaching_session_uuid,
            driver_id,
            coaching_state,
            due_date,
            updated_at,
            uuid,
            coachable_item_type,
            date
        FROM
            delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_v0`
    ),
    coachable_item_share_ca AS (
        SELECT
            org_id,
            driver_id,
            shared_at,
            share_type,
            coachable_item_uuid
        FROM
            delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_share_v0`
    ),
    self_coaching_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            ci.driver_id,
            cs.coach_id,
            ci.coaching_state,
            ci.due_date,
            ci.updated_at,
            ci.uuid AS coachable_item_uuid
        FROM coaching_sessions_eu cs
        JOIN coachable_item_eu ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_eu cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_eu lo
            ON cs.org_id = lo.org_id
        JOIN definitions.behavior_label_to_coachable_item_type cit
            ON ci.coachable_item_type = cit.coachable_item_id
        JOIN definitions.behavior_label_type_enums blt
            ON cit.behavior_label = blt.enum
        JOIN safety_event_types et
            ON blt.behavior_label_type = et.trigger_label_name
        WHERE
            ci.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 1 -- Self-Coaching
    ),
    self_coaching_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            ci.driver_id,
            cs.coach_id,
            ci.coaching_state,
            ci.due_date,
            ci.updated_at,
            ci.uuid AS coachable_item_uuid
        FROM coaching_sessions_ca cs
        JOIN coachable_item_ca ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_ca cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_ca lo
            ON cs.org_id = lo.org_id
        JOIN definitions.behavior_label_to_coachable_item_type cit
            ON ci.coachable_item_type = cit.coachable_item_id
        JOIN definitions.behavior_label_type_enums blt
            ON cit.behavior_label = blt.enum
        JOIN safety_event_types et
            ON blt.behavior_label_type = et.trigger_label_name
        WHERE
            ci.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 1 -- Self-Coaching
    ),
    manager_led_coaching_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            cs.uuid,
            ci.uuid AS coachable_item_uuid,
            cs.coach_id,
            ci.driver_id,
            cs.date,
            cs.completed_date,
            cs.due_date,
            ci.date AS coachable_item_date,
            ci.coaching_state,
            ci.updated_at,
            ci.due_date AS coachable_item_due_date
        FROM coachingdb_shards.coaching_sessions cs
        JOIN coachingdb_shards.coachable_item ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachingdb_shards.coachable_item_share cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_us lo
            ON cs.org_id = lo.org_id
        JOIN definitions.behavior_label_to_coachable_item_type cit
            ON ci.coachable_item_type = cit.coachable_item_id
        JOIN definitions.behavior_label_type_enums blt
            ON cit.behavior_label = blt.enum
        JOIN safety_event_types et
            ON blt.behavior_label_type = et.trigger_label_name
        WHERE
            ci.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 2 -- Manager-Led Coaching
    ),
    manager_led_coaching_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            cs.uuid,
            ci.uuid AS coachable_item_uuid,
            cs.coach_id,
            ci.driver_id,
            cs.date,
            cs.completed_date,
            cs.due_date,
            ci.date AS coachable_item_date,
            ci.coaching_state,
            ci.updated_at,
            ci.due_date AS coachable_item_due_date
        FROM coaching_sessions_eu cs
        JOIN coachable_item_eu ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_eu cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_eu lo
            ON cs.org_id = lo.org_id
        JOIN definitions.behavior_label_to_coachable_item_type cit
            ON ci.coachable_item_type = cit.coachable_item_id
        JOIN definitions.behavior_label_type_enums blt
            ON cit.behavior_label = blt.enum
        JOIN safety_event_types et
            ON blt.behavior_label_type = et.trigger_label_name
        WHERE
            ci.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 2 -- Manager-Led Coaching
    ),
    manager_led_coaching_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            et.final_trigger_label_name AS event_type,
            et.dual_facing_event_type,
            cs.uuid,
            ci.uuid AS coachable_item_uuid,
            cs.coach_id,
            ci.driver_id,
            cs.date,
            cs.completed_date,
            cs.due_date,
            ci.date AS coachable_item_date,
            ci.coaching_state,
            ci.updated_at,
            ci.due_date AS coachable_item_due_date
        FROM coaching_sessions_ca cs
        JOIN coachable_item_ca ci
            ON cs.uuid = ci.coaching_session_uuid
        JOIN coachable_item_share_ca cis
            ON ci.uuid = cis.coachable_item_uuid
        JOIN latest_org_ca lo
            ON cs.org_id = lo.org_id
        JOIN definitions.behavior_label_to_coachable_item_type cit
            ON ci.coachable_item_type = cit.coachable_item_id
        JOIN definitions.behavior_label_type_enums blt
            ON cit.behavior_label = blt.enum
        JOIN safety_event_types et
            ON blt.behavior_label_type = et.trigger_label_name
        WHERE
            ci.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND cis.share_type = 2 -- Manager-Led Coaching
    ),
    {METRIC_CTES},
    final AS (
        SELECT DISTINCT
            {COALESCE_STATEMENTS},
            {COALESCE_METRIC_STATEMENTS}
        FROM enabled_aggregated
        {FULL_OUTER_JOIN_STATEMENTS}
    )
    SELECT
        org.date_month,
        org.period_start,
        org.period_end,
        org.sam_number,
        org.account_size_segment,
        org.account_arr_segment,
        org.account_billing_country,
        org.account_industry,
        COALESCE(final.industry_vertical, 'Unknown') AS industry_vertical,
        CASE WHEN org.is_paid_safety_customer = 1 THEN TRUE ELSE FALSE END AS is_paid_safety_customer,
        CASE WHEN org.is_paid_telematics_customer = 1 THEN TRUE ELSE FALSE END AS is_paid_telematics_customer,
        CASE WHEN org.is_paid_stce_customer = 1 THEN TRUE ELSE FALSE END AS is_paid_stce_customer,
        org.account_csm_email,
        org.account_name,
        org.account_id,
        org.account_created_date,
        COALESCE(MAX(org.account_renewal_12mo), FALSE) AS account_renewal_12mo,
        COALESCE(MAX(org.dual_facing_cam), FALSE) AS dual_facing_cam,
        org.event_type,
        org.dual_facing_event_type,
        COALESCE(MAX(final.event_type_enabled), 0) AS event_type_enabled,
        COALESCE(MAX(final.in_cab_alerts_enabled), 0) AS in_cab_alerts_enabled,
        COALESCE(MAX(final.send_to_safety_inbox_enabled), 0) AS send_to_safety_inbox_enabled,
        COALESCE(MAX(final.inbox_events), 0) AS inbox_events,
        COALESCE(MAX(final.inbox_events_viewed), 0) AS inbox_events_viewed,
        COALESCE(MAX(final.inbox_events_dismissed), 0) AS inbox_events_dismissed,
        COALESCE(MAX(final.inbox_events_viewed_dismissed), 0) AS inbox_events_viewed_dismissed,
        COALESCE(MAX(final.inbox_events_viewed_not_useful), 0) AS inbox_events_viewed_not_useful,
        COALESCE(MAX(final.eligible_events), 0) AS eligible_events,
        COALESCE(MAX(final.eligible_events_with_driver), 0) AS eligible_events_with_driver,
        COALESCE(MAX(final.detection_log_events), 0) AS detection_log_events,
        COALESCE(MAX(final.in_cab_alerts), 0) AS in_cab_alerts,
        COALESCE(MAX(final.events_without_alert), 0) AS events_without_alert,
        COALESCE(MAX(final.in_cab_alerts), 0) + COALESCE(MAX(final.events_without_alert), 0) AS total_detections,
        COALESCE(MAX(final.detection_log_events_caps), 0) AS detection_log_events_caps,
        COALESCE(MAX(final.detection_log_events_threshold), 0) AS detection_log_events_threshold,
        COALESCE(MAX(final.detection_log_events_nudges), 0) AS detection_log_events_nudges,
        COALESCE(MAX(final.detection_log_events_speed), 0) AS detection_log_events_speed,
        COALESCE(MAX(final.detection_log_events_confidence), 0) AS detection_log_events_confidence,
        COALESCE(MAX(final.detection_log_events_severity), 0) AS detection_log_events_severity,
        COALESCE(MAX(final.detection_log_events_ser), 0) AS detection_log_events_ser,
        COALESCE(MAX(final.detection_log_events_incab), 0) AS detection_log_events_incab,
        COALESCE(MAX(final.detection_log_events_other), 0) AS detection_log_events_other,
        COALESCE(MAX(final.detection_log_events_no_filter), 0) AS detection_log_events_no_filter,
        COALESCE(MAX(final.self_coaching_events), 0) AS self_coaching_events,
        COALESCE(MAX(final.self_coaching_events_completed), 0) AS self_coaching_events_completed,
        COALESCE(MAX(final.total_drivers_assigned_self_coaching), 0) AS total_drivers_assigned_self_coaching,
        COALESCE(MAX(final.total_drivers_completed_self_coaching), 0) AS total_drivers_completed_self_coaching,
        COALESCE(MAX(final.manager_led_coaching_sessions), 0) AS manager_led_coaching_sessions,
        COALESCE(MAX(final.manager_led_coaching_events), 0) AS manager_led_coaching_events,
        COALESCE(MAX(final.manager_led_coaching_events_completed), 0) AS manager_led_coaching_events_completed,
        COALESCE(MAX(final.manager_led_coaches), 0) AS manager_led_coaches,
        COALESCE(MAX(final.manager_led_coaches_coaching_on_time), 0) AS manager_led_coaches_coaching_on_time,
        COALESCE(MAX(final.completed_manager_led_coaching_sessions), 0) AS completed_manager_led_coaching_sessions,
        COALESCE(MAX(final.completed_manager_led_coaching_sessions_14d), 0) AS completed_manager_led_coaching_sessions_14d,
        COALESCE(MAX(final.manager_led_coaching_drivers), 0) AS manager_led_coaching_drivers,
        COALESCE(MAX(final.manager_led_coaching_drivers_coached), 0) AS manager_led_coaching_drivers_coached,
        COALESCE(MAX(final.miles_event), 0) AS miles_event
    FROM latest_org org
    LEFT OUTER JOIN final
        ON org.sam_number = final.sam_number
        AND org.account_id = final.account_id
        AND org.event_type = final.event_type
    GROUP BY
        org.date_month,
        org.period_start,
        org.period_end,
        org.sam_number,
        org.account_size_segment,
        org.account_arr_segment,
        org.account_billing_country,
        org.account_industry,
        final.industry_vertical,
        org.is_paid_safety_customer,
        org.is_paid_telematics_customer,
        org.is_paid_stce_customer,
        org.account_csm_email,
        org.account_name,
        org.account_id,
        org.account_created_date,
        org.event_type,
        org.dual_facing_event_type
"""
