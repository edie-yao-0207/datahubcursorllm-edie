from dataweb.userpkgs.constants import DimensionConfig, DimensionDataType

PLATFORM_STANDARD_DIMENSIONS = {
    "time_dimensions": [
        ("root", "'{DATEID}'"),
        ("period", "'{PERIOD}'"),
        ("period_start", "'{START_OF_PERIOD}'"),
        ("period_end", "'{END_OF_PERIOD}'"),
    ],
    "region_dimensions": [
        ("cloud_region", "'{CLOUD_REGION}'"),
        ("region", "{REGION_ALIAS}"),
    ],
    "account_dimensions": [
        ("account_size_segment", "CASE WHEN {ALIAS}.account_size_segment IS NULL THEN 'Unknown' ELSE {ALIAS}.account_size_segment END"),
        ("account_arr_segment", "CASE WHEN {ALIAS}.account_arr_segment IS NULL THEN 'Unknown' ELSE {ALIAS}.account_arr_segment END"),
        ("account_industry", "CASE WHEN {ALIAS}.account_industry IS NULL THEN 'Unknown' ELSE {ALIAS}.account_industry END"),
    ],
    "product_dimensions": [
        ("is_paid_safety_customer", "CASE WHEN {ALIAS}.is_paid_safety_customer IS NULL THEN FALSE ELSE {ALIAS}.is_paid_safety_customer END"),
        ("is_paid_telematics_customer", "CASE WHEN {ALIAS}.is_paid_telematics_customer IS NULL THEN FALSE ELSE {ALIAS}.is_paid_telematics_customer END"),
        ("is_paid_stce_customer", "CASE WHEN {ALIAS}.is_paid_stce_customer IS NULL THEN FALSE ELSE {ALIAS}.is_paid_stce_customer END"),
    ],
    "org_dimensions": [
        ("org_id", "{ALIAS}.org_id"),
        ("org_name", "{ALIAS}.org_name"),
        ("account_csm_email", "{ALIAS}.account_csm_email"),
    ]
}

PLATFORM_METRICS = {
    "full_admin_orgs": "COUNT(DISTINCT CASE WHEN fraction_admin >= 1 THEN f.org_id END)",
    "custom_role_orgs": "COUNT(DISTINCT CASE WHEN fraction_custom_role > 0 THEN f.org_id END)",
    "total_orgs": "COUNT(DISTINCT f.org_id)",
    "device_attributes": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.device_id END)",
    "device_attributes_orgs": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.org_id END)",
    "device_tags": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.device_id END)",
    "device_tags_orgs": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.org_id END)",
    "device_tags_attributes": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 AND attributes_size > 0 THEN d.device_id END)",
    "total_devices": "COUNT(DISTINCT d.device_id)",
    "total_device_orgs": "COUNT(DISTINCT d.org_id)",
    "driver_attributes": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN CONCAT(d.org_id, '_', d.driver_id) END)",
    "driver_attributes_orgs": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.org_id END)",
    "driver_tags": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN CONCAT(d.org_id, '_', d.driver_id) END)",
    "driver_tags_orgs": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.org_id END)",
    "driver_tags_attributes": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 AND attributes_size > 0 THEN CONCAT(d.org_id, '_', d.driver_id) END)",
    "total_drivers": "COUNT(DISTINCT CONCAT(d.org_id, '_', d.driver_id))",
    "total_driver_orgs": "COUNT(DISTINCT d.org_id)",
    "csv_upload_failures": "COUNT(DISTINCT CASE WHEN d.total_rows_errored > 0 THEN d.uuid END)",
    "csv_uploads": "COUNT(DISTINCT d.uuid)",
    "average_row_error_rate": "AVG(d.row_error_rate)",
    "active_cloud_dashboard_users": "COUNT(DISTINCT CONCAT(u.org_id, '_', u.user_id))",
    "active_cloud_dashboard_orgs": "COUNT(DISTINCT u.org_id)",
    "total_possible_cloud_dashboard_users": "COUNT(DISTINCT CONCAT(u.org_id, '_', u.user_id))",
}

PLATFORM_METRICS_ORG = {
    "admin_users": "COUNT(DISTINCT CASE WHEN is_admin = 1 THEN user_id END)",
    "custom_role_users": "COUNT(DISTINCT CASE WHEN is_custom_role = 1 THEN user_id END)",
    "total_users": "COUNT(DISTINCT user_id)",
    "device_attributes": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.device_id END)",
    "device_tags": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.device_id END)",
    "device_tags_attributes": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 AND attributes_size > 0 THEN d.device_id END)",
    "total_devices": "COUNT(DISTINCT d.device_id)",
    "driver_attributes": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN CONCAT(d.org_id, '_', d.driver_id) END)",
    "driver_tags": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN CONCAT(d.org_id, '_', d.driver_id) END)",
    "driver_tags_attributes": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 AND attributes_size > 0 THEN CONCAT(d.org_id, '_', d.driver_id) END)",
    "total_drivers": "COUNT(DISTINCT CONCAT(d.org_id, '_', d.driver_id))",
    "csv_upload_failures": "COUNT(DISTINCT CASE WHEN d.total_rows_errored > 0 THEN d.uuid END)",
    "csv_uploads": "COUNT(DISTINCT d.uuid)",
    "average_row_error_rate": "AVG(d.row_error_rate)",
    "total_possible_cloud_dashboard_users": "COUNT(DISTINCT user_id)",
    "daily_active_cloud_dashboard_users": "COUNT(DISTINCT CASE WHEN last_web_usage_date = '{END_OF_PERIOD}' THEN user_id END)",
    "weekly_active_cloud_dashboard_users": "COUNT(DISTINCT CASE WHEN last_web_usage_date BETWEEN DATE_SUB('{END_OF_PERIOD}', 6) AND '{END_OF_PERIOD}' THEN user_id END)",
    "monthly_active_cloud_dashboard_users": "COUNT(DISTINCT CASE WHEN last_web_usage_date BETWEEN DATE_SUB('{END_OF_PERIOD}', 29) AND '{END_OF_PERIOD}' THEN user_id END)",
}

PLATFORM_METRIC_CONFIGS_TEMPLATE = {
    "user_roles_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "user_roles_flattened_us_final",
        "eu_source": "user_roles_flattened_eu_final",
        "ca_source": "user_roles_flattened_ca_final",
        "table_alias": "f",
        "join_alias": "f",
        "metrics": ["full_admin_orgs", "custom_role_orgs", "total_orgs"]
    },
    "device_details_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "device_details_us",
        "eu_source": "device_details_eu",
        "ca_source": "device_details_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["device_attributes", "device_attributes_orgs", "device_tags", "device_tags_orgs", "device_tags_attributes", "total_devices", "total_device_orgs"]
    },
    "driver_details_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "driver_breakdown_us",
        "eu_source": "driver_breakdown_eu",
        "ca_source": "driver_breakdown_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["driver_attributes", "driver_attributes_orgs", "driver_tags", "driver_tags_orgs", "driver_tags_attributes", "total_drivers", "total_driver_orgs"]
    },
    "csv_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "csv_breakdown_us",
        "eu_source": "csv_breakdown_eu",
        "ca_source": "csv_breakdown_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["csv_upload_failures", "csv_uploads", "average_row_error_rate"]
    },
    "active_users_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "active_users_us",
        "eu_source": "active_users_eu",
        "ca_source": "active_users_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["active_cloud_dashboard_users", "active_cloud_dashboard_orgs"]
    },
    "total_users_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "total_users_us",
        "eu_source": "total_users_eu",
        "ca_source": "total_users_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["total_possible_cloud_dashboard_users"]
    },
}

PLATFORM_METRIC_CONFIGS_TEMPLATE_ORG = {
    "user_roles_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "user_roles_us",
        "eu_source": "user_roles_eu",
        "ca_source": "user_roles_ca",
        "table_alias": "f",
        "join_alias": "f",
        "metrics": ["admin_users", "custom_role_users", "total_users"]
    },
    "device_details_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "device_details_us",
        "eu_source": "device_details_eu",
        "ca_source": "device_details_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["device_attributes", "device_tags", "device_tags_attributes", "total_devices"]
    },
    "driver_details_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "driver_breakdown_us",
        "eu_source": "driver_breakdown_eu",
        "ca_source": "driver_breakdown_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["driver_attributes", "driver_tags", "driver_tags_attributes", "total_drivers"]
    },
    "csv_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "csv_breakdown_us",
        "eu_source": "csv_breakdown_eu",
        "ca_source": "csv_breakdown_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["csv_upload_failures", "csv_uploads", "average_row_error_rate"]
    },
    "active_users_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "active_users_us",
        "eu_source": "active_users_eu",
        "ca_source": "active_users_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["daily_active_cloud_dashboard_users", "weekly_active_cloud_dashboard_users", "monthly_active_cloud_dashboard_users", "total_possible_cloud_dashboard_users"]
    },
}

PLATFORM_DIMENSION_TEMPLATES = {
    "DIMENSIONS": {
        "US": {
            "alias": "o",
            "region": "us-west-2",
        },
        "EU": {
            "alias": "o",
            "region": "eu-west-1",
        },
        "CA": {
            "alias": "o",
            "region": "ca-central-1",
        }
    }
}

PLATFORM_REGION_MAPPINGS = {
    "us-west-2": {
        "cloud_region": "us-west-2",
        "region_alias": "CASE WHEN c.region IS NULL THEN 'Unknown' ELSE c.region END"
    },
    "eu-west-1": {
        "cloud_region": "eu-west-1",
        "region_alias": "'EU'"
    },
    "ca-central-1": {
        "cloud_region": "ca-central-1",
        "region_alias": "'Canada'"
    }
}

PLATFORM_JOIN_MAPPINGS = {
    "us-west-2": {
        "join_statements": """
            join latest_org_us o
            on {join_alias}.org_id = o.org_id
            left outer join latest_org_categories_us c
            on o.org_id = c.org_id
        """,
    },
    "eu-west-1": {
        "join_statements": """
            join latest_org_eu o
            on {join_alias}.org_id = o.org_id
        """,
    },
    "ca-central-1": {
        "join_statements": """
            join latest_org_ca o
            on {join_alias}.org_id = o.org_id
        """,
    }
}

PLATFORM_SCHEMA = [
    {
        "name": "date_placeholder",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The start date of the period."
        },
    },
    {
        "name": "period_start",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Start of time period"
        },
    },
    {
        "name": "period_end",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "End of time period"
        },
    },
    {
        "name": "cloud_region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "AWS cloud region where record comes from"},
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization into 'region' category, one of ('USA - Northeast', 'USA - Midwest', 'USA - Southeast', 'USA - Southwest', 'USA - West', 'USA - All', 'Canada', 'Mexico', 'All', 'Multiple Regions - USA, Canada, Mexico', 'Multiple Regions - USA, Canada', 'Multiple Regions - USA, Mexico', 'Multiple Regions - Canada, Mexico', 'Multiple Regions - USA', 'Unknown')"
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
        "name": "account_industry",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Industry classification of the account."
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
        "name": "full_admin_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of orgs having all users as admins"
        },
    },
    {
        "name": "custom_role_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of orgs having at least one user with a custom role"
        },
    },
    {
        "name": "total_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of orgs with 2+ users at the end of the period"
        },
    },
    {
        "name": "device_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices with at least one attribute"
        },
    },
    {
        "name": "device_attributes_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of orgs having a device with at least one attribute"
        },
    },
    {
        "name": "device_tags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices with at least one tag"
        },
    },
    {
        "name": "device_tags_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of orgs having a device with at least one tag"
        },
    },
    {
        "name": "device_tags_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices with at least one attribute and at least one tag"
        },
    },
    {
        "name": "total_devices",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices within the grouping (to serve as the denominator)"
        },
    },
    {
        "name": "total_device_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of orgs for devices within said grouping"
        },
    },
    {
        "name": "driver_tags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers with at least one tag"
        },
    },
    {
        "name": "driver_tags_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of orgs with a driver with at least one tag"
        },
    },
    {
        "name": "driver_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers with at least one attribute"
        },
    },
    {
        "name": "driver_attributes_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of orgs with a driver with at least one attribute"
        },
    },
    {
        "name": "driver_tags_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers with at least one attribute and at least one tag"
        },
    },
    {
        "name": "total_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers within the grouping"
        },
    },
    {
        "name": "total_driver_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of orgs for drivers within said grouping"
        },
    },
    {
        "name": "csv_upload_failures",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of CSV uploads with at least one error row"
        },
    },
    {
        "name": "csv_uploads",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of CSV uploads"
        },
    },
    {
        "name": "average_row_error_rate",
        "type": "decimal(38,20)",
        "nullable": False,
        "metadata": {
            "comment": "Average error rate (rows errored/rows submitted) for CSV uploads"
        },
    },
    {
        "name": "active_cloud_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active cloud dashboard users within the period"
        },
    },
    {
        "name": "active_cloud_dashboard_orgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active orgs accessing the cloud dashboard within the period"
        },
    },
    {
        "name": "total_possible_cloud_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of total active users within the grouping"
        },
    },
]

PLATFORM_SCHEMA_ORG = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Date partition in `YYYY-MM-DD` format"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The Samsara cloud dashboard ID that the data belongs to"
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
        "name": "account_csm_email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email for the CSM of the org"
        },
    },
    {
        "name": "cloud_region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "AWS cloud region where record comes from"},
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization into 'region' category, one of ('USA - Northeast', 'USA - Midwest', 'USA - Southeast', 'USA - Southwest', 'USA - West', 'USA - All', 'Canada', 'Mexico', 'All', 'Multiple Regions - USA, Canada, Mexico', 'Multiple Regions - USA, Canada', 'Multiple Regions - USA, Mexico', 'Multiple Regions - Canada, Mexico', 'Multiple Regions - USA', 'Unknown')"
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
        "name": "account_industry",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Industry classification of the account."
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
        "name": "admin_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of admin users"
        },
    },
    {
        "name": "custom_role_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of custom role users"
        },
    },
    {
        "name": "total_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of users"
        },
    },
    {
        "name": "device_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices with at least one attribute"
        },
    },
    {
        "name": "device_tags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices with at least one tag"
        },
    },
    {
        "name": "device_tags_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices with at least one attribute and at least one tag"
        },
    },
    {
        "name": "total_devices",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices within the grouping (to serve as the denominator)"
        },
    },
    {
        "name": "driver_tags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers with at least one tag"
        },
    },
    {
        "name": "driver_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers with at least one attribute"
        },
    },
    {
        "name": "driver_tags_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers with at least one attribute and at least one tag"
        },
    },
    {
        "name": "total_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers within the grouping"
        },
    },
    {
        "name": "csv_upload_failures",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of CSV uploads with at least one error row"
        },
    },
    {
        "name": "csv_uploads",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of CSV uploads"
        },
    },
    {
        "name": "average_row_error_rate",
        "type": "decimal(38,20)",
        "nullable": False,
        "metadata": {
            "comment": "Average error rate (rows errored/rows submitted) for CSV uploads"
        },
    },
    {
        "name": "daily_active_cloud_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active cloud dashboard users for the day"
        },
    },
    {
        "name": "weekly_active_cloud_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active cloud dashboard users within the last week"
        },
    },
    {
        "name": "monthly_active_cloud_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active cloud dashboard users within the last month"
        },
    },
    {
        "name": "total_possible_cloud_dashboard_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of total active users within the org"
        },
    },
]

QUERY_TEMPLATE = """
    with latest_org_us as (
    -- org data at end of period
        select distinct
        org_id,
        account_arr_segment,
        account_size_segment,
        account_industry,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer
        from default.datamodel_core.dim_organizations
        where date = '{END_OF_PERIOD}'
        AND is_paid_customer
        AND (
            is_paid_safety_customer = TRUE
            OR is_paid_telematics_customer = TRUE
            OR is_paid_stce_customer = TRUE
        )
    ),
    latest_org_eu as (
    -- org data at end of period
        select distinct
        org_id,
        account_arr_segment,
        account_size_segment,
        account_industry,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer
        from delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`
        where date = '{END_OF_PERIOD}'
        AND is_paid_customer
        AND (
            is_paid_safety_customer = TRUE
            OR is_paid_telematics_customer = TRUE
            OR is_paid_stce_customer = TRUE
        )
    ),
    latest_org_ca as (
    -- org data at end of period
        select distinct
        org_id,
        account_arr_segment,
        account_size_segment,
        account_industry,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer
        from data_tools_delta_share_ca.datamodel_core.dim_organizations
        where date = '{END_OF_PERIOD}'
        AND is_paid_customer
        AND (
            is_paid_safety_customer = TRUE
            OR is_paid_telematics_customer = TRUE
            OR is_paid_stce_customer = TRUE
        )
    ),
    latest_org_categories_us as (
        select
        org_id,
        CASE WHEN region = 'midwest' THEN 'USA - Midwest'
        WHEN region = 'northeast' THEN 'USA - Northeast'
        WHEN region = 'southeast' THEN 'USA - Southeast'
        WHEN region = 'southwest' THEN 'USA - Southwest'
        WHEN region = 'west' THEN 'USA - West'
        WHEN region = 'canada' THEN 'Canada'
        WHEN region = 'mexico' THEN 'Mexico'
        WHEN region = 'all' THEN 'All'
        WHEN region = 'usa_all' THEN 'Multiple Regions - USA'
        WHEN region IS NULL THEN 'Unknown'
        END as region,
        industry_vertical
        from default.product_analytics_staging.stg_organization_categories
        where date = (SELECT MAX(date) from product_analytics_staging.stg_organization_categories)
    ),
    user_roles_eu AS (
        SELECT date,
        user_id,
        org_id,
        custom_role_uuids,
        role_ids,
        CASE
            WHEN ARRAY_CONTAINS(role_ids, 1) OR ARRAY_CONTAINS(role_ids, 20) THEN 1
            ELSE 0
            END AS is_admin,
        CASE
            WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
            ELSE 0
        END AS is_custom_role
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email IS FALSE
        AND date = '{END_OF_PERIOD}'
    ),
    user_roles_ca AS (
        SELECT date,
        user_id,
        org_id,
        custom_role_uuids,
        role_ids,
        CASE
            WHEN ARRAY_CONTAINS(role_ids, 1) OR ARRAY_CONTAINS(role_ids, 20) THEN 1
            ELSE 0
            END AS is_admin,
        CASE
            WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
            ELSE 0
        END AS is_custom_role
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email IS FALSE
        AND date = '{END_OF_PERIOD}'
    ),
    user_roles_us AS (
        SELECT date,
        user_id,
        org_id,
        custom_role_uuids,
        role_ids,
        CASE
            WHEN ARRAY_CONTAINS(role_ids, 1) OR ARRAY_CONTAINS(role_ids, 20) THEN 1
            ELSE 0
            END AS is_admin,
        CASE
            WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
            ELSE 0
        END AS is_custom_role
        FROM datamodel_platform.dim_users_organizations
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email IS FALSE
        AND date = '{END_OF_PERIOD}'
    ),
    user_roles_flattened_us as (
        SELECT
        user_roles.date,
        user_roles.org_id,
        COUNT(DISTINCT user_roles.user_id) AS user_count,
        AVG(is_admin) as fraction_admin,
        AVG(is_custom_role) AS fraction_custom_role
        FROM user_roles_us user_roles
        JOIN latest_org_us o
        ON user_roles.org_id = o.org_id
        GROUP BY 1, 2
    ),
    user_roles_flattened_eu as (
        SELECT
        user_roles.date,
        user_roles.org_id,
        COUNT(DISTINCT user_roles.user_id) AS user_count,
        AVG(is_admin) as fraction_admin,
        AVG(is_custom_role) AS fraction_custom_role
        FROM user_roles_eu user_roles
        JOIN latest_org_eu o
        ON user_roles.org_id = o.org_id
        GROUP BY 1, 2
    ),
    user_roles_flattened_ca as (
        SELECT
        user_roles.date,
        user_roles.org_id,
        COUNT(DISTINCT user_roles.user_id) AS user_count,
        AVG(is_admin) as fraction_admin,
        AVG(is_custom_role) AS fraction_custom_role
        FROM user_roles_ca user_roles
        JOIN latest_org_ca o
        ON user_roles.org_id = o.org_id
        GROUP BY 1, 2
    ),
    user_roles_flattened_us_final as (
        SELECT
            org_id,
            fraction_admin,
            fraction_custom_role
        FROM user_roles_flattened_us
        WHERE user_count >= 2
    ),
    user_roles_flattened_eu_final as (
        SELECT
            org_id,
            fraction_admin,
            fraction_custom_role
        FROM user_roles_flattened_eu
        WHERE user_count >= 2
    ),
    user_roles_flattened_ca_final as (
        SELECT
            org_id,
            fraction_admin,
            fraction_custom_role
        FROM user_roles_flattened_ca
        WHERE user_count >= 2
    ),
    active_devices_eu AS (
        SELECT DISTINCT
        lda.org_id,
        lda.device_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` AS lda
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
        ON lda.device_id = d.device_id
        AND lda.org_id = d.org_id
        AND lda.date = d.date
        JOIN latest_org_eu o
        ON lda.org_id = o.org_id
        WHERE DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
        AND lda.date = '{END_OF_PERIOD}'
    ),
    device_attributes_eu AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
                    name AS attribute_name,
                    CAST(created_at AS DATE) AS attribute_created_at,
                    CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles

            union all

            SELECT uuid AS attribute_id,
                    name AS attribute_name,
                    CAST(created_at AS DATE) AS attribute_created_at,
                    CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles
        ),
        vehicle_attributes AS (
            SELECT entity_id AS device_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles

            union all

            SELECT entity_id AS device_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id

            union all

            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT va.device_id,
        COLLECT_SET(va.attribute_id) AS attribute_ids,
        COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
        COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN vehicle_attributes va
        ON a.attribute_id = va.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        GROUP BY va.device_id
    ),
    device_tags_eu as (
        SELECT device_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/tag_devices_v0` AS td
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY device_id
    ),
    active_devices_ca AS (
        SELECT DISTINCT
        lda.org_id,
        lda.device_id
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_activity AS lda
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
        ON lda.device_id = d.device_id
        AND lda.org_id = d.org_id
        AND lda.date = d.date
        JOIN latest_org_ca o
        ON lda.org_id = o.org_id
        WHERE DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
        AND lda.date = '{END_OF_PERIOD}'
    ),
    device_attributes_ca AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
                    name AS attribute_name,
                    CAST(created_at AS DATE) AS attribute_created_at,
                    CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles
        ),
        vehicle_attributes AS (
            SELECT entity_id AS device_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT va.device_id,
        COLLECT_SET(va.attribute_id) AS attribute_ids,
        COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
        COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN vehicle_attributes va
        ON a.attribute_id = va.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        GROUP BY va.device_id
    ),
    device_tags_ca as (
        SELECT device_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-productsdb/prod_db/tag_devices_v0` AS td
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY device_id
    ),
    device_details_us as (
        select org_id,
        device_id,
        tags_size,
        attributes_size
        from metrics_repo.device_tags_attributes_count
        where date = '{END_OF_PERIOD}'
    ),
    device_details_eu as (
        SELECT
        d.org_id,
        d.device_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM active_devices_eu d
        LEFT OUTER JOIN device_tags_eu t
        ON d.device_id = t.device_id
        LEFT OUTER JOIN device_attributes_eu a
        ON d.device_id = a.device_id
    ),
    device_details_ca as (
        SELECT
        d.org_id,
        d.device_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM active_devices_ca d
        LEFT OUTER JOIN device_tags_ca t
        ON d.device_id = t.device_id
        LEFT OUTER JOIN device_attributes_ca a
        ON d.device_id = a.device_id
    ),
    drivers_eu AS (
        SELECT DISTINCT
        d.org_id,
        d.driver_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` d
        JOIN latest_org_eu o
        ON d.org_id = o.org_id
        WHERE DATEDIFF(d.date, d.last_mobile_login_date) < 30  -- filter to drivers that have logged in in the last month
        AND d.date = '{END_OF_PERIOD}'
    ),
    driver_attributes_eu AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
            name AS attribute_name,
            CAST(created_at AS DATE) AS attribute_created_at,
            CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers

            union all

            SELECT uuid AS attribute_id,
            name AS attribute_name,
            CAST(created_at AS DATE) AS attribute_created_at,
            CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers
        ),
        driver_attributes AS (
            SELECT entity_id AS driver_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers

            union all

            SELECT entity_id AS driver_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_values_v0`
            GROUP BY attribute_id

            union all

            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT da.driver_id,
            COLLECT_SET(da.attribute_id) AS attribute_ids,
            COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
            COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN driver_attributes da
        ON a.attribute_id = da.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        WHERE da.driver_id IS NOT NULL
        GROUP BY da.driver_id
    ),
    driver_tags_eu AS (
        SELECT driver_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tag_drivers_v1` AS td
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY driver_id
    ),
    drivers_ca AS (
        SELECT DISTINCT
        d.org_id,
        d.driver_id
        FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers d
        JOIN latest_org_ca o
        ON d.org_id = o.org_id
        WHERE DATEDIFF(d.date, d.last_mobile_login_date) < 30  -- filter to drivers that have logged in in the last month
        AND d.date = '{END_OF_PERIOD}'
    ),
    driver_attributes_ca AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
            name AS attribute_name,
            CAST(created_at AS DATE) AS attribute_created_at,
            CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers
        ),
        driver_attributes AS (
            SELECT entity_id AS driver_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT da.driver_id,
            COLLECT_SET(da.attribute_id) AS attribute_ids,
            COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
            COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN driver_attributes da
        ON a.attribute_id = da.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        WHERE da.driver_id IS NOT NULL
        GROUP BY da.driver_id
    ),
    driver_tags_ca AS (
        SELECT driver_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tag_drivers_v1` AS td
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY driver_id
    ),
    driver_breakdown_us as (
        select org_id,
        driver_id,
        tags_size,
        attributes_size
        from metrics_repo.driver_tags_attributes_count
        where date = '{END_OF_PERIOD}'
    ),
    driver_breakdown_eu AS (
        SELECT
        d.org_id,
        d.driver_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM drivers_eu d
        LEFT OUTER JOIN driver_tags_eu t
        ON d.driver_id = t.driver_id
        LEFT OUTER JOIN driver_attributes_eu a
        ON d.driver_id = a.driver_id
        WHERE d.driver_id IS NOT NULL
    ),
    driver_breakdown_ca AS (
        SELECT
        d.org_id,
        d.driver_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM drivers_ca d
        LEFT OUTER JOIN driver_tags_ca t
        ON d.driver_id = t.driver_id
        LEFT OUTER JOIN driver_attributes_ca a
        ON d.driver_id = a.driver_id
        WHERE d.driver_id IS NOT NULL
    ),
    internal_users AS (
        SELECT user_id
        FROM datamodel_platform.dim_users
        WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_users)
        AND (
            is_samsara_email = TRUE
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )

        UNION ALL

        SELECT DISTINCT user_id
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations
        WHERE date = (SELECT MAX(date) FROM data_tools_delta_share.datamodel_platform.dim_users_organizations)
        AND (
            is_samsara_email = TRUE
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )

        UNION ALL

        SELECT DISTINCT user_id
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations
        WHERE date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations)
        AND (
            is_samsara_email = TRUE
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )
    ),
    csv_breakdown_us AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM csvuploadsdb_shards.csv_uploads c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL
    ),
    csv_breakdown_eu AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-csvuploadsdb/csvuploadsdb/csv_uploads_v0` c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL

        UNION ALL

        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-csvuploads-shard-1db/csvuploadsdb/csv_uploads_v0` c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL
    ),
    csv_breakdown_ca AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-csvuploadsdb/csvuploadsdb/csv_uploads_v0` c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL
    ),
    active_users_us AS (
        SELECT scr.org_id,
        scr.user_id
        FROM datamodel_platform_silver.stg_cloud_routes scr
        JOIN datamodel_core.dim_organizations o
            ON scr.org_id = o.org_id
        JOIN datamodel_platform.dim_users_organizations duo
            ON scr.org_id = duo.org_id
            AND scr.user_id = duo.user_id
        JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE scr.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        AND o.date = '{END_OF_PERIOD}'
        AND duo.date = '{END_OF_PERIOD}'
        AND rcuo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email = FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    total_users_us AS (
        SELECT duo.org_id,
        duo.user_id
        FROM datamodel_platform.dim_users_organizations duo
        JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE duo.date = '{END_OF_PERIOD}'
        AND rcuo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email IS FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    active_users_eu AS (
        SELECT scr.org_id,
        scr.useremail AS user_id -- Using email for EU as user_id is null
        FROM datamodel_platform_silver.stg_cloud_routes scr
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
            ON scr.org_id = o.org_id
        JOIN data_tools_delta_share.datamodel_platform.dim_users_organizations duo
            ON scr.org_id = duo.org_id
            AND scr.useremail = duo.email
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE scr.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        AND o.date = '{END_OF_PERIOD}'
        AND duo.date = '{END_OF_PERIOD}'
        AND rcuo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email = FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    total_users_eu AS (
        SELECT duo.org_id,
        duo.email AS user_id
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations duo
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE duo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email IS FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND rcuo.date = '{END_OF_PERIOD}'
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    active_users_ca AS (
        SELECT scr.org_id,
        scr.useremail AS user_id -- Using email for CA as user_id is null
        FROM datamodel_platform_silver.stg_cloud_routes scr
        JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o
            ON scr.org_id = o.org_id
        JOIN data_tools_delta_share_ca.datamodel_platform.dim_users_organizations duo
            ON scr.org_id = duo.org_id
            AND scr.useremail = duo.email
        JOIN data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE scr.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        AND o.date = '{END_OF_PERIOD}'
        AND duo.date = '{END_OF_PERIOD}'
        AND rcuo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email = FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    total_users_ca AS (
        SELECT duo.org_id,
        duo.email AS user_id
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations duo
        JOIN data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE duo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email IS FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND rcuo.date = '{END_OF_PERIOD}'
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    {METRIC_CTES}
    select
    {COALESCE_STATEMENTS},
    {COALESCE_METRIC_STATEMENTS}
    from user_roles_aggregated user_roles
    {FULL_OUTER_JOIN_STATEMENTS}
    """

QUERY_TEMPLATE_ORG = """
    with latest_org_us as (
    -- org data at end of period
        select distinct
        org_id,
        org_name,
        account_csm_email,
        account_arr_segment,
        account_size_segment,
        account_industry,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer
        from default.datamodel_core.dim_organizations
        where date = '{END_OF_PERIOD}'
        AND is_paid_customer
        AND (
            is_paid_safety_customer = TRUE
            OR is_paid_telematics_customer = TRUE
            OR is_paid_stce_customer = TRUE
        )
    ),
    latest_org_eu as (
    -- org data at end of period
        select distinct
        org_id,
        org_name,
        account_csm_email,
        account_arr_segment,
        account_size_segment,
        account_industry,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer
        from delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`
        where date = '{END_OF_PERIOD}'
        AND is_paid_customer
        AND (
            is_paid_safety_customer = TRUE
            OR is_paid_telematics_customer = TRUE
            OR is_paid_stce_customer = TRUE
        )
    ),
    latest_org_ca as (
    -- org data at end of period
        select distinct
        org_id,
        org_name,
        account_csm_email,
        account_arr_segment,
        account_size_segment,
        account_industry,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer
        from data_tools_delta_share_ca.datamodel_core.dim_organizations
        where date = '{END_OF_PERIOD}'
        AND is_paid_customer
        AND (
            is_paid_safety_customer = TRUE
            OR is_paid_telematics_customer = TRUE
            OR is_paid_stce_customer = TRUE
        )
    ),
    latest_org_categories_us as (
        select
        org_id,
        CASE WHEN region = 'midwest' THEN 'USA - Midwest'
        WHEN region = 'northeast' THEN 'USA - Northeast'
        WHEN region = 'southeast' THEN 'USA - Southeast'
        WHEN region = 'southwest' THEN 'USA - Southwest'
        WHEN region = 'west' THEN 'USA - West'
        WHEN region = 'canada' THEN 'Canada'
        WHEN region = 'mexico' THEN 'Mexico'
        WHEN region = 'all' THEN 'All'
        WHEN region = 'usa_all' THEN 'Multiple Regions - USA'
        WHEN region IS NULL THEN 'Unknown'
        END as region,
        industry_vertical
        from default.product_analytics_staging.stg_organization_categories
        where date = (SELECT MAX(date) from product_analytics_staging.stg_organization_categories)
    ),
    user_roles_eu AS (
        SELECT date,
        user_id,
        org_id,
        custom_role_uuids,
        role_ids,
        CASE
            WHEN ARRAY_CONTAINS(role_ids, 1) OR ARRAY_CONTAINS(role_ids, 20) THEN 1
            ELSE 0
            END AS is_admin,
        CASE
            WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
            ELSE 0
        END AS is_custom_role
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email IS FALSE
        AND date = '{END_OF_PERIOD}'
    ),
    user_roles_ca AS (
        SELECT date,
        user_id,
        org_id,
        custom_role_uuids,
        role_ids,
        CASE
            WHEN ARRAY_CONTAINS(role_ids, 1) OR ARRAY_CONTAINS(role_ids, 20) THEN 1
            ELSE 0
            END AS is_admin,
        CASE
            WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
            ELSE 0
        END AS is_custom_role
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email IS FALSE
        AND date = '{END_OF_PERIOD}'
    ),
    user_roles_us AS (
        SELECT date,
        user_id,
        org_id,
        custom_role_uuids,
        role_ids,
        CASE
            WHEN ARRAY_CONTAINS(role_ids, 1) OR ARRAY_CONTAINS(role_ids, 20) THEN 1
            ELSE 0
            END AS is_admin,
        CASE
            WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
            ELSE 0
        END AS is_custom_role
        FROM datamodel_platform.dim_users_organizations
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email IS FALSE
        AND date = '{END_OF_PERIOD}'
    ),
    active_devices_eu AS (
        SELECT DISTINCT
        lda.org_id,
        lda.device_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` AS lda
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
        ON lda.device_id = d.device_id
        AND lda.org_id = d.org_id
        AND lda.date = d.date
        JOIN latest_org_eu o
        ON lda.org_id = o.org_id
        WHERE DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
        AND lda.date = '{END_OF_PERIOD}'
    ),
    device_attributes_eu AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
                    name AS attribute_name,
                    CAST(created_at AS DATE) AS attribute_created_at,
                    CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles

            union all

            SELECT uuid AS attribute_id,
                    name AS attribute_name,
                    CAST(created_at AS DATE) AS attribute_created_at,
                    CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles
        ),
        vehicle_attributes AS (
            SELECT entity_id AS device_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles

            union all

            SELECT entity_id AS device_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id

            union all

            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT va.device_id,
        COLLECT_SET(va.attribute_id) AS attribute_ids,
        COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
        COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN vehicle_attributes va
        ON a.attribute_id = va.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        GROUP BY va.device_id
    ),
    device_tags_eu as (
        SELECT device_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/tag_devices_v0` AS td
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY device_id
    ),
    active_devices_ca AS (
        SELECT DISTINCT
        lda.org_id,
        lda.device_id
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_activity AS lda
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
        ON lda.device_id = d.device_id
        AND lda.org_id = d.org_id
        AND lda.date = d.date
        JOIN latest_org_ca o
        ON lda.org_id = o.org_id
        WHERE DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
        AND lda.date = '{END_OF_PERIOD}'
    ),
    device_attributes_ca AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
                    name AS attribute_name,
                    CAST(created_at AS DATE) AS attribute_created_at,
                    CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles
        ),
        vehicle_attributes AS (
            SELECT entity_id AS device_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT va.device_id,
        COLLECT_SET(va.attribute_id) AS attribute_ids,
        COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
        COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN vehicle_attributes va
        ON a.attribute_id = va.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        GROUP BY va.device_id
    ),
    device_tags_ca as (
        SELECT device_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-productsdb/prod_db/tag_devices_v0` AS td
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY device_id
    ),
    device_details_us as (
        select org_id,
        device_id,
        tags_size,
        attributes_size
        from metrics_repo.device_tags_attributes_count
        where date = '{END_OF_PERIOD}'
    ),
    device_details_eu as (
        SELECT
        d.org_id,
        d.device_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM active_devices_eu d
        LEFT OUTER JOIN device_tags_eu t
        ON d.device_id = t.device_id
        LEFT OUTER JOIN device_attributes_eu a
        ON d.device_id = a.device_id
    ),
    device_details_ca as (
        SELECT
        d.org_id,
        d.device_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM active_devices_ca d
        LEFT OUTER JOIN device_tags_ca t
        ON d.device_id = t.device_id
        LEFT OUTER JOIN device_attributes_ca a
        ON d.device_id = a.device_id
    ),
    drivers_eu AS (
        SELECT DISTINCT
        d.org_id,
        d.driver_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` d
        JOIN latest_org_eu o
        ON d.org_id = o.org_id
        WHERE DATEDIFF(d.date, d.last_mobile_login_date) < 30  -- filter to drivers that have logged in in the last month
        AND d.date = '{END_OF_PERIOD}'
    ),
    driver_attributes_eu AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
            name AS attribute_name,
            CAST(created_at AS DATE) AS attribute_created_at,
            CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers

            union all

            SELECT uuid AS attribute_id,
            name AS attribute_name,
            CAST(created_at AS DATE) AS attribute_created_at,
            CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers
        ),
        driver_attributes AS (
            SELECT entity_id AS driver_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers

            union all

            SELECT entity_id AS driver_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_values_v0`
            GROUP BY attribute_id

            union all

            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT da.driver_id,
            COLLECT_SET(da.attribute_id) AS attribute_ids,
            COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
            COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN driver_attributes da
        ON a.attribute_id = da.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        WHERE da.driver_id IS NOT NULL
        GROUP BY da.driver_id
    ),
    driver_tags_eu AS (
        SELECT driver_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tag_drivers_v1` AS td
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY driver_id
    ),
    drivers_ca AS (
        SELECT DISTINCT
        d.org_id,
        d.driver_id
        FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers d
        JOIN latest_org_ca o
        ON d.org_id = o.org_id
        WHERE DATEDIFF(d.date, d.last_mobile_login_date) < 30  -- filter to drivers that have logged in in the last month
        AND d.date = '{END_OF_PERIOD}'
    ),
    driver_attributes_ca AS (
        WITH attributes AS (
            SELECT uuid AS attribute_id,
            name AS attribute_name,
            CAST(created_at AS DATE) AS attribute_created_at,
            CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers
        ),
        driver_attributes AS (
            SELECT entity_id AS driver_id,
            attribute_id,
            CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers
        ),
        attribute_values AS (
            SELECT attribute_id,
            COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT da.driver_id,
            COLLECT_SET(da.attribute_id) AS attribute_ids,
            COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
            COLLECT_SET(a.attribute_name) AS attribute_names
        FROM attributes a
        LEFT OUTER JOIN driver_attributes da
        ON a.attribute_id = da.attribute_id
        LEFT OUTER JOIN attribute_values av
        ON a.attribute_id = av.attribute_id
        WHERE da.driver_id IS NOT NULL
        GROUP BY da.driver_id
    ),
    driver_tags_ca AS (
        SELECT driver_id,
        COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tag_drivers_v1` AS td
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
        ON t.id = td.tag_id
        GROUP BY driver_id
    ),
    driver_breakdown_us as (
        SELECT org_id,
        driver_id,
        tags_size,
        attributes_size
        FROM metrics_repo.driver_tags_attributes_count
        WHERE date = '{END_OF_PERIOD}'
    ),
    driver_breakdown_eu AS (
        SELECT
        d.org_id,
        d.driver_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM drivers_eu d
        LEFT OUTER JOIN driver_tags_eu t
        ON d.driver_id = t.driver_id
        LEFT OUTER JOIN driver_attributes_eu a
        ON d.driver_id = a.driver_id
        WHERE d.driver_id IS NOT NULL
    ),
    driver_breakdown_ca AS (
        SELECT
        d.org_id,
        d.driver_id,
        COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
        COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM drivers_ca d
        LEFT OUTER JOIN driver_tags_ca t
        ON d.driver_id = t.driver_id
        LEFT OUTER JOIN driver_attributes_ca a
        ON d.driver_id = a.driver_id
        WHERE d.driver_id IS NOT NULL
    ),
    internal_users AS (
        SELECT user_id
        FROM datamodel_platform.dim_users
        WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_users)
        AND (
            is_samsara_email = TRUE
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )

        UNION ALL

        SELECT DISTINCT user_id
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations
        WHERE date = (SELECT MAX(date) FROM data_tools_delta_share.datamodel_platform.dim_users_organizations)
        AND (
            is_samsara_email = TRUE
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )

        UNION ALL

        SELECT DISTINCT user_id
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations
        WHERE date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations)
        AND (
            is_samsara_email = TRUE
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )
    ),
    csv_breakdown_us AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM csvuploadsdb_shards.csv_uploads c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) = '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL
    ),
    csv_breakdown_eu AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-csvuploadsdb/csvuploadsdb/csv_uploads_v0` c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) = '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL

        UNION ALL

        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-csvuploads-shard-1db/csvuploadsdb/csv_uploads_v0` c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) = '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL
    ),
    csv_breakdown_ca AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) as date,
            c.org_id,
            c.uuid,
            c.total_rows_errored,
            CASE
            WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
            THEN 1
            ELSE total_rows_errored * 1.0 / total_rows_submitted
            END
            AS row_error_rate
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-csvuploadsdb/csvuploadsdb/csv_uploads_v0` c
        LEFT OUTER JOIN internal_users e
        ON c.created_by = e.user_id
        WHERE DATE(c._timestamp) = '{END_OF_PERIOD}'
        AND total_rows_submitted > 0
        AND e.user_id IS NULL
    ),
    active_users_us AS (
        SELECT duo.org_id,
        duo.user_id,
        duo.last_web_usage_date
        FROM datamodel_platform.dim_users_organizations duo
        JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE duo.date = '{END_OF_PERIOD}'
        AND rcuo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email IS FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    active_users_eu AS (
        SELECT duo.org_id,
        duo.user_id,
        duo.last_web_usage_date
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations duo
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE duo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email IS FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND rcuo.date = '{END_OF_PERIOD}'
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    active_users_ca AS (
        SELECT duo.org_id,
        duo.user_id,
        duo.last_web_usage_date
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations duo
        JOIN data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        WHERE duo.date = '{END_OF_PERIOD}'
        AND (
            duo.is_samsara_email IS FALSE
            AND duo.email NOT LIKE '%samsara.canary%'
            AND duo.email NOT LIKE '%samsara.forms.canary%'
            AND duo.email NOT LIKE '%samsaracanarydevcontractor%'
            AND duo.email NOT LIKE '%samsaratest%'
            AND duo.email NOT LIKE '%@samsara%'
            AND duo.email NOT LIKE '%@samsara-service-account.com'
        )
        AND rcuo.date = '{END_OF_PERIOD}'
        AND COALESCE(DATE(rcuo.expire_at), DATE_ADD('{START_OF_PERIOD}', 1)) >= '{START_OF_PERIOD}' -- looking at start of period in case users were disabled during the period
    ),
    {METRIC_CTES}
    select
    {COALESCE_STATEMENTS},
    {COALESCE_METRIC_STATEMENTS}
    from user_roles_aggregated user_roles
    {FULL_OUTER_JOIN_STATEMENTS}
    """


DIMENSION_CONFIGS = {
    "period_start": DimensionConfig(name="period_start", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "period_end": DimensionConfig(name="period_end", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "cloud_region": DimensionConfig(name="cloud_region", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "region": DimensionConfig(name="region", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_size_segment": DimensionConfig(name="account_size_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_arr_segment": DimensionConfig(name="account_arr_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_industry": DimensionConfig(name="account_industry", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "industry_vertical": DimensionConfig(name="industry_vertical", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "is_paid_safety_customer": DimensionConfig(name="is_paid_safety_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_telematics_customer": DimensionConfig(name="is_paid_telematics_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_stce_customer": DimensionConfig(name="is_paid_stce_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
}

DIMENSION_CONFIGS_ORG = {
    "org_id": DimensionConfig(name="org_id", data_type=DimensionDataType.BIGINT, default_value=0),
    "org_name": DimensionConfig(name="org_name", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_csm_email": DimensionConfig(name="account_csm_email", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "cloud_region": DimensionConfig(name="cloud_region", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "region": DimensionConfig(name="region", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_size_segment": DimensionConfig(name="account_size_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_arr_segment": DimensionConfig(name="account_arr_segment", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "account_industry": DimensionConfig(name="account_industry", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "industry_vertical": DimensionConfig(name="industry_vertical", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "is_paid_safety_customer": DimensionConfig(name="is_paid_safety_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_telematics_customer": DimensionConfig(name="is_paid_telematics_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
    "is_paid_stce_customer": DimensionConfig(name="is_paid_stce_customer", data_type=DimensionDataType.BOOLEAN, default_value="false"),
}
