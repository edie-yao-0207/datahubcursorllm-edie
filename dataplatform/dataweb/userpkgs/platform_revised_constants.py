from dataweb.userpkgs.constants import (
    ColumnDescription,
    DimensionConfig,
    DimensionDataType,
)

PLATFORM_STANDARD_DIMENSIONS = {
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
    ],
    "product_dimensions": [
        ("is_paid_safety_customer", "CASE WHEN {ALIAS}.is_paid_safety_customer = 1 THEN TRUE ELSE FALSE END"),
        ("is_paid_telematics_customer", "CASE WHEN {ALIAS}.is_paid_telematics_customer = 1 THEN TRUE ELSE FALSE END"),
        ("is_paid_stce_customer", "CASE WHEN {ALIAS}.is_paid_stce_customer = 1 THEN TRUE ELSE FALSE END"),
    ],
    "sam_dimensions": [
        ("sam_number", "CASE WHEN {ALIAS}.sam_number IS NULL THEN 'Unknown' ELSE {ALIAS}.sam_number END"),
    ],
}

PLATFORM_METRICS = {
    "full_admin_account": "COUNT(DISTINCT CASE WHEN fraction_admin >= 1 THEN f.account_id END)",
    "custom_role_account": "COUNT(DISTINCT CASE WHEN fraction_custom_role > 0 THEN f.account_id END)",
    "account_with_two_plus_users": "COUNT(DISTINCT f.account_id)",
    "device_attributes_account": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.account_id END)",
    "device_account": "COUNT(DISTINCT d.account_id)",
    "device_tags_account": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.account_id END)",
    "driver_attributes_account": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.account_id END)",
    "driver_tags_account": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.account_id END)",
    "driver_account": "COUNT(DISTINCT d.account_id)",
    "installed_vg": "COUNT(DISTINCT CASE WHEN d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' AND d.device_type LIKE 'VG%' THEN d.device_id END)",
    "installed_at": "COUNT(DISTINCT CASE WHEN d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' AND d.device_type LIKE 'AT%' THEN d.device_id END)",
    "installed_ag": "COUNT(DISTINCT CASE WHEN d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' AND d.device_type LIKE 'AG%' THEN d.device_id END)",
    "installed_cm": "COUNT(DISTINCT CASE WHEN d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' AND d.device_type LIKE 'CM%' THEN d.device_id END)",
    "at_with_photo": "COUNT(DISTINCT CASE WHEN d.has_dashboard_upload_photo = 1 OR d.has_mobile_upload_photo = 1 THEN d.device_id END)",
    "ag_with_photo": "COUNT(DISTINCT CASE WHEN d.has_dashboard_upload_photo = 1 OR d.has_mobile_upload_photo = 1 THEN d.device_id END)",
    "vg_with_photo": "COUNT(DISTINCT CASE WHEN d.has_dashboard_upload_photo = 1 OR d.has_mobile_upload_photo = 1 OR d.has_dvir_fallback = 1 THEN d.device_id END)",
    "complete_flow_vg": "COUNT(DISTINCT CASE WHEN d.device_type LIKE 'VG%' THEN d.device_id END)",
    "complete_flow_at": "COUNT(DISTINCT CASE WHEN d.device_type LIKE 'AT%' THEN d.device_id END)",
    "complete_flow_ag": "COUNT(DISTINCT CASE WHEN d.device_type LIKE 'AG%' THEN d.device_id END)",
    "complete_flow_cm": "COUNT(DISTINCT d.cm_device_id)",
    "total_time_to_install_vg": "SUM(CASE WHEN timestamp_diff_seconds >= 0 AND device_type = 'vg' THEN timestamp_diff_seconds END)",
    "total_time_to_install_ag": "SUM(CASE WHEN timestamp_diff_seconds >= 0 AND device_type = 'ag' THEN timestamp_diff_seconds END)",
    "total_time_to_install_at": "SUM(CASE WHEN timestamp_diff_seconds >= 0 AND device_type = 'at' THEN timestamp_diff_seconds END)",
    "total_time_to_install_cm": "SUM(CASE WHEN timestamp_diff_seconds >= 0 AND device_type = 'cm3x' THEN timestamp_diff_seconds END)",
    "correctly_installed_vg": "COUNT(DISTINCT CASE WHEN d.vindb_contains_make_model = 1 THEN d.device_id END)",
    "correctly_installed_cm": "COUNT(DISTINCT CASE WHEN d.active = 1 THEN d.device_id END)",
    "csv_upload_failures": "COUNT(DISTINCT CASE WHEN d.total_rows_errored > 0 THEN d.uuid END)",
    "csv_uploads": "COUNT(DISTINCT d.uuid)",
    "full_admin_users": "COUNT(DISTINCT CASE WHEN is_admin = 1 THEN CONCAT(f.sam_number, '_', f.account_id, '_', f.user_id) END)",
    "custom_role_users": "COUNT(DISTINCT CASE WHEN is_custom_role = 1 THEN CONCAT(f.sam_number, '_', f.account_id, '_', f.user_id) END)",
    "total_users": "COUNT(DISTINCT CONCAT(f.sam_number, '_', f.account_id, '_', f.user_id))",
    "device_attributes": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN d.device_id END)",
    "device_tags": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN d.device_id END)",
    "total_devices": "COUNT(DISTINCT d.device_id)",
    "driver_attributes": "COUNT(DISTINCT CASE WHEN d.attributes_size > 0 THEN CONCAT(d.sam_number, '_', d.account_id, '_', d.driver_id) END)",
    "driver_tags": "COUNT(DISTINCT CASE WHEN d.tags_size > 0 THEN CONCAT(d.sam_number, '_', d.account_id, '_', d.driver_id) END)",
    "total_drivers": "COUNT(DISTINCT CONCAT(d.sam_number, '_', d.account_id, '_', d.driver_id))",
}

PLATFORM_METRIC_CONFIGS_TEMPLATE = {
    "user_counts_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "user_roles_us",
        "eu_source": "user_roles_eu",
        "ca_source": "user_roles_ca",
        "table_alias": "f",
        "join_alias": "f",
        "metrics": ["full_admin_users", "custom_role_users", "total_users"]
    },
    "user_roles_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "user_roles_flattened_us_final",
        "eu_source": "user_roles_flattened_eu_final",
        "ca_source": "user_roles_flattened_ca_final",
        "table_alias": "f",
        "join_alias": "f",
        "metrics": ["full_admin_account", "custom_role_account", "account_with_two_plus_users"]
    },
    "device_details_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "device_details_us",
        "eu_source": "device_details_eu",
        "ca_source": "device_details_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["device_attributes_account", "device_tags_account", "device_account", "device_attributes", "device_tags", "total_devices"]
    },
    "driver_details_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "driver_breakdown_us",
        "eu_source": "driver_breakdown_eu",
        "ca_source": "driver_breakdown_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["driver_attributes_account", "driver_tags_account", "driver_account", "driver_attributes", "driver_tags", "total_drivers"]
    },
    "device_installs_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "installed_devices_us",
        "eu_source": "installed_devices_eu",
        "ca_source": "installed_devices_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["installed_vg", "installed_at", "installed_ag", "installed_cm"]
    },
    "at_photo_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "at_photo_install_us",
        "eu_source": "at_photo_install_eu",
        "ca_source": "at_photo_install_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["at_with_photo"]
    },
    "ag_photo_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "ag_photo_install_us",
        "eu_source": "ag_photo_install_eu",
        "ca_source": "ag_photo_install_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["ag_with_photo"]
    },
    "complete_flow_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "complete_flow_us",
        "eu_source": "complete_flow_eu",
        "ca_source": "complete_flow_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["complete_flow_at", "complete_flow_ag", "complete_flow_vg"]
    },
    "complete_flow_cm_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "successful_install_cm_us",
        "eu_source": "successful_install_cm_eu",
        "ca_source": "successful_install_cm_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["complete_flow_cm"]
    },
    "vg_photo_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "vg_photo_install_us",
        "eu_source": "vg_photo_install_eu",
        "ca_source": "vg_photo_install_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["vg_with_photo"]
    },
    "install_time_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "time_to_install_us",
        "eu_source": "time_to_install_eu",
        "ca_source": "time_to_install_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["total_time_to_install_vg", "total_time_to_install_ag", "total_time_to_install_at"]
    },
    "install_time_cm_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "time_to_install_cm_us",
        "eu_source": "time_to_install_cm_eu",
        "ca_source": "time_to_install_cm_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["total_time_to_install_cm"]
    },
    "vg_install_correctness_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "install_correctness_vg_us",
        "eu_source": "install_correctness_vg_eu",
        "ca_source": "install_correctness_vg_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["correctly_installed_vg"]
    },
    "cm_install_correctness_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "install_correctness_cm_us",
        "eu_source": "install_correctness_cm_eu",
        "ca_source": "install_correctness_cm_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["correctly_installed_cm"]
    },
    "csv_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "csv_breakdown_us",
        "eu_source": "csv_breakdown_eu",
        "ca_source": "csv_breakdown_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["csv_upload_failures", "csv_uploads"]
    },
}

PLATFORM_DIMENSION_TEMPLATES = {
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
        },
    },
}

PLATFORM_REGION_MAPPINGS = {
    "us-west-2": {
        "cloud_region": "us-west-2",
        "region_alias": "US"
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

PLATFORM_JOIN_MAPPINGS = {
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
    },
}

PLATFORM_SCHEMA = [
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
            "comment": "Queried from edw.silver.dim_customer. Geographic information about the account"
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
        "name": "full_admin_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has all users as admins or not"
        },
    },
    {
        "name": "full_admin_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of users in the account with a full admin role"
        },
    },
    {
        "name": "custom_role_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least one user with a custom role or not"
        },
    },
    {
        "name": "custom_role_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of users in the account with a custom role"
        },
    },
    {
        "name": "account_with_two_plus_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least two users or not"
        },
    },
    {
        "name": "total_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of users in the account"
        },
    },
    {
        "name": "device_attributes_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least one device with an attribute or not"
        },
    },
    {
        "name": "device_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices in the account with an attribute"
        },
    },
    {
        "name": "device_tags_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least one device with a tag or not"
        },
    },
    {
        "name": "device_tags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of devices in the account with a tag"
        },
    },
    {
        "name": "device_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least one device or not"
        },
    },
    {
        "name": "total_devices",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active devices in the account"
        },
    },
    {
        "name": "driver_tags_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least one driver with a tag or not"
        },
    },
    {
        "name": "driver_tags",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers in the account with a tag"
        },
    },
    {
        "name": "driver_attributes_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the driver has at least one driver with an attribute or not"
        },
    },
    {
        "name": "driver_attributes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drivers in the account with an attribute"
        },
    },
    {
        "name": "driver_account",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account has at least one driver or not"
        },
    },
    {
        "name": "total_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active drivers in the account"
        },
    },
    {
        "name": "installed_vg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed VG devices"
        },
    },
    {
        "name": "installed_at",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed AT devices"
        },
    },
    {
        "name": "installed_ag",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed AG devices"
        },
    },
    {
        "name": "installed_cm",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed CM devices"
        },
    },
    {
        "name": "at_with_photo",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed AT devices with a photo"
        },
    },
    {
        "name": "ag_with_photo",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed AG devices with a photo"
        },
    },
    {
        "name": "vg_with_photo",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of installed VG devices with a photo"
        },
    },
    {
        "name": "complete_flow_vg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of VG devices that successfully completed mobile installation"
        },
    },
    {
        "name": "complete_flow_ag",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of AG devices that successfully completed mobile installation"
        },
    },
    {
        "name": "complete_flow_cm",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of CM devices that successfully completed mobile installation"
        },
    },
    {
        "name": "complete_flow_at",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of AT devices that successfully completed mobile installation"
        },
    },
    {
        "name": "total_time_to_install_vg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total time taken to install VG devices (seconds)"
        },
    },
    {
        "name": "total_time_to_install_ag",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total time taken to install AG devices (seconds)"
        },
    },
    {
        "name": "total_time_to_install_at",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total time taken to install AT devices (seconds)"
        },
    },
    {
        "name": "total_time_to_install_cm",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total time taken to install CM devices (seconds)"
        },
    },
    {
        "name": "correctly_installed_vg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of VG devices successfully installed"
        },
    },
    {
        "name": "correctly_installed_cm",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of CM devices successfully installed"
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
]

QUERY_TEMPLATE = """
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
            COALESCE(o.account_id, 'Unknown') AS account_id
        FROM datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_us p
            ON o.sam_number = p.sam_number
        WHERE o.date = '{END_OF_PERIOD}'
        AND o.is_paid_customer
        AND (
            o.is_paid_safety_customer = TRUE
            OR o.is_paid_telematics_customer = TRUE
            OR o.is_paid_stce_customer = TRUE
        )
    ),
    us_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            sam_number,
            account_csm_email,
            account_name,
            account_id,
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
            COALESCE(o.account_id, 'Unknown') AS account_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_eu p
            ON o.sam_number = p.sam_number
        WHERE o.date = '{END_OF_PERIOD}'
        AND o.is_paid_customer
        AND (
            o.is_paid_safety_customer = TRUE
            OR o.is_paid_telematics_customer = TRUE
            OR o.is_paid_stce_customer = TRUE
        )
    ),
    eu_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            sam_number,
            account_csm_email,
            account_name,
            account_id,
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
            COALESCE(o.account_id, 'Unknown') AS account_id
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_ca p
            ON o.sam_number = p.sam_number
        WHERE o.date = '{END_OF_PERIOD}'
        AND o.is_paid_customer
        AND (
            o.is_paid_safety_customer = TRUE
            OR o.is_paid_telematics_customer = TRUE
            OR o.is_paid_stce_customer = TRUE
        )
    ),
    ca_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            sam_number,
            account_csm_email,
            account_name,
            account_id,
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
    user_roles_eu AS (
        SELECT
            date,
            user_id,
            o.sam_number,
            o.account_id,
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
        FROM data_tools_delta_share.datamodel_platform.dim_users_organizations duo
        JOIN latest_org_eu o
            ON duo.sam_number = o.sam_number
        WHERE
            login_count_30d IS NOT NULL  -- User logged in the last month
            AND login_count_30d > 0
            AND is_samsara_email = FALSE
            AND email NOT LIKE '%@samsara.com'
            AND email NOT LIKE '%samsara.canary%'
            AND email NOT LIKE '%samsara.forms.canary%'
            AND email NOT LIKE '%samsaracanarydevcontractor%'
            AND email NOT LIKE '%samsaratest%'
            AND email NOT LIKE '%@samsara%'
            AND email NOT LIKE '%@samsara-service-account.com'
            AND date = '{END_OF_PERIOD}'
    ),
    user_roles_ca AS (
        SELECT
            date,
            user_id,
            o.sam_number,
            o.account_id,
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
        FROM data_tools_delta_share_ca.datamodel_platform.dim_users_organizations duo
        JOIN latest_org_ca o
            ON duo.sam_number = o.sam_number
        WHERE
            login_count_30d IS NOT NULL  -- User logged in the last month
            AND login_count_30d > 0
            AND is_samsara_email = FALSE
            AND email NOT LIKE '%@samsara.com'
            AND email NOT LIKE '%samsara.canary%'
            AND email NOT LIKE '%samsara.forms.canary%'
            AND email NOT LIKE '%samsaracanarydevcontractor%'
            AND email NOT LIKE '%samsaratest%'
            AND email NOT LIKE '%@samsara%'
            AND email NOT LIKE '%@samsara-service-account.com'
            AND date = '{END_OF_PERIOD}'
    ),
    user_roles_us AS (
        SELECT
            date,
            user_id,
            o.sam_number,
            o.account_id,
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
        FROM datamodel_platform.dim_users_organizations duo
        JOIN latest_org_us o
            ON duo.sam_number = o.sam_number
        WHERE login_count_30d IS NOT NULL  -- User logged in the last month
        AND login_count_30d > 0
        AND is_samsara_email = FALSE
        AND email NOT LIKE '%@samsara.com'
        AND email NOT LIKE '%samsara.canary%'
        AND email NOT LIKE '%samsara.forms.canary%'
        AND email NOT LIKE '%samsaracanarydevcontractor%'
        AND email NOT LIKE '%samsaratest%'
        AND email NOT LIKE '%@samsara%'
        AND email NOT LIKE '%@samsara-service-account.com'
        AND date = '{END_OF_PERIOD}'
    ),
    user_roles_flattened_us AS (
        SELECT
            user_roles.date,
            sam_number,
            account_id,
            COUNT(DISTINCT user_roles.user_id) AS user_count,
            AVG(is_admin) AS fraction_admin,
            AVG(is_custom_role) AS fraction_custom_role
        FROM user_roles_us user_roles
        GROUP BY 1, 2, 3
    ),
    user_roles_flattened_eu AS (
        SELECT
            user_roles.date,
            sam_number,
            account_id,
            COUNT(DISTINCT user_roles.user_id) AS user_count,
            AVG(is_admin) AS fraction_admin,
            AVG(is_custom_role) AS fraction_custom_role
        FROM user_roles_eu user_roles
        GROUP BY 1, 2, 3
    ),
    user_roles_flattened_ca AS (
        SELECT
            user_roles.date,
            sam_number,
            account_id,
            COUNT(DISTINCT user_roles.user_id) AS user_count,
            AVG(is_admin) AS fraction_admin,
            AVG(is_custom_role) AS fraction_custom_role
        FROM user_roles_ca user_roles
        GROUP BY 1, 2, 3
    ),
    user_roles_flattened_us_final AS (
        SELECT
            sam_number,
            account_id,
            fraction_admin,
            fraction_custom_role
        FROM user_roles_flattened_us
        WHERE user_count >= 2
    ),
    user_roles_flattened_eu_final AS (
        SELECT
            sam_number,
            account_id,
            fraction_admin,
            fraction_custom_role
        FROM user_roles_flattened_eu
        WHERE
            user_count >= 2
    ),
    user_roles_flattened_ca_final AS (
        SELECT
            sam_number,
            account_id,
            fraction_admin,
            fraction_custom_role
        FROM user_roles_flattened_ca
        WHERE
            user_count >= 2
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
        WHERE
            DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
            AND lda.date = '{END_OF_PERIOD}'
    ),
    device_attributes_eu AS (
        WITH attributes AS (
            SELECT
                uuid AS attribute_id,
                name AS attribute_name,
                CAST(created_at AS DATE) AS attribute_created_at,
                CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles

            UNION ALL

            SELECT
                uuid AS attribute_id,
                name AS attribute_name,
                CAST(created_at AS DATE) AS attribute_created_at,
                CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles
        ),
        vehicle_attributes AS (
            SELECT
                entity_id AS device_id,
                attribute_id,
                CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles

            UNION ALL

            SELECT
                entity_id AS device_id,
                attribute_id,
                CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles
        ),
        attribute_values AS (
            SELECT
                attribute_id,
                COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id

            UNION ALL

            SELECT
                attribute_id,
                COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT
            va.device_id,
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
    device_tags_eu AS (
        SELECT
            device_id,
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
        WHERE
            DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
            AND lda.date = '{END_OF_PERIOD}'
    ),
    device_attributes_ca AS (
        WITH attributes AS (


            SELECT
                uuid AS attribute_id,
                name AS attribute_name,
                CAST(created_at AS DATE) AS attribute_created_at,
                CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 2 -- Only vehicles
        ),
        vehicle_attributes AS (
            SELECT
                entity_id AS device_id,
                attribute_id,
                CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 2 -- Only vehicles
        ),
        attribute_values AS (
            SELECT
                attribute_id,
                COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT
            va.device_id,
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
    device_tags_ca AS (
        SELECT
            device_id,
            COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-productsdb/prod_db/tag_devices_v0` AS td
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
            ON t.id = td.tag_id
        GROUP BY device_id
    ),
    device_details_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            dtac.device_id,
            dtac.tags_size,
            dtac.attributes_size
        FROM metrics_repo.device_tags_attributes_count dtac
        JOIN latest_org_us o
            ON dtac.org_id = o.org_id
        WHERE dtac.date = '{END_OF_PERIOD}'
    ),
    device_details_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            d.device_id,
            COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
            COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM active_devices_eu d
        JOIN latest_org_eu o
            ON d.org_id = o.org_id
        LEFT OUTER JOIN device_tags_eu t
            ON d.device_id = t.device_id
        LEFT OUTER JOIN device_attributes_eu a
            ON d.device_id = a.device_id
    ),
    device_details_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            d.device_id,
            COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
            COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM active_devices_ca d
        JOIN latest_org_ca o
            ON d.org_id = o.org_id
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
        WHERE
            DATEDIFF(d.date, d.last_mobile_login_date) < 30  -- filter to drivers that have logged in in the last month
            AND d.date = '{END_OF_PERIOD}'
    ),
    driver_attributes_eu AS (
        WITH attributes AS (
            SELECT
                uuid AS attribute_id,
                name AS attribute_name,
                CAST(created_at AS DATE) AS attribute_created_at,
                CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers

            UNION ALL

            SELECT
                uuid AS attribute_id,
                name AS attribute_name,
                CAST(created_at AS DATE) AS attribute_created_at,
                CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers
        ),
        driver_attributes AS (
            SELECT
                entity_id AS driver_id,
                attribute_id,
                CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers

            UNION ALL

            SELECT
                entity_id AS driver_id,
                attribute_id,
                CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers
        ),
        attribute_values AS (
            SELECT
                attribute_id,
                COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attribute-shard-1db/attributedb/attribute_values_v0`
            GROUP BY attribute_id

            UNION ALL

            SELECT
                attribute_id,
                COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT
            da.driver_id,
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
        SELECT
            driver_id,
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
        WHERE
            DATEDIFF(d.date, d.last_mobile_login_date) < 30  -- filter to drivers that have logged in in the last month
            AND d.date = '{END_OF_PERIOD}'
    ),
    driver_attributes_ca AS (
        WITH attributes AS (
            SELECT
                uuid AS attribute_id,
                name AS attribute_name,
                CAST(created_at AS DATE) AS attribute_created_at,
                CAST(updated_at AS DATE) AS attribute_updated_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attributes_v0`
            WHERE entity_type = 1 -- Only drivers
        ),
        driver_attributes AS (
            SELECT
                entity_id AS driver_id,
                attribute_id,
                CAST(created_at AS DATE) AS attribute_assigned_at
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_cloud_entities_v0` AS ace
            WHERE entity_type = 1 -- Only drivers
        ),
        attribute_values AS (
            SELECT
                attribute_id,
                COUNT(*) AS attribute_values_n
            FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-attributedb/attributedb/attribute_values_v0`
            GROUP BY attribute_id
        )
        SELECT
            da.driver_id,
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
        SELECT
            driver_id,
            COLLECT_SET(tag_id) AS tag_ids
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tag_drivers_v1` AS td
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/tags_v1` AS t
            ON t.id = td.tag_id
        GROUP BY driver_id
    ),
    driver_breakdown_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            dtac.driver_id,
            dtac.tags_size,
            dtac.attributes_size
        FROM metrics_repo.driver_tags_attributes_count dtac
        JOIN latest_org_us o
            ON dtac.org_id = o.org_id
        WHERE dtac.date = '{END_OF_PERIOD}'
    ),
    driver_breakdown_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            d.driver_id,
            COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
            COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM drivers_eu d
        JOIN latest_org_eu o
            ON d.org_id = o.org_id
        LEFT OUTER JOIN driver_tags_eu t
            ON d.driver_id = t.driver_id
        LEFT OUTER JOIN driver_attributes_eu a
            ON d.driver_id = a.driver_id
        WHERE
            d.driver_id IS NOT NULL
    ),
    driver_breakdown_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            d.driver_id,
            COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
            COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size
        FROM drivers_ca d
        JOIN latest_org_ca o
            ON d.org_id = o.org_id
        LEFT OUTER JOIN driver_tags_ca t
            ON d.driver_id = t.driver_id
        LEFT OUTER JOIN driver_attributes_ca a
            ON d.driver_id = a.driver_id
        WHERE
            d.driver_id IS NOT NULL
    ),
    installed_devices_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            ldo.device_id,
            ldo.device_type,
            ldo.first_date
        FROM datamodel_core.lifetime_device_online ldo
        JOIN latest_org_us o
            ON ldo.org_id = o.org_id
        JOIN datamodel_core.dim_devices dd
            ON ldo.org_id = dd.org_id
            AND ldo.device_id = dd.device_id
            AND ldo.date = dd.date
        WHERE
            ldo.date = '{END_OF_PERIOD}'
            AND (LOWER(dd.device_name) NOT LIKE '%deactivated%' OR dd.device_name IS NULL)
            AND CASE WHEN
                    dd.device_type LIKE 'AT%' THEN REGEXP_REPLACE(dd.device_name, '[^a-zA-Z0-9]', '') != dd.serial
                ELSE 1=1
                END
    ),
    installed_devices_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            ldo.device_id,
            ldo.device_type,
            ldo.first_date
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_online` ldo
        JOIN latest_org_eu o
            ON ldo.org_id = o.org_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
            ON ldo.org_id = dd.org_id
            AND ldo.device_id = dd.device_id
            AND ldo.date = dd.date
        WHERE
            ldo.date = '{END_OF_PERIOD}'
            AND (LOWER(dd.device_name) NOT LIKE '%deactivated%' OR dd.device_name IS NULL)
            AND CASE WHEN
                    dd.device_type LIKE 'AT%' THEN REGEXP_REPLACE(dd.device_name, '[^a-zA-Z0-9]', '') != dd.serial
                ELSE 1=1
                END
    ),
    installed_devices_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            ldo.device_id,
            ldo.device_type,
            ldo.first_date
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_online ldo
        JOIN latest_org_ca o
            ON ldo.org_id = o.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dd
            ON ldo.org_id = dd.org_id
            AND ldo.device_id = dd.device_id
            AND ldo.date = dd.date
        WHERE
            ldo.date = '{END_OF_PERIOD}'
            AND (LOWER(dd.device_name) NOT LIKE '%deactivated%' OR dd.device_name IS NULL)
            AND CASE WHEN
                    dd.device_type LIKE 'AT%' THEN REGEXP_REPLACE(dd.device_name, '[^a-zA-Z0-9]', '') != dd.serial
                ELSE 1=1
                END
    ),
    dashboard_photo_base_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            mr.DeviceId AS device_id,
            d.device_type,
            CAST(BOOL_OR(mr.PhotoType = 'AssetPhoto') AS INTEGER) AS has_dashboard_upload_photo
        FROM dynamodb.device_installation_media_records_dynamo AS mr
        JOIN latest_org_us o
            ON mr.OrgId = o.org_id
        JOIN installed_devices_us d
            ON mr.DeviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        WHERE mr.HardwareIdentifier = 'DASHBOARD_UPLOAD'
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (d.device_type LIKE 'AT%' OR d.device_type LIKE 'VG%' OR d.device_type LIKE 'AG%')
        GROUP BY mr.DeviceId, o.sam_number, o.account_id, d.device_type
    ),
    filtered_media_us AS (
        SELECT *
        FROM dynamodb.device_installation_media_records_dynamo
        WHERE
            PhotoType = 'AssetPhoto'
            AND MediaStatus = 'Exists'
            AND HardwareIdentifier != 'DASHBOARD_UPLOAD'
            AND HardwareType IN ('ag', 'vg', 'at')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    dashboard_photo_base_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            mr.DeviceId AS device_id,
            d.device_type,
            CAST(BOOL_OR(mr.PhotoType = 'AssetPhoto') AS INTEGER) AS has_dashboard_upload_photo
        FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/device-installation-media-records-dynamo` AS mr
        JOIN latest_org_eu o
            ON mr.OrgId = o.org_id
        JOIN installed_devices_eu d
            ON mr.DeviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        WHERE mr.HardwareIdentifier = 'DASHBOARD_UPLOAD'
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (d.device_type LIKE 'AT%' OR d.device_type LIKE 'VG%' OR d.device_type LIKE 'AG%')
        GROUP BY mr.DeviceId, o.sam_number, o.account_id, d.device_type
    ),
    filtered_media_eu AS (
        SELECT *
        FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/device-installation-media-records-dynamo`
        WHERE
            PhotoType = 'AssetPhoto'
            AND MediaStatus = 'Exists'
            AND HardwareIdentifier != 'DASHBOARD_UPLOAD'
            AND HardwareType IN ('ag', 'vg', 'at')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    dashboard_photo_base_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            mr.DeviceId AS device_id,
            d.device_type,
            CAST(BOOL_OR(mr.PhotoType = 'AssetPhoto') AS INTEGER) AS has_dashboard_upload_photo
        FROM delta.`s3://samsara-ca-dynamodb-delta-lake/table/device-installation-media-records-dynamo` AS mr
        JOIN latest_org_ca o
            ON mr.OrgId = o.org_id
        JOIN installed_devices_ca d
            ON mr.DeviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        WHERE mr.HardwareIdentifier = 'DASHBOARD_UPLOAD'
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (d.device_type LIKE 'AT%' OR d.device_type LIKE 'VG%' OR d.device_type LIKE 'AG%')
        GROUP BY mr.DeviceId, o.sam_number, o.account_id, d.device_type
    ),
    filtered_media_ca AS (
        SELECT *
        FROM delta.`s3://samsara-ca-dynamodb-delta-lake/table/device-installation-media-records-dynamo`
        WHERE
            PhotoType = 'AssetPhoto'
            AND MediaStatus = 'Exists'
            AND HardwareIdentifier != 'DASHBOARD_UPLOAD'
            AND HardwareType IN ('ag', 'vg', 'at')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_install_upload_photo_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.json_params:deviceId AS device_id,
            CAST(
                COUNT(DISTINCT fm.DeviceInstallationMediaUUID) > 0 AS
                INTEGER
            ) AS has_mobile_upload_photo
        FROM datastreams_history.mobile_logs l
        JOIN latest_org_us o
            ON l.org_id = o.org_id
        JOIN installed_devices_us d
            ON l.json_params:deviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        LEFT OUTER JOIN filtered_media_us fm
            ON fm.DeviceId = l.json_params:deviceId
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND CASE WHEN d.device_type LIKE 'AT%' THEN l.json_params:hasAT = TRUE
                WHEN d.device_type LIKE 'VG%' THEN l.json_params:hasVG = TRUE
                WHEN d.device_type LIKE 'AG%' THEN l.json_params:hasAG = TRUE
            END
        GROUP BY 1, 2, 3
    ),
    mobile_install_upload_photo_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.json_params:deviceId AS device_id,
            CAST(
                COUNT(DISTINCT fm.DeviceInstallationMediaUUID) > 0 AS
                INTEGER
            ) AS has_mobile_upload_photo
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs` l
        JOIN latest_org_eu o
            ON l.org_id = o.org_id
        JOIN installed_devices_eu d
            ON l.json_params:deviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        LEFT OUTER JOIN filtered_media_eu fm
            ON fm.DeviceId = l.json_params:deviceId
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND CASE WHEN d.device_type LIKE 'AT%' THEN l.json_params:hasAT = TRUE
                WHEN d.device_type LIKE 'VG%' THEN l.json_params:hasVG = TRUE
                WHEN d.device_type LIKE 'AG%' THEN l.json_params:hasAG = TRUE
            END
        GROUP BY 1, 2, 3
    ),
    mobile_install_upload_photo_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.json_params:deviceId AS device_id,
            CAST(
                COUNT(DISTINCT fm.DeviceInstallationMediaUUID) > 0 AS
                INTEGER
            ) AS has_mobile_upload_photo
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs` l
        JOIN latest_org_ca o
            ON l.org_id = o.org_id
        JOIN installed_devices_ca d
            ON l.json_params:deviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        LEFT OUTER JOIN filtered_media_ca fm
            ON fm.DeviceId = l.json_params:deviceId
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND CASE WHEN d.device_type LIKE 'AT%' THEN l.json_params:hasAT = TRUE
                WHEN d.device_type LIKE 'VG%' THEN l.json_params:hasVG = TRUE
                WHEN d.device_type LIKE 'AG%' THEN l.json_params:hasAG = TRUE
            END
        GROUP BY 1, 2, 3
    ),
    at_ag_photo_install_base_us AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            i.device_type,
            COALESCE(dp.has_dashboard_upload_photo, 0) AS has_dashboard_upload_photo,
            COALESCE(mi.has_mobile_upload_photo, 0) AS has_mobile_upload_photo
        FROM installed_devices_us i
        LEFT OUTER JOIN dashboard_photo_base_us dp
            ON i.device_id = dp.device_id
            AND i.sam_number = dp.sam_number
            AND i.account_id = dp.account_id
            AND i.device_type = dp.device_type
        LEFT OUTER JOIN mobile_install_upload_photo_us mi
            ON i.device_id = mi.device_id
            AND i.sam_number = mi.sam_number
            AND i.account_id = mi.account_id
        WHERE
            (i.device_type LIKE 'AT%' OR i.device_type LIKE 'AG%')
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    at_photo_install_us AS (
        SELECT
            sam_number,
            account_id,
            device_id,
            has_dashboard_upload_photo,
            has_mobile_upload_photo
        FROM at_ag_photo_install_base_us
            WHERE device_type LIKE 'AT%'
    ),
    ag_photo_install_us AS (
        SELECT
            sam_number,
            account_id,
            device_id,
            has_dashboard_upload_photo,
            has_mobile_upload_photo
        FROM at_ag_photo_install_base_us
            WHERE device_type LIKE 'AG%'
    ),
    at_ag_photo_install_base_eu AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            i.device_type,
            COALESCE(dp.has_dashboard_upload_photo, 0) AS has_dashboard_upload_photo,
            COALESCE(mi.has_mobile_upload_photo, 0) AS has_mobile_upload_photo
        FROM installed_devices_eu i
        LEFT OUTER JOIN dashboard_photo_base_eu dp
            ON i.device_id = dp.device_id
            AND i.sam_number = dp.sam_number
            AND i.account_id = dp.account_id
            AND i.device_type = dp.device_type
        LEFT OUTER JOIN mobile_install_upload_photo_eu mi
            ON i.device_id = mi.device_id
            AND i.sam_number = mi.sam_number
            AND i.account_id = mi.account_id
        WHERE
            (i.device_type LIKE 'AT%' OR i.device_type LIKE 'AG%')
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    at_photo_install_eu AS (
        SELECT
            sam_number,
            account_id,
            device_id,
            has_dashboard_upload_photo,
            has_mobile_upload_photo
        FROM at_ag_photo_install_base_eu
            WHERE device_type LIKE 'AT%'
    ),
    ag_photo_install_eu AS (
        SELECT
            sam_number,
            account_id,
            device_id,
            has_dashboard_upload_photo,
            has_mobile_upload_photo
        FROM at_ag_photo_install_base_eu
            WHERE device_type LIKE 'AG%'
    ),
    at_ag_photo_install_base_ca AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            i.device_type,
            COALESCE(dp.has_dashboard_upload_photo, 0) AS has_dashboard_upload_photo,
            COALESCE(mi.has_mobile_upload_photo, 0) AS has_mobile_upload_photo
        FROM installed_devices_ca i
        LEFT OUTER JOIN dashboard_photo_base_ca dp
            ON i.device_id = dp.device_id
            AND i.sam_number = dp.sam_number
            AND i.account_id = dp.account_id
            AND i.device_type = dp.device_type
        LEFT OUTER JOIN mobile_install_upload_photo_ca mi
            ON i.device_id = mi.device_id
            AND i.sam_number = mi.sam_number
            AND i.account_id = mi.account_id
        WHERE
            (i.device_type LIKE 'AT%' OR i.device_type LIKE 'AG%')
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    at_photo_install_ca AS (
        SELECT
            sam_number,
            account_id,
            device_id,
            has_dashboard_upload_photo,
            has_mobile_upload_photo
        FROM at_ag_photo_install_base_ca
            WHERE device_type LIKE 'AT%'
    ),
    ag_photo_install_ca AS (
        SELECT
            sam_number,
            account_id,
            device_id,
            has_dashboard_upload_photo,
            has_mobile_upload_photo
        FROM at_ag_photo_install_base_ca
            WHERE device_type LIKE 'AG%'
    ),
    complete_flow_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.json_params:deviceId AS device_id,
            d.device_type
        FROM datastreams_history.mobile_logs l
        JOIN latest_org_us o
            ON l.org_id = o.org_id
        JOIN installed_devices_us d
            ON l.json_params:deviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    complete_flow_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.json_params:deviceId AS device_id,
            d.device_type
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs` l
        JOIN latest_org_eu o
            ON l.org_id = o.org_id
        JOIN installed_devices_eu d
            ON l.json_params:deviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    complete_flow_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.json_params:deviceId AS device_id,
            d.device_type
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs` l
        JOIN latest_org_ca o
            ON l.org_id = o.org_id
        JOIN installed_devices_ca d
            ON l.json_params:deviceId = d.device_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND d.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    exploded_dvirs_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            dv.vehicle_id,
            CASE
                WHEN t.photo.name = 'Trailer Back' THEN dv.trailer_device_id
                WHEN t.photo.name = 'Front' THEN dv.vehicle_id
            END AS effective_device_id,
            1 AS has_dvir_fallback,
            t.photo.uuid AS photo_uuid,
            t.photo.name AS photo_name,
            dv.created_at,
            ROW_NUMBER() OVER (
                PARTITION BY
                CASE
                    WHEN t.photo.name = 'Trailer Back' THEN dv.trailer_device_id
                    WHEN t.photo.name = 'Front' THEN dv.vehicle_id
                    END
                ORDER BY
                    dv.created_at ASC,
                    CASE t.photo.name
                        WHEN 'Front' THEN 1
                        WHEN 'Trailer Back' THEN 2
                        ELSE 3
                    END ASC
            ) AS rn
        FROM statsdb.dvirs dv
        JOIN latest_org_us o
            ON o.org_id = dv.org_id
        JOIN installed_devices_us d
            ON d.device_id = dv.vehicle_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        LATERAL VIEW explode(
            from_json(
                GET_JSON_OBJECT(TO_JSON(dv.proto), '$.walkaround_photos'),
                'array<struct<uuid:string,name:string,created_at_ms:string,org_id:string>>'
            )
        ) t AS photo
        WHERE
            t.photo.name IN ('Trailer Back', 'Front')
            AND dv.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    exploded_dvirs_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            dv.vehicle_id,
            CASE
                WHEN t.photo.name = 'Trailer Back' THEN dv.trailer_device_id
                WHEN t.photo.name = 'Front' THEN dv.vehicle_id
            END AS effective_device_id,
            1 AS has_dvir_fallback,
            t.photo.uuid AS photo_uuid,
            t.photo.name AS photo_name,
            dv.created_at,
            ROW_NUMBER() OVER (
                PARTITION BY
                CASE
                    WHEN t.photo.name = 'Trailer Back' THEN dv.trailer_device_id
                    WHEN t.photo.name = 'Front' THEN dv.vehicle_id
                    END
                ORDER BY
                    dv.created_at ASC,
                    CASE t.photo.name
                        WHEN 'Front' THEN 1
                        WHEN 'Trailer Back' THEN 2
                        ELSE 3
                    END ASC
            ) AS rn
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-statsdb/stats_db/dvirs_v0` dv
        JOIN latest_org_eu o
            ON o.org_id = dv.org_id
        JOIN installed_devices_eu d
            ON d.device_id = dv.vehicle_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        LATERAL VIEW explode(
            from_json(
                GET_JSON_OBJECT(TO_JSON(dv.proto), '$.walkaround_photos'),
                'array<struct<uuid:string,name:string,created_at_ms:string,org_id:string>>'
            )
        ) t AS photo
        WHERE
            t.photo.name IN ('Trailer Back', 'Front')
            AND dv.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    exploded_dvirs_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            dv.vehicle_id,
            CASE
                WHEN t.photo.name = 'Trailer Back' THEN dv.trailer_device_id
                WHEN t.photo.name = 'Front' THEN dv.vehicle_id
            END AS effective_device_id,
            1 AS has_dvir_fallback,
            t.photo.uuid AS photo_uuid,
            t.photo.name AS photo_name,
            dv.created_at,
            ROW_NUMBER() OVER (
                PARTITION BY
                CASE
                    WHEN t.photo.name = 'Trailer Back' THEN dv.trailer_device_id
                    WHEN t.photo.name = 'Front' THEN dv.vehicle_id
                    END
                ORDER BY
                    dv.created_at ASC,
                    CASE t.photo.name
                        WHEN 'Front' THEN 1
                        WHEN 'Trailer Back' THEN 2
                        ELSE 3
                    END ASC
            ) AS rn
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-statsdb/stats_db/dvirs_v0` dv
        JOIN latest_org_ca o
            ON o.org_id = dv.org_id
        JOIN installed_devices_ca d
            ON d.device_id = dv.vehicle_id
            AND o.sam_number = d.sam_number
            AND o.account_id = d.account_id
        LATERAL VIEW explode(
            from_json(
                GET_JSON_OBJECT(TO_JSON(dv.proto), '$.walkaround_photos'),
                'array<struct<uuid:string,name:string,created_at_ms:string,org_id:string>>'
            )
        ) t AS photo
        WHERE
            t.photo.name IN ('Trailer Back', 'Front')
            AND dv.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    vg_photo_install_us AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(dp.has_dashboard_upload_photo, 0) AS has_dashboard_upload_photo,
            COALESCE(mi.has_mobile_upload_photo, 0) AS has_mobile_upload_photo,
            COALESCE(dv.has_dvir_fallback, 0) AS has_dvir_fallback
        FROM installed_devices_us i
        LEFT OUTER JOIN dashboard_photo_base_us dp
            ON i.device_id = dp.device_id
            AND i.sam_number = dp.sam_number
            AND i.account_id = dp.account_id
            AND dp.device_type LIKE 'VG%'
        LEFT OUTER JOIN mobile_install_upload_photo_us mi
            ON i.device_id = mi.device_id
            AND i.sam_number = mi.sam_number
            AND i.account_id = mi.account_id
        LEFT OUTER JOIN exploded_dvirs_us dv
            ON i.device_id = dv.effective_device_id
            AND i.sam_number = dv.sam_number
            AND i.account_id = dv.account_id
        WHERE
            i.device_type LIKE 'VG%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    vg_photo_install_eu AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(dp.has_dashboard_upload_photo, 0) AS has_dashboard_upload_photo,
            COALESCE(mi.has_mobile_upload_photo, 0) AS has_mobile_upload_photo,
            COALESCE(dv.has_dvir_fallback, 0) AS has_dvir_fallback
        FROM installed_devices_eu i
        LEFT OUTER JOIN dashboard_photo_base_eu dp
            ON i.device_id = dp.device_id
            AND i.sam_number = dp.sam_number
            AND i.account_id = dp.account_id
            AND dp.device_type LIKE 'VG%'
        LEFT OUTER JOIN mobile_install_upload_photo_eu mi
            ON i.device_id = mi.device_id
            AND i.sam_number = mi.sam_number
            AND i.account_id = mi.account_id
        LEFT OUTER JOIN exploded_dvirs_eu dv
            ON i.device_id = dv.effective_device_id
            AND i.sam_number = dv.sam_number
            AND i.account_id = dv.account_id
        WHERE
            i.device_type LIKE 'VG%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    vg_photo_install_ca AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(dp.has_dashboard_upload_photo, 0) AS has_dashboard_upload_photo,
            COALESCE(mi.has_mobile_upload_photo, 0) AS has_mobile_upload_photo,
            COALESCE(dv.has_dvir_fallback, 0) AS has_dvir_fallback
        FROM installed_devices_ca i
        LEFT OUTER JOIN dashboard_photo_base_ca dp
            ON i.device_id = dp.device_id
            AND i.sam_number = dp.sam_number
            AND i.account_id = dp.account_id
            AND dp.device_type LIKE 'VG%'
        LEFT OUTER JOIN mobile_install_upload_photo_ca mi
            ON i.device_id = mi.device_id
            AND i.sam_number = mi.sam_number
            AND i.account_id = mi.account_id
        LEFT OUTER JOIN exploded_dvirs_ca dv
            ON i.device_id = dv.effective_device_id
            AND i.sam_number = dv.sam_number
            AND i.account_id = dv.account_id
        WHERE
            i.device_type LIKE 'VG%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_activation_us AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM datastreams_history.mobile_logs
        WHERE event_type = 'FLEET_INSTALLER_EXPERIENCE_ACTIVATION_SUCCESS'
            AND json_params:deviceType IN ('vg', 'ag', 'at')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_verification_us AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM datastreams_history.mobile_logs
        WHERE event_type = 'FLEET_INSTALLER_LOG_VERIFICATION'
            AND json_params:type IN ('cm3x')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_completed_us AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM datastreams_history.mobile_logs
        WHERE event_type IN ('FLEET_INSTALLER_LOG_INSTALLATION_COMPLETED', 'FLEET_INSTALLER_LOG_INSTALLATION_CANCELED')
            AND json_params:deviceType IN ('vg', 'ag', 'at', 'cm3x')
            AND date BETWEEN '{START_OF_PERIOD}' AND ADD_MONTHS('{END_OF_PERIOD}', 1)
    ),
    time_to_install_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            s.json_params:deviceId AS device_id,
            s.json_params:deviceType AS device_type,
            CASE
                WHEN e.timestamp IS NOT NULL THEN
                UNIX_TIMESTAMP(e.timestamp) - UNIX_TIMESTAMP(s.timestamp)
                ELSE
                UNIX_TIMESTAMP(LAST_DAY(ADD_MONTHS(s.timestamp, 1)) + INTERVAL 86399 SECOND) - UNIX_TIMESTAMP(s.timestamp) --86399 seconds until right before midnight
            END AS timestamp_diff_seconds
        FROM mobile_logs_activation_us s
        LEFT OUTER JOIN mobile_logs_completed_us e
            ON e.org_id = s.org_id
            AND e.json_params:deviceType = s.json_params:deviceType
            AND e.json_params:deviceId = s.json_params:deviceId
            AND e.json_params:deviceSerial = s.json_params:serial
        JOIN latest_org_us o
            ON s.org_id = o.org_id
        JOIN installed_devices_us i
            ON o.sam_number = i.sam_number
            AND o.account_id = i.account_id
            AND i.device_id = s.json_params:deviceId
        WHERE i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    time_to_install_cm_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            s.json_params:deviceId AS device_id,
            s.json_params:type AS device_type,
            CASE
                WHEN e.timestamp IS NOT NULL THEN
                UNIX_TIMESTAMP(e.timestamp) - (s.json_params:cmStartTimestamp / 1000)
                ELSE
                UNIX_TIMESTAMP(LAST_DAY(ADD_MONTHS(FROM_UNIXTIME(s.json_params:cmStartTimestamp / 1000), 1)) + INTERVAL 86399 SECOND) - (s.json_params:cmStartTimestamp / 1000) --86399 seconds until right before midnight
            END AS timestamp_diff_seconds
        FROM mobile_logs_verification_us s
        LEFT OUTER JOIN mobile_logs_completed_us e
            ON e.org_id = s.org_id
            AND e.json_params:deviceType = s.json_params:type
            AND e.json_params:deviceId = s.json_params:deviceId
            AND e.json_params:deviceSerial = s.json_params:deviceSerial
        JOIN latest_org_us o
            ON s.org_id = o.org_id
        JOIN installed_devices_us i
            ON o.sam_number = i.sam_number
            AND o.account_id = i.account_id
            AND i.device_id = s.json_params:deviceId
        WHERE i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_activation_eu AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs`
        WHERE event_type = 'FLEET_INSTALLER_EXPERIENCE_ACTIVATION_SUCCESS'
            AND json_params:deviceType IN ('vg', 'ag', 'at')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_verification_eu AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs`
        WHERE event_type = 'FLEET_INSTALLER_LOG_VERIFICATION'
            AND json_params:type IN ('cm3x')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_completed_eu AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs`
        WHERE event_type IN ('FLEET_INSTALLER_LOG_INSTALLATION_COMPLETED', 'FLEET_INSTALLER_LOG_INSTALLATION_CANCELED')
            AND json_params:deviceType IN ('vg', 'ag', 'at', 'cm3x')
            AND date BETWEEN '{START_OF_PERIOD}' AND ADD_MONTHS('{END_OF_PERIOD}', 1)
    ),
    time_to_install_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            s.json_params:deviceId AS device_id,
            s.json_params:deviceType AS device_type,
            CASE
                WHEN e.timestamp IS NOT NULL THEN
                UNIX_TIMESTAMP(e.timestamp) - UNIX_TIMESTAMP(s.timestamp)
                ELSE
                UNIX_TIMESTAMP(LAST_DAY(ADD_MONTHS(s.timestamp, 1)) + INTERVAL 86399 SECOND) - UNIX_TIMESTAMP(s.timestamp) --86399 seconds until right before midnight
            END AS timestamp_diff_seconds
        FROM mobile_logs_activation_eu s
        LEFT OUTER JOIN mobile_logs_completed_eu e
            ON e.org_id = s.org_id
            AND e.json_params:deviceType = s.json_params:deviceType
            AND e.json_params:deviceId = s.json_params:deviceId
            AND e.json_params:deviceSerial = s.json_params:serial
        JOIN latest_org_eu o
            ON s.org_id = o.org_id
        JOIN installed_devices_eu i
            ON o.sam_number = i.sam_number
            AND o.account_id = i.account_id
            AND i.device_id = s.json_params:deviceId
        WHERE i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    time_to_install_cm_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            s.json_params:deviceId AS device_id,
            s.json_params:type AS device_type,
            CASE
                WHEN e.timestamp IS NOT NULL THEN
                UNIX_TIMESTAMP(e.timestamp) - (s.json_params:cmStartTimestamp / 1000)
                ELSE
                UNIX_TIMESTAMP(LAST_DAY(ADD_MONTHS(FROM_UNIXTIME(s.json_params:cmStartTimestamp / 1000), 1)) + INTERVAL 86399 SECOND) - (s.json_params:cmStartTimestamp / 1000) --86399 seconds until right before midnight
            END AS timestamp_diff_seconds
        FROM mobile_logs_verification_eu s
        LEFT OUTER JOIN mobile_logs_completed_eu e
            ON e.org_id = s.org_id
            AND e.json_params:deviceType = s.json_params:type
            AND e.json_params:deviceId = s.json_params:deviceId
            AND e.json_params:deviceSerial = s.json_params:deviceSerial
        JOIN latest_org_eu o
            ON s.org_id = o.org_id
        JOIN installed_devices_eu i
            ON o.sam_number = i.sam_number
            AND o.account_id = i.account_id
            AND i.device_id = s.json_params:deviceId
        WHERE i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_activation_ca AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs`
        WHERE event_type = 'FLEET_INSTALLER_EXPERIENCE_ACTIVATION_SUCCESS'
            AND json_params:deviceType IN ('vg', 'ag', 'at')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_verification_ca AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs`
        WHERE event_type = 'FLEET_INSTALLER_LOG_VERIFICATION'
            AND json_params:type IN ('cm3x')
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    mobile_logs_completed_ca AS (
        SELECT
            org_id,
            timestamp,
            json_params
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs`
        WHERE event_type IN ('FLEET_INSTALLER_LOG_INSTALLATION_COMPLETED', 'FLEET_INSTALLER_LOG_INSTALLATION_CANCELED')
            AND json_params:deviceType IN ('vg', 'ag', 'at', 'cm3x')
            AND date BETWEEN '{START_OF_PERIOD}' AND ADD_MONTHS('{END_OF_PERIOD}', 1)
    ),
    time_to_install_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            s.json_params:deviceId AS device_id,
            s.json_params:deviceType AS device_type,
            CASE
                WHEN e.timestamp IS NOT NULL THEN
                UNIX_TIMESTAMP(e.timestamp) - UNIX_TIMESTAMP(s.timestamp)
                ELSE
                UNIX_TIMESTAMP(LAST_DAY(ADD_MONTHS(s.timestamp, 1)) + INTERVAL 86399 SECOND) - UNIX_TIMESTAMP(s.timestamp) --86399 seconds until right before midnight
            END AS timestamp_diff_seconds
        FROM mobile_logs_activation_ca s
        LEFT OUTER JOIN mobile_logs_completed_ca e
            ON e.org_id = s.org_id
            AND e.json_params:deviceType = s.json_params:deviceType
            AND e.json_params:deviceId = s.json_params:deviceId
            AND e.json_params:deviceSerial = s.json_params:serial
        JOIN latest_org_ca o
            ON s.org_id = o.org_id
        JOIN installed_devices_ca i
            ON o.sam_number = i.sam_number
            AND o.account_id = i.account_id
            AND i.device_id = s.json_params:deviceId
        WHERE i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    time_to_install_cm_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            s.json_params:deviceId AS device_id,
            s.json_params:type AS device_type,
            CASE
                WHEN e.timestamp IS NOT NULL THEN
                UNIX_TIMESTAMP(e.timestamp) - (s.json_params:cmStartTimestamp / 1000)
                ELSE
                UNIX_TIMESTAMP(LAST_DAY(ADD_MONTHS(FROM_UNIXTIME(s.json_params:cmStartTimestamp / 1000), 1)) + INTERVAL 86399 SECOND) - (s.json_params:cmStartTimestamp / 1000) --86399 seconds until right before midnight
            END AS timestamp_diff_seconds
        FROM mobile_logs_verification_ca s
        LEFT OUTER JOIN mobile_logs_completed_ca e
            ON e.org_id = s.org_id
            AND e.json_params:deviceType = s.json_params:type
            AND e.json_params:deviceId = s.json_params:deviceId
            AND e.json_params:deviceSerial = s.json_params:deviceSerial
        JOIN latest_org_ca o
            ON s.org_id = o.org_id
        JOIN installed_devices_ca i
            ON o.sam_number = i.sam_number
            AND o.account_id = i.account_id
            AND i.device_id = s.json_params:deviceId
        WHERE i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    vindb_check_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            v.device_id,
            CASE
                WHEN v.make IS NOT NULL AND v.model IS NOT NULL THEN 1
                ELSE 0
            END AS vindb_contains_make_model
        FROM
            datamodel_core_bronze.raw_vindb_shards_device_vin_metadata v
        JOIN latest_org_us o
            ON v.org_id = o.org_id
        JOIN installed_devices_us i
            ON v.device_id = i.device_id
            AND o.sam_number = i.sam_number
            AND o.account_id = i.account_id
        WHERE v.date = (SELECT MAX(DATE) FROM datamodel_core_bronze.raw_vindb_shards_device_vin_metadata WHERE date <= '{END_OF_PERIOD}')
    ),
    vindb_check_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            v.device_id,
            CASE
                WHEN v.make IS NOT NULL AND v.model IS NOT NULL THEN 1
                ELSE 0
            END AS vindb_contains_make_model
        FROM
            delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_bronze.db/raw_vindb_shards_device_vin_metadata` v
        JOIN latest_org_eu o
            ON v.org_id = o.org_id
        JOIN installed_devices_eu i
            ON v.device_id = i.device_id
            AND o.sam_number = i.sam_number
            AND o.account_id = i.account_id
        WHERE v.date = (SELECT MAX(DATE) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_bronze.db/raw_vindb_shards_device_vin_metadata` WHERE date <= '{END_OF_PERIOD}')
    ),
    vindb_check_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            v.device_id,
            CASE
                WHEN v.make IS NOT NULL AND v.model IS NOT NULL THEN 1
                ELSE 0
            END AS vindb_contains_make_model
        FROM
            data_tools_delta_share_ca.datamodel_core_bronze.raw_vindb_shards_device_vin_metadata v
        JOIN latest_org_ca o
            ON v.org_id = o.org_id
        JOIN installed_devices_ca i
            ON v.device_id = i.device_id
            AND o.sam_number = i.sam_number
            AND o.account_id = i.account_id
        WHERE v.date = (SELECT MAX(DATE) FROM data_tools_delta_share_ca.datamodel_core_bronze.raw_vindb_shards_device_vin_metadata WHERE date <= '{END_OF_PERIOD}')
    ),
    install_correctness_vg_us AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(vc.vindb_contains_make_model) AS vindb_contains_make_model
        FROM installed_devices_us i
        LEFT OUTER JOIN vindb_check_us vc
            ON i.device_id = vc.device_id
            AND i.sam_number = vc.sam_number
            AND i.account_id = vc.account_id
        WHERE
            i.device_type LIKE 'VG%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    install_correctness_vg_eu AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(vc.vindb_contains_make_model) AS vindb_contains_make_model
        FROM installed_devices_eu i
        LEFT OUTER JOIN vindb_check_eu vc
            ON i.device_id = vc.device_id
            AND i.sam_number = vc.sam_number
            AND i.account_id = vc.account_id
        WHERE
            i.device_type LIKE 'VG%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    install_correctness_vg_ca AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(vc.vindb_contains_make_model) AS vindb_contains_make_model
        FROM installed_devices_ca i
        LEFT OUTER JOIN vindb_check_ca vc
            ON i.device_id = vc.device_id
            AND i.sam_number = vc.sam_number
            AND i.account_id = vc.account_id
        WHERE
            i.device_type LIKE 'VG%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    successful_install_cm_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            GET_JSON_OBJECT(l.json_params, '$.deviceId') AS device_id,
            GET_JSON_OBJECT(l.json_params, '$.hasCM3x') AS CM3x_present,
            GET_JSON_OBJECT(l.json_params, '$.hasCm') AS CM_present,
            GET_JSON_OBJECT(l.json_params, '$.productId') AS product_id,
            d.device_id AS cm_device_id,
            1 AS installed_with_app
        FROM datastreams_history.mobile_logs l
        JOIN latest_org_us o
            ON l.org_id = o.org_id
        JOIN datamodel_core.dim_devices d
            ON l.org_id = d.org_id
            AND l.date = d.date
            AND GET_JSON_OBJECT(l.json_params, '$.deviceId') = d.associated_devices['vg_device_id']
        JOIN installed_devices_us id
            ON d.device_id = id.device_id
            AND o.sam_number = id.sam_number
            AND o.account_id = id.account_id
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND (GET_JSON_OBJECT(l.json_params, '$.hasCM3x') = 'true' OR GET_JSON_OBJECT(l.json_params, '$.hasCm') = 'true')
            AND id.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND id.device_type LIKE 'CM%'
    ),
    successful_install_cm_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            GET_JSON_OBJECT(l.json_params, '$.deviceId') AS device_id,
            GET_JSON_OBJECT(l.json_params, '$.hasCM3x') AS CM3x_present,
            GET_JSON_OBJECT(l.json_params, '$.hasCm') AS CM_present,
            GET_JSON_OBJECT(l.json_params, '$.productId') AS product_id,
            d.device_id AS cm_device_id,
            1 AS installed_with_app
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs` l
        JOIN latest_org_eu o
            ON l.org_id = o.org_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
            ON l.org_id = d.org_id
            AND l.date = d.date
            AND GET_JSON_OBJECT(l.json_params, '$.deviceId') = d.associated_devices['vg_device_id']
        JOIN installed_devices_eu id
            ON d.device_id = id.device_id
            AND o.sam_number = id.sam_number
            AND o.account_id = id.account_id
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND (GET_JSON_OBJECT(l.json_params, '$.hasCM3x') = 'true' OR GET_JSON_OBJECT(l.json_params, '$.hasCm') = 'true')
            AND id.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND id.device_type LIKE 'CM%'
    ),
    successful_install_cm_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            GET_JSON_OBJECT(l.json_params, '$.deviceId') AS device_id,
            GET_JSON_OBJECT(l.json_params, '$.hasCM3x') AS CM3x_present,
            GET_JSON_OBJECT(l.json_params, '$.hasCm') AS CM_present,
            GET_JSON_OBJECT(l.json_params, '$.productId') AS product_id,
            d.device_id AS cm_device_id,
            1 AS installed_with_app
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs` l
        JOIN latest_org_ca o
            ON l.org_id = o.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON l.org_id = d.org_id
            AND l.date = d.date
            AND GET_JSON_OBJECT(l.json_params, '$.deviceId') = d.associated_devices['vg_device_id']
        JOIN installed_devices_ca id
            ON d.device_id = id.device_id
            AND o.sam_number = id.sam_number
            AND o.account_id = id.account_id
        WHERE
            l.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND l.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
            AND (GET_JSON_OBJECT(l.json_params, '$.hasCM3x') = 'true' OR GET_JSON_OBJECT(l.json_params, '$.hasCm') = 'true')
            AND id.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND id.device_type LIKE 'CM%'
    ),
    active_device_cm_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            a.device_id,
            CASE WHEN a.latest_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' THEN 1 ELSE 0 END AS active
        FROM datamodel_core.lifetime_device_activity a
        JOIN latest_org_us o
            ON a.org_id = o.org_id
        JOIN datamodel_core.dim_devices d
            ON a.org_id = d.org_id
            AND a.device_id = d.device_id
            AND a.date = d.date
        WHERE
            a.date = '{END_OF_PERIOD}'
            AND a.device_type LIKE 'CM%'
    ),
    active_device_cm_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            a.device_id,
            CASE WHEN a.latest_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' THEN 1 ELSE 0 END AS active
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` a
        JOIN latest_org_eu o
            ON a.org_id = o.org_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
            ON a.org_id = d.org_id
            AND a.device_id = d.device_id
            AND a.date = d.date
        WHERE
            a.date = '{END_OF_PERIOD}'
            AND a.device_type LIKE 'CM%'
    ),
    active_device_cm_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            a.device_id,
            CASE WHEN a.latest_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}' THEN 1 ELSE 0 END AS active
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_activity a
        JOIN latest_org_ca o
            ON a.org_id = o.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON a.org_id = d.org_id
            AND a.device_id = d.device_id
            AND a.date = d.date
        WHERE
            a.date = '{END_OF_PERIOD}'
            AND a.device_type LIKE 'CM%'
    ),
    install_correctness_cm_us AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(ad.active, 0) AS active
        FROM installed_devices_us i
        JOIN latest_org_us o
            ON i.sam_number = o.sam_number
            AND i.account_id = o.account_id
        JOIN datamodel_core.dim_devices d
            ON i.device_id = d.device_id
            AND o.org_id = d.org_id
        LEFT OUTER JOIN active_device_cm_us ad
            ON i.device_id = ad.device_id
            AND i.sam_number = ad.sam_number
            AND i.account_id = ad.account_id
        WHERE
            d.date = '{END_OF_PERIOD}'
            AND i.device_type LIKE 'CM%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    install_correctness_cm_eu AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(ad.active, 0) AS active
        FROM installed_devices_eu i
        JOIN latest_org_eu o
            ON i.sam_number = o.sam_number
            AND i.account_id = o.account_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
            ON i.device_id = d.device_id
            AND o.org_id = d.org_id
        LEFT OUTER JOIN active_device_cm_eu ad
            ON i.device_id = ad.device_id
            AND i.sam_number = ad.sam_number
            AND i.account_id = ad.account_id
        WHERE
            d.date = '{END_OF_PERIOD}'
            AND i.device_type LIKE 'CM%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    install_correctness_cm_ca AS (
        SELECT
            i.sam_number,
            i.account_id,
            i.device_id,
            COALESCE(ad.active, 0) AS active
        FROM installed_devices_ca i
        JOIN latest_org_ca o
            ON i.sam_number = o.sam_number
            AND i.account_id = o.account_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON i.device_id = d.device_id
            AND o.org_id = d.org_id
        LEFT OUTER JOIN active_device_cm_ca ad
            ON i.device_id = ad.device_id
            AND i.sam_number = ad.sam_number
            AND i.account_id = ad.account_id
        WHERE
            d.date = '{END_OF_PERIOD}'
            AND i.device_type LIKE 'CM%'
            AND i.first_date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
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
            CAST(DATE(c._timestamp) AS STRING) AS date,
            o.sam_number,
            o.account_id,
            c.uuid,
            c.total_rows_errored,
            CASE
                WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
                THEN 1
                ELSE total_rows_errored * 1.0 / total_rows_submitted
            END AS row_error_rate
        FROM csvuploadsdb_shards.csv_uploads c
        JOIN latest_org_us o
            ON c.org_id = o.org_id
        LEFT OUTER JOIN internal_users e
            ON c.created_by = e.user_id
        WHERE
            DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
            AND total_rows_submitted > 0
            AND e.user_id IS NULL
    ),
    csv_breakdown_eu AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) AS date,
            o.sam_number,
            o.account_id,
            c.uuid,
            c.total_rows_errored,
            CASE
                WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
                THEN 1
                ELSE total_rows_errored * 1.0 / total_rows_submitted
            END AS row_error_rate
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-csvuploadsdb/csvuploadsdb/csv_uploads_v0` c
        JOIN latest_org_eu o
            ON c.org_id = o.org_id
        LEFT OUTER JOIN internal_users e
            ON c.created_by = e.user_id
        WHERE
            DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
            AND total_rows_submitted > 0
            AND e.user_id IS NULL

        UNION ALL

        SELECT
            CAST(DATE(c._timestamp) AS STRING) AS date,
            o.sam_number,
            o.account_id,
            c.uuid,
            c.total_rows_errored,
            CASE
                WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
                THEN 1
                ELSE total_rows_errored * 1.0 / total_rows_submitted
            END AS row_error_rate
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-csvuploads-shard-1db/csvuploadsdb/csv_uploads_v0` c
        JOIN latest_org_eu o
            ON c.org_id = o.org_id
        LEFT OUTER JOIN internal_users e
            ON c.created_by = e.user_id
        WHERE
            DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
            AND total_rows_submitted > 0
            AND e.user_id IS NULL
    ),
    csv_breakdown_ca AS (
        SELECT
            CAST(DATE(c._timestamp) AS STRING) AS date,
            o.sam_number,
            o.account_id,
            c.uuid,
            c.total_rows_errored,
            CASE
                WHEN total_rows_errored * 1.0 / total_rows_submitted > 1
                THEN 1
                ELSE total_rows_errored * 1.0 / total_rows_submitted
            END AS row_error_rate
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-csvuploadsdb/csvuploadsdb/csv_uploads_v0` c
        JOIN latest_org_ca o
            ON c.org_id = o.org_id
        LEFT OUTER JOIN internal_users e
            ON c.created_by = e.user_id
        WHERE
            DATE(c._timestamp) between '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
            AND total_rows_submitted > 0
            AND e.user_id IS NULL
    ),
    {METRIC_CTES},
    final AS (
        SELECT DISTINCT
        {COALESCE_STATEMENTS},
        {COALESCE_METRIC_STATEMENTS}
        FROM user_roles_aggregated user_roles_aggregated
        {FULL_OUTER_JOIN_STATEMENTS}
    )
    SELECT DISTINCT
        date_month,
        period_start,
        period_end,
        sam_number,
        account_size_segment,
        account_arr_segment,
        account_billing_country,
        account_industry,
        industry_vertical,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer,
        account_csm_email,
        account_name,
        account_id,
        CAST(CASE WHEN SUM(full_admin_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS full_admin_account,
        MAX(full_admin_users) AS full_admin_users,
        CAST(CASE WHEN SUM(custom_role_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS custom_role_account,
        MAX(custom_role_users) AS custom_role_users,
        CAST(CASE WHEN SUM(account_with_two_plus_users) > 0 THEN 1 ELSE 0 END AS BIGINT) AS account_with_two_plus_users,
        MAX(total_users) AS total_users,
        CAST(CASE WHEN SUM(device_tags_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS device_tags_account,
        MAX(device_tags) AS device_tags,
        CAST(CASE WHEN SUM(device_attributes_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS device_attributes_account,
        MAX(device_attributes) AS device_attributes,
        CAST(CASE WHEN SUM(device_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS device_account,
        MAX(total_devices) AS total_devices,
        CAST(CASE WHEN SUM(driver_tags_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS driver_tags_account,
        MAX(driver_tags) AS driver_tags,
        CAST(CASE WHEN SUM(driver_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS driver_account,
        MAX(total_drivers) AS total_drivers,
        CAST(CASE WHEN SUM(driver_attributes_account) > 0 THEN 1 ELSE 0 END AS BIGINT) AS driver_attributes_account,
        MAX(driver_attributes) AS driver_attributes,
        MAX(installed_vg) AS installed_vg,
        MAX(installed_at) AS installed_at,
        MAX(installed_ag) AS installed_ag,
        MAX(installed_cm) AS installed_cm,
        MAX(at_with_photo) AS at_with_photo,
        MAX(ag_with_photo) AS ag_with_photo,
        MAX(vg_with_photo) AS vg_with_photo,
        MAX(complete_flow_ag) AS complete_flow_ag,
        MAX(complete_flow_vg) AS complete_flow_vg,
        MAX(complete_flow_cm) AS complete_flow_cm,
        MAX(complete_flow_at) AS complete_flow_at,
        CAST(MAX(total_time_to_install_vg) AS BIGINT) AS total_time_to_install_vg,
        CAST(MAX(total_time_to_install_ag) AS BIGINT) AS total_time_to_install_ag,
        CAST(MAX(total_time_to_install_at) AS BIGINT) AS total_time_to_install_at,
        CAST(MAX(total_time_to_install_cm) AS BIGINT) AS total_time_to_install_cm,
        MAX(correctly_installed_vg) AS correctly_installed_vg,
        MAX(correctly_installed_cm) AS correctly_installed_cm,
        MAX(csv_upload_failures) AS csv_upload_failures,
        MAX(csv_uploads) AS csv_uploads
    FROM final
    GROUP BY
        date_month,
        period_start,
        period_end,
        sam_number,
        account_size_segment,
        account_arr_segment,
        account_billing_country,
        account_industry,
        industry_vertical,
        is_paid_safety_customer,
        is_paid_telematics_customer,
        is_paid_stce_customer,
        account_csm_email,
        account_name,
        account_id
    """


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
}
