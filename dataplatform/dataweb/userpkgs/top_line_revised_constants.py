from dataweb.userpkgs.constants import (
    ColumnDescription,
    DimensionConfig,
    DimensionDataType,
)

TOP_LINE_DIMENSION_TEMPLATES = {
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

TOP_LINE_STANDARD_DIMENSIONS = {
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
    "driver_assignment_source_dimensions": [
        ("driver_assignment_source", "CASE WHEN driver_assignment_source IS NULL THEN 'Unknown' ELSE driver_assignment_source END"),
        ("sign_in_reminder_enabled", "CASE WHEN sign_in_reminder_enabled = 1 THEN TRUE ELSE FALSE END"),
    ]
}

TOP_LINE_REGION_MAPPINGS = {
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

TOP_LINE_JOIN_MAPPINGS = {
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

TOP_LINE_METRIC_CONFIGS_TEMPLATE = {
    "trips": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "trips_deduped_us",
        "eu_source": "trips_deduped_eu",
        "ca_source": "trips_deduped_ca",
        "table_alias": "t",
        "join_alias": "t",
        "metrics": ["trips"]
    },
    "miles": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "miles_deduped_us",
        "eu_source": "miles_deduped_eu",
        "ca_source": "miles_deduped_ca",
        "table_alias": "t",
        "join_alias": "t",
        "metrics": ["miles"]
    },
    "active_drivers": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "active_drivers_us",
        "eu_source": "active_drivers_eu",
        "ca_source": "active_drivers_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["active_drivers"]
    },
    "active_users_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "active_users_us",
        "eu_source": "active_users_eu",
        "ca_source": "active_users_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["active_cloud_dashboard_users"],
    },
    "active_driver_app_users": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "driver_app_us",
        "eu_source": "driver_app_eu",
        "ca_source": "driver_app_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["active_driver_app_users"],
    },
    "marketplace_integrations": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "marketplace_integrations_us",
        "eu_source": "marketplace_integrations_eu",
        "ca_source": "marketplace_integrations_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["marketplace_integrations"],
    },
    "api_requests": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "api_requests_us",
        "eu_source": "api_requests_eu",
        "ca_source": "api_requests_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["api_requests"],
    },
    "total_device_licenses": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "licenses_deduped_us",
        "eu_source": "licenses_deduped_eu",
        "ca_source": "licenses_deduped_ca",
        "table_alias": "l",
        "join_alias": "l",
        "metrics": ["device_licenses_ag", "device_licenses_cm", "device_licenses_vg", "device_licenses_at", "device_licenses_sg"],
    },
    "assigned_licenses": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "license_assignment_total_us",
        "eu_source": "license_assignment_total_eu",
        "ca_source": "license_assignment_total_ca",
        "table_alias": "l",
        "join_alias": "l",
        "metrics": ["total_licenses_assigned", "total_licenses"],
    },
    "assigned_licenses_software": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "license_assignment_software_us",
        "eu_source": "license_assignment_software_eu",
        "ca_source": "license_assignment_software_ca",
        "table_alias": "l",
        "join_alias": "l",
        "metrics": ["total_licenses_assigned_software", "total_licenses_software"],
    },
    "assigned_licenses_hardware": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "license_assignment_hardware_us",
        "eu_source": "license_assignment_hardware_eu",
        "ca_source": "license_assignment_hardware_ca",
        "table_alias": "l",
        "join_alias": "l",
        "metrics": ["total_licenses_assigned_hardware", "total_licenses_hardware"],
    },
    "active_devices": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "active_devices_us",
        "eu_source": "active_devices_eu",
        "ca_source": "active_devices_ca",
        "table_alias": "dh",
        "join_alias": "dh",
        "metrics": ["active_devices_ag", "active_devices_cm", "active_devices_vg", "active_devices_at", "active_devices_sg"],
    },
}

DRIVER_ASSIGNMENT_METRIC_CONFIGS_TEMPLATE = {
    "driver_assignment_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "trips_deduped_us",
        "eu_source": "trips_deduped_eu",
        "ca_source": "trips_deduped_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["inward_assigned_trip_duration", "inward_total_trip_duration", "outward_assigned_trip_duration", "outward_total_trip_duration", "no_cm_assigned_trip_duration", "no_cm_total_trip_duration"]
    },
}

TOP_LINE_METRICS = {
    "trips": "COUNT(DISTINCT t.trip_id)",
    "miles": "SUM(COALESCE(t.distance_miles, 0))",
    "active_drivers": "COUNT(DISTINCT u.driver_id)",
    "active_cloud_dashboard_users": "COUNT(DISTINCT u.user_id)",
    "active_driver_app_users": "COUNT(DISTINCT u.driver_id)",
    "marketplace_integrations": "SUM(num_integrations)",
    "api_requests": "SUM(num_requests)",
    "device_licenses_ag": "SUM(CASE WHEN l.device_type IN ('AG Powered', 'AG Unpowered') AND l.is_core_license THEN l.quantity END)",
    "device_licenses_vg": "SUM(CASE WHEN l.device_type = 'VG' AND l.is_core_license THEN l.quantity END)",
    "device_licenses_cm": "SUM(CASE WHEN l.device_type IN ('CM-M', 'CM-S', 'CM-D') AND l.is_core_license THEN l.quantity END)",
    "device_licenses_at": "SUM(CASE WHEN l.device_type = 'AT' AND l.is_core_license THEN l.quantity END)",
    "device_licenses_sg": "SUM(CASE WHEN l.device_type = 'SG' AND l.is_core_license THEN l.quantity END)",
    "active_devices_ag": "COUNT(DISTINCT CASE WHEN dh.device_type LIKE 'AG%' AND dh.total_active > 0 THEN dh.device_id END)",
    "active_devices_vg": "COUNT(DISTINCT CASE WHEN dh.device_type LIKE 'VG%' AND dh.total_active > 0 THEN dh.device_id END)",
    "active_devices_cm": "COUNT(DISTINCT CASE WHEN dh.device_type LIKE 'CM%' AND dh.total_active > 0 THEN dh.device_id END)",
    "active_devices_at": "COUNT(DISTINCT CASE WHEN dh.device_type LIKE 'AT%' AND dh.total_active > 0 THEN dh.device_id END)",
    "active_devices_sg": "COUNT(DISTINCT CASE WHEN dh.device_type LIKE 'SG%' AND dh.total_active > 0 THEN dh.device_id END)",
    "total_licenses": "SUM(l.quantity)",
    "total_licenses_software": "SUM(l.quantity)",
    "total_licenses_hardware": "SUM(l.quantity)",
    "total_licenses_assigned": "SUM(l.devices_assigned + l.drivers_assigned + l.users_assigned)",
    "total_licenses_assigned_hardware": "SUM(l.devices_assigned + l.drivers_assigned + l.users_assigned)",
    "total_licenses_assigned_software": "SUM(l.devices_assigned + l.drivers_assigned + l.users_assigned)"
}

DRIVER_ASSIGNMENT_METRICS = {
    "inward_assigned_trip_duration": "SUM(CASE WHEN d.camera_product_id IN (31, 43, 155) AND d.driver_id != 0 AND d.driver_id IS NOT NULL THEN d.duration_mins END)",
    "inward_total_trip_duration": "SUM(CASE WHEN d.camera_product_id IN (31, 43, 155) THEN d.duration_mins END)",
    "outward_assigned_trip_duration": "SUM(CASE WHEN d.camera_product_id IN (25, 30, 44, 167) AND d.driver_id != 0 AND d.driver_id IS NOT NULL THEN d.duration_mins END)",
    "outward_total_trip_duration": "SUM(CASE WHEN d.camera_product_id IN (25, 30, 44, 167) THEN d.duration_mins END)",
    "no_cm_assigned_trip_duration": "SUM(CASE WHEN d.camera_product_id IS NULL AND d.driver_id != 0 AND d.driver_id IS NOT NULL THEN d.duration_mins END)",
    "no_cm_total_trip_duration": "SUM(CASE WHEN d.camera_product_id IS NULL THEN d.duration_mins END)"
}

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
    trips_base_us AS (
        SELECT
            o.org_id,
            o.sam_number,
            o.account_id,
            FORMAT_STRING('%s:%s', t.device_id, t.start_time_ms) AS trip_id,
            COALESCE(t.distance_miles, 0) AS distance_miles,
            t.driver_id
        FROM datamodel_telematics.fct_trips t
        JOIN latest_org_us o
            ON o.org_id = t.org_id
        JOIN datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = t.device_id
            AND d.date = t.date
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.trip_type = 'location_based'
    ),
    trips_base_eu AS (
        SELECT
            o.org_id,
            o.sam_number,
            o.account_id,
            FORMAT_STRING('%s:%s', t.device_id, t.start_time_ms) AS trip_id,
            COALESCE(t.distance_miles, 0) AS distance_miles,
            t.driver_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_trips` t
        JOIN latest_org_eu o
            ON o.org_id = t.org_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
            ON o.org_id = d.org_id
            AND d.device_id = t.device_id
            AND d.date = t.date
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.trip_type = 'location_based'
    ),
    trips_base_ca AS (
        SELECT
            o.org_id,
            o.sam_number,
            o.account_id,
            FORMAT_STRING('%s:%s', t.device_id, t.start_time_ms) AS trip_id,
            COALESCE(t.distance_miles, 0) AS distance_miles,
            t.driver_id
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_trips t
        JOIN latest_org_ca o
            ON o.org_id = t.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = t.device_id
            AND d.date = t.date
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.trip_type = 'location_based'
    ),
    miles_deduped_us AS (
        SELECT
            sam_number,
            account_id,
            SUM(distance_miles) AS distance_miles
        FROM trips_base_us
        GROUP BY 1, 2
    ),
    miles_deduped_eu AS (
        SELECT
            sam_number,
            account_id,
            SUM(distance_miles) AS distance_miles
        FROM trips_base_eu
        GROUP BY 1, 2
    ),
    miles_deduped_ca AS (
        SELECT
            sam_number,
            account_id,
            SUM(distance_miles) AS distance_miles
        FROM trips_base_ca
        GROUP BY 1, 2
    ),
    trips_deduped_us AS (
        SELECT
            sam_number,
            account_id,
            trip_id
        FROM trips_base_us
    ),
    trips_deduped_eu AS (
        SELECT
            sam_number,
            account_id,
            trip_id
        FROM trips_base_eu
    ),
    trips_deduped_ca AS (
        SELECT
            sam_number,
            account_id,
            trip_id
        FROM trips_base_ca
    ),
    driver_app_us AS (
        SELECT
            CONCAT(d.org_id, '_', d.driver_id) AS driver_id,
            o.sam_number,
            o.account_id
        FROM datamodel_platform.dim_drivers d
        JOIN latest_org_us o
            ON o.org_id = d.org_id
        WHERE d.date = '{END_OF_PERIOD}'
        AND d.driver_id != 0
        AND last_mobile_login_date >= '{START_OF_PERIOD}' -- Only consider drivers who logged in during the period
    ),
    driver_app_eu AS (
        SELECT
            CONCAT(d.org_id, '_', d.driver_id) AS driver_id,
            o.sam_number,
            o.account_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` d
        JOIN latest_org_eu o
            ON o.org_id = d.org_id
        WHERE d.date = '{END_OF_PERIOD}'
        AND d.driver_id != 0
        AND last_mobile_login_date >= '{START_OF_PERIOD}' -- Only consider drivers who logged in during the period
    ),
    driver_app_ca AS (
        SELECT
            CONCAT(d.org_id, '_', d.driver_id) AS driver_id,
            o.sam_number,
            o.account_id
        FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers d
        JOIN latest_org_ca o
            ON o.org_id = d.org_id
        WHERE d.date = '{END_OF_PERIOD}'
        AND d.driver_id != 0
        AND last_mobile_login_date >= '{START_OF_PERIOD}' -- Only consider drivers who logged in during the period
    ),
    active_drivers_us AS (
        SELECT
            CONCAT(org_id, '_', driver_id) AS driver_id,
            sam_number,
            account_id
        FROM trips_base_us
        WHERE driver_id != 0
    ),
    active_drivers_eu AS (
        SELECT
            CONCAT(org_id, '_', driver_id) AS driver_id,
            sam_number,
            account_id
        FROM trips_base_eu
        WHERE driver_id != 0
    ),
    active_drivers_ca AS (
        SELECT
            CONCAT(u.org_id, '_', u.driver_id) AS driver_id,
            o.sam_number,
            o.account_id
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_trips u
        JOIN latest_org_ca o
            ON o.org_id = u.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = u.device_id
            AND d.date = u.date
        WHERE
            u.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND u.driver_id != 0
            AND u.trip_type = 'location_based'
    ),
    active_users_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            CONCAT(scr.org_id, '_', scr.user_id) AS user_id
        FROM datamodel_platform_silver.stg_cloud_routes scr
        JOIN datamodel_core.dim_organizations o
            ON scr.org_id = o.org_id
        JOIN datamodel_platform.dim_users_organizations duo
            ON scr.org_id = duo.org_id
            AND scr.user_id = duo.user_id
        JOIN datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        JOIN latest_org_us lo
            ON lo.org_id = scr.org_id
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
    active_users_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            CONCAT(scr.org_id, '_', scr.useremail) AS user_id -- Using email for EU as user_id is null
        FROM datamodel_platform_silver.stg_cloud_routes scr
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
            ON scr.org_id = o.org_id
        JOIN data_tools_delta_share.datamodel_platform.dim_users_organizations duo
            ON scr.org_id = duo.org_id
            AND scr.useremail = duo.email
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        JOIN latest_org_eu lo
            ON lo.org_id = scr.org_id
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
    active_users_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            CONCAT(scr.org_id, '_', scr.useremail) AS user_id -- Using email for EU as user_id is null
        FROM datamodel_platform_silver.stg_cloud_routes scr
        JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o
            ON scr.org_id = o.org_id
        JOIN data_tools_delta_share_ca.datamodel_platform.dim_users_organizations duo
            ON scr.org_id = duo.org_id
            AND scr.useremail = duo.email
        JOIN data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
            ON duo.org_id = rcuo.organization_id
            AND duo.user_id = rcuo.user_id
        JOIN latest_org_ca lo
            ON lo.org_id = scr.org_id
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
    marketplace_integrations_us AS (
        -- Integrations are defined as OAuth or API tokens
        SELECT
            o.sam_number,
            o.account_id,
            COUNT(DISTINCT dai.oauth_token_app_uuid) AS num_integrations
        FROM product_analytics.fct_api_usage dai
        JOIN latest_org_us o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND oauth_token_app_uuid != ""
            AND oauth_token_app_uuid IS NOT NULL
        GROUP BY
            o.sam_number,
            o.account_id

        UNION

        SELECT
            o.sam_number,
            o.account_id,
            COUNT(DISTINCT dai.api_token_id) AS num_integrations
        FROM product_analytics.fct_api_usage dai
        JOIN latest_org_us o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                oauth_token_app_uuid = ""
                OR oauth_token_app_uuid IS NULL
            )
        GROUP BY
            o.sam_number,
            o.account_id
    ),
    marketplace_integrations_eu AS (
        -- Integrations are defined as OAuth or API tokens
        SELECT
            o.sam_number,
            o.account_id,
            COUNT(DISTINCT oauth_token_app_uuid) AS num_integrations
        FROM data_tools_delta_share.product_analytics.fct_api_usage dai
        JOIN latest_org_eu o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND oauth_token_app_uuid != ""
            AND oauth_token_app_uuid IS NOT NULL
        GROUP BY
            o.sam_number,
            o.account_id

        UNION

        SELECT
            o.sam_number,
            o.account_id,
            COUNT(DISTINCT api_token_id) AS num_integrations
        FROM data_tools_delta_share.product_analytics.fct_api_usage dai
        JOIN latest_org_eu o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                oauth_token_app_uuid = ""
                OR oauth_token_app_uuid IS NULL
            )
        GROUP BY
            o.sam_number,
            o.account_id
    ),
    marketplace_integrations_ca AS (
        -- Integrations are defined as OAuth or API tokens
        SELECT
            o.sam_number,
            o.account_id,
            COUNT(DISTINCT oauth_token_app_uuid) AS num_integrations
        FROM data_tools_delta_share_ca.product_analytics.fct_api_usage dai
        JOIN latest_org_ca o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND oauth_token_app_uuid != ""
            AND oauth_token_app_uuid IS NOT NULL
        GROUP BY
            o.sam_number,
            o.account_id

        UNION

        SELECT
            o.sam_number,
            o.account_id,
            COUNT(DISTINCT api_token_id) AS num_integrations
        FROM data_tools_delta_share_ca.product_analytics.fct_api_usage dai
        JOIN latest_org_ca o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                oauth_token_app_uuid = ""
                OR oauth_token_app_uuid IS NULL
            )
        GROUP BY
            o.sam_number,
            o.account_id
    ),
    api_requests_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            num_requests
        FROM product_analytics.fct_api_usage dai
        JOIN latest_org_us o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND status_code = 200
    ),
    api_requests_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            num_requests
        FROM data_tools_delta_share.product_analytics.fct_api_usage dai
        JOIN latest_org_eu o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND status_code = 200
    ),
    api_requests_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            num_requests
        FROM data_tools_delta_share_ca.product_analytics.fct_api_usage dai
        JOIN latest_org_ca o
            ON o.org_id = dai.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND status_code = 200
    ),
    licenses_deduped_us AS (
        -- licenses should be aggregated at SAM level
        SELECT
            o.sam_number,
            o.account_id,
            a.product_sku AS sku,
            xref.product_family,
            xref.sub_product_line,
            xref.device_type,
            xref.hardware_skus,
            xref.is_core_license,
            SUM(a.product_net_qty) AS quantity -- sum all relevant orders
        FROM edw.silver.fct_license_orders a
        JOIN (SELECT DISTINCT COALESCE(salesforce_sam_number, sam_number) AS sam_number, account_id FROM latest_org_us) o
            ON o.sam_number = a.sam_number
        JOIN edw.silver.license_hw_sku_xref xref
            ON a.product_sku = xref.license_sku
        WHERE DATE(a.contract_start_date) <= '{END_OF_PERIOD}'
            AND (DATE(a.contract_end_date) >= '{START_OF_PERIOD}' OR a.contract_end_date IS NULL)
            AND a.product_net_qty > 0 -- at least one license
            AND a.is_trial = FALSE -- non-trial
        GROUP BY ALL
    ),
    licenses_deduped_eu AS (
        -- licenses should be aggregated at SAM level
        SELECT
            o.sam_number,
            o.account_id,
            a.product_sku AS sku,
            xref.product_family,
            xref.sub_product_line,
            xref.device_type,
            xref.hardware_skus,
            xref.is_core_license,
            SUM(a.product_net_qty) AS quantity -- sum all relevant orders
        FROM edw.silver.fct_license_orders a
        JOIN (SELECT DISTINCT COALESCE(salesforce_sam_number, sam_number) AS sam_number, account_id FROM latest_org_eu) o
            ON o.sam_number = a.sam_number
        JOIN edw.silver.license_hw_sku_xref xref
            ON a.product_sku = xref.license_sku
        WHERE DATE(a.contract_start_date) <= '{END_OF_PERIOD}'
            AND (DATE(a.contract_end_date) >= '{START_OF_PERIOD}' OR a.contract_end_date IS NULL)
            AND a.product_net_qty > 0 -- at least one license
            AND a.is_trial = FALSE -- non-trial
        GROUP BY ALL
    ),
    licenses_deduped_ca AS (
        -- licenses should be aggregated at SAM level
        SELECT
            o.sam_number,
            o.account_id,
            a.product_sku AS sku,
            xref.product_family,
            xref.sub_product_line,
            xref.device_type,
            xref.hardware_skus,
            xref.is_core_license,
            SUM(a.product_net_qty) AS quantity -- sum all relevant orders
        FROM edw.silver.fct_license_orders a
        JOIN (SELECT DISTINCT COALESCE(salesforce_sam_number, sam_number) AS sam_number, account_id FROM latest_org_ca) o
            ON o.sam_number = a.sam_number
        JOIN edw.silver.license_hw_sku_xref xref
            ON a.product_sku = xref.license_sku
        WHERE DATE(a.contract_start_date) <= '{END_OF_PERIOD}'
            AND (DATE(a.contract_end_date) >= '{START_OF_PERIOD}' OR a.contract_end_date IS NULL)
            AND a.product_net_qty > 0 -- at least one license
            AND a.is_trial = FALSE -- non-trial
        GROUP BY ALL
    ),
    assignment_summary_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            dla.sku,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'device' THEN dla.uuid ELSE NULL END) AS devices_assigned,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'user' THEN dla.uuid ELSE NULL END) AS users_assigned,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'driver' THEN dla.uuid ELSE NULL END) AS drivers_assigned
        FROM datamodel_platform.dim_license_assignment dla
        JOIN (SELECT DISTINCT COALESCE(salesforce_sam_number, sam_number) AS sam_number, sam_number AS sam_number_untouched, account_id FROM latest_org_us) o
            ON o.sam_number_untouched = dla.sam_number
        WHERE
            dla.date = '{END_OF_PERIOD}'
            AND COALESCE(DATE(dla.end_time), '{START_OF_PERIOD}') >= '{START_OF_PERIOD}'
        GROUP BY
            o.sam_number,
            o.account_id,
            dla.sku
    ),
    assignment_summary_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            dla.sku,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'device' THEN dla.uuid ELSE NULL END) AS devices_assigned,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'user' THEN dla.uuid ELSE NULL END) AS users_assigned,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'driver' THEN dla.uuid ELSE NULL END) AS drivers_assigned
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_license_assignment` dla
        JOIN (SELECT DISTINCT COALESCE(salesforce_sam_number, sam_number) AS sam_number, sam_number AS sam_number_untouched, account_id FROM latest_org_eu) o
            ON o.sam_number_untouched = dla.sam_number
        WHERE
            dla.date = '{END_OF_PERIOD}'
            AND COALESCE(DATE(dla.end_time), '{START_OF_PERIOD}') >= '{START_OF_PERIOD}'
        GROUP BY
            o.sam_number,
            o.account_id,
            dla.sku
    ),
    assignment_summary_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            dla.sku,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'device' THEN dla.uuid ELSE NULL END) AS devices_assigned,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'user' THEN dla.uuid ELSE NULL END) AS users_assigned,
            COUNT(DISTINCT CASE WHEN dla.entity_type_name = 'driver' THEN dla.uuid ELSE NULL END) AS drivers_assigned
        FROM data_tools_delta_share_ca.datamodel_platform.dim_license_assignment dla
        JOIN (SELECT DISTINCT COALESCE(salesforce_sam_number, sam_number) AS sam_number, sam_number AS sam_number_untouched, account_id FROM latest_org_ca) o
            ON o.sam_number_untouched = dla.sam_number
        WHERE
            dla.date = '{END_OF_PERIOD}'
            AND COALESCE(DATE(dla.end_time), '{START_OF_PERIOD}') >= '{START_OF_PERIOD}'
        GROUP BY
            o.sam_number,
            o.account_id,
            dla.sku
    ),
    software_skus_us AS (
        SELECT DISTINCT
            l.sku
        FROM licenses_deduped_us l
        LEFT JOIN assignment_summary_us a
            ON l.sam_number = a.sam_number
            AND l.sku = a.sku
        WHERE
            COALESCE(a.users_assigned, 0) > 0
            OR COALESCE(a.drivers_assigned, 0) > 0
    ),
    software_skus_eu AS (
        SELECT DISTINCT
            l.sku
        FROM licenses_deduped_eu l
        LEFT JOIN assignment_summary_eu a
            ON l.sam_number = a.sam_number
            AND l.sku = a.sku
        WHERE
            COALESCE(a.users_assigned, 0) > 0
            OR COALESCE(a.drivers_assigned, 0) > 0
    ),
    software_skus_ca AS (
        SELECT DISTINCT
            l.sku
        FROM licenses_deduped_ca l
        LEFT JOIN assignment_summary_ca a
            ON l.sam_number = a.sam_number
            AND l.sku = a.sku
        WHERE
            COALESCE(a.users_assigned, 0) > 0
            OR COALESCE(a.drivers_assigned, 0) > 0
    ),
    license_assignment_base_us AS (
        SELECT
            l.sam_number,
            l.account_id,
            l.sku,
            l.quantity,
            COALESCE(a.devices_assigned, 0) AS devices_assigned,
            COALESCE(a.users_assigned, 0) AS users_assigned,
            COALESCE(a.drivers_assigned, 0) AS drivers_assigned,
            CASE WHEN s.sku IS NOT NULL THEN 'software' ELSE 'hardware' END AS license_category
        FROM licenses_deduped_us l
        LEFT OUTER JOIN assignment_summary_us a
            ON l.sam_number = a.sam_number
            AND l.account_id = a.account_id
            AND l.sku = a.sku
        LEFT OUTER JOIN software_skus_us s
            ON l.sku = s.sku
    ),
    license_assignment_base_eu AS (
        SELECT
            l.sam_number,
            l.account_id,
            l.sku,
            l.quantity,
            COALESCE(a.devices_assigned, 0) AS devices_assigned,
            COALESCE(a.users_assigned, 0) AS users_assigned,
            COALESCE(a.drivers_assigned, 0) AS drivers_assigned,
            CASE WHEN s.sku IS NOT NULL THEN 'software' ELSE 'hardware' END AS license_category
        FROM licenses_deduped_eu l
        LEFT OUTER JOIN assignment_summary_eu a
            ON l.sam_number = a.sam_number
            AND l.account_id = a.account_id
            AND l.sku = a.sku
        LEFT OUTER JOIN software_skus_eu s
            ON l.sku = s.sku
    ),
    license_assignment_base_ca AS (
        SELECT
            l.sam_number,
            l.account_id,
            l.sku,
            l.quantity,
            COALESCE(a.devices_assigned, 0) AS devices_assigned,
            COALESCE(a.users_assigned, 0) AS users_assigned,
            COALESCE(a.drivers_assigned, 0) AS drivers_assigned,
            CASE WHEN s.sku IS NOT NULL THEN 'software' ELSE 'hardware' END AS license_category
        FROM licenses_deduped_ca l
        LEFT OUTER JOIN assignment_summary_ca a
            ON l.sam_number = a.sam_number
            AND l.account_id = a.account_id
            AND l.sku = a.sku
        LEFT OUTER JOIN software_skus_ca s
            ON l.sku = s.sku
    ),
    license_assignment_total_us AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_us
    ),
    license_assignment_total_eu AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_eu
    ),
    license_assignment_total_ca AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_ca
    ),
    license_assignment_software_us AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_us
        WHERE license_category = 'software'
    ),
    license_assignment_software_eu AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_eu
        WHERE license_category = 'software'
    ),
    license_assignment_software_ca AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_ca
        WHERE license_category = 'software'
    ),
    license_assignment_hardware_us AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_us
        WHERE license_category = 'hardware'
    ),
    license_assignment_hardware_eu AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_eu
        WHERE license_category = 'hardware'
    ),
    license_assignment_hardware_ca AS (
        SELECT
            sam_number,
            account_id,
            quantity,
            devices_assigned,
            users_assigned,
            drivers_assigned
        FROM license_assignment_base_ca
        WHERE license_category = 'hardware'
    ),
    device_activity_us AS (
        SELECT DISTINCT
            DATE(ldo.date) AS date,
            ldo.org_id,
            ldo.device_id,
            org.sam_number,
            dev.product_name AS device_type,
            'us-west-2' as region,
            CASE WHEN ldo.has_heartbeat THEN 1 ELSE 0 END AS is_device_online, -- has heartbeat
            CASE WHEN ldo.is_active THEN 1 ELSE 0 END AS is_device_active -- has activity
        FROM datamodel_core_silver.stg_device_activity_daily ldo
        INNER JOIN datamodel_core.dim_devices dev
            ON ldo.device_id = dev.device_id
            AND ldo.org_id = dev.org_id
            AND ldo.date = dev.date
        INNER JOIN latest_org_us org
            ON dev.org_id = org.org_id
        WHERE TRUE -- arbitrary for readability
            AND ldo.date BETWEEN '{START_OF_PERIOD}' and '{END_OF_PERIOD}' -- grab relavant dates
    ),
    device_activity_eu AS (
        SELECT DISTINCT
            DATE(ldo.date) AS date,
            ldo.org_id,
            ldo.device_id,
            org.sam_number,
            dev.product_name AS device_type,
            'eu-west-1' as region,
            CASE WHEN ldo.has_heartbeat THEN 1 ELSE 0 END AS is_device_online, -- has heartbeat
            CASE WHEN ldo.is_active THEN 1 ELSE 0 END AS is_device_active -- has activity
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_silver.db/stg_device_activity_daily` ldo
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dev
            ON ldo.device_id = dev.device_id
            AND ldo.org_id = dev.org_id
            AND ldo.date = dev.date
        INNER JOIN latest_org_eu org
            ON dev.org_id = org.org_id
        WHERE TRUE -- arbitrary for readability
            AND ldo.date BETWEEN '{START_OF_PERIOD}' and '{END_OF_PERIOD}' -- grab relavant dates
    ),
    device_activity_ca AS (
        SELECT DISTINCT
            DATE(ldo.date) AS date,
            ldo.org_id,
            ldo.device_id,
            org.sam_number,
            dev.product_name AS device_type,
            'ca-central-1' as region,
            CASE WHEN ldo.has_heartbeat THEN 1 ELSE 0 END AS is_device_online, -- has heartbeat
            CASE WHEN ldo.is_active THEN 1 ELSE 0 END AS is_device_active -- has activity
        FROM data_tools_delta_share_ca.datamodel_core_silver.stg_device_activity_daily ldo
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dev
            ON ldo.device_id = dev.device_id
            AND ldo.org_id = dev.org_id
            AND ldo.date = dev.date
        INNER JOIN latest_org_ca org
            ON dev.org_id = org.org_id
        WHERE TRUE -- arbitrary for readability
            AND ldo.date BETWEEN '{START_OF_PERIOD}' and '{END_OF_PERIOD}' -- grab relavant dates
    ),
    device_activity AS (
        -- Compute total active days per device and capture latest org_id in one scan
        SELECT
            device_id,
            org_id,
            device_type,
            region,
            SUM(CASE WHEN is_device_active = 1 THEN 1 ELSE 0 END) AS total_active
        FROM device_activity_us
        WHERE date BETWEEN '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        GROUP BY device_id, org_id, device_type, region

        UNION ALL

        SELECT
            device_id,
            org_id,
            device_type,
            region,
            SUM(CASE WHEN is_device_active = 1 THEN 1 ELSE 0 END) AS total_active
        FROM device_activity_eu
        WHERE date BETWEEN '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        GROUP BY device_id, org_id, device_type, region

        UNION ALL

        SELECT
            device_id,
            org_id,
            device_type,
            region,
            SUM(CASE WHEN is_device_active = 1 THEN 1 ELSE 0 END) AS total_active
        FROM device_activity_ca
        WHERE date BETWEEN '{START_OF_PERIOD}' and '{END_OF_PERIOD}'
        GROUP BY device_id, org_id, device_type, region
    ),
    active_devices_us AS (
        -- Copying the same logic for device_type as used in dim_devices
        SELECT
            dh.device_id,
            dh.total_active,
            dh.device_type,
            o.sam_number,
            o.account_id
        FROM device_activity dh
        JOIN latest_org_us o
            ON dh.org_id = o.org_id
        WHERE dh.region = 'us-west-2'
    ),
    active_devices_eu AS (
        -- Copying the same logic for device_type as used in dim_devices
        SELECT
            dh.device_id,
            dh.total_active,
            dh.device_type,
            o.sam_number,
            o.account_id
        FROM device_activity dh
        JOIN latest_org_eu o
            ON dh.org_id = o.org_id
        WHERE dh.region = 'eu-west-1'
    ),
    active_devices_ca AS (
        -- Copying the same logic for device_type as used in dim_devices
        SELECT
            dh.device_id,
            dh.total_active,
            dh.device_type,
            o.sam_number,
            o.account_id
        FROM device_activity dh
        JOIN latest_org_ca o
            ON dh.org_id = o.org_id
        WHERE dh.region = 'ca-central-1'
    ),
    {METRIC_CTES},
    final AS (
        SELECT DISTINCT
            {COALESCE_STATEMENTS},
            {COALESCE_METRIC_STATEMENTS}
        FROM trips trips
            {FULL_OUTER_JOIN_STATEMENTS}
    )
    SELECT
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
        MAX(trips) AS trips,
        MAX(miles) AS miles,
        MAX(active_drivers) AS active_drivers,
        MAX(active_cloud_dashboard_users) AS active_cloud_dashboard_users,
        MAX(active_driver_app_users) AS active_driver_app_users,
        MAX(marketplace_integrations) AS marketplace_integrations,
        MAX(api_requests) AS api_requests,
        MAX(device_licenses_ag) AS device_licenses_ag,
        MAX(device_licenses_vg) AS device_licenses_vg,
        MAX(device_licenses_cm) AS device_licenses_cm,
        MAX(device_licenses_at) AS device_licenses_at,
        MAX(device_licenses_sg) AS device_licenses_sg,
        MAX(active_devices_ag) AS active_devices_ag,
        MAX(active_devices_vg) AS active_devices_vg,
        MAX(active_devices_cm) AS active_devices_cm,
        MAX(active_devices_at) AS active_devices_at,
        MAX(active_devices_sg) AS active_devices_sg,
        MAX(total_licenses) AS total_licenses,
        MAX(total_licenses_hardware) AS total_licenses_hardware,
        MAX(total_licenses_software) AS total_licenses_software,
        MAX(total_licenses_assigned) AS total_licenses_assigned,
        MAX(total_licenses_assigned_hardware) AS total_licenses_assigned_hardware,
        MAX(total_licenses_assigned_software) AS total_licenses_assigned_software
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

DRIVER_ASSIGNMENT_QUERY_TEMPLATE = """
    WITH account_arr AS (
        -- Handles edge case where multiple accounts with different ARRs exist for a given SAM
        SELECT
            account_id,
            customer_arr,
            customer_arr_segment
        FROM edw.silver.dim_account_enriched
    ),
    account_arr_sam AS (
        -- Handles cases where there's no account_id match (take max for SAM instead)
        SELECT sam_number_undecorated AS sam_number,
        MAX_BY(customer_arr, customer_arr) AS customer_arr,
        MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
        FROM edw.silver.dim_account_enriched
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
    sign_in_reminder_us AS (
        SELECT
            OrgId AS org_id,
            FROM_JSON(SettingsData, 'struct<reminder_enabled:boolean>').reminder_enabled AS sign_in_reminder_enabled
        FROM dynamodb.driver_assignment_settings
        WHERE
            OrgDeviceIdentifier LIKE 'ORG#%'
            AND SettingsTypeIdentifier in ('DRIVER_SIGNIN_REMINDER')
            AND DATE(FROM_UNIXTIME(`_approximate_creation_date_time` / 1000)) <= '{END_OF_PERIOD}'
    ),
    qr_id_us AS (
        SELECT
            OrgId AS org_id,
            MAX(FROM_JSON(SettingsData, 'struct<enabled:boolean>').enabled) AS enabled
        FROM dynamodb.driver_assignment_settings
        WHERE
            OrgDeviceIdentifier LIKE 'ORG#%'
            AND SettingsTypeIdentifier in ('QRCODE_SETTINGS', 'IDCARD_SETTINGS')
            AND DATE(FROM_UNIXTIME(`_approximate_creation_date_time` / 1000)) <= '{END_OF_PERIOD}'
        GROUP BY OrgId
    ),
    sign_in_reminder_eu AS (
        SELECT
            OrgId AS org_id,
            FROM_JSON(SettingsData, 'struct<reminder_enabled:boolean>').reminder_enabled AS sign_in_reminder_enabled
        FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/driver-assignment-settings`
        WHERE
            OrgDeviceIdentifier LIKE 'ORG#%'
            AND SettingsTypeIdentifier in ('DRIVER_SIGNIN_REMINDER')
            AND DATE(FROM_UNIXTIME(`_approximate_creation_date_time` / 1000)) <= '{END_OF_PERIOD}'
    ),
    qr_id_eu AS (
        SELECT
            OrgId AS org_id,
            MAX(FROM_JSON(SettingsData, 'struct<enabled:boolean>').enabled) AS enabled
        FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/driver-assignment-settings`
        WHERE
            OrgDeviceIdentifier LIKE 'ORG#%'
            AND SettingsTypeIdentifier in ('QRCODE_SETTINGS', 'IDCARD_SETTINGS')
            AND DATE(FROM_UNIXTIME(`_approximate_creation_date_time` / 1000)) <= '{END_OF_PERIOD}'
        GROUP BY OrgId
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
            COALESCE(s.sign_in_reminder_enabled AND q.enabled, FALSE) AS sign_in_reminder_enabled
        FROM datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_us p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN sign_in_reminder_us s
            ON o.org_id = s.org_id
        LEFT OUTER JOIN qr_id_us q
            ON o.org_id = q.org_id
        WHERE
            o.date = '{END_OF_PERIOD}'
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
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer,
            MAX(CASE WHEN sign_in_reminder_enabled = true THEN 1 ELSE 0 END) AS sign_in_reminder_enabled
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
            COALESCE(s.sign_in_reminder_enabled AND q.enabled, FALSE) AS sign_in_reminder_enabled
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_eu p
            ON o.sam_number = p.sam_number
        LEFT OUTER JOIN sign_in_reminder_eu s
            ON o.org_id = s.org_id
        LEFT OUTER JOIN qr_id_eu q
            ON o.org_id = q.org_id
        WHERE
            o.date = '{END_OF_PERIOD}'
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
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer,
            MAX(CASE WHEN sign_in_reminder_enabled = true THEN 1 ELSE 0 END) AS sign_in_reminder_enabled
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
            FALSE AS sign_in_reminder_enabled
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations o
        LEFT OUTER JOIN account_arr a
            ON o.account_id = a.account_id
        LEFT OUTER JOIN account_arr_sam a_sam
            ON o.sam_number = a_sam.sam_number
        LEFT OUTER JOIN pubsec_ca p
            ON o.sam_number = p.sam_number
        WHERE
            o.date = '{END_OF_PERIOD}'
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
            MAX(CASE WHEN is_paid_stce_customer = true THEN 1 ELSE 0 END) AS is_paid_stce_customer,
            MAX(CASE WHEN sign_in_reminder_enabled = true THEN 1 ELSE 0 END) AS sign_in_reminder_enabled
        FROM latest_org_ca
        GROUP BY ALL
    ),
    trips_deduped_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            t.duration_mins,
            t.driver_assignment_source,
            t.driver_id,
            d.associated_devices['camera_product_id'] AS camera_product_id
        FROM datamodel_telematics.fct_trips t
        JOIN latest_org_us o
            ON o.org_id = t.org_id
        JOIN datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = t.device_id
            AND d.date = t.date
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.trip_type = 'location_based'
            AND d.device_type = 'VG - Vehicle Gateway'
    ),
    trips_deduped_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            t.duration_mins,
            t.driver_assignment_source,
            t.driver_id,
            d.associated_devices['camera_product_id'] AS camera_product_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_trips` t
        JOIN latest_org_eu o
            ON o.org_id = t.org_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
            ON o.org_id = d.org_id
            AND d.device_id = t.device_id
            AND d.date = t.date
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.trip_type = 'location_based'
            AND d.device_type = 'VG - Vehicle Gateway'
    ),
    trips_deduped_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            t.duration_mins,
            t.driver_assignment_source,
            t.driver_id,
            d.associated_devices['camera_product_id'] AS camera_product_id
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_trips t
        JOIN latest_org_ca o
            ON o.org_id = t.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = t.device_id
            AND d.date = t.date
        WHERE
            t.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND t.trip_type = 'location_based'
            AND d.device_type = 'VG - Vehicle Gateway'
    ),
    {METRIC_CTES},
    final AS (
        SELECT DISTINCT
            {COALESCE_STATEMENTS},
            {COALESCE_METRIC_STATEMENTS}
        FROM driver_assignment_aggregated driver_assignment_aggregated
    )
    SELECT
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
        driver_assignment_source,
        sign_in_reminder_enabled,
        MAX(inward_assigned_trip_duration) AS inward_assigned_trip_duration,
        MAX(inward_total_trip_duration) AS inward_total_trip_duration,
        MAX(outward_assigned_trip_duration) AS outward_assigned_trip_duration,
        MAX(outward_total_trip_duration) AS outward_total_trip_duration,
        MAX(no_cm_assigned_trip_duration) AS no_cm_assigned_trip_duration,
        MAX(no_cm_total_trip_duration) AS no_cm_total_trip_duration
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
        account_id,
        driver_assignment_source,
        sign_in_reminder_enabled
    """

TOP_LINE_SCHEMA = [
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
        "name": "trips",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Trip total over period"
        },
    },
    {
        "name": "miles",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total distance covered by trips over period"
        },
    },
    {
        "name": "active_drivers",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active drivers over period"
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
        "name": "active_driver_app_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active driver app users within the period"
        },
    },
    {
        "name": "marketplace_integrations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active API integrations"
        },
    },
    {
        "name": "api_requests",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of API requests placed during the period"
        },
    },
    {
        "name": "device_licenses_ag",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active AG licenses",
        },
    },
    {
        "name": "device_licenses_vg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active VG licenses"
        },
    },
    {
        "name": "device_licenses_cm",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active CM licenses"
        },
    },
    {
        "name": "device_licenses_at",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active AT licenses"
        },
    },
    {
        "name": "device_licenses_sg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of active SG licenses"
        },
    },
    {
        "name": "active_devices_ag",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Active AG devices over period"
        },
    },
    {
        "name": "active_devices_vg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Active VG devices over period"
        },
    },
    {
        "name": "active_devices_cm",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Active CM devices over period"
        },
    },
    {
        "name": "active_devices_at",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Active AT devices over period"
        },
    },
    {
        "name": "active_devices_sg",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Active SG devices over period"
        },
    },
    {
        "name": "total_licenses",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active licenses"
        },
    },
    {
        "name": "total_licenses_assigned",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active assigned licenses"
        },
    },
    {
        "name": "total_licenses_software",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active software licenses"
        },
    },
    {
        "name": "total_licenses_assigned_software",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active assigned software licenses"
        },
    },
    {
        "name": "total_licenses_hardware",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active hardware licenses"
        },
    },
    {
        "name": "total_licenses_assigned_hardware",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active assigned hardware licenses"
        },
    },
]

DRIVER_ASSIGNMENT_SCHEMA = [
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
        "name": "driver_assignment_source",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Driver assignment source of trip"
        },
    },
    {
        "name": "sign_in_reminder_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether sign in reminders are enbaled for the account or not"
        },
    },
    {
        "name": "inward_assigned_trip_duration",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total time for VG trips connected to an inward-facing CM where the driver was assigned (minutes)"
        },
    },
    {
        "name": "inward_total_trip_duration",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total time for VG trips connected to an inward-facing CM (minutes)"
        },
    },
    {
        "name": "outward_assigned_trip_duration",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total time for VG trips connected to an outward-facing CM where the driver was assigned (minutes)"
        },
    },
    {
        "name": "outward_total_trip_duration",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total time for VG trips connected to an outward-facing CM (minutes)"
        },
    },
    {
        "name": "no_cm_assigned_trip_duration",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total time for VG trips not connected to a CM where the driver was assigned (minutes)"
        },
    },
    {
        "name": "no_cm_total_trip_duration",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total time for VG trips not connected to a CM (minutes)"
        },
    },
]

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

DRIVER_ASSIGNMENT_DIMENSION_CONFIGS = {
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
    "driver_assignment_source": DimensionConfig(name="driver_assignment_source", data_type=DimensionDataType.STRING, default_value="'Unknown'"),
    "sign_in_reminder_enabled": DimensionConfig(name="sign_in_reminder_enabled", data_type=DimensionDataType.BOOLEAN, default_value="false"),
}
