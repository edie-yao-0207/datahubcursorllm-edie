from dataweb.userpkgs.constants import (
    ColumnDescription,
    DimensionConfig,
    DimensionDataType,
)

TELEMATICS_STANDARD_DIMENSIONS = {
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
}

TELEMATICS_METRICS = {
    "routes": "COUNT(DISTINCT route_id)",
    "completed_routes": "COUNT(DISTINCT CASE WHEN is_completed_route = TRUE THEN route_id END)",
    "vg_trips": "COUNT(DISTINCT trip_id)",
    "active_vgs": "COUNT(DISTINCT device_id)",
    "idle_duration_ms": "SUM(duration_ms)",
    "on_duration_ms": "SUM(duration_ms)",
    "unproductive_idle_duration_ms": "SUM(duration_ms)",
    "total_drivers": "COUNT(DISTINCT driver_id)",
    "total_drivers_using_app_month": "COUNT(DISTINCT CASE WHEN last_mobile_login_date >= '{START_OF_PERIOD}' THEN driver_id END)",
    "total_drivers_using_app": "COUNT(DISTINCT CASE WHEN last_mobile_login_date IS NOT NULL THEN driver_id END)",
    "violation_duration_ms": "SUM(violation_duration_ms)",
    "possible_driver_ms": "SUM(possible_driver_ms)",
    "unassigned_driver_time_hours": "SUM(unassigned_driver_time_hours)",
    "assigned_driver_time_hours": "SUM(assigned_driver_time_hours)",
    "p50_mpg": "APPROX_PERCENTILE(mpge, 0.5)",
    "vehicles_ev": "COUNT(DISTINCT CASE WHEN fuel_group_name = 'FUEL_GROUP_ELECTRICITY' THEN device_id END)",
    "vehicles_ice": "COUNT(DISTINCT CASE WHEN powertrain_name = 'POWERTRAIN_INTERNAL_COMBUSTION_ENGINE' THEN device_id END)",
    "vehicles_hybrid": "COUNT(DISTINCT CASE WHEN powertrain_name IN ('POWERTRAIN_HYBRID', 'POWERTRAIN_PLUG_IN_HYBRID_ELECTRIC_VEHICLE') THEN device_id END)",
    "sleeper_berth_split_violations": "COUNT(DISTINCT CONCAT(driver_id, '_', merged_start))",
    "yard_move_miles": "CAST(SUM(distance_miles) AS DOUBLE)",
    "total_miles": "SUM(distance_miles)",
}

TELEMATICS_METRIC_CONFIGS_TEMPLATE = {
    "routes_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "routes_us",
        "eu_source": "routes_eu",
        "ca_source": "routes_ca",
        "table_alias": "r",
        "join_alias": "r",
        "metrics": ["routes", "completed_routes"]
    },
    "trips_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "trips_deduped_us",
        "eu_source": "trips_deduped_eu",
        "ca_source": "trips_deduped_ca",
        "table_alias": "t",
        "join_alias": "t",
        "metrics": ["vg_trips"]
    },
    "vg_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "active_vg_us",
        "eu_source": "active_vg_eu",
        "ca_source": "active_vg_ca",
        "table_alias": "v",
        "join_alias": "v",
        "metrics": ["active_vgs"]
    },
    "idling_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "idling_us",
        "eu_source": "idling_eu",
        "ca_source": "idling_ca",
        "table_alias": "i",
        "join_alias": "i",
        "metrics": ["idle_duration_ms"]
    },
    "engage_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "engage_us",
        "eu_source": "engage_eu",
        "ca_source": "engage_ca",
        "table_alias": "e",
        "join_alias": "e",
        "metrics": ["on_duration_ms"]
    },
    "unproductive_idling_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "unproductive_idling_us",
        "eu_source": "unproductive_idling_eu",
        "ca_source": "unproductive_idling_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["unproductive_idle_duration_ms"]
    },
    "drivers_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "drivers_us",
        "eu_source": "drivers_eu",
        "ca_source": "drivers_ca",
        "table_alias": "d",
        "join_alias": "d",
        "metrics": ["total_drivers", "total_drivers_using_app_month", "total_drivers_using_app"]
    },
    "hos_violation_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "hos_violation_us",
        "eu_source": "hos_violation_eu",
        "ca_source": "hos_violation_ca",
        "table_alias": "h",
        "join_alias": "h",
        "metrics": ["violation_duration_ms", "possible_driver_ms"]
    },
    "unassigned_hos_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "unassigned_hos_us",
        "eu_source": "unassigned_hos_eu",
        "ca_source": "unassigned_hos_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["unassigned_driver_time_hours"]
    },
    "assigned_hos_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "assigned_hos_us",
        "eu_source": "assigned_hos_eu",
        "ca_source": "assigned_hos_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["assigned_driver_time_hours"]
    },
    "mpg_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "mpg_us",
        "eu_source": "mpg_eu",
        "ca_source": "mpg_ca",
        "table_alias": "u",
        "join_alias": "u",
        "metrics": ["p50_mpg"]
    },
    "vehicle_mix_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "vehicle_mix_us",
        "eu_source": "vehicle_mix_eu",
        "ca_source": "vehicle_mix_ca",
        "table_alias": "v",
        "join_alias": "v",
        "metrics": ["vehicles_ev", "vehicles_ice", "vehicles_hybrid"]
    },
    "split_berth_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "split_berth_us",
        "eu_source": "split_berth_eu",
        "ca_source": "split_berth_ca",
        "table_alias": "s",
        "join_alias": "s",
        "metrics": ["sleeper_berth_split_violations"]
    },
    "yard_move_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "yard_move_distance_us",
        "eu_source": "yard_move_distance_eu",
        "ca_source": "yard_move_distance_ca",
        "table_alias": "y",
        "join_alias": "y",
        "metrics": ["yard_move_miles"]
    },
    "total_movement_aggregated": {
        "dimensions_template": "DIMENSIONS_{region}",
        "us_source": "total_movement_us",
        "eu_source": "total_movement_eu",
        "ca_source": "total_movement_ca",
        "table_alias": "y",
        "join_alias": "y",
        "metrics": ["total_miles"]
    }
}

TELEMATICS_DIMENSION_TEMPLATES = {
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

TELEMATICS_REGION_MAPPINGS = {
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
        "region_alias": "Canada"
    }
}


TELEMATICS_JOIN_MAPPINGS = {
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

TELEMATICS_SCHEMA = [
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
        "name": "account_renewal_12mo",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the account is renewing in the next 12 months or not"
        },
    },
    {
        "name": "routes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of dispatched routes for the account over the time period"
        },
    },
    {
        "name": "completed_routes",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of dispatched routes for the account over the time period that were completed"
        },
    },
    {
        "name": "vg_trips",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of trips taken by a VG in the period"
        },
    },
    {
        "name": "active_vgs",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of active VGs in the last 28 days"
        },
    },
    {
        "name": "idle_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total engine idle duration in milliseconds for VGs in the period"
        },
    },
    {
        "name": "on_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total engine on duration in milliseconds for VGs in the period"
        },
    },
    {
        "name": "unproductive_idle_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total unproductive engine idle duration in milliseconds for VGs in the period"
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
        "name": "violation_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of HoS violation time for drivers during the period"
        },
    },
    {
        "name": "possible_driver_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of possible driver time for drivers during the period"
        },
    },
    {
        "name": "unassigned_driver_time_hours",
        "type": "decimal(38,7)",
        "nullable": False,
        "metadata": {
            "comment": "Total number of hours drivers were unassigned during HoS logs in the period"
        },
    },
    {
        "name": "assigned_driver_time_hours",
        "type": "decimal(38,7)",
        "nullable": False,
        "metadata": {
            "comment": "Total number of hours drivers were assigned during HoS logs in the period"
        },
    },
    {
        "name": "driver_time_hours",
        "type": "decimal(38,6)",
        "nullable": False,
        "metadata": {
            "comment": "Total number of hours drivers were assigned/unassigned during HoS logs in the period"
        },
    },
    {
        "name": "p50_mpg",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "50th percentile of MPG for vehicles in the account during the period"
        },
    },
    {
        "name": "vehicles_ev",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of EV vehicles in the account during the period"
        },
    },
    {
        "name": "vehicles_ice",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of ICE vehicles in the account during the period"
        },
    },
    {
        "name": "vehicles_hybrid",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of hybrid vehicles in the account during the period"
        },
    },
    {
        "name": "sleeper_berth_split_violations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of HoS violations on days where drivers had a sleeper berth split"
        },
    },
    {
        "name": "yard_move_miles",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total number of miles covered during yard move status in the period"
        },
    },
    {
        "name": "total_miles",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total number of miles covered in the period"
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
            COALESCE(o.account_id, 'Unknown') AS account_id,
            FALSE AS account_renewal_12mo
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
            o.is_paid_telematics_customer = TRUE
        )
    ),
    us_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_renewal_12mo,
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
            FALSE AS account_renewal_12mo
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
            o.is_paid_telematics_customer = TRUE
        )
    ),
    eu_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_renewal_12mo,
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
            FALSE AS account_renewal_12mo
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
            o.is_paid_telematics_customer = TRUE
        )
    ),
    ca_customer_check AS (
        -- handling edge case of a few SAMs with customers that have both T/F values
        SELECT
            sam_number,
            account_csm_email,
            account_name,
            account_id,
            account_renewal_12mo,
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
    routes_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            dr.route_id,
            dr.is_completed_route
        FROM datamodel_telematics.fct_dispatch_routes dr
        JOIN latest_org_us lo
            ON lo.org_id = dr.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    routes_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            dr.route_id,
            dr.is_completed_route
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_dispatch_routes` dr
        JOIN latest_org_eu lo
            ON lo.org_id = dr.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    routes_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            dr.route_id,
            dr.is_completed_route
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_dispatch_routes dr
        JOIN latest_org_ca lo
            ON lo.org_id = dr.org_id
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    trips_base_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            FORMAT_STRING('%s:%s', t.device_id, t.start_time_ms) AS trip_id,
            t.distance_miles
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
    trips_deduped_us AS (
        SELECT
            sam_number,
            account_id,
            trip_id
        FROM trips_base_us
    ),
    trips_base_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            FORMAT_STRING('%s:%s', t.device_id, t.start_time_ms) AS trip_id,
            t.distance_miles
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
    trips_deduped_eu AS (
        SELECT
            sam_number,
            account_id,
            trip_id
        FROM trips_base_eu
    ),
    trips_base_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            FORMAT_STRING('%s:%s', t.device_id, t.start_time_ms) AS trip_id,
            t.distance_miles
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
    trips_deduped_ca AS (
        SELECT
            sam_number,
            account_id,
            trip_id
        FROM trips_base_ca
    ),
    active_vg_us AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.device_id
        FROM datamodel_core.lifetime_device_activity l
        JOIN latest_org_us o
            ON o.org_id = l.org_id
        JOIN datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = l.device_id
            AND d.date = l.date
        WHERE
            l.date = '{END_OF_PERIOD}'
            AND d.device_type = 'VG - Vehicle Gateway'
            AND l.l28 > 0 -- active in last 28 days
    ),
    active_vg_eu AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.device_id
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` l
        JOIN latest_org_eu o
            ON o.org_id = l.org_id
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
            ON o.org_id = d.org_id
            AND d.device_id = l.device_id
            AND d.date = l.date
        WHERE
            l.date = '{END_OF_PERIOD}'
            AND d.device_type = 'VG - Vehicle Gateway'
            AND l.l28 > 0 -- active in last 28 days
    ),
    active_vg_ca AS (
        SELECT
            o.sam_number,
            o.account_id,
            l.device_id
        FROM data_tools_delta_share_ca.datamodel_core.lifetime_device_activity l
        JOIN latest_org_ca o
            ON o.org_id = l.org_id
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices d
            ON o.org_id = d.org_id
            AND d.device_id = l.device_id
            AND d.date = l.date
        WHERE
            l.date = '{END_OF_PERIOD}'
            AND d.device_type = 'VG - Vehicle Gateway'
            AND l.l28 > 0 -- active in last 28 days
    ),
    idling_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM engineactivitydb_shards.engine_idle_events i
        LEFT OUTER JOIN productsdb.devices d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_us lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= 2 * 60 * 1000 -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
    ),
    idling_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/engine_idle_events_v0` i
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_eu lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= 2 * 60 * 1000 -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivity-shard-1db/engineactivitydb/engine_idle_events_v0` i
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_eu lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= 2 * 60 * 1000 -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
    ),
    idling_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/engine_idle_events_v0` i
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_ca lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= 2 * 60 * 1000 -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
    ),
    engage_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM engineactivitydb_shards.engine_engage_events i
        LEFT OUTER JOIN productsdb.devices d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_us lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
    ),
    engage_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/engine_engage_events_v0` i
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_eu lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivity-shard-1db/engineactivitydb/engine_engage_events_v0` i
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_eu lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
    ),
    engage_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/engine_engage_events_v0` i
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_ca lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
    ),
    unproductive_idling_config_us as (
        SELECT
            org_id,
            include_pto_off,
            include_pto_on,
            minimum_duration_ms,
            include_unknown_air_temperature,
            air_temperature_min_milli_c,
            air_temperature_max_milli_c,
            CASE -- mimic logic here to determine diesel_regen_filter_state: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/fleet/engineactivity/engineactivitymodels/unproductive_idling_config.go#L66
            WHEN
                COALESCE(include_diesel_regen_active, 0) > COALESCE(include_diesel_regen_inactive, 0)
            THEN
                1 -- DieselRegenFilter_State_ACTIVE
            WHEN
                COALESCE(include_diesel_regen_active, 0) < COALESCE(include_diesel_regen_inactive, 0)
            THEN
                2 -- DieselRegenFilter_State_INACTIVE
            ELSE 0 -- DieselRegenFilter_State_NONE
            END AS diesel_regen_filter_state,
            max_diesel_regen_active_duration_ms,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY updated_at DESC) AS rn
        FROM engineactivitydb_shards.unproductive_idling_config
        WHERE DATE(updated_at) <= '{END_OF_PERIOD}'
    ),
    unproductive_idling_config_us_latest as (
        SELECT
            *
        FROM unproductive_idling_config_us
        WHERE rn = 1
    ),
    unproductive_idling_us as (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM engineactivitydb_shards.engine_idle_events i
        LEFT JOIN unproductive_idling_config_us_latest c
            ON i.org_id = c.org_id
        LEFT OUTER JOIN productsdb.devices d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_us lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= COALESCE(c.minimum_duration_ms, 2 * 60 * 1000) -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
            AND (
                (
                    COALESCE(c.include_unknown_air_temperature, 1) = 1
                    AND i.ambient_temperature_milli_celsius IS NULL
                )
                OR (
                    i.ambient_temperature_milli_celsius IS NOT NULL
                    AND i.ambient_temperature_milli_celsius BETWEEN COALESCE(c.air_temperature_min_milli_c, 0) AND COALESCE(c.air_temperature_max_milli_c, 38000)
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_on, 0) = 1
                    AND i.pto_state = 1
                )
                OR (
                    COALESCE(c.include_pto_on, 0) = 0
                    AND i.pto_state <> 1
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_off, 1) = 1
                    AND i.pto_state = 2
                )
                OR (
                    COALESCE(c.include_pto_off, 1) = 0
                    AND i.pto_state <> 2
                )
            )
            AND CASE c.diesel_regen_filter_state
                WHEN 1 -- DieselRegenFilter_State_ACTIVE: only include ACTIVE events of duration less than max
                THEN COALESCE(i.diesel_regen_state, 0) = 1 AND i.duration_ms <= c.max_diesel_regen_active_duration_ms
                WHEN 2 -- DieselRegenFilter_State_INACTIVE: only include ACTIVE events if event duration exceeds max
                THEN COALESCE(i.diesel_regen_state, 0) <> 1 OR i.duration_ms > c.max_diesel_regen_active_duration_ms
                ELSE true -- DieselRegenFilter_State_NONE: include by default
            END
    ),
    unproductive_idling_config_eu as (
        SELECT
            org_id,
            include_pto_off,
            include_pto_on,
            minimum_duration_ms,
            include_unknown_air_temperature,
            air_temperature_min_milli_c,
            air_temperature_max_milli_c,
            CASE -- mimic logic here to determine diesel_regen_filter_state: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/fleet/engineactivity/engineactivitymodels/unproductive_idling_config.go#L66
            WHEN
                COALESCE(include_diesel_regen_active, 0) > COALESCE(include_diesel_regen_inactive, 0)
            THEN
                1 -- DieselRegenFilter_State_ACTIVE
            WHEN
                COALESCE(include_diesel_regen_active, 0) < COALESCE(include_diesel_regen_inactive, 0)
            THEN
                2 -- DieselRegenFilter_State_INACTIVE
            ELSE 0 -- DieselRegenFilter_State_NONE
            END AS diesel_regen_filter_state,
            max_diesel_regen_active_duration_ms,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY updated_at DESC) AS rn
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivity-shard-1db/engineactivitydb/unproductive_idling_config_v0`
        WHERE DATE(updated_at) <= '{END_OF_PERIOD}'

        UNION ALL

        SELECT
            org_id,
            include_pto_off,
            include_pto_on,
            minimum_duration_ms,
            include_unknown_air_temperature,
            air_temperature_min_milli_c,
            air_temperature_max_milli_c,
            CASE -- mimic logic here to determine diesel_regen_filter_state: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/fleet/engineactivity/engineactivitymodels/unproductive_idling_config.go#L66
            WHEN
                COALESCE(include_diesel_regen_active, 0) > COALESCE(include_diesel_regen_inactive, 0)
            THEN
                1 -- DieselRegenFilter_State_ACTIVE
            WHEN
                COALESCE(include_diesel_regen_active, 0) < COALESCE(include_diesel_regen_inactive, 0)
            THEN
                2 -- DieselRegenFilter_State_INACTIVE
            ELSE 0 -- DieselRegenFilter_State_NONE
            END AS diesel_regen_filter_state,
            max_diesel_regen_active_duration_ms,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY updated_at DESC) AS rn
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/unproductive_idling_config_v0`
        WHERE DATE(updated_at) <= '{END_OF_PERIOD}'
    ),
    unproductive_idling_config_eu_latest as (
        SELECT
            *
        FROM unproductive_idling_config_eu
        WHERE rn = 1
    ),
    unproductive_idling_eu as (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/engine_idle_events_v0` i
        LEFT JOIN unproductive_idling_config_eu_latest c
            ON i.org_id = c.org_id
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_eu lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= COALESCE(c.minimum_duration_ms, 2 * 60 * 1000) -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
            AND (
                (
                    COALESCE(c.include_unknown_air_temperature, 1) = 1
                    AND i.ambient_temperature_milli_celsius IS NULL
                )
                OR (
                    i.ambient_temperature_milli_celsius IS NOT NULL
                    AND i.ambient_temperature_milli_celsius BETWEEN COALESCE(c.air_temperature_min_milli_c, 0) AND COALESCE(c.air_temperature_max_milli_c, 38000)
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_on, 0) = 1
                    AND i.pto_state = 1
                )
                OR (
                    COALESCE(c.include_pto_on, 0) = 0
                    AND i.pto_state <> 1
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_off, 1) = 1
                    AND i.pto_state = 2
                )
                OR (
                    COALESCE(c.include_pto_off, 1) = 0
                    AND i.pto_state <> 2
                )
            )
            AND CASE c.diesel_regen_filter_state
                WHEN 1 -- DieselRegenFilter_State_ACTIVE: only include ACTIVE events of duration less than max
                THEN COALESCE(i.diesel_regen_state, 0) = 1 AND i.duration_ms <= c.max_diesel_regen_active_duration_ms
                WHEN 2 -- DieselRegenFilter_State_INACTIVE: only include ACTIVE events if event duration exceeds max
                THEN COALESCE(i.diesel_regen_state, 0) <> 1 OR i.duration_ms > c.max_diesel_regen_active_duration_ms
                ELSE true -- DieselRegenFilter_State_NONE: include by default
            END

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-engineactivity-shard-1db/engineactivitydb/engine_idle_events_v0` i
        LEFT JOIN unproductive_idling_config_eu_latest c
            ON i.org_id = c.org_id
        LEFT OUTER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_eu lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= COALESCE(c.minimum_duration_ms, 2 * 60 * 1000) -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
            AND (
                (
                    COALESCE(c.include_unknown_air_temperature, 1) = 1
                    AND i.ambient_temperature_milli_celsius IS NULL
                )
                OR (
                    i.ambient_temperature_milli_celsius IS NOT NULL
                    AND i.ambient_temperature_milli_celsius BETWEEN COALESCE(c.air_temperature_min_milli_c, 0) AND COALESCE(c.air_temperature_max_milli_c, 38000)
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_on, 0) = 1
                    AND i.pto_state = 1
                )
                OR (
                    COALESCE(c.include_pto_on, 0) = 0
                    AND i.pto_state <> 1
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_off, 1) = 1
                    AND i.pto_state = 2
                )
                OR (
                    COALESCE(c.include_pto_off, 1) = 0
                    AND i.pto_state <> 2
                )
            )
            AND CASE c.diesel_regen_filter_state
                WHEN 1 -- DieselRegenFilter_State_ACTIVE: only include ACTIVE events of duration less than max
                THEN COALESCE(i.diesel_regen_state, 0) = 1 AND i.duration_ms <= c.max_diesel_regen_active_duration_ms
                WHEN 2 -- DieselRegenFilter_State_INACTIVE: only include ACTIVE events if event duration exceeds max
                THEN COALESCE(i.diesel_regen_state, 0) <> 1 OR i.duration_ms > c.max_diesel_regen_active_duration_ms
                ELSE true -- DieselRegenFilter_State_NONE: include by default
            END
    ),
    unproductive_idling_config_ca as (
        SELECT
            org_id,
            include_pto_off,
            include_pto_on,
            minimum_duration_ms,
            include_unknown_air_temperature,
            air_temperature_min_milli_c,
            air_temperature_max_milli_c,
            CASE -- mimic logic here to determine diesel_regen_filter_state: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/fleet/engineactivity/engineactivitymodels/unproductive_idling_config.go#L66
            WHEN
                COALESCE(include_diesel_regen_active, 0) > COALESCE(include_diesel_regen_inactive, 0)
            THEN
                1 -- DieselRegenFilter_State_ACTIVE
            WHEN
                COALESCE(include_diesel_regen_active, 0) < COALESCE(include_diesel_regen_inactive, 0)
            THEN
                2 -- DieselRegenFilter_State_INACTIVE
            ELSE 0 -- DieselRegenFilter_State_NONE
            END AS diesel_regen_filter_state,
            max_diesel_regen_active_duration_ms,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY updated_at DESC) AS rn
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/unproductive_idling_config_v0`
        WHERE DATE(updated_at) <= '{END_OF_PERIOD}'
    ),
    unproductive_idling_config_ca_latest as (
        SELECT
            *
        FROM unproductive_idling_config_eu
        WHERE rn = 1
    ),
    unproductive_idling_ca as (
        SELECT
            lo.sam_number,
            lo.account_id,
            i.duration_ms
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-engineactivitydb/engineactivitydb/engine_idle_events_v0` i
        LEFT JOIN unproductive_idling_config_ca_latest c
            ON i.org_id = c.org_id
        LEFT OUTER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-productsdb/prod_db/devices_v0` d
            ON d.id = i.device_id
        JOIN definitions.products p
            ON p.product_id = d.product_id
            AND p.name ILIKE '%vg%'
        JOIN latest_org_ca lo
            ON lo.org_id = i.org_id
        WHERE
            i.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND i.duration_ms >= COALESCE(c.minimum_duration_ms, 2 * 60 * 1000) -- 2mins
            AND i.duration_ms <= 24 * 60 * 60 * 1000 -- 24 hours
            AND (
                (
                    COALESCE(c.include_unknown_air_temperature, 1) = 1
                    AND i.ambient_temperature_milli_celsius IS NULL
                )
                OR (
                    i.ambient_temperature_milli_celsius IS NOT NULL
                    AND i.ambient_temperature_milli_celsius BETWEEN COALESCE(c.air_temperature_min_milli_c, 0) AND COALESCE(c.air_temperature_max_milli_c, 38000)
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_on, 0) = 1
                    AND i.pto_state = 1
                )
                OR (
                    COALESCE(c.include_pto_on, 0) = 0
                    AND i.pto_state <> 1
                )
            )
            AND (
                (
                    COALESCE(c.include_pto_off, 1) = 1
                    AND i.pto_state = 2
                )
                OR (
                    COALESCE(c.include_pto_off, 1) = 0
                    AND i.pto_state <> 2
                )
            )
            AND CASE c.diesel_regen_filter_state
                WHEN 1 -- DieselRegenFilter_State_ACTIVE: only include ACTIVE events of duration less than max
                THEN COALESCE(i.diesel_regen_state, 0) = 1 AND i.duration_ms <= c.max_diesel_regen_active_duration_ms
                WHEN 2 -- DieselRegenFilter_State_INACTIVE: only include ACTIVE events if event duration exceeds max
                THEN COALESCE(i.diesel_regen_state, 0) <> 1 OR i.duration_ms > c.max_diesel_regen_active_duration_ms
                ELSE true -- DieselRegenFilter_State_NONE: include by default
            END
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
    hos_violation_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            hos.violation_duration_ms,
            hos.possible_driver_ms
        FROM metrics_repo.org_hos_violation_ratio hos
        JOIN latest_org_us lo
            ON hos.org_id = lo.org_id
        WHERE hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    hos_violations_us AS (
        -- For violations that are still ongoing, take current time for end_at
        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM eldhosdb_shards.driver_hos_violations hos
        JOIN datamodel_platform.dim_drivers dd -- Restrict to only eligible drivers
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM datamodel_platform.dim_drivers)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )
    ),
    ignored_violations_us AS (
        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM eldhosdb_shards.driver_hos_ignored_violations hos
        JOIN datamodel_platform.dim_drivers dd
            ON hos.org_id = dd.org_id
        AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM datamodel_platform.dim_drivers)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )
    ),
    exploded_violations_us AS (
        -- Explode violations into daily buckets
        SELECT
            org_id,
            driver_id,
            type,
            date_series AS date,
            CASE
            WHEN DATE(start_at) = date_series THEN start_at
            ELSE CAST(date_series AS TIMESTAMP)
            END AS adjusted_start,
            CASE
            WHEN DATE(end_at) = date_series THEN end_at
            ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
            END AS adjusted_end
        FROM hos_violations_us
        LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
    ),
    exploded_ignored_violations_us AS (
        SELECT
        org_id,
        driver_id,
        type,
        date_series AS date,
        CASE
            WHEN DATE(start_at) = date_series THEN start_at
            ELSE CAST(date_series AS TIMESTAMP)
        END AS adjusted_start,
        CASE
            WHEN DATE(end_at) = date_series THEN end_at
            ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
        END AS adjusted_end
        FROM ignored_violations_us
        LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
    ),
    split_violations_us AS (
        -- Split violations into multiple segments if they overlap with ignored violations (for the same driver/type)
        SELECT
            v.org_id,
            v.driver_id,
            v.date,
            -- First segment: start when violation starts, end when ignore begins
            CASE
            WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
            THEN v.adjusted_start
            ELSE NULL
            END AS first_segment_start,
            CASE
            WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
            THEN i.adjusted_start
            ELSE NULL
            END AS first_segment_end,
            -- Second segment: start when ignored ends, end when violation ends
            CASE
            WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
            THEN i.adjusted_end
            ELSE NULL
            END AS second_segment_start,
            CASE
            WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
            THEN v.adjusted_end
            ELSE NULL
            END AS second_segment_end,
            -- No overlap at all, take original violation
            CASE
            WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
            THEN v.adjusted_start
            ELSE NULL
            END AS no_overlap_start,
            CASE
            WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
            THEN v.adjusted_end
            ELSE NULL
            END AS no_overlap_end
        FROM exploded_violations_us v
        LEFT OUTER JOIN exploded_ignored_violations_us i
            ON v.org_id = i.org_id
            AND v.driver_id = i.driver_id
            AND v.date = i.date
            AND v.type = i.type
    ),
    flattened_split_violations_us AS (
        -- Merge all the splits into a common structure
        SELECT org_id, driver_id, date, first_segment_start AS adjusted_start, first_segment_end AS adjusted_end
        FROM split_violations_us WHERE first_segment_start IS NOT NULL

        UNION ALL

        SELECT org_id, driver_id, date, second_segment_start AS adjusted_start, second_segment_end AS adjusted_end
        FROM split_violations_us WHERE second_segment_start IS NOT NULL

        UNION ALL

        SELECT org_id, driver_id, date, no_overlap_start AS adjusted_start, no_overlap_end AS adjusted_end
        FROM split_violations_us WHERE no_overlap_start IS NOT NULL
    ),
    assign_group_us AS (
        -- Assign a unique group ID to overlapping records
        SELECT
            org_id,
            driver_id,
            date,
            adjusted_start,
            adjusted_end,
            SUM(
                CASE
                WHEN adjusted_start > LAG(adjusted_end) OVER (
                    PARTITION BY org_id, driver_id, date
                    ORDER BY adjusted_start
                ) THEN 1
                ELSE 0
                END
            ) OVER (
                PARTITION BY org_id, driver_id, date
                ORDER BY adjusted_start
            ) AS group_id
        FROM flattened_split_violations_us
    ),
    merged_violations_us AS (
        -- Merge overlapping violations within each assigned group
        SELECT
            org_id,
            driver_id,
            date,
            MIN(adjusted_start) AS merged_start,
            MAX(adjusted_end) AS merged_end
        FROM assign_group_us
        WHERE date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
        GROUP BY org_id, driver_id, date, group_id
    ),
    all_violations_eu AS (
        -- For violations that are still ongoing, take current time for end_at
        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-eldhosdb/eldhosdb/driver_hos_violations_v0` hos
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd -- Restrict to only eligible drivers
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers`)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )

        UNION ALL

        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-eldhos-shard-1db/eldhosdb/driver_hos_violations_v0` hos
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd -- Restrict to only eligible drivers
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers`)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )
    ),
    ignored_violations_eu AS (
        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-eldhosdb/eldhosdb/driver_hos_ignored_violations_v0` hos
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers`)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )

        UNION ALL

        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-eldhos-shard-1db/eldhosdb/driver_hos_ignored_violations_v0` hos
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers`)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )
    ),
    exploded_violations_eu AS (
        -- Explode violations into daily buckets
        SELECT
            org_id,
            driver_id,
            type,
            date_series AS date,
            CASE
            WHEN DATE(start_at) = date_series THEN start_at
            ELSE CAST(date_series AS TIMESTAMP)
            END AS adjusted_start,
            CASE
            WHEN DATE(end_at) = date_series THEN end_at
            ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
            END AS adjusted_end
        FROM all_violations_eu
        LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
    ),
    exploded_ignored_violations_eu AS (
        SELECT
        org_id,
        driver_id,
        type,
        date_series AS date,
        CASE
            WHEN DATE(start_at) = date_series THEN start_at
            ELSE CAST(date_series AS TIMESTAMP)
        END AS adjusted_start,
        CASE
            WHEN DATE(end_at) = date_series THEN end_at
            ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
        END AS adjusted_end
        FROM ignored_violations_eu
        LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
    ),
    split_violations_eu AS (
        -- Split violations into multiple segments if they overlap with ignored violations (for the same driver/type)
        SELECT
            v.org_id,
            v.driver_id,
            v.date,
            -- First segment: start when violation starts, end when ignore begins
            CASE
            WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
            THEN v.adjusted_start
            ELSE NULL
            END AS first_segment_start,
            CASE
            WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
            THEN i.adjusted_start
            ELSE NULL
            END AS first_segment_end,
            -- Second segment: start when ignored ends, end when violation ends
            CASE
            WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
            THEN i.adjusted_end
            ELSE NULL
            END AS second_segment_start,
            CASE
            WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
            THEN v.adjusted_end
            ELSE NULL
            END AS second_segment_end,
            -- No overlap at all, take original violation
            CASE
            WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
            THEN v.adjusted_start
            ELSE NULL
            END AS no_overlap_start,
            CASE
            WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
            THEN v.adjusted_end
            ELSE NULL
            END AS no_overlap_end
        FROM exploded_violations_eu v
        LEFT OUTER JOIN exploded_ignored_violations_eu i
            ON v.org_id = i.org_id
            AND v.driver_id = i.driver_id
            AND v.date = i.date
            AND v.type = i.type
    ),
    flattened_split_violations_eu AS (
        -- Merge all the splits into a common structure
        SELECT org_id, driver_id, date, first_segment_start AS adjusted_start, first_segment_end AS adjusted_end
        FROM split_violations_eu WHERE first_segment_start IS NOT NULL

        UNION ALL

        SELECT org_id, driver_id, date, second_segment_start AS adjusted_start, second_segment_end AS adjusted_end
        FROM split_violations_eu WHERE second_segment_start IS NOT NULL

        UNION ALL

        SELECT org_id, driver_id, date, no_overlap_start AS adjusted_start, no_overlap_end AS adjusted_end
        FROM split_violations_eu WHERE no_overlap_start IS NOT NULL
    ),
    assign_group_eu AS (
        -- Assign a unique group ID to overlapping records
        SELECT
            org_id,
            driver_id,
            date,
            adjusted_start,
            adjusted_end,
            SUM(
                CASE
                WHEN adjusted_start > LAG(adjusted_end) OVER (
                    PARTITION BY org_id, driver_id, date
                    ORDER BY adjusted_start
                ) THEN 1
                ELSE 0
                END
            ) OVER (
                PARTITION BY org_id, driver_id, date
                ORDER BY adjusted_start
            ) AS group_id
        FROM flattened_split_violations_eu
    ),
    merged_violations_eu AS (
        -- Merge overlapping violations within each assigned group
        SELECT
            org_id,
            driver_id,
            date,
            MIN(adjusted_start) AS merged_start,
            MAX(adjusted_end) AS merged_end
        FROM assign_group_eu
        GROUP BY org_id, driver_id, date, group_id
    ),
    final_daily_totals_eu AS (
        -- Compute final daily violation time per driver
        -- Cap at 24 hours per driver per day to handle edge cases where violations might exceed a day
        SELECT
            org_id,
            driver_id,
            date,
            LEAST(
                SUM(TIMESTAMPDIFF(MILLISECOND, merged_start, merged_end)),
                24 * 60 * 60 * 1000  -- 24 hours in milliseconds
            ) AS total_violation_time_ms
        FROM merged_violations_eu
        GROUP BY org_id, driver_id, date
    ),
    base_eu AS (
        -- Aggregate all durations for an org per day
        SELECT
            date,
            org_id,
            SUM(total_violation_time_ms) AS violation_duration_ms
        FROM final_daily_totals_eu
        GROUP BY date, org_id
    ),
    total_drivers_eu AS (
        -- Use latest date for all eligible drivers
        SELECT
            org_id,
            COUNT(driver_id) as count_drivers
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers`
        WHERE
            NOT is_deleted
            AND eld_exempt = 0
            AND date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers`)
        GROUP BY 1
    ),
    hos_violation_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            v.violation_duration_ms,
            COALESCE(d.count_drivers, 0) * 24 * 60 * 60 * 1000 AS possible_driver_ms
        FROM base_eu v
        LEFT OUTER JOIN total_drivers_eu d
            ON v.org_id = d.org_id
        JOIN latest_org_eu lo
            ON v.org_id = lo.org_id
        WHERE v.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    all_violations_ca AS (
        -- For violations that are still ongoing, take current time for end_at
        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-eldhosdb/eldhosdb/driver_hos_violations_v0` hos
        JOIN data_tools_delta_share_ca.datamodel_platform.dim_drivers dd -- Restrict to only eligible drivers
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )
    ),
    ignored_violations_ca AS (
        SELECT
            hos.org_id,
            hos.driver_id,
            start_at,
            COALESCE(end_at, current_timestamp()) as end_at,
            DATE(start_at) AS start_date,
            COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
            type
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-eldhosdb/eldhosdb/driver_hos_ignored_violations_v0` hos
        JOIN data_tools_delta_share_ca.datamodel_platform.dim_drivers dd
            ON hos.org_id = dd.org_id
            AND hos.driver_id = dd.driver_id
        WHERE
            hos.deleted_at = TIMESTAMP '+0101'
            AND NOT dd.is_deleted
            AND dd.eld_exempt = 0
            AND dd.date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers)
            AND type in (
            -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
                1, -- SHIFT_HOURS
                2, -- SHIFT_DRIVING_HOURS
                100, -- RESTBREAK_MISSED
                101, -- CALIFORNIA_MEALBREAK_MISSED
                200, -- CYCLE_HOURS_ON
                201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
                1000, -- SHIFT_ON_DUTY_HOURS
                1001, -- DAILY_ON_DUTY_HOURS
                1002, -- DAILY_DRIVING_HOURS
                1009 -- MANDATORY_24_HOURS_OFF_DUTY
            )
    ),
    exploded_violations_ca AS (
        -- Explode violations into daily buckets
        SELECT
            org_id,
            driver_id,
            type,
            date_series AS date,
            CASE
            WHEN DATE(start_at) = date_series THEN start_at
            ELSE CAST(date_series AS TIMESTAMP)
            END AS adjusted_start,
            CASE
            WHEN DATE(end_at) = date_series THEN end_at
            ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
            END AS adjusted_end
        FROM all_violations_ca
        LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
    ),
    exploded_ignored_violations_ca AS (
        SELECT
        org_id,
        driver_id,
        type,
        date_series AS date,
        CASE
            WHEN DATE(start_at) = date_series THEN start_at
            ELSE CAST(date_series AS TIMESTAMP)
        END AS adjusted_start,
        CASE
            WHEN DATE(end_at) = date_series THEN end_at
            ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
        END AS adjusted_end
        FROM ignored_violations_ca
        LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
    ),
    split_violations_ca AS (
        -- Split violations into multiple segments if they overlap with ignored violations (for the same driver/type)
        SELECT
            v.org_id,
            v.driver_id,
            v.date,
            -- First segment: start when violation starts, end when ignore begins
            CASE
            WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
            THEN v.adjusted_start
            ELSE NULL
            END AS first_segment_start,
            CASE
            WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
            THEN i.adjusted_start
            ELSE NULL
            END AS first_segment_end,
            -- Second segment: start when ignored ends, end when violation ends
            CASE
            WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
            THEN i.adjusted_end
            ELSE NULL
            END AS second_segment_start,
            CASE
            WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
            THEN v.adjusted_end
            ELSE NULL
            END AS second_segment_end,
            -- No overlap at all, take original violation
            CASE
            WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
            THEN v.adjusted_start
            ELSE NULL
            END AS no_overlap_start,
            CASE
            WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
            THEN v.adjusted_end
            ELSE NULL
            END AS no_overlap_end
        FROM exploded_violations_ca v
        LEFT OUTER JOIN exploded_ignored_violations_ca i
            ON v.org_id = i.org_id
            AND v.driver_id = i.driver_id
            AND v.date = i.date
            AND v.type = i.type
    ),
    flattened_split_violations_ca AS (
        -- Merge all the splits into a common structure
        SELECT org_id, driver_id, date, first_segment_start AS adjusted_start, first_segment_end AS adjusted_end
        FROM split_violations_ca WHERE first_segment_start IS NOT NULL

        UNION ALL

        SELECT org_id, driver_id, date, second_segment_start AS adjusted_start, second_segment_end AS adjusted_end
        FROM split_violations_ca WHERE second_segment_start IS NOT NULL

        UNION ALL

        SELECT org_id, driver_id, date, no_overlap_start AS adjusted_start, no_overlap_end AS adjusted_end
        FROM split_violations_ca WHERE no_overlap_start IS NOT NULL
    ),
    assign_group_ca AS (
        -- Assign a unique group ID to overlapping records
        SELECT
            org_id,
            driver_id,
            date,
            adjusted_start,
            adjusted_end,
            SUM(
                CASE
                WHEN adjusted_start > LAG(adjusted_end) OVER (
                    PARTITION BY org_id, driver_id, date
                    ORDER BY adjusted_start
                ) THEN 1
                ELSE 0
                END
            ) OVER (
                PARTITION BY org_id, driver_id, date
                ORDER BY adjusted_start
            ) AS group_id
        FROM flattened_split_violations_ca
    ),
    merged_violations_ca AS (
        -- Merge overlapping violations within each assigned group
        SELECT
            org_id,
            driver_id,
            date,
            MIN(adjusted_start) AS merged_start,
            MAX(adjusted_end) AS merged_end
        FROM assign_group_ca
        GROUP BY org_id, driver_id, date, group_id
    ),
    final_daily_totals_ca AS (
        -- Compute final daily violation time per driver
        -- Cap at 24 hours per driver per day to handle edge cases where violations might exceed a day
        SELECT
            org_id,
            driver_id,
            date,
            LEAST(
                SUM(TIMESTAMPDIFF(MILLISECOND, merged_start, merged_end)),
                24 * 60 * 60 * 1000  -- 24 hours in milliseconds
            ) AS total_violation_time_ms
        FROM merged_violations_ca
        GROUP BY org_id, driver_id, date
    ),
    base_ca AS (
        -- Aggregate all durations for an org per day
        SELECT
            date,
            org_id,
            SUM(total_violation_time_ms) AS violation_duration_ms
        FROM final_daily_totals_ca
        GROUP BY date, org_id
    ),
    total_drivers_ca AS (
        -- Use latest date for all eligible drivers
        SELECT
            org_id,
            COUNT(driver_id) as count_drivers
        FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers
        WHERE
            NOT is_deleted
            AND eld_exempt = 0
            AND date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform.dim_drivers)
        GROUP BY 1
    ),
    hos_violation_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            v.violation_duration_ms,
            COALESCE(d.count_drivers, 0) * 24 * 60 * 60 * 1000 AS possible_driver_ms
        FROM base_ca v
        LEFT OUTER JOIN total_drivers_ca d
            ON v.org_id = d.org_id
        JOIN latest_org_ca lo
            ON v.org_id = lo.org_id
        WHERE v.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
    ),
    active_unassigned_logs_us AS (
        SELECT *
        FROM compliancedb_shards.driver_hos_logs
        WHERE
            driver_id = 0
            AND (log_proto['active_to_inactive_changed_at_ms'] IS NULL OR log_proto['active_to_inactive_changed_at_ms'] = 0)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
            AND (log_type & 65536) != 0
    ),
    active_unassigned_logs_and_end_time_us AS (
        SELECT
            *,
            log_at AS start_time,
            LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
        FROM active_unassigned_logs_us
    ),
    active_unassigned_logs_and_duration_us AS (
        SELECT
            *,
            (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS unassigned_driver_time_hours
        FROM active_unassigned_logs_and_end_time_us
        WHERE status_code = 2
    ),
    unassigned_hos_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ul.unassigned_driver_time_hours
        FROM active_unassigned_logs_and_duration_us ul
        JOIN latest_org_us lo
            ON ul.org_id = lo.org_id
    ),
    active_unassigned_logs_eu AS (
        SELECT *
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0`
        WHERE
            driver_id = 0
            AND (log_proto['active_to_inactive_changed_at_ms'] IS NULL OR log_proto['active_to_inactive_changed_at_ms'] = 0)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
            AND (log_type & 65536) != 0

        UNION ALL

        SELECT *
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliance-shard-1db/compliancedb/driver_hos_logs_v0`
        WHERE
            driver_id = 0
            AND (log_proto['active_to_inactive_changed_at_ms'] IS NULL OR log_proto['active_to_inactive_changed_at_ms'] = 0)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
            AND (log_type & 65536) != 0
    ),
    active_unassigned_logs_and_end_time_eu AS (
        SELECT
            *,
            log_at AS start_time,
            LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
        FROM active_unassigned_logs_eu
    ),
    active_unassigned_logs_and_duration_eu AS (
        SELECT
            *,
            (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS unassigned_driver_time_hours
        FROM active_unassigned_logs_and_end_time_eu
        WHERE status_code = 2
    ),
    unassigned_hos_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ul.unassigned_driver_time_hours
        FROM active_unassigned_logs_and_duration_eu ul
        JOIN latest_org_eu lo
            ON ul.org_id = lo.org_id
    ),
    active_unassigned_logs_ca AS (
        SELECT *
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0`
        WHERE
            driver_id = 0
            AND (log_proto['active_to_inactive_changed_at_ms'] IS NULL OR log_proto['active_to_inactive_changed_at_ms'] = 0)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
            AND (log_type & 65536) != 0
    ),
    active_unassigned_logs_and_end_time_ca AS (
        SELECT
            *,
            log_at AS start_time,
            LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
        FROM active_unassigned_logs_ca
    ),
    active_unassigned_logs_and_duration_ca AS (
        SELECT
            *,
            (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS unassigned_driver_time_hours
        FROM active_unassigned_logs_and_end_time_ca
        WHERE status_code = 2
    ),
    unassigned_hos_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ul.unassigned_driver_time_hours
        FROM active_unassigned_logs_and_duration_ca ul
        JOIN latest_org_ca lo
            ON ul.org_id = lo.org_id
    ),
    active_assigned_logs_us AS (
        SELECT *
        FROM compliancedb_shards.driver_hos_logs
        WHERE
            driver_id != 0
            AND (log_proto['event_record_status'] = 0 OR log_proto['event_record_status'] = 3)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
    ),
    active_assigned_logs_and_end_time_us AS (
        SELECT
            *,
            log_at AS start_time,
            LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
        FROM active_assigned_logs_us
    ),
    active_assigned_logs_and_duration_us AS (
        SELECT
            *,
            (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS assigned_driver_time_hours
        FROM active_assigned_logs_and_end_time_us
        WHERE status_code = 2
    ),
    assigned_hos_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ul.assigned_driver_time_hours
        FROM active_assigned_logs_and_duration_us ul
        JOIN latest_org_us lo
            ON ul.org_id = lo.org_id
        JOIN drivers_us d
            ON lo.sam_number = d.sam_number
            AND lo.account_id = d.account_id
            AND ul.driver_id = d.driver_id
    ),
    active_assigned_logs_eu AS (
        SELECT *
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0`
        WHERE
            driver_id != 0
            AND (log_proto['event_record_status'] = 0 OR log_proto['event_record_status'] = 3)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')

        UNION ALL

        SELECT *
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliance-shard-1db/compliancedb/driver_hos_logs_v0`
        WHERE
            driver_id != 0
            AND (log_proto['event_record_status'] = 0 OR log_proto['event_record_status'] = 3)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
    ),
    active_assigned_logs_and_end_time_eu AS (
        SELECT
            *,
            log_at AS start_time,
            LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
        FROM active_assigned_logs_eu
    ),
    active_assigned_logs_and_duration_eu AS (
        SELECT
            *,
            (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS assigned_driver_time_hours
        FROM active_assigned_logs_and_end_time_eu
        WHERE status_code = 2
    ),
    assigned_hos_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ul.assigned_driver_time_hours
        FROM active_assigned_logs_and_duration_eu ul
        JOIN latest_org_eu lo
            ON ul.org_id = lo.org_id
        JOIN drivers_eu d
            ON lo.sam_number = d.sam_number
            AND lo.account_id = d.account_id
            AND ul.driver_id = d.driver_id
    ),
    active_assigned_logs_ca AS (
        SELECT *
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0`
        WHERE
            driver_id != 0
            AND (log_proto['event_record_status'] = 0 OR log_proto['event_record_status'] = 3)
            AND date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            -- Exclude annotated records (annotated segments have non-empty notes)
            AND (notes IS NULL OR TRIM(notes) = '')
    ),
    active_assigned_logs_and_end_time_ca AS (
        SELECT
            *,
            log_at AS start_time,
            LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
        FROM active_assigned_logs_ca
    ),
    active_assigned_logs_and_duration_ca AS (
        SELECT
            *,
            (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / (60.0 * 60) AS assigned_driver_time_hours
        FROM active_assigned_logs_and_end_time_ca
        WHERE status_code = 2
    ),
    assigned_hos_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ul.assigned_driver_time_hours
        FROM active_assigned_logs_and_duration_ca ul
        JOIN latest_org_ca lo
            ON ul.org_id = lo.org_id
        JOIN drivers_ca d
            ON lo.sam_number = d.sam_number
            AND lo.account_id = d.account_id
            AND ul.driver_id = d.driver_id
    ),
    mpg_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            IF(mpg.weighted_mpg between 2 and 200, mpg.weighted_mpg, NULL) AS mpge
        FROM dataengineering.vehicle_mpg_lookback mpg
        JOIN latest_org_us lo
            ON mpg.org_id = lo.org_id
        JOIN active_vg_us avg
            ON lo.sam_number = avg.sam_number
            AND lo.account_id = avg.account_id
            AND mpg.device_id = avg.device_id
        WHERE
            mpg.date = '{END_OF_PERIOD}'
            AND mpg.lookback_window = '28d' -- 28 day lookback
    ),
    mpg_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            IF(mpg.weighted_mpg between 2 and 200, mpg.weighted_mpg, NULL) AS mpge
        FROM data_tools_delta_share.dataengineering.vehicle_mpg_lookback mpg
        JOIN latest_org_eu lo
            ON mpg.org_id = lo.org_id
        JOIN active_vg_eu avg
            ON lo.sam_number = avg.sam_number
            AND lo.account_id = avg.account_id
            AND mpg.device_id = avg.device_id
        WHERE
            mpg.date = '{END_OF_PERIOD}'
            AND mpg.lookback_window = '28d' -- 28 day lookback
    ),
    mpg_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            IF(mpg.weighted_mpg between 2 and 200, mpg.weighted_mpg, NULL) AS mpge
        FROM data_tools_delta_share_ca.dataengineering.vehicle_mpg_lookback mpg
        JOIN latest_org_ca lo
            ON mpg.org_id = lo.org_id
        JOIN active_vg_ca avg
            ON lo.sam_number = avg.sam_number
            AND lo.account_id = avg.account_id
            AND mpg.device_id = avg.device_id
        WHERE
            mpg.date = '{END_OF_PERIOD}'
            AND mpg.lookback_window = '28d' -- 28 day lookback
    ),
    vehicle_mix_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ddv.device_id,
            fg.name AS fuel_group_name,
            pt.name AS powertrain_name
        FROM product_analytics_staging.dim_device_vehicle_properties ddv
        JOIN datamodel_core.dim_devices dd
            ON ddv.org_id = dd.org_id
            AND ddv.device_id = dd.device_id
        JOIN latest_org_us lo
            ON ddv.org_id = lo.org_id
        JOIN definitions.properties_fuel_fuelgroup fg
            ON ddv.fuel_group = fg.id
        JOIN definitions.properties_fuel_powertrain pt
            ON ddv.powertrain = pt.id
        WHERE
            ddv.date = GREATEST('{END_OF_PERIOD}', '2025-04-01')
            AND dd.date = '{END_OF_PERIOD}'
    ),
    vehicle_mix_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ddv.device_id,
            fg.name AS fuel_group_name,
            pt.name AS powertrain_name
        FROM data_tools_delta_share.product_analytics_staging.dim_device_vehicle_properties ddv
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
            ON ddv.org_id = dd.org_id
            AND ddv.device_id = dd.device_id
        JOIN latest_org_eu lo
            ON ddv.org_id = lo.org_id
        JOIN definitions.properties_fuel_fuelgroup fg
            ON ddv.fuel_group = fg.id
        JOIN definitions.properties_fuel_powertrain pt
            ON ddv.powertrain = pt.id
        WHERE
            ddv.date = GREATEST('2025-09-26', '{END_OF_PERIOD}')
            AND dd.date = '{END_OF_PERIOD}'
    ),
    vehicle_mix_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            ddv.device_id,
            fg.name AS fuel_group_name,
            pt.name AS powertrain_name
        FROM data_tools_delta_share_ca.product_analytics_staging.dim_device_vehicle_properties ddv
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dd
            ON ddv.org_id = dd.org_id
            AND ddv.device_id = dd.device_id
        JOIN latest_org_ca lo
            ON ddv.org_id = lo.org_id
        JOIN definitions.properties_fuel_fuelgroup fg
            ON ddv.fuel_group = fg.id
        JOIN definitions.properties_fuel_powertrain pt
            ON ddv.powertrain = pt.id
        WHERE
            ddv.date = GREATEST('2025-09-26', '{END_OF_PERIOD}')
            AND dd.date = '{END_OF_PERIOD}'
    ),
    split_berth_us AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            hos.driver_id,
            mv.merged_start
        FROM compliancedb_shards.driver_hos_logs hos
        JOIN merged_violations_us mv
            ON hos.org_id = mv.org_id
            AND hos.driver_id = mv.driver_id
            AND hos.date = mv.date
        JOIN latest_org_us lo
            ON hos.org_id = lo.org_id
        WHERE
            hos.status_code = 2
            AND hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )
    ),
    split_berth_eu AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            hos.driver_id,
            mv.merged_start
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliance-shard-1db/compliancedb/driver_hos_logs_v0` hos
        JOIN merged_violations_eu mv
            ON hos.org_id = mv.org_id
            AND hos.driver_id = mv.driver_id
            AND hos.date = mv.date
        JOIN latest_org_eu lo
            ON hos.org_id = lo.org_id
        WHERE
            hos.status_code = 2
            AND hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )

        UNION ALL

        SELECT
            lo.sam_number,
            lo.account_id,
            hos.driver_id,
            mv.merged_start
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0` hos
        JOIN merged_violations_eu mv
            ON hos.org_id = mv.org_id
            AND hos.driver_id = mv.driver_id
            AND hos.date = mv.date
        JOIN latest_org_eu lo
            ON hos.org_id = lo.org_id
        WHERE
            hos.status_code = 2
            AND hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )
    ),
    split_berth_ca AS (
        SELECT
            lo.sam_number,
            lo.account_id,
            hos.driver_id,
            mv.merged_start
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0` hos
        JOIN merged_violations_ca mv
            ON hos.org_id = mv.org_id
            AND hos.driver_id = mv.driver_id
            AND hos.date = mv.date
        JOIN latest_org_ca lo
            ON hos.org_id = lo.org_id
        WHERE
            hos.status_code = 2
            AND hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )
    ),
    hos_logs_with_next_odometer_us AS (
        -- Get all HOS logs with GPS odometer and compute next log's odometer
        -- LEAD() computed on ALL logs so we get the true next log regardless of status
        SELECT
            hos.org_id,
            hos.vehicle_id,
            hos.driver_id,
            hos.log_at,
            hos.status_code,
            hos.log_proto.gps_odometer_meters AS start_odometer,
            LEAD(hos.log_proto.gps_odometer_meters) OVER (
                PARTITION BY hos.org_id, hos.vehicle_id
                ORDER BY hos.log_at
            ) AS end_odometer
        FROM compliancedb_shards.driver_hos_logs hos
        JOIN datamodel_core.dim_devices dd
            ON hos.org_id = dd.org_id
            AND hos.vehicle_id = dd.device_id
            AND hos.date = dd.date
        WHERE
            hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND dd.device_type = 'VG - Vehicle Gateway'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )
    ),
    yard_move_distance_us AS (
        -- Filter to Yard Move logs and calculate distance from GPS odometer difference
        SELECT
            lo.sam_number,
            lo.account_id,
            (COALESCE(ym.end_odometer, 0) - COALESCE(ym.start_odometer, 0)) * 0.000621371 AS distance_miles
        FROM hos_logs_with_next_odometer_us ym
        JOIN latest_org_us lo
            ON ym.org_id = lo.org_id
        WHERE
            ym.status_code = 5  -- Yard Move
            AND ym.end_odometer > ym.start_odometer  -- Valid positive distance
    ),
    total_movement_us AS (
        SELECT
            sam_number,
            account_id,
            distance_miles
        FROM trips_base_us
    ),
    hos_logs_with_next_odometer_eu AS (
        -- Get all HOS logs with GPS odometer and compute next log's odometer
        -- LEAD() computed on ALL logs so we get the true next log regardless of status
        SELECT
            hos.org_id,
            hos.vehicle_id,
            hos.driver_id,
            hos.log_at,
            hos.status_code,
            hos.log_proto.gps_odometer_meters AS start_odometer,
            LEAD(hos.log_proto.gps_odometer_meters) OVER (
                PARTITION BY hos.org_id, hos.vehicle_id
                ORDER BY hos.log_at
            ) AS end_odometer
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0` hos
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
            ON hos.org_id = dd.org_id
            AND hos.vehicle_id = dd.device_id
            AND hos.date = dd.date
        WHERE
            hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND dd.device_type = 'VG - Vehicle Gateway'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )

        UNION ALL

        SELECT
            hos.org_id,
            hos.vehicle_id,
            hos.driver_id,
            hos.log_at,
            hos.status_code,
            hos.log_proto.gps_odometer_meters AS start_odometer,
            LEAD(hos.log_proto.gps_odometer_meters) OVER (
                PARTITION BY hos.org_id, hos.vehicle_id
                ORDER BY hos.log_at
            ) AS end_odometer
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-compliance-shard-1db/compliancedb/driver_hos_logs_v0` hos
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
            ON hos.org_id = dd.org_id
            AND hos.vehicle_id = dd.device_id
            AND hos.date = dd.date
        WHERE
            hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND dd.device_type = 'VG - Vehicle Gateway'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )
    ),
    yard_move_distance_eu AS (
        -- Filter to Yard Move logs and calculate distance from GPS odometer difference
        SELECT
            lo.sam_number,
            lo.account_id,
            (COALESCE(ym.end_odometer, 0) - COALESCE(ym.start_odometer, 0)) * 0.000621371 AS distance_miles
        FROM hos_logs_with_next_odometer_eu ym
        JOIN latest_org_eu lo
            ON ym.org_id = lo.org_id
        WHERE
            ym.status_code = 5  -- Yard Move
            AND ym.end_odometer > ym.start_odometer  -- Valid positive distance
    ),
    total_movement_eu AS (
        SELECT
            sam_number,
            account_id,
            distance_miles
        FROM trips_base_eu
    ),
    hos_logs_with_next_odometer_ca AS (
        -- Get all HOS logs with GPS odometer and compute next log's odometer
        -- LEAD() computed on ALL logs so we get the true next log regardless of status
        SELECT
            hos.org_id,
            hos.vehicle_id,
            hos.driver_id,
            hos.log_at,
            hos.status_code,
            hos.log_proto.gps_odometer_meters AS start_odometer,
            LEAD(hos.log_proto.gps_odometer_meters) OVER (
                PARTITION BY hos.org_id, hos.vehicle_id
                ORDER BY hos.log_at
            ) AS end_odometer
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-compliancedb/compliancedb/driver_hos_logs_v0` hos
        JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dd
            ON hos.org_id = dd.org_id
            AND hos.vehicle_id = dd.device_id
            AND hos.date = dd.date
        WHERE
            hos.date BETWEEN '{START_OF_PERIOD}' AND '{END_OF_PERIOD}'
            AND dd.device_type = 'VG - Vehicle Gateway'
            AND (
                -- Exclude logs marked as inactive
                hos.log_proto.event_record_status <> 2
                OR hos.log_proto.event_record_status IS NULL
            )
    ),
    yard_move_distance_ca AS (
        -- Filter to Yard Move logs and calculate distance from GPS odometer difference
        SELECT
            lo.sam_number,
            lo.account_id,
            (COALESCE(ym.end_odometer, 0) - COALESCE(ym.start_odometer, 0)) * 0.000621371 AS distance_miles
        FROM hos_logs_with_next_odometer_ca ym
        JOIN latest_org_ca lo
            ON ym.org_id = lo.org_id
        WHERE
            ym.status_code = 5  -- Yard Move
            AND ym.end_odometer > ym.start_odometer  -- Valid positive distance
    ),
    total_movement_ca AS (
        -- Calculate total distance traveled from trips
        SELECT
            sam_number,
            account_id,
            distance_miles
        FROM trips_base_ca
    ),
    {METRIC_CTES},
    final AS (
        SELECT DISTINCT
        {COALESCE_STATEMENTS},
        {COALESCE_METRIC_STATEMENTS}
        FROM routes_aggregated routes_aggregated
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
        account_renewal_12mo,
        MAX(routes) AS routes,
        MAX(completed_routes) AS completed_routes,
        MAX(vg_trips) AS vg_trips,
        MAX(active_vgs) AS active_vgs,
        MAX(idle_duration_ms) AS idle_duration_ms,
        MAX(on_duration_ms) AS on_duration_ms,
        MAX(unproductive_idle_duration_ms) AS unproductive_idle_duration_ms,
        MAX(total_drivers) AS total_drivers,
        MAX(total_drivers_using_app_month) AS total_drivers_using_app_month,
        MAX(total_drivers_using_app) AS total_drivers_using_app,
        MAX(violation_duration_ms) AS violation_duration_ms,
        MAX(possible_driver_ms) AS possible_driver_ms,
        MAX(unassigned_driver_time_hours) AS unassigned_driver_time_hours,
        MAX(assigned_driver_time_hours) AS assigned_driver_time_hours,
        MAX(unassigned_driver_time_hours) + MAX(assigned_driver_time_hours) AS driver_time_hours,
        MAX(p50_mpg) AS p50_mpg,
        MAX(vehicles_ev) AS vehicles_ev,
        MAX(vehicles_hybrid) AS vehicles_hybrid,
        MAX(vehicles_ice) AS vehicles_ice,
        MAX(sleeper_berth_split_violations) AS sleeper_berth_split_violations,
        MAX(yard_move_miles) AS yard_move_miles,
        MAX(total_miles) AS total_miles
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
        account_renewal_12mo
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
    "account_renewal_12mo": DimensionConfig(name="account_renewal_12mo", data_type=DimensionDataType.BOOLEAN, default_value="false"),
}
