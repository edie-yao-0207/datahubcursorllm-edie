from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import build_table_description

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""A dataset containing a daily summary of safety domain activity.""",
        row_meaning="""Each row represents an API call against the safety domain""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date on which the API request was made"}},
        {"name": "org_id", "type": "double", "nullable": False, "metadata": {"comment": "The Internal ID for the customer`s Samsara org."}},
        {"name": "timestamp", "type": "timestamp", "nullable": False, "metadata": {"comment": "Timestamp of record"}},
        {"name": "api_route", "type": "string", "nullable": False, "metadata": {"comment": "Event type in the API"}},
        {
            "name": "sam_number",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "Identification of the account"},
        },
        {"name": "api_domain", "type": "string", "nullable": False, "metadata": {"comment": "Domain corresponding to the API record"}},
        {"name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2:datamodel_platform_silver.stg_cloud_routes",
    ],
    primary_keys=["date", "timestamp", "org_id", "api_route", "api_domain", "region"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_api_usage_global"),
        NonNullDQCheck(name="dq_non_null_safety_api_usage_global", non_null_columns=["date", "org_id", "timestamp", "region"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_safety_api_usage_global", primary_keys=["date", "timestamp", "org_id", "api_route", "api_domain", "region"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=8,
        driver_instance_type=InstanceType.MD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
)
def safety_api_usage_global(context: AssetExecutionContext) -> str:

    query = """
    WITH
    global_orgs AS (
        SELECT
            date,
            org_id,
            sam_number,
            internal_type,
            account_first_purchase_date
        FROM datamodel_core.dim_organizations
        WHERE
            date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'

        UNION

        SELECT
            date,
            org_id,
            sam_number,
            internal_type,
            account_first_purchase_date
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`
        WHERE
            date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'

        UNION

        SELECT
            date,
            org_id,
            sam_number,
            internal_type,
            account_first_purchase_date
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations
        WHERE
            date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    ),
    us_mobile_logs_base AS (
        SELECT  ml.date,
                ml.org_id,
                ml.timestamp,
                LOWER(ml.event_type) as api_route,
                org.sam_number,
                'Safety Driver App' AS api_domain
        FROM datastreams_history.mobile_logs ml -- HERE WE GRAB MOBILE LOG DATA TO EXTRACT CUSTOMERS WHO ACCESS SAFETY RELATED DRIVER APP ROUTES
        INNER JOIN datamodel_core.dim_organizations AS org -- this join is to find non-internal records
        ON org.org_id = ml.org_id
        AND ml.date = org.date
        WHERE TRUE -- ARBITRARY FOR READABILITY
        AND ml.event_type = "GLOBAL_NAVIGATE" -- SPECIFIED TO PULL CORRECT ROUTE LOCATIONS
        AND org.internal_type = 0 -- NON-INTERNAL
        AND org.account_first_purchase_date IS NOT NULL -- HAS HAD AT LEAST ONE PURCHASE
        AND ( -- THESE WE GET WITH HELP FROM JOEL DAVIS; THEY ARE MEANT TO BE "SAFETY" RELATED DRIVER APP ROUTES
            GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/coaching%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/leaderboard/:modal?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safety/safetyScoreExplanation%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safetyWebview/:safetyWebviewTabName?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboard/:scoreDashboardTabIdx(\d+)?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboardBreakdown/:ecoDrivingFactor(\d+)?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/leaderboard/:modal%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safetyWebview/:safetyWebviewTabName%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboard/:scoreDashboardTabId%'
        )
        AND ml.DATE BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- PULL RELAVANT DATES
        GROUP BY 1,2,3,4,5,6  -- LIKE "SELECT DISTINCT", BUT FASTER
    ),

    -- THIS FINDS THE UNIQUE CUSTOMERS THAT ACCESS SAFETY RELATED API ENDPOINTS
    us_api_logs_base AS (
        SELECT al.date,
            al.org_id,
            al.timestamp,
            LOWER(al.path_template) as api_route,
            org.sam_number,
            'Safety API' AS api_domain
        FROM datastreams_history.api_logs al -- HERE WE GRAB API LOG DATA TO EXTRACT CUSTOMERS WHO ACCESS SAFETY RELATED API ENDPOINTS
        INNER JOIN datamodel_core.dim_organizations AS org -- this join is to find non-internal records
        ON org.org_id = al.org_id
        AND al.date = org.date
        LEFT JOIN data_analytics.api_mappings am -- THIS JOIN IS TO ENSURE WE ARE EXAMINING SAFETY-RELATED API ENDPOINTS
        ON al.http_method = am.http_method
        AND al.path_template = am.path_template
        WHERE org.internal_type = 0 -- EXTERNAL ONLY
        AND org.account_first_purchase_date IS NOT NULL -- HAS HAD AT LEAST ONE PURCHASE
        AND (am.business_unit = 'Video-based Safety' OR al.path_template like '%coach%') -- SAFETY RELATED ENDPOINTS
        AND al.DATE BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- PULL RELAVANT DATES
        GROUP BY 1,2,3,4,5,6  -- LIKE "SELECT DISTINCT", BUT FASTER
    ),
    eu_mobile_logs_base AS (
        SELECT  ml.date,
                ml.org_id,
                ml.timestamp,
                LOWER(ml.event_type) as api_route,
                org.sam_number,
                'Safety Driver App' AS api_domain
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs` AS ml -- HERE WE GRAB MOBILE LOG DATA TO EXTRACT CUSTOMERS WHO ACCESS SAFETY RELATED DRIVER APP ROUTES
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` AS org -- this join is to find non-internal records
        ON org.org_id = ml.org_id
        AND ml.date = org.date
        WHERE TRUE -- ARBITRARY FOR READABILITY
        AND ml.event_type = "GLOBAL_NAVIGATE" -- SPECIFIED TO PULL CORRECT ROUTE LOCATIONS
        AND org.internal_type = 0 -- NON-INTERNAL
        AND org.account_first_purchase_date IS NOT NULL -- HAS HAD AT LEAST ONE PURCHASE
        AND ( -- THESE WE GET WITH HELP FROM JOEL DAVIS; THEY ARE MEANT TO BE "SAFETY" RELATED DRIVER APP ROUTES
            GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/coaching%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/leaderboard/:modal?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safety/safetyScoreExplanation%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safetyWebview/:safetyWebviewTabName?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboard/:scoreDashboardTabIdx(\d+)?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboardBreakdown/:ecoDrivingFactor(\d+)?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/leaderboard/:modal%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safetyWebview/:safetyWebviewTabName%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboard/:scoreDashboardTabId%'
        )
        AND ml.DATE BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- PULL RELAVANT DATES
        GROUP BY 1,2,3,4,5,6  -- LIKE "SELECT DISTINCT", BUT FASTER
    ),

    -- THIS FINDS THE UNIQUE CUSTOMERS THAT ACCESS SAFETY RELATED API ENDPOINTS
    eu_api_logs_base AS (
        SELECT al.date,
            al.org_id,
            al.timestamp,
            LOWER(al.path_template) as api_route,
            org.sam_number,
            'Safety API' AS api_domain
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/api_logs` AS al -- HERE WE GRAB API LOG DATA TO EXTRACT CUSTOMERS WHO ACCESS SAFETY RELATED API ENDPOINTS
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` AS org -- this join is to find non-internal records
        ON org.org_id = al.org_id
        AND al.date = org.date
        LEFT JOIN data_analytics.api_mappings am -- THIS JOIN IS TO ENSURE WE ARE EXAMINING SAFETY-RELATED API ENDPOINTS
        ON al.http_method = am.http_method
        AND al.path_template = am.path_template
        WHERE org.internal_type = 0 -- EXTERNAL ONLY
        AND org.account_first_purchase_date IS NOT NULL -- HAS HAD AT LEAST ONE PURCHASE
        AND (am.business_unit = 'Video-based Safety' OR al.path_template like '%coach%') -- SAFETY RELATED ENDPOINTS
        AND al.DATE BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- PULL RELAVANT DATES
        GROUP BY 1,2,3,4,5,6  -- LIKE "SELECT DISTINCT", BUT FASTER
    ),
    ca_mobile_logs_base AS (
        SELECT  ml.date,
                ml.org_id,
                ml.timestamp,
                LOWER(ml.event_type) as api_route,
                org.sam_number,
                'Safety Driver App' AS api_domain
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/mobile_logs` AS ml -- HERE WE GRAB MOBILE LOG DATA TO EXTRACT CUSTOMERS WHO ACCESS SAFETY RELATED DRIVER APP ROUTES
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations AS org -- this join is to find non-internal records
        ON org.org_id = ml.org_id
        AND ml.date = org.date
        WHERE TRUE -- ARBITRARY FOR READABILITY
        AND ml.event_type = "GLOBAL_NAVIGATE" -- SPECIFIED TO PULL CORRECT ROUTE LOCATIONS
        AND org.internal_type = 0 -- NON-INTERNAL
        AND org.account_first_purchase_date IS NOT NULL -- HAS HAD AT LEAST ONE PURCHASE
        AND ( -- THESE WE GET WITH HELP FROM JOEL DAVIS; THEY ARE MEANT TO BE "SAFETY" RELATED DRIVER APP ROUTES
            GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/coaching%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/leaderboard/:modal?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safety/safetyScoreExplanation%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safetyWebview/:safetyWebviewTabName?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboard/:scoreDashboardTabIdx(\d+)?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboardBreakdown/:ecoDrivingFactor(\d+)?%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/leaderboard/:modal%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/safetyWebview/:safetyWebviewTabName%'
            OR GET_JSON_OBJECT(json_params, "$.cleanDestination") LIKE '%/driver/scoreDashboard/:scoreDashboardTabId%'
        )
        AND ml.DATE BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- PULL RELAVANT DATES
        GROUP BY 1,2,3,4,5,6  -- LIKE "SELECT DISTINCT", BUT FASTER
    ),

    -- THIS FINDS THE UNIQUE CUSTOMERS THAT ACCESS SAFETY RELATED API ENDPOINTS
    ca_api_logs_base AS (
        SELECT al.date,
            al.org_id,
            al.timestamp,
            LOWER(al.path_template) as api_route,
            org.sam_number,
            'Safety API' AS api_domain
        FROM delta.`s3://samsara-ca-data-streams-delta-lake/api_logs` AS al -- HERE WE GRAB API LOG DATA TO EXTRACT CUSTOMERS WHO ACCESS SAFETY RELATED API ENDPOINTS
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations AS org -- this join is to find non-internal records
        ON org.org_id = al.org_id
        AND al.date = org.date
        LEFT JOIN data_analytics.api_mappings am -- THIS JOIN IS TO ENSURE WE ARE EXAMINING SAFETY-RELATED API ENDPOINTS
        ON al.http_method = am.http_method
        AND al.path_template = am.path_template
        WHERE org.internal_type = 0 -- EXTERNAL ONLY
        AND org.account_first_purchase_date IS NOT NULL -- HAS HAD AT LEAST ONE PURCHASE
        AND (am.business_unit = 'Video-based Safety' OR al.path_template like '%coach%') -- SAFETY RELATED ENDPOINTS
        AND al.DATE BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- PULL RELAVANT DATES
        GROUP BY 1,2,3,4,5,6  -- LIKE "SELECT DISTINCT", BUT FASTER
    ),
    global_cloud_routes AS (
        -- THIS FINDS THE UNIQUE CUSTOMERS THAT ACCESS SAFETY RELATED CLOUD DASHBOARD PAGES
        SELECT mr.date,
                mr.org_id,
                mr.time as timestamp,
                LOWER(mr.routename) as api_route,
                org.sam_number,
                CASE -- these are defined using Superuser functionality in the Cloud Dashbaord and being on the specified pages
                    WHEN mr.routename IN ('coaching_session','coaching_sessions') THEN 'Coaching Session'
                    WHEN mr.routename = 'fleet_safety_inbox' THEN 'Safety Inbox'
                    WHEN mr.routename = 'fleet_safety_aggregated_inbox' THEN 'Safety Aggregated Inbox'
                    WHEN mr.routename = 'fleet_video_library_page' THEN 'Video Library Page'
                    WHEN mr.routename = 'tag_safety_report' THEN 'Safety Overview Dashboard'
                    WHEN mr.routename = 'safety_dashboard_table' THEN 'Driver Performance Dashboard'
                    WHEN mr.routename = 'coaching_timeliness_report' THEN 'Coaching Performance Dashboard'
                    WHEN mr.routename = 'event_resolution_report' THEN 'Event Resolution Dashboard'
                    WHEN mr.routename = 'fleet_cameras_overview' THEN 'Cameras Page'
                    WHEN mr.routename = 'camera_health_report' THEN 'Camera Health Report'
                    WHEN mr.routename = 'recognition' THEN 'Recognition'
                END AS api_domain,
                CASE WHEN mr.mp_current_url LIKE '%cloud.eu.samsara.com%' THEN 'eu-west-1' WHEN mr.mp_current_url LIKE '%cloud.ca.samsara.com%' THEN 'ca-central-1' ELSE 'us-west-2' END as region
        FROM datamodel_platform_silver.stg_cloud_routes mr -- here we grab cloud route data to extract customers who access Safety related cloud pages
        INNER JOIN global_orgs org -- understand the SAMs associated with the orgs
            ON mr.org_id = org.org_id
            AND mr.date = org.`date`
        WHERE TRUE -- arbitrary for readability
            AND mr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
            AND mr.is_customer_email = TRUE -- exclude records produced by a Samsara employee
            AND mr.routename IN ('coaching_session','coaching_sessions','fleet_safety_inbox','fleet_safety_aggregated_inbox','fleet_video_library_page','tag_safety_report',
                                'safety_dashboard_table','coaching_timeliness_report','event_resolution_report','fleet_cameras_overview','camera_health_report','recognition') -- Safety related Cloud Dash routes
            AND org.sam_number IS NOT NULL
        GROUP BY 1,2,3,4,5,6,7
    ),
    global_modal_views AS (
    -- THIS FINDS THE UNIQUE CUSTOMERS THAT ACCESS THE VIDEO RETRIEVAL MODAL WITH THE CLOUD DASHBOARD
    SELECT
            FROM_UNIXTIME(mr.time, 'yyyy-MM-dd') AS date,
            coalesce(mr.orgid, CAST(regexp_extract(mr.mp_current_url, '/o/([0-9]+)/') AS BIGINT)) as org_id,
            CAST(mr.time AS TIMESTAMP) as timestamp,
            LOWER(mr.routename) as api_route,
            org.sam_number,
            'Video Retrieval Modal' AS api_domain,
             CASE WHEN mr.mp_current_url LIKE '%cloud.eu.samsara.com%' THEN 'eu-west-1' WHEN mr.mp_current_url LIKE '%cloud.ca.samsara.com%' THEN 'ca-central-1' ELSE 'us-west-2' END as region
    FROM mixpanel_samsara.vr4_modalbeingviewed_count mr -- HERE WE GRAB MIXPANEL DATA TO EXTRACT CUSTOMERS WHO ACCESS THE VIDEO RETRIEVAL MODAL WITH THE CLOUD DASHBOARD
    INNER JOIN global_orgs org -- understand the SAMs associated with the orgs
        ON mr.orgid = org.org_id
        AND DATE(FROM_UNIXTIME(mr.time, 'yyyy-MM-dd')) = org.`date`
    WHERE TRUE -- ARBITRARY FOR READABILITY
        AND FROM_UNIXTIME(mr.time, 'yyyy-MM-dd') BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- GRAB RELAVANT DATES
        AND COALESCE(mr.mp_user_id, mr.mp_device_id) NOT LIKE '%@samsara.com' -- EXCLUDE RECORDS PRODUCED BY A SAMSARA EMPLOYEE
    GROUP BY 1,2,3,4,5,6,7
    )

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        'us-west-2' as region
    FROM us_mobile_logs_base

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        'us-west-2' as region
    FROM us_api_logs_base

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        'eu-west-1' as region
    FROM eu_mobile_logs_base

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        'eu-west-1' as region
    FROM eu_api_logs_base

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        'ca-central-1' as region
    FROM ca_mobile_logs_base

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        'ca-central-1' as region
    FROM ca_api_logs_base

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        region
    FROM global_cloud_routes

    UNION

    SELECT
        date,
        org_id,
        timestamp,
        api_route,
        sam_number,
        api_domain,
        region
    FROM global_modal_views
    """
    return query
