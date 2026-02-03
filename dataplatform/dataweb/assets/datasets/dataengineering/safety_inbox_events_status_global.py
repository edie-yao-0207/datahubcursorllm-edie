from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    CA_SER_LICENCES_CTE,
    DATAENGINEERING,
    EU_SER_LICENCES_CTE,
    FRESHNESS_SLO_12PM_PST,
    US_SER_LICENCES_CTE,
    AWSRegion,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import build_table_description

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""(Global) A dataset containing safety inbox events and associated statuses.""",
        row_meaning="""Safety inbox event, along with associated status""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {
            "name": "date",
            "type": "string",
            "nullable": False,
            "metadata": {
                "comment": "The date of the safety inbox event, in `YYYY-mm-dd` format. `date` is derived from `event_ms`."
            },
        },
        {
            "name": "event_id",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "ID of the event that was triggered by the device. May not be unique, due to int overflow issues in the current safety events framework."
            },
        },
        {
            "name": "detection_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "The detection label that was applied to the safety event"
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
            "name": "sam_number",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "This is the internal Samsara Customer account ID."
            },
        },
        {
            "name": "account_size_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "account_industry",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Queried from edw.silver.dim_customer. Industry classification of the account."
            },
        },
        {
            "name": "account_arr_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "ARR of an org_id from the previously publicly reporting quarter in the buckets: <0 0 - 100k 100k - 500K, 500K - 1M 1M+"
            },
        },
        {
            "name": "has_ser_license",
            "type": "integer",
            "nullable": False,
            "metadata": {
                "comment": "A flag indicating whether the customer is paying for SER. 1==true and 0==false"
            },
        },
        {
            "name": "device_id",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "The ID of the customer device that the data belongs to"
            },
        },
        {
            "name": "event_ms",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "Unixtime (in milliseconds) that the event was triggered"
            },
        },
        {
            "name": "is_coaching_assigned",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether coaching has been assigned or not"},
        },
        {"name": "is_car_viewed", "type": "integer", "nullable": False, "metadata": {"comment": "Whether car has been viewed or not"}},
        {
            "name": "is_customer_dismissed",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether customer has dismissed the inbox event or not"},
        },
        {"name": "is_viewed", "type": "integer", "nullable": False, "metadata": {"comment": "Whether the inbox event has been viewed or not"}},
        {"name": "is_actioned", "type": "integer", "nullable": False, "metadata": {"comment": "Whether the inbox event has been actioned or not"}},
        {
            "name": "is_auto_coached",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether the inbox event has been auto coached or not"},
        },
        {
            "name": "is_critical_event",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether the event is deemed critical or not"},
        },
        {
            "name": "is_useful",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether the event is deemed useful or not"},
        },
        {"name": "days_to_resolve", "type": "double", "nullable": True, "metadata": {"comment": "Days needed to resolve inbox event"}},
        {
            "name": "has_safety_event_review",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether the safety event has a review or not"},
        },
        {
            "name": "is_coached",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether the event has been coached or not"},
        },
        {"name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
        {"name": "coaching_status", "type": "string", "nullable": True, "metadata": {"comment": "Coaching status on safety event"}},
        {"name": "auto_needs_coaching", "type": "integer", "nullable": False, "metadata": {"comment": "Whether auto needs coaching has been assigned or not"}},
    ],
    upstreams=[],
    primary_keys=["date", "event_id", "event_ms", "org_id", "device_id", "region"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_inbox_events_status_global"),
        NonNullDQCheck(name="dq_non_null_safety_inbox_events_status_global", non_null_columns=["date", "org_id", "is_coaching_assigned", "is_customer_dismissed", "is_viewed", "is_actioned", "is_auto_coached", "is_critical_event", "has_safety_event_review", "region", "device_id", "event_ms"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_safety_inbox_events_status_global", primary_keys=["date", "event_id", "event_ms", "org_id", "device_id", "region"], block_before_write=True)
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
def safety_inbox_events_status_global(context: AssetExecutionContext) -> str:

    query = """
    WITH
    --Finds all current users (both internal and external)
    us_users AS (
    SELECT *,
        CASE WHEN email ilike '%@samsara.com' THEN TRUE ELSE FALSE END AS is_samsara_email
    FROM datamodel_platform_bronze.raw_clouddb_users
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    ),
    --All behavior labels for each org ID that are immediately sent to coaching (automatically sent to coaching)
    us_auto_needs_coaching_orgs AS (
    SELECT org.id AS org_id,
            org.settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled AS auto_needs_coaching,
            EXPLODE(org.settings_proto.safety.coaching_settings.safety_labels_to_automatically_triage) AS behavior_type_auto_needs_coaching
    FROM clouddb.organizations org -- THIS SUPPLIES ORG LEVEL SETTINGS
    INNER JOIN (SELECT * FROM datamodel_core.dim_organizations WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)) cmd -- THIS HELPS US IDENTIFY CUSTOMERS WITH PURCHASE HISTORY
        ON org.id = cmd.org_id
    WHERE TRUE -- ARBITRARY FOR READABILITY
        AND cmd.internal_type = 0 -- NON-INTERNAL ORG
        AND cmd.account_first_purchase_date IS NOT NULL -- HAS AT LEAST ONE PURCHASE RECORD
        AND org.settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled = TRUE -- FIND AUTO TRIAGE = ON SETTING
    ),
    -- Modified new query; pulls each safety event that we think is in the inbox
    us_safety_events AS (
    SELECT
        fse.org_id,
        do.sam_number,
        do.account_size_segment,
        do.account_industry,
        do.account_arr_segment,
        fse.device_id AS vg_device_id,
        fse.event_ms,
        fse.event_id,
        fse.date,
        fse.detection_label AS detection_type,
        fse.behavior_label_names_array,
        fse.coaching_state_details_array,
        fse.harsh_event_surveys
    FROM datamodel_safety.fct_safety_events fse
    INNER JOIN datamodel_core.dim_devices dd ON fse.org_id = dd.org_id
        AND fse.device_id = dd.device_id
        AND fse.`date` = dd.`date`
    INNER JOIN datamodel_core.dim_organizations do ON fse.org_id = do.org_id
        AND fse.`date` = do.`date`
    WHERE fse.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}' -- dates of interest
        AND fse.release_stage in (2, 3, 4) -- closed beta, open beta, or GA
        AND do.internal_type = 0 -- non-internal org
        AND do.account_first_purchase_date IS NOT NULL -- has purchase history
    ),
    us_event_behavior_explode AS (
    SELECT org_id,
        vg_device_id,
        event_ms,
        EXPLODE(behavior_label_names_array) AS behavior_label_name
    FROM us_safety_events
    ),
    us_auto_needs_coaching_events AS (
    SELECT a.org_id,
        a.vg_device_id,
        a.event_ms,
        CASE
            WHEN b.behavior_type_auto_needs_coaching IS NOT NULL
            THEN 1
            ELSE 0
        END as auto_needs_coaching
    FROM us_event_behavior_explode a
    LEFT JOIN definitions.behavior_label_type_enums blte
    ON a.behavior_label_name = blte.behavior_label_type
    LEFT JOIN us_auto_needs_coaching_orgs b
    ON a.org_id = b.org_id
    AND blte.enum = b.behavior_type_auto_needs_coaching
    QUALIFY ROW_NUMBER() OVER(PARTITION BY a.org_id, a.vg_device_id, a.event_ms ORDER BY b.auto_needs_coaching DESC) = 1 -- If at least one behavior type is auto needs coaching, event gets pushed to auto_needs coaching
    ),
    us_coaching_status_explode AS (
    SELECT org_id,
        date,
        vg_device_id,
        event_ms,
        EXPLODE(coaching_state_details_array) AS coaching_state
    FROM us_safety_events
    ),
    us_coaching_status_cutoff AS (
    SELECT org_id,
        date,
        vg_device_id,
        event_ms,
        coaching_state,
        coaching_state.coaching_state_name AS coaching_status
    FROM us_coaching_status_explode
    WHERE (unix_millis(TIMESTAMP(coaching_state.created_at)) - event_ms)/(1000*60*60*24) <= 14
    ),

    us_coaching_status AS (
    SELECT *
    FROM us_coaching_status_cutoff
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, vg_device_id, event_ms ORDER BY coaching_state.created_at DESC) = 1 -- most recent coaching status
    ),

    us_time_to_resolution AS (
    SELECT
        a.org_id,
        a.vg_device_id,
        a.event_ms,
        a.coaching_state.coaching_state_name,
        unix_millis(TIMESTAMP(a.coaching_state.created_at)) AS created_at_ms,
        CASE
            WHEN ance.auto_needs_coaching = 1 AND coaching_state.coaching_state_name != 'Needs Coaching'
            THEN (unix_millis(TIMESTAMP(a.coaching_state.created_at)) - a.event_ms)/(1000*60*60*24)
            WHEN ance.auto_needs_coaching = 0
            THEN (unix_millis(TIMESTAMP(a.coaching_state.created_at)) - a.event_ms)/(1000*60*60*24)
            ELSE NULL
            END AS days_to_resolve
    FROM us_coaching_status a
    INNER JOIN us_users du
        ON a.coaching_state.user_id = du.id
        AND a.date = du.date
    LEFT JOIN us_auto_needs_coaching_events ance
        ON a.org_id = ance.org_id
        AND a.vg_device_id = ance.vg_device_id
        AND a.event_ms = ance.event_ms
    WHERE du.is_samsara_email = FALSE -- remove samsara coaching status changes (auto-triage)
        AND coaching_state.coaching_state_name IN ('Needs Coaching', 'Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed') -- Is actioned/resolved
    ),

    us_time_to_resolution_final AS(
    SELECT *
    FROM us_time_to_resolution
    QUALIFY ROW_NUMBER() OVER(PARTITION BY vg_device_id, event_ms ORDER BY created_at_ms ASC) = 1 --Find the first coaching related status
    ),

    us_safety_event_details AS (
    SELECT a.*,
        COALESCE(cs.coaching_status,'Needs Review') AS coaching_status,
        COALESCE(ance.auto_needs_coaching,0) AS auto_needs_coaching,
                COUNT(CASE WHEN ance.auto_needs_coaching = 1 THEN 1 ELSE NULL END) OVER() AS debug_auto_needs_coaching_count, --debugging column
        b.days_to_resolve
    FROM us_safety_events a
    LEFT JOIN us_coaching_status cs
        ON a.org_id = cs.org_id
        AND a.vg_device_id = cs.vg_device_id
        AND a.event_ms = cs.event_ms
    LEFT JOIN us_auto_needs_coaching_events ance
        ON a.org_id = ance.org_id
        AND a.vg_device_id = ance.vg_device_id
        AND a.event_ms = ance.event_ms
    LEFT JOIN us_time_to_resolution_final b
        ON a.org_id = b.org_id
        AND a.vg_device_id = b.vg_device_id
        AND a.event_ms = b.event_ms
    ),

    us_coaching_orgs AS (
    SELECT org_id
    FROM us_safety_event_details a
    WHERE coaching_status IN ('Needs Coaching','Coached')
    GROUP BY 1
    ),

    us_safety_event_review_jobs AS (
    SELECT *
    FROM (
        SELECT a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at,
            a.accel_type
        FROM safetyeventreviewdb.review_request_metadata a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN safetyeventreviewdb.jobs b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
            ON a.job_uuid = b.uuid
        INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
            ON a.org_id = o.org_id
            AND a.date = o.date
        WHERE TRUE
            AND b.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}'
            AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
            AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
            AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
            AND b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
        ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    us_viewed_safety_events AS (
    SELECT * FROM (
        SELECT sae.date,
            sae.org_id,
            sae.device_id,
            sae.event_ms,
            sae.activity_name,
            sae.created_at,
            unix_millis(TIMESTAMP(sae.created_at)) AS created_at_ms,
            (unix_millis(TIMESTAMP(sae.created_at)) - event_ms)/(1000*60*60*24) AS days_to_view
        FROM datamodel_safety_silver.stg_activity_events sae
        INNER JOIN us_users du
        ON sae.detail_proto.user_id = du.id
        AND sae.`date` = du.`date`
        INNER JOIN datamodel_core.dim_organizations do ON sae.org_id = do.org_id
        AND sae.`date` = do.`date`
        WHERE sae.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}'
        AND du.is_samsara_email = FALSE
        AND do.internal_type = 0
        AND sae.activity_enum IN (8,4,1,11,9,16,14)
        -- ('BehaviorLabelActivityType', 'CoachingStateActivityType', 'CommentActivityType', 'VideoDownloadActivityType', 'ViewedByActivityType', 'ShareSafetyEventActivityType', 'DriverCommentActivityType')
        --https://github.com/samsara-dev/backend/blob/067d3e46149d1276fbd39f8464961c75187f0b41/go/src/samsaradev.io/fleet/safety/safetyproto/safetyactivity.proto#L16
        --https://docs.google.com/spreadsheets/d/1VGJBamCSeICLyeOlxo4eb0hV_4OT2WRDEAbb8eKMysk/edit#gid=359523161
        QUALIFY ROW_NUMBER() OVER(PARTITION BY sae.event_ms, sae.device_id, sae.org_id ORDER BY sae.created_at ASC) = 1 -- first viewed qualifying activity
    ) a
    WHERE days_to_view <= 14
    ),
    us_most_recent_survey AS (
        SELECT a.date,
            a.org_id,
            a.device_id,
            a.event_ms,
            a.detection_label,
            EXPLODE(a.harsh_event_surveys) AS harsh_event_survey
        FROM datamodel_safety.fct_safety_events a -- find event thumbs data
        INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
        WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}' -- find relevant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        AND SIZE(a.harsh_event_surveys) > 0
        QUALIFY ROW_NUMBER() OVER(PARTITION BY a.org_id, a.device_id, a.event_ms ORDER BY harsh_event_survey.created_at DESC) = 1 -- most recent survey
    ),
    us_ser_licenses AS ("""+US_SER_LICENCES_CTE+"""
    ),
    us_safety_inbox_events_status AS (
    SELECT
        sie.date,
        sie.event_ms,
        sie.event_id,
        sie.detection_type,
        sie.org_id,
        sie.sam_number,
        sie.account_size_segment,
        sie.account_industry,
        sie.account_arr_segment,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license,
        sie.vg_device_id,
        CASE
            WHEN auto_needs_coaching = 0
            AND coaching_status IN ('Needs Coaching', 'Coached') -- event transitioned into coaching workflow
            AND ve.event_ms IS NOT NULL -- viewed event
            AND co.org_id IS NOT NULL -- active coaching org
            THEN 1 ELSE 0
        END AS is_coaching_assigned,
        CASE -- an event is viewed if the coaching status is no longer the default (Needs Review OR Needs Coaching)
            WHEN ve.event_ms is not null
            AND auto_needs_coaching = 0
            AND coaching_status NOT IN ('Dismissed','Auto-Dismissed','Manual Review Dismissed')
            AND co.org_id IS NOT NULL -- active coaching org
            THEN 1
            ELSE 0
        END  AS is_car_viewed,
        CASE WHEN sie.coaching_status IN ('Dismissed') THEN 1 ELSE 0 END AS is_customer_dismissed,
        CASE WHEN ve.event_ms IS NOT NULL THEN 1 ELSE 0 END AS is_viewed,
        CASE
            WHEN (auto_needs_coaching = 0 AND coaching_status IN ('Needs Coaching', 'Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed'))
            OR (auto_needs_coaching = 1 AND coaching_status IN ('Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed'))
            THEN 1
            ELSE 0
        END AS is_actioned,
        CASE WHEN auto_needs_coaching = 1 THEN 1 ELSE 0 END AS is_auto_coached,
        CASE
            WHEN sie.detection_type IN ('haCrash', 'haDrowsinessDetection')
            THEN 1 ELSE 0
        END AS is_critical_event,
        sie.days_to_resolve,
        CASE
            WHEN serj.job_uuid IS NULL
            THEN 0
            ELSE 1
        END as has_safety_event_review,
        mrs.harsh_event_survey.is_useful::INTEGER AS is_useful,
        CASE
            WHEN coaching_status IN ('Coached')
            THEN 1
            ELSE 0
        END as is_coached,
        coaching_status,
        auto_needs_coaching
    FROM us_safety_event_details AS sie
    LEFT JOIN us_viewed_safety_events AS ve
        ON sie.event_ms = ve.event_ms AND sie.vg_device_id = ve.device_id AND sie.org_id = ve.org_id
    LEFT JOIN us_coaching_orgs co
        ON sie.org_id = co.org_id
    LEFT JOIN us_safety_event_review_jobs serj
        ON sie.org_id = serj.org_id
        AND sie.vg_device_id = serj.device_id
        AND sie.event_ms = serj.event_ms
    LEFT JOIN us_most_recent_survey mrs
        ON sie.vg_device_id = mrs.device_id
        AND sie.event_ms = mrs.event_ms
        AND sie.org_id = mrs.org_id
    LEFT JOIN us_ser_licenses sl -- find ser paying customers
        ON sie.org_id = sl.org_id
    GROUP BY ALL
    ),


    -- EU Region Data
    eu_users AS (
    SELECT *,
        CASE WHEN email ilike '%@samsara.com' THEN TRUE ELSE FALSE END AS is_samsara_email
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users`
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    ),
    --All behavior labels for each org ID that are immediately sent to coaching (automatically sent to coaching)
    eu_auto_needs_coaching_orgs AS (
    SELECT org.id AS org_id,
            org.settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled AS auto_needs_coaching,
            EXPLODE(org.settings_proto.safety.coaching_settings.safety_labels_to_automatically_triage) AS behavior_type_auto_needs_coaching
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` org -- THIS SUPPLIES ORG LEVEL SETTINGS
    INNER JOIN (SELECT * FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` WHERE date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`)) cmd -- THIS HELPS US IDENTIFY CUSTOMERS WITH PURCHASE HISTORY
        ON org.id = cmd.org_id
    WHERE TRUE -- ARBITRARY FOR READABILITY
        AND cmd.internal_type = 0 -- NON-INTERNAL ORG
        AND cmd.account_first_purchase_date IS NOT NULL -- HAS AT LEAST ONE PURCHASE RECORD
        AND org.settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled = TRUE -- FIND AUTO TRIAGE = ON SETTING
    ),
    -- Modified new query; pulls each safety event that we think is in the inbox
    eu_safety_events AS (
    SELECT
        fse.org_id,
        do.sam_number,
        do.account_size_segment,
        do.account_industry,
        do.account_arr_segment,
        fse.device_id AS vg_device_id,
        fse.event_ms,
        fse.event_id,
        fse.date,
        fse.detection_label AS detection_type,
        fse.behavior_label_names_array,
        fse.coaching_state_details_array,
        fse.harsh_event_surveys
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_safety.db/fct_safety_events` fse
    INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd ON fse.org_id = dd.org_id
        AND fse.device_id = dd.device_id
        AND fse.`date` = dd.`date`
    INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` do ON fse.org_id = do.org_id
        AND fse.`date` = do.`date`
    WHERE fse.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}' -- dates of interest
        AND fse.release_stage in (2, 3, 4) -- closed beta, open beta, or GA
        AND do.internal_type = 0 -- non-internal org
        AND do.account_first_purchase_date IS NOT NULL -- has purchase history
    ),
    eu_event_behavior_explode AS (
    SELECT org_id,
        vg_device_id,
        event_ms,
        EXPLODE(behavior_label_names_array) AS behavior_label_name
    FROM eu_safety_events
    ),
    eu_auto_needs_coaching_events AS (
    SELECT a.org_id,
        a.vg_device_id,
        a.event_ms,
        CASE
            WHEN b.behavior_type_auto_needs_coaching IS NOT NULL
            THEN 1
            ELSE 0
        END as auto_needs_coaching
    FROM eu_event_behavior_explode a
    LEFT JOIN definitions.behavior_label_type_enums blte
    ON a.behavior_label_name = blte.behavior_label_type
    LEFT JOIN eu_auto_needs_coaching_orgs b
    ON a.org_id = b.org_id
    AND blte.enum = b.behavior_type_auto_needs_coaching
    QUALIFY ROW_NUMBER() OVER(PARTITION BY a.org_id, a.vg_device_id, a.event_ms ORDER BY b.auto_needs_coaching DESC) = 1 -- If at least one behavior type is auto needs coaching, event gets pushed to auto_needs coaching
    ),
    eu_coaching_status_explode AS (
        SELECT org_id,
            date,
            vg_device_id,
            event_ms,
            EXPLODE(coaching_state_details_array) AS coaching_state
        FROM eu_safety_events
    ),
    eu_coaching_status_cutoff AS (
    SELECT org_id,
            date,
            vg_device_id,
            event_ms,
            coaching_state,
            coaching_state.coaching_state_name AS coaching_status
        FROM eu_coaching_status_explode
        WHERE (unix_millis(TIMESTAMP(coaching_state.created_at)) - event_ms)/(1000*60*60*24) <= 14
    ),

    eu_coaching_status AS (
        SELECT *
        FROM eu_coaching_status_cutoff
        QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, vg_device_id, event_ms ORDER BY coaching_state.created_at DESC) = 1 -- most recent coaching status
    ),

    eu_time_to_resolution AS (
        SELECT
            a.org_id,
            a.vg_device_id,
            a.event_ms,
            a.coaching_state.coaching_state_name,
            unix_millis(TIMESTAMP(a.coaching_state.created_at)) AS created_at_ms,
            CASE
                WHEN ance.auto_needs_coaching = 1 AND coaching_state.coaching_state_name != 'Needs Coaching'
                THEN (unix_millis(TIMESTAMP(a.coaching_state.created_at)) - a.event_ms)/(1000*60*60*24)
                WHEN ance.auto_needs_coaching = 0
                THEN (unix_millis(TIMESTAMP(a.coaching_state.created_at)) - a.event_ms)/(1000*60*60*24)
                ELSE NULL
                END AS days_to_resolve
        FROM eu_coaching_status a
        INNER JOIN eu_users du
        ON a.coaching_state.user_id = du.id
        AND a.date = du.`date`
        LEFT JOIN eu_auto_needs_coaching_events ance
        ON a.org_id = ance.org_id
        AND a.vg_device_id = ance.vg_device_id
        AND a.event_ms = ance.event_ms
        WHERE du.is_samsara_email = FALSE -- remove samsara coaching status changes (auto-triage)
        AND coaching_state.coaching_state_name IN ('Needs Coaching', 'Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed') -- Is actioned/resolved
    ),

    eu_time_to_resolution_final AS(
        SELECT *
        FROM eu_time_to_resolution
        QUALIFY ROW_NUMBER() OVER(PARTITION BY vg_device_id, event_ms ORDER BY created_at_ms ASC) = 1 --Find the first coaching related status
    ),

    eu_safety_event_details AS (
    SELECT a.*,
            COALESCE(cs.coaching_status,'Needs Review') AS coaching_status,
            COALESCE(ance.auto_needs_coaching,0) AS auto_needs_coaching,
                    COUNT(CASE WHEN ance.auto_needs_coaching = 1 THEN 1 ELSE NULL END) OVER() AS debug_auto_needs_coaching_count, --debugging column
            b.days_to_resolve
    FROM eu_safety_events a
    LEFT JOIN eu_coaching_status cs
        ON a.org_id = cs.org_id
        AND a.vg_device_id = cs.vg_device_id
        AND a.event_ms = cs.event_ms
    LEFT JOIN eu_auto_needs_coaching_events ance
        ON a.org_id = ance.org_id
        AND a.vg_device_id = ance.vg_device_id
        AND a.event_ms = ance.event_ms
    LEFT JOIN eu_time_to_resolution_final b
        ON a.org_id = b.org_id
        AND a.vg_device_id = b.vg_device_id
        AND a.event_ms = b.event_ms
    ),

    eu_coaching_orgs AS (
    SELECT org_id
    FROM eu_safety_event_details a
    WHERE coaching_status IN ('Needs Coaching','Coached')
    GROUP BY 1
    ),

    eu_safety_event_review_jobs AS (
        SELECT * FROM (
        SELECT a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at,
            a.accel_type
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_request_metadata_v0` a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/jobs_v0` b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
        ON a.job_uuid = b.uuid
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
        WHERE TRUE
        AND b.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    eu_viewed_safety_events AS (
        SELECT * FROM (
        SELECT sae.date,
            sae.org_id,
            sae.device_id,
            sae.event_ms,
            sae.activity_name,
            sae.created_at,
            unix_millis(TIMESTAMP(sae.created_at)) AS created_at_ms,
            (unix_millis(TIMESTAMP(sae.created_at)) - event_ms)/(1000*60*60*24) AS days_to_view
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_safety_silver.db/stg_activity_events` sae
        INNER JOIN eu_users du
        ON sae.detail_proto.user_id = du.id
        AND sae.`date` = du.`date`
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` do ON sae.org_id = do.org_id
        AND sae.`date` = do.`date`
        WHERE sae.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}'
        AND du.is_samsara_email = FALSE
        AND do.internal_type = 0
        AND sae.activity_enum IN (8,4,1,11,9,16,14)
        -- ('BehaviorLabelActivityType', 'CoachingStateActivityType', 'CommentActivityType', 'VideoDownloadActivityType', 'ViewedByActivityType', 'ShareSafetyEventActivityType', 'DriverCommentActivityType')
        --https://github.com/samsara-dev/backend/blob/067d3e46149d1276fbd39f8464961c75187f0b41/go/src/samsaradev.io/fleet/safety/safetyproto/safetyactivity.proto#L16
        --https://docs.google.com/spreadsheets/d/1VGJBamCSeICLyeOlxo4eb0hV_4OT2WRDEAbb8eKMysk/edit#gid=359523161
        QUALIFY ROW_NUMBER() OVER(PARTITION BY sae.event_ms, sae.device_id, sae.org_id ORDER BY sae.created_at ASC) = 1 -- first viewed qualifying activity
    ) a
    WHERE days_to_view <= 14
    ),
    eu_most_recent_survey AS (
        SELECT a.date,
            a.org_id,
            a.device_id,
            a.event_ms,
            a.detection_label,
            EXPLODE(a.harsh_event_surveys) AS harsh_event_survey
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_safety.db/fct_safety_events` a -- find event thumbs data
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
        WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}' -- find relevant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        AND SIZE(a.harsh_event_surveys) > 0
        QUALIFY ROW_NUMBER() OVER(PARTITION BY a.org_id, a.device_id, a.event_ms ORDER BY harsh_event_survey.created_at DESC) = 1 -- most recent survey
    ),

    eu_ser_licenses AS ("""+EU_SER_LICENCES_CTE+"""),

    eu_safety_inbox_events_status AS (
        SELECT
        sie.date,
        sie.event_ms,
        sie.event_id,
        sie.detection_type,
        sie.org_id,
        sie.sam_number,
        sie.account_size_segment,
        sie.account_industry,
        sie.account_arr_segment,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license,
        sie.vg_device_id,
        CASE
                    WHEN auto_needs_coaching = 0
                        AND coaching_status IN ('Needs Coaching', 'Coached') -- event transitioned into coaching workflow
                        AND ve.event_ms IS NOT NULL -- viewed event
                        AND co.org_id IS NOT NULL -- active coaching org
                    THEN 1 ELSE 0
                END AS is_coaching_assigned,
        CASE -- an event is viewed if the coaching status is no longer the default (Needs Review OR Needs Coaching)
                        WHEN ve.event_ms is not null
                        AND auto_needs_coaching = 0
                        AND coaching_status NOT IN ('Dismissed','Auto-Dismissed','Manual Review Dismissed')
                        AND co.org_id IS NOT NULL -- active coaching org
                        THEN 1
                        ELSE 0
                    END  AS is_car_viewed,
        CASE WHEN sie.coaching_status IN ('Dismissed') THEN 1 ELSE 0 END AS is_customer_dismissed,
        CASE WHEN ve.event_ms IS NOT NULL THEN 1 ELSE 0 END AS is_viewed,
        CASE
                WHEN (auto_needs_coaching = 0 AND coaching_status IN ('Needs Coaching', 'Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed'))
                        OR (auto_needs_coaching = 1 AND coaching_status IN ('Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed'))
                THEN 1 ELSE 0
            END AS is_actioned,
            CASE WHEN auto_needs_coaching = 1 THEN 1 ELSE 0 END AS is_auto_coached,
            CASE
                WHEN sie.detection_type IN ('haCrash', 'haDrowsinessDetection')
                THEN 1 ELSE 0
            END AS is_critical_event,
            sie.days_to_resolve,
                CASE
                    WHEN serj.job_uuid IS NULL
                    THEN 0
                    ELSE 1
            END as has_safety_event_review,
            mrs.harsh_event_survey.is_useful::INTEGER AS is_useful,
        CASE
            WHEN coaching_status IN ('Coached')
            THEN 1
            ELSE 0
        END as is_coached,
        coaching_status,
        auto_needs_coaching
        FROM eu_safety_event_details AS sie
        LEFT JOIN eu_viewed_safety_events AS ve
            ON sie.event_ms = ve.event_ms AND sie.vg_device_id = ve.device_id AND sie.org_id = ve.org_id
        LEFT JOIN eu_coaching_orgs co
        ON sie.org_id = co.org_id
        LEFT JOIN eu_safety_event_review_jobs serj
        ON sie.org_id = serj.org_id
        AND sie.vg_device_id = serj.device_id
        AND sie.event_ms = serj.event_ms
        LEFT JOIN eu_most_recent_survey mrs
        ON sie.vg_device_id = mrs.device_id
        AND sie.event_ms = mrs.event_ms
        AND sie.org_id = mrs.org_id
        LEFT JOIN eu_ser_licenses sl -- find ser paying customers
            ON sie.org_id = sl.org_id
        GROUP BY ALL
    ),

    -- CA Region Data
    ca_users AS (
    SELECT *,
        CASE WHEN email ilike '%@samsara.com' THEN TRUE ELSE FALSE END AS is_samsara_email
    FROM data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    ),
    --All behavior labels for each org ID that are immediately sent to coaching (automatically sent to coaching)
    ca_auto_needs_coaching_orgs AS (
    SELECT org.id AS org_id,
            org.settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled AS auto_needs_coaching,
            EXPLODE(org.settings_proto.safety.coaching_settings.safety_labels_to_automatically_triage) AS behavior_type_auto_needs_coaching
    FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` org -- THIS SUPPLIES ORG LEVEL SETTINGS
    INNER JOIN (SELECT * FROM data_tools_delta_share_ca.datamodel_core.dim_organizations WHERE date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_core.dim_organizations)) cmd -- THIS HELPS US IDENTIFY CUSTOMERS WITH PURCHASE HISTORY
        ON org.id = cmd.org_id
    WHERE TRUE -- ARBITRARY FOR READABILITY
        AND cmd.internal_type = 0 -- NON-INTERNAL ORG
        AND cmd.account_first_purchase_date IS NOT NULL -- HAS AT LEAST ONE PURCHASE RECORD
        AND org.settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled = TRUE -- FIND AUTO TRIAGE = ON SETTING
    ),
    -- Modified new query; pulls each safety event that we think is in the inbox
    ca_safety_events AS (
    SELECT
        fse.org_id,
        do.sam_number,
        do.account_size_segment,
        do.account_industry,
        do.account_arr_segment,
        fse.device_id AS vg_device_id,
        fse.event_ms,
        fse.event_id,
        fse.date,
        fse.detection_label AS detection_type,
        fse.behavior_label_names_array,
        fse.coaching_state_details_array,
        fse.harsh_event_surveys
    FROM data_tools_delta_share_ca.datamodel_safety.fct_safety_events fse
    INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices dd ON fse.org_id = dd.org_id
        AND fse.device_id = dd.device_id
        AND fse.`date` = dd.`date`
    INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations do ON fse.org_id = do.org_id
        AND fse.`date` = do.`date`
    WHERE fse.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}' -- dates of interest
        AND fse.release_stage in (2, 3, 4) -- closed beta, open beta, or GA
        AND do.internal_type = 0 -- non-internal org
        AND do.account_first_purchase_date IS NOT NULL -- has purchase history
    ),
    ca_event_behavior_explode AS (
    SELECT org_id,
        vg_device_id,
        event_ms,
        EXPLODE(behavior_label_names_array) AS behavior_label_name
    FROM ca_safety_events
    ),
    ca_auto_needs_coaching_events AS (
    SELECT a.org_id,
        a.vg_device_id,
        a.event_ms,
        CASE
            WHEN b.behavior_type_auto_needs_coaching IS NOT NULL
            THEN 1
            ELSE 0
        END as auto_needs_coaching
    FROM ca_event_behavior_explode a
    LEFT JOIN definitions.behavior_label_type_enums blte
    ON a.behavior_label_name = blte.behavior_label_type
    LEFT JOIN ca_auto_needs_coaching_orgs b
    ON a.org_id = b.org_id
    AND blte.enum = b.behavior_type_auto_needs_coaching
    QUALIFY ROW_NUMBER() OVER(PARTITION BY a.org_id, a.vg_device_id, a.event_ms ORDER BY b.auto_needs_coaching DESC) = 1 -- If at least one behavior type is auto needs coaching, event gets pushed to auto_needs coaching
    ),
    ca_coaching_status_explode AS (
        SELECT org_id,
            date,
            vg_device_id,
            event_ms,
            EXPLODE(coaching_state_details_array) AS coaching_state
        FROM ca_safety_events
    ),
    ca_coaching_status_cutoff AS (
    SELECT org_id,
            date,
            vg_device_id,
            event_ms,
            coaching_state,
            coaching_state.coaching_state_name AS coaching_status
        FROM ca_coaching_status_explode
        WHERE (unix_millis(TIMESTAMP(coaching_state.created_at)) - event_ms)/(1000*60*60*24) <= 14
    ),

    ca_coaching_status AS (
        SELECT *
        FROM ca_coaching_status_cutoff
        QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, vg_device_id, event_ms ORDER BY coaching_state.created_at DESC) = 1 -- most recent coaching status
    ),

    ca_time_to_resolution AS (
        SELECT
            a.org_id,
            a.vg_device_id,
            a.event_ms,
            a.coaching_state.coaching_state_name,
            unix_millis(TIMESTAMP(a.coaching_state.created_at)) AS created_at_ms,
            CASE
                WHEN ance.auto_needs_coaching = 1 AND coaching_state.coaching_state_name != 'Needs Coaching'
                THEN (unix_millis(TIMESTAMP(a.coaching_state.created_at)) - a.event_ms)/(1000*60*60*24)
                WHEN ance.auto_needs_coaching = 0
                THEN (unix_millis(TIMESTAMP(a.coaching_state.created_at)) - a.event_ms)/(1000*60*60*24)
                ELSE NULL
                END AS days_to_resolve
        FROM ca_coaching_status a
        INNER JOIN ca_users du
        ON a.coaching_state.user_id = du.id
        AND a.date = du.`date`
        LEFT JOIN ca_auto_needs_coaching_events ance
        ON a.org_id = ance.org_id
        AND a.vg_device_id = ance.vg_device_id
        AND a.event_ms = ance.event_ms
        WHERE du.is_samsara_email = FALSE -- remove samsara coaching status changes (auto-triage)
        AND coaching_state.coaching_state_name IN ('Needs Coaching', 'Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed') -- Is actioned/resolved
    ),

    ca_time_to_resolution_final AS(
        SELECT *
        FROM ca_time_to_resolution
        QUALIFY ROW_NUMBER() OVER(PARTITION BY vg_device_id, event_ms ORDER BY created_at_ms ASC) = 1 --Find the first coaching related status
    ),

    ca_safety_event_details AS (
    SELECT a.*,
            COALESCE(cs.coaching_status,'Needs Review') AS coaching_status,
            COALESCE(ance.auto_needs_coaching,0) AS auto_needs_coaching,
                    COUNT(CASE WHEN ance.auto_needs_coaching = 1 THEN 1 ELSE NULL END) OVER() AS debug_auto_needs_coaching_count, --debugging column
            b.days_to_resolve
    FROM ca_safety_events a
    LEFT JOIN ca_coaching_status cs
        ON a.org_id = cs.org_id
        AND a.vg_device_id = cs.vg_device_id
        AND a.event_ms = cs.event_ms
    LEFT JOIN ca_auto_needs_coaching_events ance
        ON a.org_id = ance.org_id
        AND a.vg_device_id = ance.vg_device_id
        AND a.event_ms = ance.event_ms
    LEFT JOIN ca_time_to_resolution_final b
        ON a.org_id = b.org_id
        AND a.vg_device_id = b.vg_device_id
        AND a.event_ms = b.event_ms
    ),

    ca_coaching_orgs AS (
    SELECT org_id
    FROM ca_safety_event_details a
    WHERE coaching_status IN ('Needs Coaching','Coached')
    GROUP BY 1
    ),

    ca_safety_event_review_jobs AS (
        SELECT * FROM (
        SELECT a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at,
            a.accel_type
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_request_metadata_v0` a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/jobs_v0` b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
        ON a.job_uuid = b.uuid
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
        WHERE TRUE
        AND b.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    ca_viewed_safety_events AS (
        SELECT * FROM (
        SELECT sae.date,
            sae.org_id,
            sae.device_id,
            sae.event_ms,
            sae.activity_name,
            sae.created_at,
            unix_millis(TIMESTAMP(sae.created_at)) AS created_at_ms,
            (unix_millis(TIMESTAMP(sae.created_at)) - event_ms)/(1000*60*60*24) AS days_to_view
        FROM data_tools_delta_share_ca.datamodel_safety_silver.stg_activity_events sae
        INNER JOIN ca_users du
        ON sae.detail_proto.user_id = du.id
        AND sae.`date` = du.`date`
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations do ON sae.org_id = do.org_id
        AND sae.`date` = do.`date`
        WHERE sae.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}'
        AND du.is_samsara_email = FALSE
        AND do.internal_type = 0
        AND sae.activity_enum IN (8,4,1,11,9,16,14)
        -- ('BehaviorLabelActivityType', 'CoachingStateActivityType', 'CommentActivityType', 'VideoDownloadActivityType', 'ViewedByActivityType', 'ShareSafetyEventActivityType', 'DriverCommentActivityType')
        --https://github.com/samsara-dev/backend/blob/067d3e46149d1276fbd39f8464961c75187f0b41/go/src/samsaradev.io/fleet/safety/safetyproto/safetyactivity.proto#L16
        --https://docs.google.com/spreadsheets/d/1VGJBamCSeICLyeOlxo4eb0hV_4OT2WRDEAbb8eKMysk/edit#gid=359523161
        QUALIFY ROW_NUMBER() OVER(PARTITION BY sae.event_ms, sae.device_id, sae.org_id ORDER BY sae.created_at ASC) = 1 -- first viewed qualifying activity
    ) a
    WHERE days_to_view <= 14
    ),
    ca_most_recent_survey AS (
        SELECT a.date,
            a.org_id,
            a.device_id,
            a.event_ms,
            a.detection_label,
            EXPLODE(a.harsh_event_surveys) AS harsh_event_survey
        FROM data_tools_delta_share_ca.datamodel_safety.fct_safety_events a -- find event thumbs data
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
        WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN date_sub('{FIRST_PARTITION_START}', 30) AND '{FIRST_PARTITION_END}' -- find relevant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        AND SIZE(a.harsh_event_surveys) > 0
        QUALIFY ROW_NUMBER() OVER(PARTITION BY a.org_id, a.device_id, a.event_ms ORDER BY harsh_event_survey.created_at DESC) = 1 -- most recent survey
    ),

    ca_ser_licenses AS ("""+CA_SER_LICENCES_CTE+"""),

    ca_safety_inbox_events_status AS (
        SELECT
        sie.date,
        sie.event_ms,
        sie.event_id,
        sie.detection_type,
        sie.org_id,
        sie.sam_number,
        sie.account_size_segment,
        sie.account_industry,
        sie.account_arr_segment,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license,
        sie.vg_device_id,
        CASE
                    WHEN auto_needs_coaching = 0
                        AND coaching_status IN ('Needs Coaching', 'Coached') -- event transitioned into coaching workflow
                        AND ve.event_ms IS NOT NULL -- viewed event
                        AND co.org_id IS NOT NULL -- active coaching org
                    THEN 1 ELSE 0
                END AS is_coaching_assigned,
        CASE -- an event is viewed if the coaching status is no longer the default (Needs Review OR Needs Coaching)
                        WHEN ve.event_ms is not null
                        AND auto_needs_coaching = 0
                        AND coaching_status NOT IN ('Dismissed','Auto-Dismissed','Manual Review Dismissed')
                        AND co.org_id IS NOT NULL -- active coaching org
                        THEN 1
                        ELSE 0
                    END  AS is_car_viewed,
        CASE WHEN sie.coaching_status IN ('Dismissed') THEN 1 ELSE 0 END AS is_customer_dismissed,
        CASE WHEN ve.event_ms IS NOT NULL THEN 1 ELSE 0 END AS is_viewed,
        CASE
                WHEN (auto_needs_coaching = 0 AND coaching_status IN ('Needs Coaching', 'Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed'))
                        OR (auto_needs_coaching = 1 AND coaching_status IN ('Coached', 'Needs Recognition', 'Recognized', 'Reviewed', 'Dismissed'))
                THEN 1 ELSE 0
            END AS is_actioned,
            CASE WHEN auto_needs_coaching = 1 THEN 1 ELSE 0 END AS is_auto_coached,
            CASE
                WHEN sie.detection_type IN ('haCrash', 'haDrowsinessDetection')
                THEN 1 ELSE 0
            END AS is_critical_event,
            sie.days_to_resolve,
                CASE
                    WHEN serj.job_uuid IS NULL
                    THEN 0
                    ELSE 1
            END as has_safety_event_review,
            mrs.harsh_event_survey.is_useful::INTEGER AS is_useful,
        CASE
            WHEN coaching_status IN ('Coached')
            THEN 1
            ELSE 0
        END as is_coached,
        coaching_status,
        auto_needs_coaching
        FROM ca_safety_event_details AS sie
        LEFT JOIN ca_viewed_safety_events AS ve
            ON sie.event_ms = ve.event_ms AND sie.vg_device_id = ve.device_id AND sie.org_id = ve.org_id
        LEFT JOIN ca_coaching_orgs co
        ON sie.org_id = co.org_id
        LEFT JOIN ca_safety_event_review_jobs serj
        ON sie.org_id = serj.org_id
        AND sie.vg_device_id = serj.device_id
        AND sie.event_ms = serj.event_ms
        LEFT JOIN ca_most_recent_survey mrs
        ON sie.vg_device_id = mrs.device_id
        AND sie.event_ms = mrs.event_ms
        AND sie.org_id = mrs.org_id
        LEFT JOIN ca_ser_licenses sl -- find ser paying customers
            ON sie.org_id = sl.org_id
        GROUP BY ALL
    )

    SELECT
        date,
        event_id,
        detection_type,
        org_id,
        sam_number,
        account_size_segment,
        account_industry,
        account_arr_segment,
        has_ser_license,
        vg_device_id AS device_id,
        event_ms,
        is_coaching_assigned,
        is_car_viewed,
        is_customer_dismissed,
        is_viewed,
        is_actioned,
        is_auto_coached,
        is_critical_event,
        days_to_resolve,
        has_safety_event_review,
        is_useful,
        is_coached,
        coaching_status,
        auto_needs_coaching,
        'us-west-2' AS region
    FROM us_safety_inbox_events_status
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'

    UNION

    SELECT
        date,
        event_id,
        detection_type,
        org_id,
        sam_number,
        account_size_segment,
        account_industry,
        account_arr_segment,
        has_ser_license,
        vg_device_id AS device_id,
        event_ms,
        is_coaching_assigned,
        is_car_viewed,
        is_customer_dismissed,
        is_viewed,
        is_actioned,
        is_auto_coached,
        is_critical_event,
        days_to_resolve,
        has_safety_event_review,
        is_useful,
        is_coached,
        coaching_status,
        auto_needs_coaching,
        'eu-west-1' AS region
    FROM eu_safety_inbox_events_status
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'

    UNION

    SELECT
        date,
        event_id,
        detection_type,
        org_id,
        sam_number,
        account_size_segment,
        account_industry,
        account_arr_segment,
        has_ser_license,
        vg_device_id AS device_id,
        event_ms,
        is_coaching_assigned,
        is_car_viewed,
        is_customer_dismissed,
        is_viewed,
        is_actioned,
        is_auto_coached,
        is_critical_event,
        days_to_resolve,
        has_safety_event_review,
        is_useful,
        is_coached,
        coaching_status,
        auto_needs_coaching,
        'ca-central-1' AS region
    FROM ca_safety_inbox_events_status
    WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    """
    return query
