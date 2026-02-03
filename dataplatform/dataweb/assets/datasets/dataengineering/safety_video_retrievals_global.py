from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import build_table_description

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""This table contains user transactions through the safety video retrieval funnel.""",
        row_meaning="""A safety video retrieval event""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
    { "name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date of the safety video retrieval, in `YYYY-mm-dd` format."}},
    { "name": "org_id", "type": "double", "nullable": False, "metadata": {"comment": "The Internal ID for the customer`s Samsara org."}},
    { "name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
    { "name": "distinct_id", "type": "string", "nullable": False, "metadata": {"comment": "Unique ID of record"}},
    { "name": "vd_device_id", "type": "string", "nullable": True, "metadata": {"comment": "Device ID from vehicle"}},
    { "name": "vd_time", "type": "long", "nullable": True, "metadata": {"comment": "Time from vehicle"}},
    { "name": "vd_datetime", "type": "string", "nullable": True, "metadata": {"comment": "Date of vehicle"}},
    { "name": "rs_device_id", "type": "string", "nullable": True, "metadata": {"comment": "Device ID from retrieval"}},
    { "name": "rs_time", "type": "long", "nullable": True, "metadata": {"comment": "Time of retrieval"}},
    { "name": "rs_datetime", "type": "string", "nullable": True, "metadata": {"comment": "Date of retrieval"}},
    { "name": "cs_device_id", "type": "string", "nullable": True, "metadata": {"comment": "Device ID from confirmation"}},
    { "name": "cs_time", "type": "long", "nullable": True, "metadata": {"comment": "Time of confirmation"}},
    { "name": "cs_datetime", "type": "string", "nullable": True, "metadata": {"comment": "Date of confirmation"}},
    { "name": "sub_device_id", "type": "string", "nullable": True, "metadata": {"comment": "Device ID from submission"}},
    { "name": "sub_time", "type": "long", "nullable": True, "metadata": {"comment": "Time of submission"}},
    { "name": "sub_datetime", "type": "string", "nullable": True, "metadata": {"comment": "Date of submission"}},
    { "name": "is_selection_conversion", "type": "integer", "nullable": False, "metadata": {"comment": "Whether video was selected or not"}},
    { "name": "is_confirmation_conversion", "type": "integer", "nullable": False, "metadata": {"comment": "Whether video was reviewed or not"}},
    { "name": "is_submitted_conversion", "type": "integer", "nullable": False, "metadata": {"comment": "Whether video was submitted or not"}}
  ],
    upstreams=[],
    primary_keys=["date", "org_id", "distinct_id", "vd_time", "vd_device_id", "rs_time", "rs_device_id", "cs_time", "cs_device_id", "sub_time", "sub_device_id"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_video_retrievals_global"),
        NonNullDQCheck(name="dq_non_null_safety_video_retrievals_global", non_null_columns=["date", "org_id", "region", "distinct_id", "is_selection_conversion", "is_confirmation_conversion", "is_submitted_conversion"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_safety_video_retrievals_global", primary_keys=["date", "org_id", "distinct_id", "vd_time", "vd_device_id", "rs_time", "rs_device_id", "cs_time", "cs_device_id", "sub_time", "sub_device_id"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
)
def safety_video_retrievals_global(context: AssetExecutionContext) -> str:

    query = """
    WITH vehicle_date AS (
    SELECT mp_date,
            distinct_id,
            orgid,
            COALESCE(mr.mp_user_id, mr.mp_device_id) AS mp_user_id,
            mp_current_url,
            mp_device_id AS vd_device_id,
            time AS vd_time,
            FROM_UNIXTIME(time, 'yyyy-MM-dd HH:mm:ss') AS vd_datetime
    FROM mixpanel_samsara.vr4_modalbeingviewed_count  mr -- here we grab records of users on the Vehicle Date selection of the video retrieval modal
    WHERE TRUE -- arbitrary for readability
        AND mp_date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND COALESCE(mr.mp_user_id, mr.mp_device_id) NOT LIKE '%@samsara.com' -- non-Samsara user
        AND displayedmodal = 'VehicleDate' -- find first "page" of video retrieval modal
    GROUP BY 1,2,3,4,5,6,7
    ),
    -- find records of users on the Retrieval selection of the video retrieval modal
    retrieval_selection AS (
    SELECT mp_date,
            distinct_id,
            orgid,
            COALESCE(mr.mp_user_id, mr.mp_device_id) AS mp_user_id,
            mp_device_id AS rs_device_id,
            time AS rs_time,
            FROM_UNIXTIME(time, 'yyyy-MM-dd HH:mm:ss') AS rs_datetime
    FROM mixpanel_samsara.vr4_modalbeingviewed_count mr -- here we grab records of users on the Retrieval selection of the video retrieval modal
    WHERE TRUE -- arbitrary for readability
        AND mp_date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND COALESCE(mr.mp_user_id, mr.mp_device_id) NOT LIKE '%@samsara.com' -- non-Samsara user
        AND displayedmodal = 'RetrievalSelection' -- find second "page" of video retrieval modal
    GROUP BY 1,2,3,4,5,6,7
    ),
    -- find records of users on the Confirmation page of the video retrieval modal
    confirmation_sheet AS (
    SELECT mp_date,
            distinct_id,
            orgid,
            COALESCE(mr.mp_user_id, mr.mp_device_id) AS mp_user_id,
            mp_device_id AS cs_device_id,
            time AS cs_time,
            FROM_UNIXTIME(time, 'yyyy-MM-dd HH:mm:ss') AS cs_datetime
    FROM mixpanel_samsara.vr4_modalbeingviewed_count mr -- here we grab records of users on the Confirmation page of the video retrieval modal
    WHERE TRUE -- arbitrary for readability
        AND mp_date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND COALESCE(mr.mp_user_id, mr.mp_device_id) NOT LIKE '%@samsara.com' -- non-Samsara user
        AND displayedmodal = 'ConfirmationSheet' -- find third "page" of video retrieval modal
    GROUP BY 1,2,3,4,5,6,7
    ),
    -- find records of users clicking confirm on the video retrieval modal
    submitted AS (
    SELECT mp_date,
            distinct_id,
            orgid,
            COALESCE(mr.mp_user_id, mr.mp_device_id) AS mp_user_id,
            mp_device_id AS sub_device_id,
            time AS sub_time,
            FROM_UNIXTIME(time, 'yyyy-MM-dd HH:mm:ss') AS sub_datetime
    FROM mixpanel_samsara.vr4_retrievalConfirmClicked_count mr -- here we grab records of users clicking confirm on the video retrieval modal
    WHERE TRUE -- arbitrary for readability
        AND mp_date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND COALESCE(mr.mp_user_id, mr.mp_device_id) NOT LIKE '%@samsara.com' -- non-Samsara user
    GROUP BY 1,2,3,4,5,6,7
    )
    -- try to find the overlaps between the different segments of the video retrieval funnel
    SELECT
        a.mp_date as date,
        coalesce(a.orgid, CAST(regexp_extract(a.mp_current_url, '/o/([0-9]+)/') AS BIGINT)) as org_id,
        CASE WHEN a.mp_current_url LIKE '%cloud.eu.samsara.com%' THEN 'eu-west-1' WHEN a.mp_current_url LIKE '%cloud.ca.samsara.com%' THEN 'ca-central-1' ELSE 'us-west-2' END as region,
        a.distinct_id,
        a.vd_device_id,
        a.vd_time,
        a.vd_datetime,
        b.rs_device_id,
        b.rs_time,
        b.rs_datetime,
        c.cs_device_id,
        c.cs_time,
        c.cs_datetime,
        d.sub_device_id,
        d.sub_time,
        d.sub_datetime,
        CASE WHEN b.rs_time IS NOT NULL THEN 1 ELSE 0 END AS is_selection_conversion,
        CASE WHEN b.rs_time IS NOT NULL AND c.cs_time IS NOT NULL THEN 1 ELSE 0 END AS is_confirmation_conversion,
        CASE WHEN b.rs_time IS NOT NULL AND c.cs_time IS NOT NULL AND d.sub_time IS NOT NULL THEN 1 ELSE 0 END AS is_submitted_conversion
    FROM vehicle_date a -- start with the records of users entering the funnel
    LEFT JOIN retrieval_selection b -- find records of users entering the second page of the funnel within one hour of the entering the start
        ON a.mp_date = b.mp_date
        AND a.distinct_id = b.distinct_id
        AND a.orgid = b.orgid
        AND a.mp_user_id = b.mp_user_id
        AND b.rs_time BETWEEN a.vd_time AND (a.vd_time + 3600) -- next step within an hour
    LEFT JOIN confirmation_sheet c -- find records of users entering the third page of the funnel within one hour of the entering the start
        ON a.mp_date = c.mp_date
        AND a.distinct_id = c.distinct_id
        AND a.orgid = b.orgid
        AND a.mp_user_id = c.mp_user_id
        AND c.cs_time BETWEEN a.vd_time AND (a.vd_time + 3600) -- next step within an hour
    LEFT JOIN submitted d -- find records of users completing the funnel within one hour of the entering the start
        ON a.mp_date = d.mp_date
        AND a.distinct_id = d.distinct_id
        AND a.orgid = b.orgid
        AND a.mp_user_id = d.mp_user_id
        AND d.sub_time BETWEEN a.vd_time AND (a.vd_time + 3600) -- next step within an hour
    """
    return query
