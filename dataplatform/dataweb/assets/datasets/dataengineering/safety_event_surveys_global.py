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
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""(Global) A dataset of safety event surveys.""",
        row_meaning="""Each row represents a safety event survey""",
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
                "comment": "The date of the safety event survey, in YYYY-mm-dd format. date is derived from event_ms."
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
            "name": "detection_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "The detection label that was applied to the safety event"
            },
        },
        {
            "name": "harsh_event_surveys",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "created_at",
                            "type": "long",
                            "nullable": True,
                            "metadata": {"comment": "Unix epoch timestamp at which survey was created"},
                        },
                        {
                            "name": "created_at_datetime",
                            "type": "string",
                            "nullable": True,
                            "metadata": {"comment": "Timestamp at which survey was created"},
                        },
                        {
                            "name": "is_useful",
                            "type": "boolean",
                            "nullable": True,
                            "metadata": {"comment": "Whether survey was determined useful or not"},
                        },
                        {
                            "name": "not_useful_reason",
                            "type": "string",
                            "nullable": True,
                            "metadata": {"comment": "Reason for why the survey was not useful"},
                        },
                        {
                            "name": "not_useful_reason_enum",
                            "type": "integer",
                            "nullable": True,
                            "metadata": {"comment": "Enum value for why survey wasn't useful"},
                        },
                        {
                            "name": "other_reason",
                            "type": "string",
                            "nullable": True,
                            "metadata": {"comment": "Other reason listed for survey"},
                        },
                        {
                            "name": "is_dismissed_through_survey",
                            "type": "boolean",
                            "nullable": True,
                            "metadata": {"comment": "Whether event is dismissed through survey or not"},
                        },
                    ],
                },
                "containsNull": False,
            },
            "nullable": True,
            "metadata": {"comment": "Struct containing metadata on harsh event surveys"},
        },
        {
            "name": "account_size_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "account_arr_segment",
            "type": "string",
            "nullable": True,
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
            "name": "is_safety_event_review",
            "type": "integer",
            "nullable": False,
            "metadata": {
                "comment": "Whether the safety event is being reviewed or not"
            },
        },
        {"name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
    ],
    upstreams=[],
    primary_keys=["date", "event_ms", "org_id", "device_id"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_event_surveys_global"),
        NonNullDQCheck(name="dq_non_null_safety_event_surveys_global", non_null_columns=["date", "org_id", "region", "event_ms", "device_id", "is_safety_event_review"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_safety_event_surveys_global", primary_keys=["date", "event_ms", "org_id", "device_id"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
)
def safety_event_surveys_global(context: AssetExecutionContext) -> str:

    COMMON_COLUMNS = """
        date,
        org_id,
        sam_number,
        has_ser_license,
        device_id,
        event_ms,
        detection_type,
        ARRAY_SORT(harsh_event_surveys,  (left, right) -> CASE
            WHEN left.created_at IS NULL and right.created_at IS NULL THEN 0
            WHEN left.created_at IS NULL THEN -1
            WHEN right.created_at IS NULL THEN 1
            WHEN left.created_at < right.created_at THEN 1
            WHEN left.created_at > right.created_at THEN -1 ELSE 0
        END) AS harsh_event_surveys,  -- sorted where first element is most recent survey
        account_size_segment,
        account_arr_segment,
        is_safety_event_review,
    """

    query = """
    WITH
    us_ser_licenses AS ("""+US_SER_LICENCES_CTE+"""
    ),
    us_ser_job_event_lookup AS (
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
            AND b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
            AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
            AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
            and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    us_safety_event_surveys AS (
    SELECT
        a.date,
        a.org_id,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license,
        a.device_id,
        a.event_ms,
        a.detection_label AS detection_type,
        TRANSFORM(a.harsh_event_surveys, x -> STRUCT(
            x.created_at,
            x.created_at_datetime,
            x.is_useful,
            x.not_useful_reason,
            x.not_useful_reason_enum,
            x.other_reason,
            x.is_dismissed_through_survey
        )) AS harsh_event_surveys,
        o.sam_number,
        o.account_size_segment,
        o.account_arr_segment,
        NVL2(ser.event_ms, 1, 0) AS is_safety_event_review
    FROM datamodel_safety.fct_safety_events a -- find event thumbs data
    INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    LEFT JOIN us_ser_job_event_lookup ser -- review request metadata will always exist; can be duplicates
        ON a.org_id = ser.org_id
        AND a.device_id = ser.device_id
        AND a.event_ms = ser.event_ms
    LEFT JOIN us_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relevant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        AND SIZE(a.harsh_event_surveys) > 0
    ),

    eu_ser_licenses AS ("""+EU_SER_LICENCES_CTE+"""),
    eu_ser_job_event_lookup AS (
    SELECT *
    FROM (
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
        AND b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    eu_safety_event_surveys AS (
    SELECT
        a.date,
        a.org_id,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license,
        a.device_id,
        a.event_ms,
        a.detection_label AS detection_type,
        TRANSFORM(a.harsh_event_surveys, x -> STRUCT(
            x.created_at,
            x.created_at_datetime,
            x.is_useful,
            x.not_useful_reason,
            x.not_useful_reason_enum,
            x.other_reason,
            x.is_dismissed_through_survey
        )) AS harsh_event_surveys,
        o.sam_number,
        o.account_size_segment,
        o.account_arr_segment,
        NVL2(ser.event_ms, 1, 0) AS is_safety_event_review
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_safety.db/fct_safety_events` a -- find event thumbs data
    INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    LEFT JOIN eu_ser_job_event_lookup ser -- review request metadata will always exist; can be duplicates
        ON a.org_id = ser.org_id
        AND a.device_id = ser.device_id
        AND a.event_ms = ser.event_ms
    LEFT JOIN eu_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relevant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        AND SIZE(a.harsh_event_surveys) > 0
    ),
    ca_ser_licenses AS ("""+CA_SER_LICENCES_CTE+"""),
    ca_ser_job_event_lookup AS (
    SELECT *
    FROM (
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
        AND b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    ca_safety_event_surveys AS (
    SELECT
        a.date,
        a.org_id,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license,
        a.device_id,
        a.event_ms,
        a.detection_label AS detection_type,
        TRANSFORM(a.harsh_event_surveys, x -> STRUCT(
            x.created_at,
            x.created_at_datetime,
            x.is_useful,
            x.not_useful_reason,
            x.not_useful_reason_enum,
            x.other_reason,
            x.is_dismissed_through_survey
        )) AS harsh_event_surveys,
        o.sam_number,
        o.account_size_segment,
        o.account_arr_segment,
        NVL2(ser.event_ms, 1, 0) AS is_safety_event_review
    FROM data_tools_delta_share_ca.datamodel_safety.fct_safety_events a -- find event thumbs data
    INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    LEFT JOIN ca_ser_job_event_lookup ser -- review request metadata will always exist; can be duplicates
        ON a.org_id = ser.org_id
        AND a.device_id = ser.device_id
        AND a.event_ms = ser.event_ms
    LEFT JOIN ca_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relevant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        AND SIZE(a.harsh_event_surveys) > 0
    )

    SELECT
        """ + COMMON_COLUMNS + """
        'us-west-2' AS region
    FROM us_safety_event_surveys

    UNION

    SELECT
        """ + COMMON_COLUMNS + """
        'eu-west-1' AS region
    FROM eu_safety_event_surveys

    UNION

    SELECT
        """ + COMMON_COLUMNS + """
        'ca-central-1' AS region
    FROM ca_safety_event_surveys
    """
    return query
