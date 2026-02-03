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
        table_desc="""(Global) A dataset safety event review transaction summary of safety events.""",
        row_meaning="""Each row represents a safety event review""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "uuid", "type": "string", "nullable": False, "metadata": {"comment": "Unique ID of the safety review"}},
        {"name": "event_id", "type": "long", "nullable": False, "metadata": {"comment": "Event that set off the safety review"}},
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date of the safety review, in `YYYY-mm-dd` format."}},
        {"name": "result_type", "type": "short", "nullable": True, "metadata": {"comment": "Status of safety review"}},
        {"name": "created_at", "type": "timestamp", "nullable": False, "metadata": {"comment": "When the review was created"}},
        {"name": "completed_at", "type": "timestamp", "nullable": True, "metadata": {"comment": "When the review was completed"}},
        {
            "name": "detection_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "The detection label that was applied to the safety event"
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
            "name": "org_id",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "The Samsara cloud dashboard ID that the data belongs to"
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
            "name": "has_ser_license",
            "type": "integer",
            "nullable": False,
            "metadata": {
                "comment": "A flag indicating whether the customer is paying for SER. 1==true and 0==false"
            },
        },
        {"name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
    ],
    upstreams=[],
    primary_keys=["date", "uuid", "event_id", "created_at"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_event_reviews_details"),
        NonNullDQCheck(name="dq_non_null_safety_event_reviews_details", non_null_columns=["date", "org_id", "region", "uuid", "event_id", "created_at"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_safety_event_reviews_details", primary_keys=["date", "uuid", "event_id", "created_at"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
)
def safety_event_reviews_details(context: AssetExecutionContext) -> str:

    query = """
    WITH
    us_ser_licenses AS ("""+US_SER_LICENCES_CTE+"""
    ),
    us_ser_job_event_lookup AS (
    SELECT * FROM (
        SELECT a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at,
            CASE -- event type definitions
            WHEN hat.enum = 6 THEN "haSharpTurn"
            ELSE hat.event_type
            END AS detection_type
        FROM safetyeventreviewdb.review_request_metadata a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN safetyeventreviewdb.jobs b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
        ON a.job_uuid = b.uuid
        INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
        LEFT JOIN definitions.harsh_accel_type_enums hat -- find event type definitions
        ON a.accel_type = hat.enum
        WHERE TRUE
        AND b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        -- and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    us_ser_details AS (
    SELECT
        a.uuid,
        a.event_id,
        a.date,
        a.result_type,
        a.created_at,
        rrmd.detection_type,
        a.completed_at,
        o.sam_number,
        o.org_id,
        o.account_size_segment,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license
    FROM safetyeventreviewdb.jobs a -- SER reviewing data
    INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    INNER JOIN us_ser_job_event_lookup rrmd -- this helps us map SER Jobs to safety events
        on a.uuid = rrmd.job_uuid
    LEFT JOIN us_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE true
        and a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relavant dates
        and o.internal_type = 0 -- indicates a non-internal org
        and o.account_first_purchase_date is not null -- indicates a customer purchase
        -- and a.completed_at is not null -- only completed reviews
    GROUP BY ALL
    ),

    eu_ser_licenses AS ("""+EU_SER_LICENCES_CTE+"""),

    eu_ser_job_event_lookup AS (
    SELECT * FROM (
        SELECT a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at,
            CASE -- event type definitions
            WHEN hat.enum = 6 THEN "haSharpTurn"
            ELSE hat.event_type
            END AS detection_type
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_request_metadata_v0` a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/jobs_v0` b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
            ON a.job_uuid = b.uuid
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o -- THIS ENSURES NON-INTERNAL ORG DATA
            ON a.org_id = o.org_id
            AND a.date = o.date
        LEFT JOIN definitions.harsh_accel_type_enums hat -- find event type definitions
            ON a.accel_type = hat.enum
        WHERE TRUE
        AND b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        -- and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    eu_ser_details AS (
    SELECT
        a.uuid,
        a.event_id,
        a.date,
        a.result_type,
        a.created_at,
        a.completed_at,
        rrmd.detection_type,
        o.sam_number,
        o.org_id,
        o.account_size_segment,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/jobs_v0` a -- SER reviewing data
    INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    INNER JOIN eu_ser_job_event_lookup rrmd -- this helps us map SER Jobs to safety events
        on a.uuid = rrmd.job_uuid
    LEFT JOIN eu_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE true
        and a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relavant dates
        and o.internal_type = 0 -- indicates a non-internal org
        and o.account_first_purchase_date is not null -- indicates a customer purchase
        -- and a.completed_at is not null -- only completed reviews
    GROUP BY ALL
    ),
    ca_ser_licenses AS ("""+CA_SER_LICENCES_CTE+"""),

    ca_ser_job_event_lookup AS (
    SELECT * FROM (
        SELECT a.job_uuid,
            SPLIT_PART(a.source_id, ',', 1) AS org_id,
            SPLIT_PART(a.source_id, ',', 2) AS device_id,
            SPLIT_PART(a.source_id, ',', 3) AS event_ms,
            b.updated_at,
            CASE -- event type definitions
            WHEN hat.enum = 6 THEN "haSharpTurn"
            ELSE hat.event_type
            END AS detection_type
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_request_metadata_v0` a -- THIS FINDS INFORMATION ON THE EVENT TYPES
        INNER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/jobs_v0` b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
            ON a.job_uuid = b.uuid
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
            ON a.org_id = o.org_id
            AND a.date = o.date
        LEFT JOIN definitions.harsh_accel_type_enums hat -- find event type definitions
            ON a.accel_type = hat.enum
        WHERE TRUE
        AND b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
        AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
        AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
        -- and b.completed_at is not null -- only completed reviews
        GROUP BY 1,2,3,4,5,6 -- LIKE "SELECT DISTINCT", BUT FASTER
    ) a
    QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
    ),
    ca_ser_details AS (
    SELECT
        a.uuid,
        a.event_id,
        a.date,
        a.result_type,
        a.created_at,
        a.completed_at,
        rrmd.detection_type,
        o.sam_number,
        o.org_id,
        o.account_size_segment,
        CASE
            WHEN sl.org_id IS NULL THEN 0
            ELSE 1
        END as has_ser_license
    FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/jobs_v0` a -- SER reviewing data
    INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    INNER JOIN ca_ser_job_event_lookup rrmd -- this helps us map SER Jobs to safety events
        on a.uuid = rrmd.job_uuid
    LEFT JOIN ca_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE true
        and a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relavant dates
        and o.internal_type = 0 -- indicates a non-internal org
        and o.account_first_purchase_date is not null -- indicates a customer purchase
        -- and a.completed_at is not null -- only completed reviews
    GROUP BY ALL
    )

    SELECT
        uuid,
        event_id,
        date,
        result_type,
        created_at,
        completed_at,
        sam_number,
        org_id,
        account_size_segment,
        has_ser_license,
        detection_type,
        'us-west-2' as region
    FROM us_ser_details

    UNION

    SELECT
        uuid,
        event_id,
        date,
        result_type,
        created_at,
        completed_at,
        sam_number,
        org_id,
        account_size_segment,
        has_ser_license,
        detection_type,
        'eu-west-1' as region
    FROM eu_ser_details

    UNION

    SELECT
        uuid,
        event_id,
        date,
        result_type,
        created_at,
        completed_at,
        sam_number,
        org_id,
        account_size_segment,
        has_ser_license,
        detection_type,
        'ca-central-1' as region
    FROM ca_ser_details
    """
    return query
