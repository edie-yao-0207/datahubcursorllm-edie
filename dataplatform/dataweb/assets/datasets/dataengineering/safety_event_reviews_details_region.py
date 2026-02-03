from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    NON_US_REGIONAL_SER_LICENCES_CTE,
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
        table_desc="""(Regional) A dataset safety event review transaction summary of safety events.""",
        row_meaning="""Each row represents a safety event review""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=[
        {"name": "uuid", "type": "string", "nullable": False, "metadata": {"comment": "Unique ID of the safety review"}},
        {"name": "event_id", "type": "long", "nullable": False, "metadata": {"comment": "Event that set off the safety review"}},
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date of the safety review, in `YYYY-mm-dd` format."}},
        {"name": "result_type", "type": "short", "nullable": True, "metadata": {"comment": "Status of safety review"}},
        {"name": "created_at", "type": "timestamp", "nullable": False, "metadata": {"comment": "When the review was created"}},
        {"name": "completed_at", "type": "timestamp", "nullable": True, "metadata": {"comment": "When the review was completed"}},
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
            "name": "detection_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "The detection label that was applied to the safety event"
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
    ],
    upstreams=[],
    primary_keys=["date", "uuid", "event_id", "created_at"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_event_reviews_details_region", block_before_write=False),
        NonNullDQCheck(name="dq_non_null_safety_event_reviews_details_region", non_null_columns=["date", "org_id", "uuid", "event_id", "created_at"]),
        PrimaryKeyDQCheck(name="dq_pk_safety_event_reviews_details_region", primary_keys=["date", "uuid", "event_id", "created_at"])
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def safety_event_reviews_details_region(context: AssetExecutionContext) -> str:

    region = context.asset_key.path[0]
    if region == AWSRegion.US_WEST_2:
        SER_CONSTANT = US_SER_LICENCES_CTE
    else:
        SER_CONSTANT = NON_US_REGIONAL_SER_LICENCES_CTE

    query = """
    WITH
    ser_licenses AS ("""+SER_CONSTANT+"""
    ),
    ser_job_event_lookup AS (
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
    ser_details AS (
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
    INNER JOIN ser_job_event_lookup rrmd -- this helps us map SER Jobs to safety events
        on a.uuid = rrmd.job_uuid
    LEFT JOIN ser_licenses sl -- find ser paying customers
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
        detection_type,
        has_ser_license
    FROM ser_details
    """
    return query
