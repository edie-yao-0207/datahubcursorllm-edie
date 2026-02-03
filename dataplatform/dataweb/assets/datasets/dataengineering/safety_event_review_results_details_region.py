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
        table_desc="""(Regional) A dataset safety event review results.""",
        row_meaning="""Each row represents a safety event review and its result""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=[
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date on which the safety review was done, in `YYYY-mm-dd` format."}},
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
        {"name": "source_id", "type": "string", "nullable": False, "metadata": {"comment": "ID corresponding to safety event"}},
        {"name": "detection_type", "type": "string", "nullable": False, "metadata": {"comment": "Event type that set off the safety review"}},
        {"name": "num_reviews", "type": "long", "nullable": True, "metadata": {"comment": "Number of reviews applied"}},
        {
            "name": "unique_decisions",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {"comment": "All unique decisions taken in the safety review"},
        },
        {"name": "disagreement", "type": "integer", "nullable": True, "metadata": {"comment": "Checks if multiple decisions were made"}},
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
    primary_keys=["date", "org_id", "source_id", "detection_type"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_event_review_results_details_region", block_before_write=False),
        NonNullDQCheck(name="dq_non_null_safety_event_review_results_details_region", non_null_columns=["date", "org_id", "source_id", "detection_type"]),
        PrimaryKeyDQCheck(name="dq_pk_safety_event_review_results_details_region", primary_keys=["date", "org_id", "source_id", "detection_type"])
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def safety_event_review_results_details_region(context: AssetExecutionContext) -> str:

    region = context.asset_key.path[0]
    if region == AWSRegion.US_WEST_2:
        SER_CONSTANT = US_SER_LICENCES_CTE
    else:
        SER_CONSTANT = NON_US_REGIONAL_SER_LICENCES_CTE

    query = """
    WITH
    ser_licenses AS ("""+SER_CONSTANT+"""
    ),

    ser_results AS (
    -- this will serve as our base SER data
    SELECT a.date,
            o.org_id,
            o.sam_number,
            CASE
                WHEN sl.org_id IS NULL THEN 0
                ELSE 1
            END as has_ser_license,
            o.account_size_segment,
            a.source_id,
            CASE -- event type definitions
            WHEN hat.enum = 6 THEN "haSharpTurn"
            ELSE hat.event_type
            END AS detection_type,
            COLLECT_SET(CASE WHEN a.result_type IN (2,3) THEN 'Dismiss' ELSE 'Submit' END) AS unique_decisions,
            CASE  -- if there is more than one unique decision, then there is disagreement
            WHEN SIZE(unique_decisions) > 1 THEN 1
            ELSE 0
            END AS disagreement,
            COUNT(a.result_type) AS num_reviews
    FROM safetyeventreviewdb.review_results a -- find SER review results
    INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    INNER JOIN safetyeventreviewdb.review_request_metadata rrmd -- ensure customer data
        ON a.review_request_metadata_uuid = rrmd.uuid
    INNER JOIN definitions.harsh_accel_type_enums hat -- find event type definitions
        ON rrmd.accel_type = hat.enum
    LEFT JOIN ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relavant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        -- AND a.completed_at IS NOT NULL -- only completed reviews
        AND rrmd.queue_name = 'CUSTOMER' -- only customer data
    GROUP BY 1,2,3,4,5,6,7
    )
    SELECT
        date,
        org_id,
        sam_number,
        account_size_segment,
        source_id,
        detection_type,
        num_reviews,
        unique_decisions,
        disagreement,
        has_ser_license
    FROM ser_results
    """
    return query
