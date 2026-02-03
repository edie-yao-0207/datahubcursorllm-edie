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
        table_desc="""(GLOBAL) A dataset safety event review results.""",
        row_meaning="""Each row represents a safety event review and its result""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date on which the safety review was done, in `YYYY-mm-dd` format."}},
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
        {"name": "source_id", "type": "string", "nullable": False, "metadata": {"comment": "ID corresponding to safety event"}},
        {"name": "detection_type", "type": "string", "nullable": False, "metadata": {"comment": "Event type that set off the safety review"}},
        {
            "name": "unique_decisions",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {"comment": "All unique decisions taken in the safety review"},
        },
        {"name": "disagreement", "type": "integer", "nullable": True, "metadata": {"comment": "Checks if multiple decisions were made"}},
        {"name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
        {"name": "num_reviews", "type": "long", "nullable": True, "metadata": {"comment": "Number of reviews applied"}},
    ],
    upstreams=[],
    primary_keys=["date", "org_id", "source_id", "detection_type", "region"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_event_review_results_details"),
        NonNullDQCheck(name="dq_non_null_safety_event_review_results_details", non_null_columns=["date", "org_id", "region", "source_id", "detection_type"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_safety_event_review_results_details", primary_keys=["date", "org_id", "source_id", "detection_type", "region"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
)
def safety_event_review_results_details(context: AssetExecutionContext) -> str:

    query = """
    WITH
    us_ser_licenses AS ("""+US_SER_LICENCES_CTE+"""
    ),

    us_ser_results AS (
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
    LEFT JOIN us_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relavant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        -- AND a.completed_at IS NOT NULL -- only completed reviews
        AND rrmd.queue_name = 'CUSTOMER' -- only customer data
    GROUP BY 1,2,3,4,5,6,7
    ),

    eu_ser_licenses AS ("""+EU_SER_LICENCES_CTE+"""),

    eu_ser_results AS (
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
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_results_v0` a -- find SER review results
    INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    INNER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_request_metadata_v0` rrmd -- ensure customer data
        ON a.review_request_metadata_uuid = rrmd.uuid
    INNER JOIN definitions.harsh_accel_type_enums hat -- find event type definitions
        ON rrmd.accel_type = hat.enum
    LEFT JOIN eu_ser_licenses sl -- find ser paying customers
        ON a.org_id = sl.org_id
    WHERE TRUE -- arbitrary for readability
        AND a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- find relavant dates
        AND o.internal_type = 0 -- indicates a non-internal org
        AND o.account_first_purchase_date IS NOT NULL -- indicates a customer purchase
        -- AND a.completed_at IS NOT NULL -- only completed reviews
        AND rrmd.queue_name = 'CUSTOMER' -- only customer data
    GROUP BY 1,2,3,4,5,6,7
    ),
    ca_ser_licenses AS ("""+CA_SER_LICENCES_CTE+"""),

    ca_ser_results AS (
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
    FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_results_v0` a -- find SER review results
    INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
        ON a.org_id = o.org_id
        AND a.date = o.date
    INNER JOIN delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-safetyeventreviewdb/safetyeventreviewdb/review_request_metadata_v0` rrmd -- ensure customer data
        ON a.review_request_metadata_uuid = rrmd.uuid
    INNER JOIN definitions.harsh_accel_type_enums hat -- find event type definitions
        ON rrmd.accel_type = hat.enum
    LEFT JOIN ca_ser_licenses sl -- find ser paying customers
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
        has_ser_license,
        account_size_segment,
        source_id,
        detection_type,
        num_reviews,
        unique_decisions,
        disagreement,
        'us-west-2' as region
    FROM us_ser_results

    UNION

    SELECT
        date,
        org_id,
        sam_number,
        has_ser_license,
        account_size_segment,
        source_id,
        detection_type,
        num_reviews,
        unique_decisions,
        disagreement,
        'eu-west-1' as region
    FROM eu_ser_results

    UNION

    SELECT
        date,
        org_id,
        sam_number,
        has_ser_license,
        account_size_segment,
        source_id,
        detection_type,
        num_reviews,
        unique_decisions,
        disagreement,
        'ca-central-1' as region
    FROM ca_ser_results
    """
    return query
