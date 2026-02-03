from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, get_timetravel_str

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique ID of the account"
        },
    },
    {
        "name": "customer_arr",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "ARR for the account"},
    },
    {
        "name": "customer_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "ARR segment for the account"},
    },
    {
        "name": "sam_number_undecorated",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "SAM number for the account"},
    },
    {
        "name": "ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted industry archetype classification for the account"
        },
    },
    {
        "name": "ai_model_primary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted primary industry classification for the account"
        },
    },
    {
        "name": "ai_model_secondary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted secondary industry classification for the account"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_END}' AS date,
    account_id,
    customer_arr,
    customer_arr_segment,
    sam_number_undecorated,
    ai_model_industry_archetype,
    ai_model_primary_industry,
    ai_model_secondary_industry
FROM edw.silver.dim_account_enriched{TIMETRAVEL_DATE}
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""This table copies edw.silver.dim_account_enriched for a few columns we use within Product Usage""",
        row_meaning="""Each row contains a single account""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "account_id"],
    partitioning=DailyPartitionsDefinition(start_date="2024-08-01"),
    upstreams=[],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_customer_enriched"),
        NonNullDQCheck(name="dq_non_null_stg_customer_enriched", non_null_columns=["date", "account_id"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_stg_customer_enriched", primary_keys=["date", "account_id"], block_before_write=True)
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def stg_customer_enriched(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_customer_enriched")
    PARTITION_END = context.partition_key
    query = QUERY.format(
        PARTITION_END=PARTITION_END,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_END),
    )
    context.log.info(f"{query}")
    return query
