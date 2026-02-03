from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_timetravel_str,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = [
    "date",
    "environment_key",
    "project_key",
    "segment_key",
    "key",
]

NON_NULL_COLUMNS = [
    "date",
    "environment_key",
    "project_key",
    "segment_key",
    "key",
]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE
        },
    },
    {
        "name": "environment_key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of environment"
        },
    },
    {
        "name": "project_key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of project"
        },
    },
    {
        "name": "segment_key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of segment"
        },
    },
    {
        "name": "key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of flag related to segment"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    environment_key,
    project_key,
    segment_key,
    key
FROM fivetran_launchdarkly_bronze.segment_flag{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing segment flags parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a segment for a given feature flag""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by="9am PST",
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2025-07-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_segment_flag"),
        PrimaryKeyDQCheck(
            name="dq_pk_segment_flag",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_segment_flag",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.segment_flag",
    ],
)
def segment_flag(context: AssetExecutionContext) -> str:
    context.log.info("Updating segment_flag")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
