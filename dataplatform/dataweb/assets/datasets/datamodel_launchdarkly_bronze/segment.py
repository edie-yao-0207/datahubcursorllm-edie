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
    "key",
    "project_key",
]

NON_NULL_COLUMNS = [
    "date",
    "environment_key",
    "key",
    "project_key",
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
        "name": "key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of segment"
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
        "name": "description",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Description of the segment"
        },
    },
    {
        "name": "creation_date",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time at which segment was created"
        },
    },
    {
        "name": "version",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Version of segment"
        },
    },
    {
        "name": "deleted",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the segment is deleted or not"
        },
    },
    {
        "name": "name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the segment"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    environment_key,
    key,
    project_key,
    description,
    creation_date,
    version,
    deleted,
    name
FROM fivetran_launchdarkly_bronze.segment{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing segment parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a given segment""",
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
        NonEmptyDQCheck(name="dq_non_empty_segment"),
        PrimaryKeyDQCheck(
            name="dq_pk_segment",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_segment",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.segment",
    ],
)
def segment(context: AssetExecutionContext) -> str:
    context.log.info("Updating segment")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
