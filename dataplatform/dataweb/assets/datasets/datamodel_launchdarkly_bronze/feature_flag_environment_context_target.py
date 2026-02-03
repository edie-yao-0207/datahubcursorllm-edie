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
    "index",
    "feature_flag_environment_index",
    "feature_flag_key",
    "feature_flag_project_key",
]

NON_NULL_COLUMNS = [
    "date",
    "index",
    "feature_flag_environment_index",
    "feature_flag_key",
    "feature_flag_project_key",
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
        "name": "index",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Index of context target"
        },
    },
    {
        "name": "feature_flag_environment_index",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Index of environment for FF"
        },
    },
    {
        "name": "feature_flag_key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of feature flag"
        },
    },
    {
        "name": "feature_flag_project_key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of project"
        },
    },
    {
        "name": "context_kind",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Context being targeted"
        },
    },
    {
        "name": "values",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Need to explode, but represents values for target"
        },
    },
    {
        "name": "variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Variation being served to the context target"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    index,
    feature_flag_environment_index,
    feature_flag_key,
    feature_flag_project_key,
    context_kind,
    values,
    variation
FROM fivetran_launchdarkly_bronze.feature_flag_environment_context_target{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing feature flag context targets parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a context target for a given feature flag""",
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
        NonEmptyDQCheck(name="dq_non_empty_feature_flag_environment_context_target"),
        PrimaryKeyDQCheck(
            name="dq_pk_feature_flag_environment_context_target",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_feature_flag_environment_context_target",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.feature_flag_environment_context_target",
    ],
)
def feature_flag_environment_context_target(context: AssetExecutionContext) -> str:
    context.log.info("Updating feature_flag_environment_context_target")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
