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
    "feature_flag_environment_index",
    "feature_flag_key",
    "feature_flag_project_key",
    "index",
]

NON_NULL_COLUMNS = [
    "date",
    "feature_flag_environment_index",
    "feature_flag_key",
    "feature_flag_project_key",
    "index",
    "values",
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
        "name": "index",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Index of target"
        },
    },
    {
        "name": "values",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Values for target, needs to be exploded"
        },
    },
    {
        "name": "context_kind",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Type of target being served"
        },
    },
    {
        "name": "variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Whether target is serving true or not"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    feature_flag_environment_index,
    feature_flag_key,
    feature_flag_project_key,
    index,
    values,
    context_kind,
    variation
FROM fivetran_launchdarkly_bronze.feature_flag_environment_target{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing feature flag targets parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a target for a given feature flag""",
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
        NonEmptyDQCheck(name="dq_non_empty_feature_flag_environment_target"),
        PrimaryKeyDQCheck(
            name="dq_pk_feature_flag_environment_target",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_feature_flag_environment_target",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.feature_flag_environment_target",
    ],
)
def feature_flag_environment_target(context: AssetExecutionContext) -> str:
    context.log.info("Updating feature_flag_environment_target")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
