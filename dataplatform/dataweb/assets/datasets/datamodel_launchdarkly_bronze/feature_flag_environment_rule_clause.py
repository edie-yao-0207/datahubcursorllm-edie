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
    "feature_flag_environment_rule_id",
    "feature_flag_key",
    "id",
    "feature_flag_project_key",
    "feature_flag_environment_index",
]

NON_NULL_COLUMNS = [
    "date",
    "feature_flag_environment_index",
    "feature_flag_environment_rule_id",
    "feature_flag_key",
    "feature_flag_project_key",
    "id",
    "values",
    "op",
    "attribute",
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
        "name": "feature_flag_environment_rule_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ID of feature flag rule"
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
        "name": "id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ID of rule clause"
        },
    },
    {
        "name": "values",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Need to explode, but represents values for given clause"
        },
    },
    {
        "name": "op",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Condition which to apply to values (in, gte, etc.)"
        },
    },
    {
        "name": "negate",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether to negate the condition or not"
        },
    },
    {
        "name": "context_kind",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Type of context being applied"
        },
    },
    {
        "name": "attribute",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The attribute being evaluated (orgId, driverId, etc.)"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    feature_flag_environment_index,
    feature_flag_environment_rule_id,
    feature_flag_key,
    feature_flag_project_key,
    id,
    values,
    op,
    negate,
    context_kind,
    attribute
FROM fivetran_launchdarkly_bronze.feature_flag_environment_rule_clause{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing feature flag rule clauses parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a rule clause for a given feature flag""",
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
        NonEmptyDQCheck(name="dq_non_empty_feature_flag_environment_rule_clause"),
        PrimaryKeyDQCheck(
            name="dq_pk_feature_flag_environment_rule_clause",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_feature_flag_environment_rule_clause",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.feature_flag_environment_rule_clause",
    ],
)
def feature_flag_environment_rule_clause(context: AssetExecutionContext) -> str:
    context.log.info("Updating feature_flag_environment_rule_clause")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
