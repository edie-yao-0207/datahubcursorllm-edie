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
    "key",
    "project_key",
]

NON_NULL_COLUMNS = [
    "date",
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
        "name": "key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of feature flag"
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
        "name": "creation_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Creation date of the feature flag"
        },
    },
    {
        "name": "deprecated",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the feature flag is deprecated or not"
        },
    },
    {
        "name": "description",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Description of feature flag"
        },
    },
    {
        "name": "archived",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the feature flag is archived or not"
        },
    },
    {
        "name": "off_variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Variation being served when off"
        },
    },
    {
        "name": "using_environment_id",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether environment is being used or not"
        },
    },
    {
        "name": "on_variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Variation being served when on"
        },
    },
    {
        "name": "deprecated_date",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time at which flag was deprecated"
        },
    },
    {
        "name": "kind",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Kind of feature flag (boolean)"
        },
    },
    {
        "name": "version",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Version of the feature flag"
        },
    },
    {
        "name": "name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the feature flag"
        },
    },
    {
        "name": "maintainer_email",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The email of the maintainer of the feature flag"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    key,
    project_key,
    DATE(FROM_UNIXTIME(creation_date / 1000)) AS creation_date,
    deprecated,
    description,
    archived,
    off_variation,
    using_environment_id,
    on_variation,
    deprecated_date,
    kind,
    version,
    name,
    maintainer_email
FROM fivetran_launchdarkly_bronze.feature_flag{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing feature flags parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a feature flag""",
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
        NonEmptyDQCheck(name="dq_non_empty_feature_flag"),
        PrimaryKeyDQCheck(
            name="dq_pk_feature_flag",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_feature_flag",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.feature_flag",
    ],
)
def feature_flag(context: AssetExecutionContext) -> str:
    context.log.info("Updating feature_flag")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
