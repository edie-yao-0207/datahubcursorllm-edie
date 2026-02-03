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
    "feature_flag_key",
    "feature_flag_project_key",
    "id",
]

NON_NULL_COLUMNS = [
    "date",
    "feature_flag_key",
    "feature_flag_project_key",
    "id",
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
            "comment": "ID of variation"
        },
    },
    {
        "name": "name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of variation"
        },
    },
    {
        "name": "description",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Description of variation"
        },
    },
    {
        "name": "value",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Value of variation"
        },
    },
]

QUERY = """
SELECT
    '{PARTITION_START}' AS date,
    feature_flag_key,
    feature_flag_project_key,
    id,
    name,
    description,
    value
FROM fivetran_launchdarkly_bronze.feature_flag_variation{TIMETRAVEL_DATE}
WHERE _fivetran_deleted = FALSE
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_BRONZE,
    description=build_table_description(
        table_desc="""A dataset containing feature flag variation parsed from LaunchDarkly. This is a snapshot of the corresponding table from fivetran_launchdarkly_bronze.""",
        row_meaning="""Each row represents a variation for a given feature flag""",
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
        NonEmptyDQCheck(name="dq_non_empty_feature_flag_variation"),
        PrimaryKeyDQCheck(
            name="dq_pk_feature_flag_variation",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_feature_flag_variation",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:fivetran_launchdarkly_bronze.feature_flag_variation",
    ],
)
def feature_flag_variation(context: AssetExecutionContext) -> str:
    context.log.info("Updating feature_flag_variation")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        TIMETRAVEL_DATE=get_timetravel_str(PARTITION_START),
    )
    return query
