from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, get_all_regions

PRIMARY_KEYS = ["feature_name"]

SCHEMA = [
    {
        "name": "feature_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the feature being tracked for threshold purposes"
        },
    },
    {
        "name": "red_lower_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Lower limit for red level"
        },
    },
    {
        "name": "red_higher_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Upper limit for red level"
        },
    },
    {
        "name": "yellow_lower_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Lower limit for yellow level"
        },
    },
    {
        "name": "yellow_higher_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Upper limit for yellow level"
        },
    },
    {
        "name": "green_lower_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Lower limit for green level"
        },
    },
    {
        "name": "green_higher_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Upper limit for green level. If -1, that indicates an infinite upper limit."
        },
    },
]

QUERY = """
SELECT 'Devices Activated' AS feature_name,
CAST(0 AS double) AS red_lower_limit,
CAST(.6 - 1e-9 AS double) AS red_higher_limit,
CAST(.6 AS double) AS yellow_lower_limit,
CAST(.85 AS double) AS yellow_higher_limit,
CAST(.85 + 1e-9 AS double) AS green_lower_limit,
CAST(-1 AS double) AS green_higher_limit
UNION ALL

SELECT 'Dual-Facing Event Detections' AS feature_name,
CAST(0 AS double) AS red_lower_limit,
CAST(1 AS double) AS red_higher_limit,
CAST(2 AS double) AS yellow_lower_limit,
CAST(4 AS double) AS yellow_higher_limit,
CAST(5 AS double) AS green_lower_limit,
CAST(-1 AS double) AS green_higher_limit
UNION ALL

SELECT 'Front-Facing Event Detections' AS feature_name,
CAST(0 AS double) AS red_lower_limit,
CAST(1 AS double) AS red_higher_limit,
CAST(2 AS double) AS yellow_lower_limit,
CAST(2 AS double) AS yellow_higher_limit,
CAST(3 AS double) AS green_lower_limit,
CAST(-1 AS double) AS green_higher_limit

UNION ALL

SELECT 'Dual-Facing Incab Alerts' AS feature_name,
CAST(0 AS double) AS red_lower_limit,
CAST(1 AS double) AS red_higher_limit,
CAST(2 AS double) AS yellow_lower_limit,
CAST(4 AS double) AS yellow_higher_limit,
CAST(5 AS double) AS green_lower_limit,
CAST(-1 AS double) AS green_higher_limit

UNION ALL

SELECT 'Front-Facing Incab Alerts' AS feature_name,
CAST(0 AS double) AS red_lower_limit,
CAST(1 AS double) AS red_higher_limit,
CAST(2 AS double) AS yellow_lower_limit,
CAST(2 AS double) AS yellow_higher_limit,
CAST(3 AS double) AS green_lower_limit,
CAST(-1 AS double) AS green_higher_limit

UNION ALL

SELECT 'Safety Events Coached' AS feature_name,
CAST(0 AS double) AS red_lower_limit,
CAST(.1 - 1e-9 AS double) AS red_higher_limit,
CAST(.1 AS double) AS yellow_lower_limit,
CAST(.2 AS double) AS yellow_higher_limit,
CAST(.2 + 1e-9 AS double) AS green_lower_limit,
CAST(-1 AS double) AS green_higher_limit
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Map of Red, Yellow, Green thresholds for the features being used to track product adoption""",
        row_meaning="""Each row represents a feature and its associated threshold limits for red, yellow, and green levels.""",
        related_table_info={},
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=None,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_product_adoption_thresholds"),
        PrimaryKeyDQCheck(
            name="dq_pk_product_adoption_thresholds",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_product_adoption_thresholds",
            non_null_columns=[
                "feature_name",
            ],
            block_before_write=True,
        ),
    ],
)
def product_adoption_thresholds(context: AssetExecutionContext) -> str:
    context.log.info("Updating product_adoption_thresholds")
    query = QUERY
    context.log.info(f"{query}")
    return query
