from dagster import AssetKey, AutoMaterializePolicy, AutoMaterializeRule

from ...common import constants
from ...common.lifetime import Lifetime, generate_lifetime_schema
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

key_prefix = "datamodel_core"

databases = {
    "database_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

SLACK_ALERTS_CHANNEL = "alerts-data-engineering"

PARTITIONS_DEF = None

sql_query_tenure = """
    --sql
    WITH org_dates AS (
        SELECT
            org_id,
            created_at,
            EXPLODE(SEQUENCE(created_at, CURRENT_DATE())) AS date
        FROM datamodel_core.dim_organizations
        WHERE internal_type = 0
        AND date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
    )

    SELECT
        org_id,
        tenure,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        CURRENT_DATE() AS run_date
    FROM
        (SELECT
            org_id,
            TO_DATE(created_at) AS created_at_date,
            TO_DATE(date) AS date,
            CEIL(months_between(trunc(date, 'MM'), created_at)) as tenure
        FROM
            org_dates
        )

    GROUP BY 1, 2
    --endsql
"""

SCHEMA = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": constants.org_id_default_description},
    },
    {
        "name": "tenure",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Number of months the org existed"},
    },
    {
        "name": "start_date",
        "type": "date",
        "nullable": True,
        "metadata": {"comment": "Date the tenure value began"},
    },
    {
        "name": "end_date",
        "type": "date",
        "nullable": True,
        "metadata": {"comment": "Date the tenure value ended"},
    },
    {
        "name": "run_date",
        "type": "date",
        "nullable": True,
        "metadata": {"comment": "Date the table was most recently run"},
    },
]

gold_assets = build_assets_from_sql(
    name="dim_organizations_tenure",
    # description="""
    #     Tenure (months of customerhood) for an org_id as of dates between start_date and end_date.
    #     Organization information, including created_at date, comes from dim_organizations.
    #     """,
    description=build_table_description(
        table_desc="""This table provides the tenure of an organization on a given date.
Frequently, we want to know the tenure of an organization in months as an attribute for slicing and dicing metrics in dashboards and analysis.
To find the tenure of an organization on a given date, filter on [date field] BETWEEN dim_organizations_tenure.start_date and dim_organizations_tenure.end_date
""",
        row_meaning="""An organizations tenure in months within the range of the start_date and end_date field
""",
        table_type=TableType.SLOWLY_CHANGING_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=sql_query_tenure,
    primary_keys=["org_id", "start_date", "end_date"],
    upstreams=[AssetKey([databases["database_gold_dev"], "dq_dim_organizations"])],
    group_name="dim_organizations_tenure",
    regions=get_all_regions(),
    database=databases["database_gold_dev"],
    databases=databases,
    schema=SCHEMA,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=PARTITIONS_DEF,
    retry_policy=None,
)
stg1 = gold_assets[AWSRegions.US_WEST_2.value]
stg2 = gold_assets[AWSRegions.EU_WEST_1.value]
stg3 = gold_assets[AWSRegions.CA_CENTRAL_1.value]

dqs = DQGroup(
    group_name="dim_organizations_tenure",
    partition_def=PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=get_all_regions(),
)

dqs["dim_organizations_tenure"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_organizations_tenure",
        table="dim_organizations_tenure",
        primary_keys=["org_id", "start_date", "end_date"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations_tenure"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_organizations_tenure",
        table="dim_organizations_tenure",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations_tenure"].append(
    NonNullDQCheck(
        name="dq_non_null_dim_organizations_tenure",
        table="dim_organizations_tenure",
        non_null_columns=["org_id", "start_date", "end_date", "tenure"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations_tenure"].append(
    JoinableDQCheck(
        name="dq_joinable_dim_organizations_tenure_to_dim_organizations",
        database=databases["database_gold_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="dim_organizations_tenure",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

dq_assets = dqs.generate()
