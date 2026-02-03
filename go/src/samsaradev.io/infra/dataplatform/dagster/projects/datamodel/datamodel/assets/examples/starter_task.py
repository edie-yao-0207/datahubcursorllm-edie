from datetime import datetime

from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.utils import (
    AWSRegions,
    DQGroup,
    NonEmptyDQCheck,
    SQLDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
)

databases = {
    "database_bronze": "dataplatform_dev",
    "database_silver": "dataplatform_dev",
}

database_dev_overrides = {
    "database_bronze_dev": "datamodel_dev",
    "database_silver_dev": "datamodel_dev",
}

databases = apply_db_overrides(databases, database_dev_overrides)

DQ_PARTITIONS_DEF = None

SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING

PARTITIONS_DEF = DailyPartitionsDefinition(start_date=datetime(2023, 11, 27))

dqs = DQGroup(
    group_name="starter_task",
    partition_def=PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=[AWSRegions.US_WEST_2.value],
)


def generate_sql(names):
    sql_query = "select 'Doug Stone' as name, '{DATEID}' as date"

    for name in names:
        sql_query += f""" union all select '{name}' as name,
                '{{DATEID}}' as date
                """

    return sql_query


# add your name here
names_to_add = [
    "Jesse Russell",
    "Michael Howard",
    "Timothy Passaro",
    "Mathieu Durand",
    "Joshua Knapp",
    "Ross Ryles",
    "Alex Govan",
    "Gabriel Ciolac",
    "Jan Lopez",
    "Charlie Friend",
    "Jared Muirhead",
    "Zoe Sarakinis",
    "Nathan Duggal",
    "Tod Cunningham",
    "Erik Skow",
]

sql_query = generate_sql(names_to_add)

SCHEMA = [
    {"name": "name", "type": "string", "nullable": False, "metadata": {}},
    {"name": "date", "type": "string", "nullable": False, "metadata": {}},
]

bronze_assets = build_assets_from_sql(
    name="starter_task_table",
    sql_query=sql_query,
    primary_keys=[],
    upstreams=[],
    group_name="starter_task",
    regions=[AWSRegions.US_WEST_2.value],
    database=databases["database_bronze_dev"],
    databases=databases,
    schema=SCHEMA,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=PARTITIONS_DEF,
    retry_policy=None,
)
starter_task_table = bronze_assets[AWSRegions.US_WEST_2.value]

dqs["starter_task_table"].append(
    NonEmptyDQCheck(
        name="starter_task_table_blocking_empty_check",
        database=databases["database_bronze_dev"],
        table="starter_task_table",
        blocking=True,
    )
)

total_users = len(names_to_add) + 1

dqs["starter_task_table"].append(
    SQLDQCheck(
        name="name_count_check",
        database=databases["database_bronze_dev"],
        sql_query=f"""select count(*) as observed_value
            from {databases['database_bronze_dev']}.starter_task_table""",
        expected_value=total_users,
        blocking=True,
    )
)

silver_assets = build_assets_from_sql(
    name="starter_task_output_table",
    sql_query=f"""
    SELECT '{{DATEID}}' as date,
    COUNT(distinct name) as unique_users from {databases['database_bronze_dev']}.starter_task_table
    """,
    primary_keys=[],
    upstreams=set(
        [
            AssetKey(
                [
                    # DO NOT INCLUDE REGION HERE, IT WILL BE ADDED AUTOMATICALLY
                    databases["database_bronze_dev"],
                    "dq_starter_task_table",
                ]
            )
        ]
    ),
    group_name="starter_task",
    regions=[AWSRegions.US_WEST_2.value],
    database=databases["database_silver_dev"],
    databases=databases,
    schema=[
        {"name": "date", "type": "string", "nullable": False, "metadata": {}},
        {"name": "unique_users", "type": "long", "nullable": False, "metadata": {}},
    ],
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=PARTITIONS_DEF,
    retry_policy=None,
)
starter_task_output_table = silver_assets[AWSRegions.US_WEST_2.value]

dqs["starter_task_output_table"].append(
    NonEmptyDQCheck(
        name="starter_task_output_table_blocking_empty_check",
        database=databases["database_bronze_dev"],
        table="starter_task_output_table",
        blocking=True,
    )
)

dq_assets = dqs.generate()
