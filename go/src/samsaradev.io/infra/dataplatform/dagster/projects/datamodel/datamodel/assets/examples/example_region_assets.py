from datetime import datetime

from dagster import AssetKey, BackfillPolicy, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    date_default_description,
    org_id_default_description,
    sam_number_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_asset_replica_in_region,
    build_assets_from_sql,
    get_all_regions,
)
from ..source_assets import kinesisstats_history

databases = {
    "database_bronze": "dataplatform_dev",
}

database_dev_overrides = {
    "database_bronze_dev": "dataplatform_dev",
}

databases = apply_db_overrides(databases, database_dev_overrides)

SCHEMA = [
    {
        "name": "org_id",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "date",
        "type": "date",
        "nullable": True,
        "metadata": {"comment": date_default_description},
    },
    {
        "name": "value",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": sam_number_default_description},
    },
]


DATABASE = Database.DATAMODEL_DEV

DQ_TEST_PARTITIONS_DEF = None

SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING


TEST_PARTITIONS_DEF = DailyPartitionsDefinition(
    start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 4)
)

kinesisstats_history_osdaccelerometer = (
    kinesisstats_history.kinesisstats_history_osdaccelerometer
)

dqs = DQGroup(
    group_name="region_test",
    partition_def=TEST_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=get_all_regions(),
)

sql_query = "select * from {database_bronze}.rdsdeltalake_source_region_test"

sql_query_partitioned = "select * from {database_bronze}.rdsdeltalake_source_region_test WHERE {PARTITION_FILTERS}"

region_assets = build_assets_from_sql(
    name="test_region_table",
    description="An example asset created from Dagster asset wrappers",
    sql_query=sql_query,
    primary_keys=[],
    upstreams=[AssetKey(["kinesisstats_history", "osdaccelerometer"])],
    group_name="region_test",
    regions=get_all_regions(),
    database=databases["database_bronze_dev"],
    databases=databases,
    schema=SCHEMA,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=None,
)
_us = region_assets[AWSRegions.US_WEST_2.value]
_eu = region_assets[AWSRegions.EU_WEST_1.value]

dqs["test_region_table"].append(
    NonEmptyDQCheck(
        name="test_region_table_blocking_empty_check",
        database=databases["database_bronze_dev"],
        table="test_region_table",
        blocking=True,
    )
)

partitioned_region_assets = build_assets_from_sql(
    name="test_region_table_partitioned",
    step_launcher="databricks_pyspark_step_launcher",
    sql_query=sql_query_partitioned,
    primary_keys=[],
    upstreams=[],
    group_name="region_test",
    regions=get_all_regions(),
    database=databases["database_bronze_dev"],
    databases=databases,
    schema=SCHEMA,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=TEST_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(
        max_partitions_per_run=7
    ),  # run batched backfills spanning 7 days
)
_us2 = partitioned_region_assets[AWSRegions.US_WEST_2.value]
_eu2 = partitioned_region_assets[AWSRegions.EU_WEST_1.value]

_eu_us = build_asset_replica_in_region(
    _eu2,
    upstreams=set(
        [AssetKey([AWSRegions.EU_WEST_1.value, "dq_test_region_table_partitioned"])]
    ),
    databases=databases,
    replication_region=AWSRegions.US_WEST_2.value,
    group_name="region_test",
)


dqs["test_region_table_partitioned"].append(
    NonEmptyDQCheck(
        name="test_region_table_partitioned_blocking_empty_check",
        database=databases["database_bronze_dev"],
        table="test_region_table_partitioned",
        blocking=True,
    )
)

dq_assets = dqs.generate()
