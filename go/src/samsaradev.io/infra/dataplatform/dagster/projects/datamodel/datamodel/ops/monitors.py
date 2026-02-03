import datetime
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Mapping, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dagster import AssetsDefinition, asset
from datamodel.ops.datahub.certified_tables import (
    admin_insights_tables,
    benchmarking_tables,
    certified_tables,
    executive_scorecard_tables,
    product_scorecard_tables,
    risk_tables,
)
from pyspark.sql import DataFrame, SparkSession

from ..common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ..common.datahub_utils import run_dbx_query
from ..common.utils import (
    DATAENGINEERING,
    DATAPLATFORM,
    AWSRegions,
    get_region,
    get_region_bucket_prefix,
    get_run_env,
    initialize_datadog,
    send_datadog_count_metric,
    send_datadog_gauge_metric,
    slack_custom_alert,
)
from ..resources import databricks_cluster_specs

if get_run_env() == "prod":
    METRIC_PREFIX = "dagster_table_monitors"
else:
    METRIC_PREFIX = "dagster_table_monitors.dev"

# Datadog metrics.
SINGLE_FILE_TABLE_UNDER_FILESIZE_THRESHOLD_METRIC: str = (
    f"{METRIC_PREFIX}.single_file_table.under_filesize_threshold"
)
SINGLE_FILE_TABLE_NOT_FOUND_METRIC: str = f"{METRIC_PREFIX}.single_file_table.not_found"
DELTA_TABLE_ZERO_ROWS_METRIC: str = f"{METRIC_PREFIX}.delta_table.zero_rows"
DELTA_TABLE_NOT_FOUND_METRIC: str = f"{METRIC_PREFIX}.delta_table.not_found"
TABLE_FRESHNESS_METRIC: str = f"{METRIC_PREFIX}.delta_table.freshness"


def send_datadog_metric_with_tags(
    metric_type: str,
    metric: str,
    value: Optional[float],
    tablename_full: str,
    extra_tags: Mapping[str, str] = {},
):
    db_name, table_name = tablename_full.split(".")
    tags: List[str] = [
        f"database:{db_name}",
        f"table:{table_name}",
        f"region:{get_region()}",
    ]

    for k, v in extra_tags.items():
        tags.append(f"{k}:{v}")

    if metric_type == "count":
        send_datadog_count_metric(metric, tags)
    elif metric_type == "gauge":
        send_datadog_gauge_metric(metric, value, tags)


def send_datadog_count_metric_with_tags(
    metric: str, tablename_full: str, extra_tags: Mapping[str, str] = {}
):
    send_datadog_metric_with_tags(
        metric_type="count",
        metric=metric,
        value=None,
        tablename_full=tablename_full,
        extra_tags=extra_tags,
    )


def send_datadog_gauge_metric_with_tags(
    metric: str, value: float, tablename_full: str, extra_tags: Mapping[str, str] = {}
):
    send_datadog_metric_with_tags(
        metric_type="gauge",
        metric=metric,
        value=value,
        tablename_full=tablename_full,
        extra_tags=extra_tags,
    )


class CheckType(Enum):
    # Data freshness check based on when table was last written to
    DATA_FRESHNESS_TABLE_LAST_UPDATED = "table_last_updated"
    # Data freshness check based on the max timestamp based on a timestamp column provided
    DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT = "table_most_recent_event"
    # Table existence check for delta tables. Checks that the table exists and has rows
    DELTA_TABLE_EXISTS_ROW_COUNT = "delta_table_row_count"
    # Table existence check for tables backed by a single file. Checks that the file exists and has data.
    SINGLE_FILE_TABLE_EXISTS_FILE_SIZE = "single_file_table_file_size"


monitors = []

DATABASE = "auditlog"

# TODO - make Slack channel parameterized by owning team
DEFAULT_SLACK_CHANNEL = f"#{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}"

# TODO - figure out multiplexed notifications


class TableDoesNotExist(Exception):
    pass


class TableBackingFileDoesNotExist(Exception):
    pass


@dataclass
class TableMonitor:
    table: str

    check: CheckType
    freshness_slo_hours: int = field(default=6)

    single_file_table_s3_bucket_suffix: str = field(default=None)
    single_file_table_s3_key: str = field(default=None)
    # The minimium filesize (KB) expected for a single file table.
    single_file_table_filesize_kb_threshold: int = field(default=1)

    timestamp_column: Optional[str] = field(default=None)
    date_partition_column: Optional[str] = field(default=None)
    slack_channel: Optional[str] = field(default=DEFAULT_SLACK_CHANNEL)

    # TO IMPLEMENT (can make freshness SLO inherit from table priority level)
    row_count_anomaly_detection: bool = field(default=None)  # TO IMPLEMENT
    schema_change_alerts: bool = field(default=None)  # TO IMPLEMENT
    subscriber_teams: List[str] = field(default=None)  # TO IMPLEMENT
    frequency: int = field(default=None)  # TO IMPLEMENT

    def get_single_file_table_bucket(self):
        return f"{get_region_bucket_prefix(get_region())}{self.single_file_table_s3_bucket_suffix}"


monitors.append(
    TableMonitor(
        table="kinesisstats.location",
        check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
        freshness_slo_hours=8,
        timestamp_column="time",
        date_partition_column="date",
    )
)


def add_dagster_table_monitors(all_assets, monitors):

    EXCLUDED_TABLES = ["stg_datastreams_mobile_events"]

    tables_to_monitor = set()

    for asset_key, asset_owner in all_assets.items():
        asset_key = asset_key.split("__")
        if len(asset_key) != 3:
            continue
        region, database, table = tuple(asset_key)

        if (
            (
                database.startswith("datamodel_")
                or database
                in (
                    "product_analytics",
                    "dataengineering",
                    "feature_store",
                    "inference_store",
                )
                # DO NOT INCLUDE product_analytics_staging - handled via metadata_15m.py
            )
            and not database.endswith("_dev")
            and not database.endswith("_bronze")
            and not database.endswith("_silver")
            and not table.startswith("dq_")
            and table not in EXCLUDED_TABLES
            and region == AWSRegions.US_WEST_2.value  # only run for US tables right now
            and (
                f"{database}.{table}" in certified_tables
                or f"{database}.{table}" in benchmarking_tables
                or f"{database}.{table}" in executive_scorecard_tables
                or f"{database}.{table}" in risk_tables
                or f"{database}.{table}" in product_scorecard_tables
            )  # only monitor certified tables
        ):
            tables_to_monitor.add(f"{database}.{table}")

    for table in tables_to_monitor:
        if table.endswith("_monthly"):
            monitors.append(
                TableMonitor(
                    table=table,
                    check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
                    date_partition_column="date_month",
                    freshness_slo_hours=772,  # 24*31 + 28
                )
            )
        elif table.endswith("_weekly"):
            monitors.append(
                TableMonitor(
                    table=table,
                    check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
                    date_partition_column="date",
                    freshness_slo_hours=196,  # 24*7 + 28
                )
            )
        else:
            monitors.append(
                TableMonitor(
                    table=table,
                    check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
                    freshness_slo_hours=28,
                )
            )

    return monitors


# not setting partition column to show warning case
monitors.append(
    TableMonitor(
        table="kinesisstats.osdeldodometermeters",
        check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
        freshness_slo_hours=8,
        timestamp_column="time",
    )
)

monitors.append(
    TableMonitor(
        table="kinesisstats.osdfirmwaremetrics",
        check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
        freshness_slo_hours=8,
        timestamp_column="time",
        date_partition_column="date",
    )
)

monitors.append(
    TableMonitor(
        table="kinesisstats.osdhev1harsheventdetectionrunning",
        check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
        freshness_slo_hours=8,
        timestamp_column="time",
        date_partition_column="date",
    )
)

monitors.append(
    TableMonitor(
        table="dataprep.active_devices",
        check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
        freshness_slo_hours=8,
    )
)

monitors.append(
    TableMonitor(
        table="customer360.customer_360_snapshot",
        check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
        freshness_slo_hours=8,
    )
)

monitors.append(
    TableMonitor(
        table="dataengineering.firmware_events",
        check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
        freshness_slo_hours=28,
        timestamp_column="time",
        date_partition_column="date",
    )
)

monitors.append(
    TableMonitor(
        table="definitions.products",
        check=CheckType.SINGLE_FILE_TABLE_EXISTS_FILE_SIZE,
        single_file_table_s3_bucket_suffix="data-eng-mapping-tables",
        single_file_table_s3_key="databricks/s3tables/definitions/products/products.csv",
        single_file_table_filesize_kb_threshold=3,
    )
)

monitors.append(
    TableMonitor(
        table="dynamodb.cached_active_skus",
        check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
        freshness_slo_hours=28,
    )
)

monitors.append(
    TableMonitor(
        table="auditlog.datahub_scrapes",
        check=CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
        freshness_slo_hours=28,
    )
)


for shard_num in range(6):
    monitors.append(
        TableMonitor(
            table=f"audits_shard_{shard_num}db.audits",
            check=CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
            freshness_slo_hours=8,
            timestamp_column="_timestamp",
            date_partition_column="date",
        )
    )


def check_table_last_update_age_in_hours(table: str) -> float:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sql_query = f"""
    with t as (DESCRIBE HISTORY {table})
    select max(timestamp) as latest_timestamp from t WHERE LOWER(operation) NOT LIKE '%vacuum%'
    """
    last_update_time = list(spark.sql(sql_query).toPandas()["latest_timestamp"])[0]
    last_update_time = pd.to_datetime(last_update_time).value
    cur_time = time.time()
    # test to see if timestamp in milliseconds instead of seconds
    while (
        last_update_time / cur_time > 100.0
    ):  # TODO - consider abstracting into function
        last_update_time /= 1000
    return round((cur_time - last_update_time) / 3600.0, 2)


def check_table_last_event_age_in_hours(
    context, table: str, timestamp_column: str, date_partition_column: str
) -> float:
    if table.lower().startswith("kinesisstats") and table.split(".")[-1] == "location":
        extra_where_clause = f"AND value.received_at_ms >= {timestamp_column}"
    elif table.lower().startswith("kinesisstats"):
        extra_where_clause = "AND value.received_delta_seconds >= 0"
    else:
        extra_where_clause = ""

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sql_query = f"""
    SELECT max({timestamp_column}) as latest_timestamp
    FROM {table} WHERE {date_partition_column} >= date_sub(current_date(), 2)
    AND {date_partition_column} <= current_date()
    {extra_where_clause}
    """
    last_update_time = float(
        list(spark.sql(sql_query).toPandas()["latest_timestamp"])[0]
    )
    cur_time = time.time()
    # test to see if timestamp in milliseconds instead of seconds
    while last_update_time / cur_time > 100.0:
        last_update_time /= 1000
    return round((cur_time - last_update_time) / 3600.0, 2)


def get_last_update_age(context, spark, monitor: TableMonitor) -> float:
    table = monitor.table

    if monitor.check == CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED:
        last_update_age = check_table_last_update_age_in_hours(table)
    elif monitor.check == CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT:
        if not monitor.timestamp_column:
            raise NotImplementedError(
                "Event based monitors must have a timestamp column"
            )
        if monitor.date_partition_column:
            date_partition_column = monitor.date_partition_column
        else:
            temp_df = spark.sql(
                f"SHOW PARTITIONS {table.replace('kinesisstats', 'kinesisstats_history')}"
            )
            if "date" in temp_df.columns:
                date_partition_column = "date"
            # Parquet tables have a different response structure for SHOW PARTITIONS
            elif (
                "partition" in temp_df.columns
                and "date=" in temp_df.collect()[0].partition
            ):
                date_partition_column = "date"
            context.log.warning(
                f"""Setting date partition column ({date_partition_column})
                                for table {table} dynamically, should update in code
                                """
            )

        if date_partition_column:
            last_update_age = check_table_last_event_age_in_hours(
                context, table, monitor.timestamp_column, date_partition_column
            )
        else:
            context.log.error(
                f"No date partition column for {table}, using table update history"
            )
            raise ValueError(
                f"No date partition column for {table}, using table update history"
            )
    else:
        raise ValueError(
            "Invalid Freshness Check Type"
        )  # should not be reachable but just to be cautious for now

    return last_update_age


def get_freshness_asset(region: str) -> AssetsDefinition:
    @asset(
        name="table_freshness",
        owners=["team:DataTools"],
        required_resource_keys={
            "databricks_pyspark_step_launcher_monitors_" + region[:2]
        },
        resource_defs={
            "databricks_pyspark_step_launcher_monitors_"
            + region[:2]: databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
                region=region,
                max_workers=1,
                instance_pool_type=databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY,
                data_security_mode="SINGLE_USER",
                single_user_name="dev-databricks-dagster@samsara.com",
                spark_conf_overrides={
                    "spark.databricks.sql.initial.catalog.name": "default",
                },
            )
        },
        key_prefix=[region, DATABASE],
        group_name="monitors"
        if region == AWSRegions.US_WEST_2.value
        else ("monitors_eu" if region == AWSRegions.EU_WEST_1.value else "monitors_ca"),
        metadata={
            "database": DATABASE,
            "owners": ["team:DataEngineering"],
            "write_mode": "merge",
            "primary_keys": ["table_name", "poll_date_hour"],
            "schema": [
                {
                    "name": "table_name",
                    "type": "string",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "poll_date_hour",
                    "type": "timestamp",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "last_update_age_hours",
                    "type": "double",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "freshness_slo_hours",
                    "type": "double",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "poll_type",
                    "type": "string",
                    "nullable": True,
                    "metadata": {},
                },
            ],
        },
    )
    def _asset(context) -> DataFrame:
        initialize_datadog()
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        df = pd.DataFrame(
            {
                "table_name": pd.Series(dtype="str"),
                "poll_date_hour": pd.Series(dtype="datetime64[ns]"),
                "last_update_age_hours": pd.Series(dtype="float"),
                "freshness_slo_hours": pd.Series(dtype="float"),
                "poll_type": pd.Series(dtype="str"),
            }
        )

        us_only_tables = [
            "auditlog.datahub_scrapes",
            "auditlog.dim_dagster_assets",
            "customer360.customer_360_snapshot",
            "datamodel_core.fct_gateway_device_intervals",
        ]

        DATABASE = "auditlog" if get_run_env() == "prod" else "datamodel_dev"

        all_assets = spark.sql(
            f"select asset_key, asset_owner from {DATABASE}.dim_dagster_assets"
        ).toPandas()

        all_assets = dict(zip(all_assets.asset_key, all_assets.asset_owner))

        context.log.info(all_assets)

        all_monitors = add_dagster_table_monitors(all_assets, monitors)

        if region == AWSRegions.EU_WEST_1.value:
            all_monitors = [
                monitor
                for monitor in all_monitors
                if monitor.table not in us_only_tables
                and not monitor.table.endswith("_eu")
                and not monitor.table.endswith("_ca")
                and "_global" not in monitor.table
                and "shard_2db" not in monitor.table
                and "shard_3db" not in monitor.table
                and "shard_4db" not in monitor.table
                and "shard_5db" not in monitor.table
            ]
        elif region == AWSRegions.CA_CENTRAL_1.value:
            all_monitors = [
                monitor
                for monitor in all_monitors
                if monitor.table not in us_only_tables
                and not monitor.table.endswith("_eu")
                and not monitor.table.endswith("_ca")
                and "_global" not in monitor.table
                and "shard_2db" not in monitor.table
                and "shard_3db" not in monitor.table
                and "shard_4db" not in monitor.table
                and "shard_5db" not in monitor.table
            ]

        context.log.info(f"Monitors: {all_monitors}")

        owner_map = {}

        for k, v in all_assets.items():
            if k.startswith(AWSRegions.US_WEST_2.value):
                owner_map[".".join(k.split("__")[1:])] = v

        context.log.info(f"owner_map: {owner_map}")

        for monitor in all_monitors:
            table = monitor.table
            poll_type = monitor.check
            poll_type_str = poll_type.value
            context.log.info(f"Checking {table} with {poll_type_str}")
            if poll_type not in [
                CheckType.DATA_FRESHNESS_TABLE_LAST_UPDATED,
                CheckType.DATA_FRESHNESS_TABLE_MOST_RECENT_EVENT,
            ]:
                continue

            try:
                last_update_age = get_last_update_age(context, spark, monitor)
            except Exception as e:
                context.log.error(f"Error checking {table} with {poll_type_str}: {e}")
                continue

            last_update_age_above_slo = last_update_age - monitor.freshness_slo_hours

            table_owner = owner_map.get(table)

            if table_owner:
                table_owner = table_owner.replace("team:", "")

            default_db_to_owner_map = {
                DATAPLATFORM.team: ["kinesisstats"],
                DATAENGINEERING.team: [
                    "customer360",
                    "dataprep",
                    "dataprep_telematics",
                    "dataengineering",
                    "dynamodb",
                    "firmware_dev",
                ],
            }

            all_dbs_lst = []
            all_dbs_set = set()

            for v in default_db_to_owner_map.values():
                all_dbs_lst.extend(v)
                for elem in v:
                    all_dbs_set.add(elem)

            assert len(all_dbs_lst) == len(
                all_dbs_set
            ), "duplicate database added to default_db_to_owner_map"

            if not table_owner:
                database, _ = table.split(".")
                for k, v in default_db_to_owner_map.items():
                    if database in v:
                        table_owner = k
                        break

            extra_tags = {
                "polltype": poll_type_str,
                "freshness_slo_hours": monitor.freshness_slo_hours,
                "owner": table_owner,
            }

            send_datadog_gauge_metric_with_tags(
                TABLE_FRESHNESS_METRIC,
                last_update_age_above_slo,
                table,
                extra_tags=extra_tags,
            )

            new_row = {
                "table_name": table,
                "poll_date_hour": pd.to_datetime(datetime.datetime.now()).round(
                    freq="H"
                ),
                "last_update_age_hours": last_update_age,
                "freshness_slo_hours": monitor.freshness_slo_hours,
                "poll_type": poll_type_str,
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        pyspark_df = spark.createDataFrame(df)

        return pyspark_df

    return _asset


us_west_2_asset = get_freshness_asset(AWSRegions.US_WEST_2.value)
eu_west_1_asset = get_freshness_asset(AWSRegions.EU_WEST_1.value)
ca_central_1_asset = get_freshness_asset(AWSRegions.CA_CENTRAL_1.value)


def get_row_count(spark, monitor: TableMonitor) -> float:
    """
    Checks if the db and table exist, and returns the row count of the table.
    """
    table_full_name = monitor.table
    db_name, table_name = table_full_name.split(".")

    # Check that DB exists.
    databases = spark.catalog.listDatabases()
    if not any(db.name == db_name for db in databases):
        raise TableDoesNotExist(f"Database {db_name} does not exist")

    # Check that table exists.
    tables = spark.catalog.listTables(db_name)
    if not any(tbl.name == table_name for tbl in tables):
        raise TableDoesNotExist(f"Table {table_name} does not exist")

    sql_query = f"""
    SELECT COUNT(*) as row_count
    FROM {table_full_name}
    """
    return float(list(spark.sql(sql_query).toPandas()["row_count"])[0])


def get_single_file_table_filesize_kb(monitor: TableMonitor) -> float:
    s3 = boto3.client("s3")
    bucket = monitor.get_single_file_table_bucket()
    try:
        response = s3.head_object(Bucket=bucket, Key=monitor.single_file_table_s3_key)
        # ContentLength is the filesize in bytes. Convert this to KB.
        return response["ContentLength"] / 1024.0
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            raise TableBackingFileDoesNotExist(
                f"File backing table {monitor.table} does not exist at bucket \
                    {bucket} and key {monitor.single_file_table_s3_key}"
            )
        else:
            raise


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher"},
    owners=["team:DataEngineering"],
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    group_name="monitors",
    metadata={
        "database": DATABASE,
        "owners": ["team:DataEngineering"],
        "write_mode": "merge",
        "primary_keys": ["table_name", "poll_date_seconds"],
        "schema": [
            {"name": "table_name", "type": "string", "nullable": True, "metadata": {}},
            {
                "name": "poll_date_seconds",
                "type": "timestamp",
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "delta_table_row_count",
                "type": "double",
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "single_table_filesize_kb",
                "type": "double",
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "table_exists",
                "type": "boolean",
                "nullable": True,
                "metadata": {},
            },
        ],
    },
)
def table_existence(context) -> DataFrame:
    initialize_datadog()
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df = pd.DataFrame(
        {
            "table_name": pd.Series(dtype="str"),
            "poll_date_seconds": pd.Series(dtype="datetime64[ns]"),
            "delta_table_row_count": pd.Series(dtype="float"),
            "single_table_filesize_kb": pd.Series(dtype="float"),
            "table_exists": pd.Series(dtype="bool"),
        }
    )

    for monitor in monitors:
        tablename = monitor.table
        poll_type = monitor.check
        if poll_type not in [
            CheckType.DELTA_TABLE_EXISTS_ROW_COUNT,
            CheckType.SINGLE_FILE_TABLE_EXISTS_FILE_SIZE,
        ]:
            continue
        elif (
            monitor.timestamp_column is not None
            or monitor.date_partition_column is not None
        ):
            raise ValueError(
                "timestamp_column and date_partition_column should not be set for table existence monitors"
            )
        elif poll_type == CheckType.SINGLE_FILE_TABLE_EXISTS_FILE_SIZE and (
            monitor.single_file_table_s3_bucket_suffix is None
            or monitor.single_file_table_s3_key is None
        ):
            raise ValueError(
                "Expect single_file_table_s3_bucket_suffix and single_file_table_s3_key to be set for table existence monitors on single file tables"
            )

        new_row = {
            "table_name": tablename,
            "poll_date_seconds": pd.to_datetime(datetime.datetime.now()).round(
                freq="S"
            ),
            "table_exists": False,
        }

        if poll_type == CheckType.DELTA_TABLE_EXISTS_ROW_COUNT:
            try:
                table_row_count = get_row_count(spark, monitor)
                new_row["delta_table_row_count"] = table_row_count
                new_row["table_exists"] = True
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                if table_row_count == 0:
                    slack_custom_alert(
                        monitor.slack_channel,
                        f"{tablename} has zero rows, but expected data to exist",
                    )
                    send_datadog_count_metric_with_tags(
                        DELTA_TABLE_ZERO_ROWS_METRIC,
                        tablename,
                    )
            except TableDoesNotExist:
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                slack_custom_alert(
                    monitor.slack_channel,
                    f"delta table {tablename} does not exist",
                )
                send_datadog_count_metric_with_tags(
                    DELTA_TABLE_NOT_FOUND_METRIC,
                    tablename,
                )

        elif poll_type == CheckType.SINGLE_FILE_TABLE_EXISTS_FILE_SIZE:
            bucket = monitor.get_single_file_table_bucket()
            try:
                table_s3_filesize_kb = get_single_file_table_filesize_kb(monitor)
                new_row["table_exists"] = True
                new_row["single_table_filesize_kb"] = table_s3_filesize_kb
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                if (
                    table_s3_filesize_kb
                    < monitor.single_file_table_filesize_kb_threshold
                ):
                    slack_custom_alert(
                        monitor.slack_channel,
                        f"Expected the S3 file backing the table {tablename} with bucket {bucket} and key {monitor.single_file_table_s3_key} to be at least \
                            {monitor.single_file_table_filesize_kb_threshold} KB, but file was only {table_s3_filesize_kb} KB",
                    )
                    send_datadog_count_metric_with_tags(
                        SINGLE_FILE_TABLE_UNDER_FILESIZE_THRESHOLD_METRIC,
                        tablename,
                    )
            except TableBackingFileDoesNotExist:
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                slack_custom_alert(
                    monitor.slack_channel,
                    f"S3 file at bucket {bucket} and key {monitor.single_file_table_s3_key} backing the table {tablename} does not exist",
                )
                send_datadog_count_metric_with_tags(
                    SINGLE_FILE_TABLE_NOT_FOUND_METRIC,
                    tablename,
                )

    pyspark_df = spark.createDataFrame(df)
    return pyspark_df
