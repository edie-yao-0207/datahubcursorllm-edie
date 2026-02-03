import json
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
from dagster import asset
from datamodel.ops.datahub.certified_tables import (
    admin_insights_tables,
    benchmarking_tables,
    executive_scorecard_tables,
    product_scorecard_tables,
    risk_tables,
)
from pyspark.sql import DataFrame, SparkSession

from ..common.utils import (
    AWSRegions,
    apply_db_overrides,
    get_code_location,
    get_datahub_env,
    get_run_env,
    send_datadog_gauge_metric,
)
from ..resources import databricks_cluster_specs


def get_s3_files(bucket, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in page_iterator:
        if "Contents" in page:
            files.extend([obj["Key"] for obj in page["Contents"]])
    return files


def process_json_data(context, bucket, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read()
    json_data = json.loads(content)

    # Assuming each JSON is a dictionary with landed_partition and landed_timestamp as k,v
    df = pd.DataFrame(
        json_data.items(), columns=["landed_partition", "landed_timestamp"]
    )
    # Extracting database and table from the key
    parts = key.strip().split("/")
    database = parts[-2]
    table = parts[-1].split(".")[0]
    region = parts[-3]

    # Adding constant columns
    df["ingested_timestamp"] = int(datetime.utcnow().timestamp())
    df["database"] = database
    df["table"] = table
    df["region"] = region

    # reorder columns so database and table come first
    df = df[
        [
            "database",
            "table",
            "region",
            "landed_partition",
            "landed_timestamp",
            "ingested_timestamp",
        ]
    ]

    return df


def aggregate_data(context, bucket, prefix):
    files = get_s3_files(bucket, prefix)
    all_data = pd.DataFrame()
    for key in files:
        if key.endswith(".json"):  # Ensuring to process only JSON files
            df = process_json_data(context, bucket, key)
            all_data = pd.concat([all_data, df], ignore_index=True)

    return all_data


databases = {
    "database_silver": "auditlog",
}

database_dev_overrides = {
    "database_silver_dev": "datamodel_dev",
}

databases = apply_db_overrides(databases, database_dev_overrides)


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher_landed_partitions"},
    resource_defs={
        "databricks_pyspark_step_launcher_landed_partitions": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region=AWSRegions.US_WEST_2.value,
            max_workers=1,
            driver_instance_type="md-fleet.xlarge",
            worker_instance_type="rd-fleet.xlarge",
            instance_pool_type=databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
        )
    },
    partitions_def=None,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver_dev"]],
    group_name="datahub_15m",
    owners=["team:DataTools"],
    description="Landed partitions in us-west-2, eu-west-1, and ca-central-1",
    metadata={
        "code_location": get_code_location(),
        "description": "Landed partitions in us-west-2, eu-west-1, and ca-central-1",
        "owners": ["team:DataTools"],
        "schema": [
            {
                "name": "database",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "database name"},
            },
            {
                "name": "table",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "table name"},
            },
            {
                "name": "region",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "region of the table"},
            },
            {
                "name": "landed_partition",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "partition (date) of the landed data"},
            },
            {
                "name": "landed_timestamp",
                "type": "double",
                "nullable": True,
                "metadata": {
                    "comment": "timestamp (as recorded in s3) of when the first file in the table's partition landed in s3"
                },
            },
            {
                "name": "ingested_timestamp",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "comment": "UTC timestamp of when the metadata was ingested from S3 (i.e. when the scraping job last ran)"
                },
            },
        ],
    },
)
def landed_partitions(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    bucket = "samsara-amundsen-metadata"
    prefix = "staging/landed_partitions/prod/"
    final_data = aggregate_data(context, bucket, prefix)

    # make sure landed_timestamp is a double
    final_data["landed_timestamp"] = final_data["landed_timestamp"].astype(float)

    # convert pandas to pyspark df
    pyspark_df = spark.createDataFrame(final_data)

    pyspark_df.createOrReplaceTempView("pyspark_df")

    DATABASES_TO_MONITOR = [
        "product_analytics_staging",
        "product_analytics",
        "feature_store",
        "inference_store",
    ]

    # Format the databases list with proper SQL string quotes
    databases_str = ", ".join(f"'{db}'" for db in DATABASES_TO_MONITOR)

    tables_to_monitor = (
        benchmarking_tables
        + executive_scorecard_tables
        + risk_tables
        + product_scorecard_tables
    )

    tables_str = ", ".join(f"'{table}'" for table in tables_to_monitor)

    validation_query = f"""
        with t as (
            select
                region,
                database,
                table,
                ANY_VALUE(asset_owner) as asset_owner,
                concat(region, '__', database, '___', table) as asset_key,
                max(landed_partition) as latest_partition
            from
                auditlog.landed_partitions
                inner join auditlog.dim_dagster_assets on concat(region, '__', database, '__', table) = dim_dagster_assets.asset_key
            where
                database in ({databases_str})
                and concat(database, '.', table) in ({tables_str})
                and table not like '%weekly%'
                and table not like '%monthly%'
                and asset_owner = 'DataEngineering'
            group by
                1,
                2,
                3
        )
        select
            *,
            asset_owner,
            round(
                cast(
                    now() - cast(latest_partition as timestamp) as int
                ) / 3600,
                2
            ) as hours_since_update
        from
            t
    """

    context.log.info(f"running validation query: {validation_query}")

    result = spark.sql(validation_query).collect()

    context.log.info(result)

    if get_run_env() == "prod":
        METRIC_PREFIX = "dagster_table_monitors"
    else:
        METRIC_PREFIX = "dagster_table_monitors.dev"

    TABLE_FRESHNESS_METRIC: str = f"{METRIC_PREFIX}.latest_partition.freshness"

    try:
        for row in result:
            database = row.database
            table = row.table
            region = row.region
            table_owner = row.asset_owner
            hours_since_update = row.hours_since_update

            tags = {
                "owner": table_owner,
                "region": region,
                "database": database,
                "table": table,
            }

            send_datadog_gauge_metric(
                TABLE_FRESHNESS_METRIC,
                hours_since_update,
                tags,
            )
    except Exception as e:
        context.log.error(f"Error sending datadog metric: {e}")

    return pyspark_df
