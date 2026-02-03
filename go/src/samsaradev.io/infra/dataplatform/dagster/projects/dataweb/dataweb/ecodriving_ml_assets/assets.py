import json
import time
from dataclasses import dataclass, field
from typing import Optional

import boto3
from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    Failure,
    IdentityPartitionMapping,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)
from dataweb._core.utils import adjust_partition_def_for_canada
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.utils import get_region_from_context, get_run_env
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import (
    get_eco_driving_features_db_name,
    get_role_name,
    get_running_env,
    get_sqs_queue_url,
)
from .processor import create_partition_processor


@dataclass
class PartitionMetricsAggregator:
    """Aggregates metrics across all partitions during processing."""

    successful_partitions: int = 0
    failed_partitions: int = 0
    total_rows_processed: int = 0
    total_orgs_processed: int = 0
    total_successful_orgs: int = 0
    failed_orgs: list = field(default_factory=list)
    total_sqs_messages_sent: int = 0
    errors: list = field(default_factory=list)
    partition_durations: list = field(default_factory=list)
    # SQS message size metrics (bytes)
    total_message_bytes: int = 0
    msg_max_sizes: list = field(default_factory=list)
    msg_min_sizes: list = field(default_factory=list)


@dataclass
class QueryFilters:
    """Filters for the 5-minute interval features query."""

    start_date: str
    end_date: str
    hour: Optional[str] = None
    org_ids: Optional[list[int]] = None
    device_ids: Optional[list[int]] = None
    limit: Optional[int] = None


# Partition interval in hours. This should be updated based on the eco_key_cycles job frequency
# and should be consistent with fct_eco_driving_key_cycle_features_hourly partition definition
PARTITION_INTERVAL_HOURS = 3
HOUR_PARTITION_KEYS = [f"{h:02d}" for h in range(0, 24, PARTITION_INTERVAL_HOURS)]

BASE_PARTITIONS_DEF = MultiPartitionsDefinition(
    {
        "date": DATAWEB_PARTITION_DATE,
        "hour": StaticPartitionsDefinition(partition_keys=HOUR_PARTITION_KEYS),
    }
)


def _build_query(db_name: str, filters: QueryFilters) -> str:
    """
    Build a SQL query to fetch 5-minute interval features for a given date range, orgs, devices, and limit.

    Args:
        db_name: Database name containing the 5-minute interval features table
        filters: QueryFilters containing date range, hour, and optional org/device/limit filters

    Returns:
        SQL query string
    """
    query = f"""
        SELECT * FROM {db_name}.fct_eco_driving_key_cycle_features_hourly
        WHERE date BETWEEN '{filters.start_date}' AND '{filters.end_date}'
    """

    if filters.hour is not None:
        query += f" AND hour = '{filters.hour}'"

    if filters.org_ids:
        query += f" AND org_id IN ({','.join(map(str, filters.org_ids))})"

    if filters.device_ids:
        query += f" AND device_id IN ({','.join(map(str, filters.device_ids))})"

    if filters.limit is not None:
        query += f" LIMIT {filters.limit}"

    return query


def _aggregate_partition_metrics(all_metrics: list[dict]) -> PartitionMetricsAggregator:
    """
    Aggregate metrics collected from all Spark partition processors.

    Args:
        all_metrics: List of metric dictionaries from each partition

    Returns:
        PartitionMetricsAggregator with aggregated metrics
    """
    agg = PartitionMetricsAggregator()

    for idx, metric in enumerate(all_metrics):
        if metric["success"]:
            agg.successful_partitions += 1
            agg.total_rows_processed += metric["total_rows_processed"]
            agg.total_orgs_processed += metric["total_orgs_processed"]
            agg.total_successful_orgs += metric["total_successful_orgs"]
            agg.failed_orgs.extend(metric["failed_orgs"])
            agg.total_sqs_messages_sent += metric["total_sqs_messages_sent"]
            agg.partition_durations.append(
                {
                    "partition_idx": idx,
                    "time_seconds": metric["time_seconds"],
                }
            )

            if metric.get("total_message_bytes", 0) > 0:
                agg.total_message_bytes += metric["total_message_bytes"]
                agg.msg_max_sizes.append(metric["max_message_size_bytes"])
                agg.msg_min_sizes.append(metric["min_message_size_bytes"])
        else:
            agg.failed_partitions += 1
            agg.errors.append(metric["error"])
            agg.partition_durations.append(
                {
                    "partition_idx": idx,
                    "time_seconds": metric["time_seconds"],
                    "rows": 0,
                    "org_count": 0,
                    "org_ids": [],
                    "error": metric["error"],
                    "log_messages": metric.get("log_messages", []),
                }
            )

    return agg


def _log_metrics_summary(
    context: AssetExecutionContext,
    agg: PartitionMetricsAggregator,
    total_partitions: int,
) -> None:
    """
    Log a summary of partition processing metrics.

    Args:
        context: Dagster execution context for logging
        agg: Aggregated metrics from all partitions
        total_partitions: Total number of partitions processed

    Raises:
        Failure: If any partitions failed
    """
    context.log.info("=" * 80)
    context.log.info("Executor metrics summary:")
    context.log.info(f"  Total Partitions: {total_partitions}")
    context.log.info(f"  Successful Partitions: {agg.successful_partitions}")
    context.log.info(f"  Failed Partitions: {agg.failed_partitions}")
    context.log.info(f"  Total Rows Processed: {agg.total_rows_processed}")
    context.log.info(f"  Total SQS Messages Sent: {agg.total_sqs_messages_sent}")
    context.log.info(f"  Total Unique Orgs Processed: {agg.total_orgs_processed}")
    context.log.info(f"  Total Successful Orgs: {agg.total_successful_orgs}")
    context.log.info(f"  Failed Orgs: {len(agg.failed_orgs)}")

    # Log SQS message size metrics (only available when track_message_sizes tag is set to true)
    if agg.total_message_bytes > 0 and agg.total_sqs_messages_sent > 0:
        overall_avg = agg.total_message_bytes / agg.total_sqs_messages_sent
        overall_max = max(agg.msg_max_sizes) if agg.msg_max_sizes else 0
        overall_min = min(agg.msg_min_sizes) if agg.msg_min_sizes else 0

        def format_size(size_bytes: float) -> str:
            """Format bytes to human-readable string with KB/MB."""
            if size_bytes >= 1024 * 1024:
                return f"{size_bytes:,.0f} bytes ({size_bytes / (1024 * 1024):.2f} MB)"
            elif size_bytes >= 1024:
                return f"{size_bytes:,.0f} bytes ({size_bytes / 1024:.2f} KB)"
            return f"{size_bytes:,.0f} bytes"

        sqs_limit_bytes = 1024 * 1024  # 1 MB SQS message limit
        exceeds_limit = overall_max > sqs_limit_bytes

        context.log.info("  SQS Message Size Metrics (gzip compressed):")
        context.log.info(f"    Average: {format_size(overall_avg)}")
        context.log.info(f"    Maximum: {format_size(overall_max)}")
        context.log.info(f"    Minimum: {format_size(overall_min)}")
        context.log.info(f"    SQS Limit: {format_size(sqs_limit_bytes)}")
        if exceeds_limit:
            context.log.warning(
                f"    ⚠️  WARNING: Max message size ({format_size(overall_max)}) EXCEEDS SQS limit!"
            )
        else:
            context.log.info(f"    ✓ All messages within SQS limit")

    context.log.info("  Partition durations:")
    for partition_duration in agg.partition_durations:
        context.log.info(
            f"    Partition {partition_duration['partition_idx']}: "
            f"{partition_duration['time_seconds']:.2f} seconds"
        )

    if agg.failed_partitions > 0:
        context.log.error(f"Encountered {agg.failed_partitions} failed partition(s):")
        for i, error in enumerate(agg.errors, 1):
            context.log.error(f"  Error {i}: {error}")
        raise Failure(
            f"Processing failed in {agg.failed_partitions} partition(s). See logs for details."
        )

    context.log.info("All partitions processed successfully!")


REGION_CONFIGS = {
    "us": {
        "region": "us-west-2",
        "resource_key": "dbx_step_launcher_gp_us",
        "partitions_def": BASE_PARTITIONS_DEF,
    },
    "eu": {
        "region": "eu-west-1",
        "resource_key": "dbx_step_launcher_gp_eu",
        "partitions_def": BASE_PARTITIONS_DEF,
    },
    "ca": {
        "region": "ca-central-1",
        "resource_key": "dbx_step_launcher_gp_ca",
        "partitions_def": adjust_partition_def_for_canada(BASE_PARTITIONS_DEF),
    },
}


def _write_eco_driving_features_logic(context: AssetExecutionContext) -> None:
    """
    Core logic for writing eco driving features to ML infrastructure.
    This is shared across all regions.
    """
    start_time = time.time()
    df = None
    try:
        # Parse multi-partition key: "date|hour" format (e.g., "2024-12-12|09")
        partition_key = context.partition_key
        date_str, hour_str = partition_key.split("|")
        start_date = date_str
        end_date = date_str  # Same date for hourly partitions

        limit: Optional[int] = None
        if "limit" in context.run.tags:
            limit = int(context.run.tags["limit"])

        org_ids: Optional[list[int]] = None
        if "org_ids" in context.run.tags:
            org_ids = [
                int(org_id.strip())
                for org_id in context.run.tags["org_ids"].split(",")
                if org_id.strip()
            ]
            if "limit" not in context.run.tags:
                limit = None

        device_ids: Optional[list[int]] = None
        if "device_ids" in context.run.tags:
            device_ids = [
                int(device_id.strip())
                for device_id in context.run.tags["device_ids"].split(",")
                if device_id.strip()
            ]
            if "limit" not in context.run.tags:
                limit = None

        # min(number of executors * cores per executor, number of partitions) can be processed in parallel:
        # number of executors = 4, cores per executor = 8, total cores = 32
        # which means number of partitions can be at most 32.

        # SQS limit = Standard queues support a very high, nearly unlimited number of API calls per second, per action.
        # Each batch (at max) = 10 asset intervals = 10 messages (each with all the features - 51 features per message)
        # Each batch is written in chunks of 10, that means 1 API call per batch (100ms delay between chunks).
        # This could support nearly unlimited concurrent batches.

        # Conservatively cap at 100 concurrent batches based on the SQS limit.
        if "max_partitions" in context.run.tags:
            max_partitions = int(context.run.tags["max_partitions"])
        else:
            max_partitions = 32

        if "max_concurrent_batches" in context.run.tags:
            max_concurrent_batches = int(context.run.tags["max_concurrent_batches"])
        else:
            max_concurrent_batches = 100

        env = get_run_env()
        region = get_region_from_context(context)

        running_env = get_running_env(env, region)

        eco_driving_features_db_name = get_eco_driving_features_db_name(env)

        role_name = get_role_name(env)

        context.log.info(
            f"Job parameters: start_date: {start_date}, hour: {hour_str}, env: {env}, region: {region}, running_env: {running_env}, eco_driving_features_db_name: {eco_driving_features_db_name}, role_name: {role_name}"
            f", org_ids: {org_ids if org_ids else 'all'}"
            f", device_ids: {device_ids if device_ids else 'all'}"
            f", limit: {limit if limit else 'all'}"
        )

        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        filters = QueryFilters(
            start_date=start_date,
            end_date=end_date,
            hour=hour_str,
            org_ids=org_ids,
            device_ids=device_ids,
            limit=limit,
        )
        query = _build_query(eco_driving_features_db_name, filters)

        context.log.info(f"Query: {query}")

        df = spark.sql(query)
        df = df.cache()

        total_rows = df.count()
        context.log.info(f"Total rows: {total_rows}")

        if total_rows == 0:
            context.log.info("No rows to process.")
            return None

        # Calculate optimal partition count based on number of unique orgs
        # Estimated scale: 3.5M input rows -> 3.5M SQS messages, 1 per asset_interval_id
        # Partitioning by org_id ensures each org stays together for efficient batching
        # This is critical because we batch writes per asset_interval_id (cycle_id)
        org_count = df.select("org_id").distinct().count()

        # Never create more partitions than orgs to avoid empty partitions. Ensure at least 1 partition
        target = max(1, min(max_partitions, org_count))

        context.log.info(
            f"Processing {total_rows} rows from {org_count} orgs with {target} partitions in parallel."
        )

        df = df.repartition(target, F.col("org_id"))

        sqs_url = get_sqs_queue_url(running_env)

        from databricks.sdk.runtime import dbutils

        boto3_session = boto3.Session(
            botocore_session=dbutils.credentials.getServiceCredentialsProvider(
                role_name
            )
        )

        # Retrieve credentials from the session. These are passed to the partition processor.
        # Each partition processor will create its own boto3 client session.
        credentials = boto3_session.get_credentials()
        frozen_creds = credentials.get_frozen_credentials()

        aws_access_key_id = frozen_creds.access_key
        aws_secret_access_key = frozen_creds.secret_key
        aws_session_token = frozen_creds.token

        # Check run tags for message size tracking (disabled by default to avoid overhead)
        track_message_sizes = False
        if "track_message_sizes" in context.run.tags:
            track_message_sizes = (
                context.run.tags["track_message_sizes"].lower() == "true"
            )

        if track_message_sizes:
            context.log.info("Message size tracking enabled via run tag")

        process_partition = create_partition_processor(
            credentials={
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "aws_session_token": aws_session_token,
            },
            config={
                "region": region,
                "sqs_url": sqs_url,
                "max_concurrent_batches": max_concurrent_batches,
                "track_message_sizes": track_message_sizes,
            },
        )

        # Convert DataFrame to RDD and use mapPartitions to collect metrics from executors
        context.log.info(
            "Converting DataFrame to RDD for distributed processing with metrics collection"
        )
        rdd = df.rdd
        metrics_rdd = rdd.mapPartitions(process_partition)

        context.log.info("Collecting metrics from Spark executors...")
        all_metrics = metrics_rdd.collect()

        agg = _aggregate_partition_metrics(all_metrics)
        _log_metrics_summary(context, agg, total_partitions=len(all_metrics))

        return None

    except Failure:
        # Re-raise Failure exceptions to preserve specific diagnostic information.
        raise
    except Exception as e:
        context.log.error(f"Unexpected error in write_eco_driving_features: {e}")
        raise Failure(
            "Write eco driving features operation failed. Job will be marked as failed."
        )

    finally:
        end_time = time.time()
        if df is not None:
            try:
                df.unpersist()
            except Exception as cleanup_error:
                context.log.warning(f"Failed to unpersist dataframe: {cleanup_error}")
        context.log.info(
            f"Total time taken: {((end_time - start_time) / 60):.1f} minutes"
        )


def create_write_eco_driving_features_asset(region_config: dict):
    """
    Factory function to create a write_eco_driving_features asset for a specific region.

    Args:
        region_config: Dictionary containing region, resource_key, partitions_def.

    Returns:
        A Dagster asset function
    """
    region = region_config["region"]
    resource_key = region_config["resource_key"]
    partitions_def = region_config["partitions_def"]

    @asset(
        key=[region, "sustainability", "write_eco_driving_features"],
        partitions_def=partitions_def,
        deps=[
            AssetDep(
                AssetKey(
                    [
                        region,
                        "feature_store",
                        "fct_eco_driving_key_cycle_features_hourly",
                    ]
                ),
                partition_mapping=IdentityPartitionMapping(),
            )
        ],
        required_resource_keys={resource_key},
        description="Writes eco driving key cycle features to SQS queue",
        owners=["team:Sustainability"],
        op_tags={
            "notifications": json.dumps(["slack:alerts-sustainability-low-urgency"]),
        },
    )
    def write_eco_driving_features(context: AssetExecutionContext) -> None:
        """
        Reads data from fct_eco_driving_key_cycle_features_hourly and
        writes Eco Driving Key Cycle features to SQS queue.
        SQS queue URL is determined based on the environment.
        """
        return _write_eco_driving_features_logic(context)

    return write_eco_driving_features


# Generate assets dynamically for all regions
write_eco_driving_features_us = create_write_eco_driving_features_asset(
    REGION_CONFIGS["us"]
)
write_eco_driving_features_eu = create_write_eco_driving_features_asset(
    REGION_CONFIGS["eu"]
)
write_eco_driving_features_ca = create_write_eco_driving_features_asset(
    REGION_CONFIGS["ca"]
)
