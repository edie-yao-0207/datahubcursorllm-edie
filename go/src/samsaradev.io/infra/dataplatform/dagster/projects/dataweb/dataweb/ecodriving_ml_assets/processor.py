"""
Partition processor for writing eco-driving features to SQS.

This module provides the core processing logic for distributed Spark workers,
handling batch writes to SQS with retry logic.
"""

from datetime import datetime
from typing import Any, Optional, TypedDict

from .config import FEATURE_NAME_TO_ID, METADATA_COLUMNS, VALID_FEATURE_NAMES

# =============================================================================
# Driver-side Types (used for create_partition_processor signature)
# =============================================================================
# These types are only used on the driver side and don't need to be serialized.

# =============================================================================
# AWS Configuration Types
# =============================================================================
# Types for AWS credentials and client configuration used across the processor.


class AWSCredentials(TypedDict):
    """AWS credentials for boto3 session."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str


class ProcessorConfig(TypedDict):
    """Configuration for the partition processor."""

    region: str
    sqs_url: str
    max_concurrent_batches: int
    track_message_sizes: bool


def create_partition_processor(
    credentials: AWSCredentials,
    config: ProcessorConfig,
):
    """
    Create a partition processing function with injected credentials and config required for the processing.

    Args:
        credentials: AWSCredentials containing access key, secret key, and session token
        config: ProcessorConfig containing region, sqs_url, and max_concurrent_batches

    Returns:
        process_partition: Function that processes a partition of data
    """
    access_key = str(credentials["aws_access_key_id"])
    secret_key = str(credentials["aws_secret_access_key"])
    session_token = str(credentials["aws_session_token"])

    region = str(config["region"])
    sqs_queue_url = str(config["sqs_url"])

    max_concurrent_batches = config["max_concurrent_batches"]
    track_message_sizes = config["track_message_sizes"]

    # =========================================================================
    # Executor-side Types (defined inside closure to avoid module references)
    # =========================================================================
    # These types are defined inside the closure so cloudpickle serializes them
    # by value rather than trying to import the dataweb module on executors.

    class ClientConfig(TypedDict):
        """AWS clients for SQS operations."""

        sqs_client: Any  # boto3 SQS client

    # =============================================================================
    # SQS Types
    # =============================================================================
    # Types for SQS message structures and batch send operations.
    class SQSMessage(TypedDict):
        """
        Message format for SQS queue using flattened sub_intervals format.

        sub_intervals is a list of interval objects, each containing:
        - start_ms: interval start timestamp in milliseconds
        - end_ms: interval end timestamp in milliseconds
        - <feature_name>: feature value (multiple features per interval)

        Example:
        {
            "efficiency_interval_id": "xxx",
            "org_id": 123,
            "sub_intervals": [
                {"start_ms": 1000, "end_ms": 2000, "1": 500, "2": 300}, // 1 and 2 are feature IDs from config.py (e.g. "cruise_control_ms" and "coasting_ms")
                {"start_ms": 2000, "end_ms": 3000, "1": 800}
            ]
        }
        """

        efficiency_interval_id: str
        org_id: int
        cloud_cell_id: int
        device_id: int
        enrichment_due_at: datetime
        sub_intervals: list[dict[str, Any]]

    class SQSBatchConfig(TypedDict):
        """Configuration for an SQS batch send operation."""

        sqs_batch: list[SQSMessage]
        sqs_client: Any  # boto3 SQS client
        sqs_url: str
        track_sizes: bool

    # Since context.log can't be serialized, we define a simple logger for worker.
    # Prefix with [SPARK-EXECUTOR] to distinguish from driver logs.
    class WorkerLogger:
        PREFIX = "[SPARK-EXECUTOR]"

        def info(self, msg):
            print(f"{self.PREFIX} INFO: {msg}", flush=True)

        def debug(self, msg):
            print(f"{self.PREFIX} DEBUG: {msg}", flush=True)

        def warning(self, msg):
            print(f"{self.PREFIX} WARNING: {msg}", flush=True)

        def error(self, msg):
            print(f"{self.PREFIX} ERROR: {msg}", flush=True)

    # Result types for processing functions - using NamedTuple for immutability and clarity
    from typing import NamedTuple

    class OrgProcessingResult(NamedTuple):
        """Result of processing rows for a single org."""

        org_id: int
        success: bool
        error_message: str
        row_count: int
        sqs_messages_sent: int
        total_message_bytes: int
        max_message_size_bytes: int
        min_message_size_bytes: int

    class BatchProcessingResult(NamedTuple):
        """Result of processing a single batch of SQS items."""

        batch_index: int
        success: bool
        error_message: str
        messages_sent: int
        total_message_bytes: int
        max_message_size_bytes: int
        min_message_size_bytes: int

    class WriteCountResult(NamedTuple):
        """Result counts from writing to SQS."""

        sqs_messages_sent: int
        total_message_bytes: int
        max_message_size_bytes: int
        min_message_size_bytes: int

    class SQSSendMessageResult(NamedTuple):
        """Result of sending messages to SQS, including size metrics."""

        success: bool
        error_message: str
        messages_sent: int
        total_message_bytes: int
        max_message_size_bytes: int
        min_message_size_bytes: int

    def process_partition(iterator):
        """
        Process a single partition of data.

        This function runs on a Spark worker node. All imports happen within the function
        and the closure to avoid import serialization issues.

        Process flow:
        - Create boto3 session with credentials from driver.
        - Create sqs client.
        - Process rows per org.
            - Generate formatted batches of SQS items.
            - Write batches to SQS in parallel.
        - Return an iterator with the results of the processing.

        Args:
            iterator: Iterator of rows from Spark DataFrame
        """
        import time
        from collections import defaultdict

        import boto3
        from dagster import Failure

        logger = WorkerLogger()

        # Metrics to track the partition processing. This will be returned to the driver.
        partition_start_time = time.time()
        total_rows_processed = 0
        total_orgs_processed = 0
        total_successful_orgs = 0
        failed_orgs = []
        total_sqs_messages_sent = 0
        success = False
        error_msg = None
        # Size metrics aggregation
        total_bytes_all_orgs = 0
        total_messages_all_orgs = 0
        all_max_sizes = []
        all_min_sizes = []

        try:
            # Create boto3 session with credentials from driver.
            boto3_session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region,
            )

            sqs_client = boto3_session.client("sqs", region_name=region)

            client_config = {
                "sqs_client": sqs_client,
            }

            rows = list(iterator)

            if not rows:
                logger.warning("No rows in partition. Skipping.")
                success = True
                return

            logger.info(f"Processing {len(rows)} rows in partition")

            rows_by_org = defaultdict(list)
            for row in rows:
                rows_by_org[row["org_id"]].append(row)

            logger.info(
                f"Processing {len(rows_by_org)} orgs. "
                f"Max concurrent batches: {max_concurrent_batches}"
            )

            for org_id, org_rows in rows_by_org.items():
                try:
                    logger.info(f"[org_id: {org_id}] Processing {len(org_rows)} rows")

                    result = process_org(
                        org_id,
                        org_rows,
                        client_config,
                        logger,
                    )

                    if result.success:
                        total_successful_orgs += 1
                        total_rows_processed += result.row_count
                        total_orgs_processed += 1
                        total_sqs_messages_sent += result.sqs_messages_sent

                        if result.sqs_messages_sent > 0:
                            total_bytes_all_orgs += result.total_message_bytes
                            total_messages_all_orgs += result.sqs_messages_sent
                            all_max_sizes.append(result.max_message_size_bytes)
                            all_min_sizes.append(result.min_message_size_bytes)
                    else:
                        failed_orgs.append((result.org_id, result.error_message))
                        logger.error(
                            f"[org_id: {org_id}] Failed: {result.error_message}"
                        )

                except Exception as e:
                    failed_orgs.append((org_id, str(e)))
                    logger.error(f"[org_id: {org_id}] Failed: {e}")

            if failed_orgs:
                error_summary = "; ".join(
                    [f"Org {org_id}: {msg}" for org_id, msg in failed_orgs]
                )
                raise Failure(
                    f"Failed to process {len(failed_orgs)} out of {len(rows_by_org)} orgs. Errors: {error_summary}"
                )

            logger.info(
                f"Successfully processed all {total_successful_orgs} orgs - "
                f"Total: {total_rows_processed} rows collected, "
                f"{total_sqs_messages_sent} messages sent to SQS"
            )

            success = True

        except Failure as f:
            error_msg = str(f)
            logger.error(f"Partition failed with Failure: {f}")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Exception in partition processing: {error_msg}")
        finally:
            partition_duration = time.time() - partition_start_time

            # Calculate aggregate size metrics across all orgs
            overall_avg_size = (
                total_bytes_all_orgs / total_messages_all_orgs
                if total_messages_all_orgs > 0
                else 0
            )
            overall_max_size = max(all_max_sizes) if all_max_sizes else 0
            overall_min_size = min(all_min_sizes) if all_min_sizes else 0

            metrics = {
                "success": success,
                "total_rows_processed": total_rows_processed,
                "total_orgs_processed": total_orgs_processed,
                "total_successful_orgs": total_successful_orgs,
                "failed_orgs": failed_orgs,
                "total_sqs_messages_sent": total_sqs_messages_sent,
                "time_seconds": partition_duration,
                "error": error_msg,
                "total_message_bytes": total_bytes_all_orgs,
                "max_message_size_bytes": overall_max_size,
                "min_message_size_bytes": overall_min_size,
            }
            return iter([metrics])

    def process_org(
        org_id: int,
        rows: list,
        client_config: ClientConfig,
        logger: WorkerLogger,
    ) -> OrgProcessingResult:
        """
        Process rows for a single org.

        Args:
            org_id: Org ID being processed
            rows: List of rows from the source table for the given org_id
            client_config: Configuration dict with sqs_client
            logger: Logger for logging

        Returns:
            OrgProcessingResult with processing status and metrics
        """
        try:
            count = len(rows)
            if count == 0:
                logger.info(f"[org_id: {org_id}] No rows found.")
                return OrgProcessingResult(
                    org_id=org_id,
                    success=True,
                    error_message="",
                    row_count=0,
                    sqs_messages_sent=0,
                    total_message_bytes=0,
                    max_message_size_bytes=0,
                    min_message_size_bytes=0,
                )

            logger.info(f"[org_id: {org_id}] Collected {count} rows.")

            write_counts = process_rows_for_org(org_id, rows, client_config, logger)

            avg_size = (
                write_counts.total_message_bytes / write_counts.sqs_messages_sent
                if write_counts.sqs_messages_sent > 0
                else 0
            )
            logger.info(
                f"[org_id: {org_id}] Successfully processed with "
                f"{count} rows collected, "
                f"{write_counts.sqs_messages_sent} SQS messages sent. "
                f"Message sizes (bytes): avg={avg_size:.0f}, "
                f"max={write_counts.max_message_size_bytes}, min={write_counts.min_message_size_bytes}"
            )
            return OrgProcessingResult(
                org_id=org_id,
                success=True,
                error_message="",
                row_count=count,
                sqs_messages_sent=write_counts.sqs_messages_sent,
                total_message_bytes=write_counts.total_message_bytes,
                max_message_size_bytes=write_counts.max_message_size_bytes,
                min_message_size_bytes=write_counts.min_message_size_bytes,
            )

        except Exception as e:
            error_msg = f"[org_id: {org_id}] Error processing: {str(e)}"
            logger.error(error_msg)
            return OrgProcessingResult(
                org_id=org_id,
                success=False,
                error_message=error_msg,
                row_count=0,
                sqs_messages_sent=0,
                total_message_bytes=0,
                max_message_size_bytes=0,
                min_message_size_bytes=0,
            )

    def process_rows_for_org(
        org_id: int, rows: list, client_config: ClientConfig, logger: WorkerLogger
    ) -> WriteCountResult:
        """
        Process rows for an org, generating and writing batches to SQS.

        Args:
            org_id: Org ID being processed
            rows: List of rows from the source table for the given org_id
            client_config: ClientConfig containing sqs_client
            logger: Logger for logging

        Returns:
            WriteCountResult with counts of SQS messages sent
        """
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            from dagster import Failure

            sqs_client = client_config["sqs_client"]

            # sqs_batch_map: asset_interval_id -> sqs_batch
            sqs_batch_map = get_formatted_sqs_batches(org_id, rows, logger)
            if sqs_batch_map is None:
                logger.warning(
                    f"[org_id: {org_id}] No SQS batches found. Skipping {len(rows)} rows for this org."
                )
                return WriteCountResult(
                    sqs_messages_sent=0,
                    total_message_bytes=0,
                    max_message_size_bytes=0,
                    min_message_size_bytes=0,
                )

            total_sqs_messages = len(sqs_batch_map)

            logger.info(
                f"[org_id: {org_id}] Prepared batches with {len(rows)} rows:: "
                f"Total asset interval ids: {total_sqs_messages}, "
                f"Total SQS messages: {total_sqs_messages}."
            )

            # Keeping the same max batch size as the max batch size for SQS.
            max_batch_size = 10

            batches_to_process = []
            sqs_write_batch = []
            batch_index = 0

            # Track sqs_write_batch for 10 items for each write action.
            for asset_interval_id in sqs_batch_map.keys():
                sqs_message = sqs_batch_map[asset_interval_id]
                sqs_write_batch.append(sqs_message)

                if len(sqs_write_batch) == max_batch_size:
                    batches_to_process.append(
                        {
                            "batch_index": batch_index,
                            "sqs_batch": sqs_write_batch.copy(),
                        }
                    )
                    batch_index += 1
                    sqs_write_batch = []

            # Handling remaining items in batches after processing all rows.
            if len(sqs_write_batch) > 0:
                batches_to_process.append(
                    {
                        "batch_index": batch_index,
                        "sqs_batch": sqs_write_batch.copy(),
                    }
                )

            total_batches = len(batches_to_process)

            if total_batches == 0:
                logger.warning(
                    f"[org_id: {org_id}] No batches to process after filtering. "
                    f"All {len(rows)} rows may have been filtered out due to null cycle_id or missing features."
                )
                return WriteCountResult(
                    sqs_messages_sent=0,
                    total_message_bytes=0,
                    max_message_size_bytes=0,
                    min_message_size_bytes=0,
                )

            max_workers = min(config["max_concurrent_batches"], total_batches)

            logger.info(
                f"[org_id: {org_id}] Processing {total_batches} batches with {max_workers} parallel workers."
            )
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(
                        process_batch,
                        batch["batch_index"],
                        {
                            "sqs_batch": batch["sqs_batch"],
                            "sqs_client": sqs_client,
                            "sqs_url": sqs_queue_url,
                            "track_sizes": track_message_sizes,
                        },
                        logger,
                    ): batch["batch_index"]
                    for batch in batches_to_process
                }

                successful_batches = 0
                failed_batches = []

                total_bytes_all_batches = 0
                total_messages_sent = 0
                all_max_sizes = []
                all_min_sizes = []

                for future in as_completed(future_to_batch):
                    batch_result = future.result()

                    if batch_result.success:
                        successful_batches += 1
                        total_messages_sent += batch_result.messages_sent
                        if batch_result.messages_sent > 0:
                            total_bytes_all_batches += batch_result.total_message_bytes
                            all_max_sizes.append(batch_result.max_message_size_bytes)
                            all_min_sizes.append(batch_result.min_message_size_bytes)
                    else:
                        failed_batches.append(
                            (batch_result.batch_index, batch_result.error_message)
                        )
                        logger.error(
                            f"Batch {batch_result.batch_index} failed: {batch_result.error_message}"
                        )

                overall_avg = (
                    total_bytes_all_batches / total_messages_sent
                    if total_messages_sent > 0
                    else 0
                )
                overall_max = max(all_max_sizes) if all_max_sizes else 0
                overall_min = min(all_min_sizes) if all_min_sizes else 0

                if failed_batches:
                    error_summary = "; ".join(
                        [
                            f"[org_id: {org_id}] Batch {idx}: {msg}"
                            for idx, msg in failed_batches
                        ]
                    )
                    raise Failure(
                        f"[org_id: {org_id}] Failed to process {len(failed_batches)} out of {total_batches} batches. Errors: {error_summary}"
                    )

                logger.info(
                    f"[org_id: {org_id}] Successfully processed all {successful_batches} batches "
                    f"({total_messages_sent} SQS messages). "
                    f"Message sizes (bytes): avg={overall_avg:.0f}, max={overall_max}, min={overall_min}"
                )

                return WriteCountResult(
                    sqs_messages_sent=total_messages_sent,
                    total_message_bytes=total_bytes_all_batches,
                    max_message_size_bytes=overall_max,
                    min_message_size_bytes=overall_min,
                )

        except Failure:
            raise
        except Exception as e:
            logger.error(f"[org_id: {org_id}] Unexpected error in process_rows: {e}")
            raise Failure(
                f"[org_id: {org_id}] Process rows operation failed. Job will be marked as failed."
            )

    def process_batch(
        batch_index: int,
        sqs_config: SQSBatchConfig,
        logger: WorkerLogger,
    ) -> BatchProcessingResult:
        """
        process_batch writes a batch of SQS items to SQS.

        Args:
            context: AssetExecutionContext for logging
            batch_index: Index of the batch
            sqs_config: Configuration for SQS batch send

        Returns:
            BatchProcessingResult with processing status and size metrics
        """
        try:
            sqs_result = send_messages_to_sqs(
                sqs_config["sqs_batch"],
                sqs_config["sqs_client"],
                sqs_config["sqs_url"],
                logger,
                track_sizes=sqs_config.get("track_sizes", False),
            )
            if not sqs_result.success:
                return BatchProcessingResult(
                    batch_index=batch_index,
                    success=False,
                    error_message=f"SQS send operation failed for batch {batch_index}: {sqs_result.error_message}",
                    messages_sent=sqs_result.messages_sent,
                    total_message_bytes=sqs_result.total_message_bytes,
                    max_message_size_bytes=sqs_result.max_message_size_bytes,
                    min_message_size_bytes=sqs_result.min_message_size_bytes,
                )

            logger.info(
                f"Successfully processed batch {batch_index} with {len(sqs_config['sqs_batch'])} asset efficiency intervals."
            )
            return BatchProcessingResult(
                batch_index=batch_index,
                success=True,
                error_message="",
                messages_sent=sqs_result.messages_sent,
                total_message_bytes=sqs_result.total_message_bytes,
                max_message_size_bytes=sqs_result.max_message_size_bytes,
                min_message_size_bytes=sqs_result.min_message_size_bytes,
            )

        except Exception as e:
            error_msg = f"Error processing batch {batch_index}: {str(e)}"
            logger.error(error_msg)
            return BatchProcessingResult(
                batch_index=batch_index,
                success=False,
                error_message=error_msg,
                messages_sent=0,
                total_message_bytes=0,
                max_message_size_bytes=0,
                min_message_size_bytes=0,
            )

    def apply_backoff_with_jitter(
        backoff: float, retry_count: int, logger: WorkerLogger
    ) -> tuple[float, int]:
        import random
        import time

        """Sleep using exponential backoff with jitter, then return updated (backoff, retry_count)."""
        sleep_time = backoff + random.uniform(0, 0.1)  # 100ms of jitter
        logger.debug(f"Sleeping for {sleep_time:.2f}s before retry {retry_count+1}")
        time.sleep(sleep_time)
        backoff = min(backoff * 2, 120)  # Cap at 2 mins
        retry_count += 1
        return backoff, retry_count

    def get_formatted_sqs_batches(
        org_id: int, rows: list, logger: WorkerLogger
    ) -> Optional[dict[str, SQSMessage]]:
        """
        Args:
            org_id: Org ID being processed
            rows: List of rows from the source table
            logger: Logger for logging
        Returns:
            Dictionary of asset_interval_id -> formatted SQS message containing all the features for the asset_interval_id.

            Uses a flattened sub_intervals format where each interval object contains:
            - start_ms: interval start timestamp in milliseconds
            - end_ms: interval end timestamp in milliseconds
            - <feature_name>: feature value (only present if not null)

            This format is more compact than the nested format because:
            1. start_ms/end_ms are shared per interval (not repeated per feature)
            2. No "value" key needed - feature name is the key directly
        """
        try:
            sqs_batch_map = {}
            invalid_row_count = 0
            skipped_columns_set = set()

            for row in rows:
                # Required columns
                org_id = row["org_id"]
                device_id = row["device_id"]
                asset_interval_id = row["cycle_id"]
                interval_start = row["interval_start"]
                interval_end = row["interval_end"]
                org_cell_id = row["org_cell_id"]
                is_j1939 = row["is_j1939"]
                associated_vehicle_engine_type = row["associated_vehicle_engine_type"]
                enrichment_due_at = row["enrichment_due_at"]

                if asset_interval_id is None:
                    invalid_row_count += 1
                    continue

                start_ms = (
                    int(interval_start.timestamp() * 1000) if interval_start else None
                )
                end_ms = int(interval_end.timestamp() * 1000) if interval_end else None

                if sqs_batch_map.get(asset_interval_id) is None:
                    sqs_batch_map[asset_interval_id] = {
                        "sub_intervals": [],
                        "efficiency_interval_id": asset_interval_id,
                        "org_id": org_id,
                        "cloud_cell_id": org_cell_id,
                        "device_id": device_id,
                        "enrichment_due_at": enrichment_due_at,
                        "is_j1939": is_j1939,
                        "associated_vehicle_engine_type": associated_vehicle_engine_type,
                    }

                # Build sub_interval object with start_ms, end_ms, and all feature values
                sub_interval = {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }

                for column_name in row.asDict().keys():
                    # Skip metadata columns
                    if column_name in METADATA_COLUMNS:
                        continue

                    if column_name not in VALID_FEATURE_NAMES:
                        skipped_columns_set.add(column_name)
                        continue

                    value = row[column_name]

                    # Skip null values
                    if value is None:
                        continue

                    # Use numeric feature ID as string key (from config.py)
                    # Convert to string for consistent JSON serialization and TypedDict compliance
                    feature_id = str(FEATURE_NAME_TO_ID[column_name])
                    sub_interval[feature_id] = value

                # Append sub_interval after collecting all features (only if has features)
                if len(sub_interval) > 2:  # More than just start_ms and end_ms
                    sqs_batch_map[asset_interval_id]["sub_intervals"].append(
                        sub_interval
                    )

            if invalid_row_count > 0:
                logger.info(
                    f"[org_id: {org_id}] Skipped {invalid_row_count} invalid rows with null asset_interval_id."
                )

            if skipped_columns_set:
                logger.warning(
                    f"[org_id: {org_id}] Skipped columns: {skipped_columns_set}."
                )

            # Filter out messages with empty sub_intervals (no valid feature data)
            empty_interval_ids = [
                asset_interval_id
                for asset_interval_id, msg in sqs_batch_map.items()
                if not msg["sub_intervals"]
            ]
            for asset_interval_id in empty_interval_ids:
                del sqs_batch_map[asset_interval_id]

            if empty_interval_ids:
                logger.info(
                    f"[org_id: {org_id}] Filtered out {len(empty_interval_ids)} "
                    f"asset intervals with no valid feature data."
                )

            return sqs_batch_map
        except Exception as e:
            logger.error(
                f"[org_id: {org_id}] Unexpected error in get_formatted_sqs_batches: {e}"
            )
            return None

    def serialize_message_gzip(message: SQSMessage) -> str:
        """
        Serialize an SQSMessage to gzip-compressed JSON, base64 encoded for SQS.
        This is used to reduce the size of the message payload to stay under the SQS message size limit.

        Args:
            message: SQSMessage dict to serialize

        Returns:
            Base64-encoded gzip-compressed JSON string
        """
        import base64
        import gzip
        import json
        from datetime import datetime as dt
        from decimal import Decimal

        def json_encoder(obj):
            """Custom JSON encoder for Spark/Python types."""
            if isinstance(obj, Decimal):
                # Convert Decimal to float for JSON serialization
                return float(obj)
            if isinstance(obj, dt):
                return obj.isoformat()
            # Fallback: convert unknown types to string
            return str(obj)

        # Serialize to JSON with custom encoder, compress with gzip, encode as base64
        json_bytes = json.dumps(
            message, separators=(",", ":"), default=json_encoder
        ).encode("utf-8")
        compressed = gzip.compress(json_bytes)
        return base64.b64encode(compressed).decode("utf-8")

    def send_messages_to_sqs(
        messages: list[SQSMessage],
        sqs_client,
        sqs_url: str,
        logger: WorkerLogger,
        track_sizes: bool = False,
    ) -> SQSSendMessageResult:
        """
        Sends messages to the SQS queue using SendMessage API.
        Messages are gzip-compressed to reduce payload size.
        SendMessage API message size limit is 1MB.

        Args:
            messages: List of messages to send
            sqs_client: SQS client
            sqs_url: URL of the SQS queue
            logger: Logger for logging
            track_sizes: Whether to track message sizes (disabled by default for performance)
        Returns:
            SQSSendMessageResult with success status, error message, and size metrics
        """
        import time

        from botocore.exceptions import ClientError

        # Track message sizes for metrics (only if enabled)
        message_sizes = [] if track_sizes else None

        try:
            successful_writes = 0
            max_retries = 8
            message_delay = 0.01  # 10ms delay between messages

            for idx, message in enumerate(messages):
                # Serialize with gzip compression.
                # TODO: Replace with protobuf serialization.
                message_body = serialize_message_gzip(message)

                if track_sizes:
                    message_size = len(message_body.encode("utf-8"))
                    message_sizes.append(message_size)

                retry_count = 0
                backoff = 1  # seconds

                while retry_count < max_retries:
                    try:
                        sqs_client.send_message(
                            QueueUrl=sqs_url,
                            MessageBody=message_body,
                        )
                        successful_writes += 1
                        break

                    except ClientError as e:
                        error_code = e.response["Error"]["Code"]
                        if error_code in [
                            "ThrottlingException",
                            "RequestLimitExceeded",
                        ]:
                            logger.warning(
                                f"SQS throttled ({error_code}); retrying message {idx} (attempt {retry_count+1})"
                            )
                            backoff, retry_count = apply_backoff_with_jitter(
                                backoff, retry_count, logger
                            )
                            continue
                        else:
                            error_msg = f"SQS send failed for message {idx}: {error_code} -> {str(e)}"
                            logger.error(error_msg)
                            return SQSSendMessageResult(
                                success=False,
                                error_message=error_msg,
                                messages_sent=successful_writes,
                                total_message_bytes=(
                                    sum(message_sizes) if message_sizes else 0
                                ),
                                max_message_size_bytes=(
                                    max(message_sizes) if message_sizes else 0
                                ),
                                min_message_size_bytes=(
                                    min(message_sizes) if message_sizes else 0
                                ),
                            )

                    except Exception as e:
                        error_msg = (
                            f"Unexpected error sending message {idx} to SQS: {e}"
                        )
                        logger.error(error_msg)
                        return SQSSendMessageResult(
                            success=False,
                            error_message=error_msg,
                            messages_sent=successful_writes,
                            total_message_bytes=(
                                sum(message_sizes) if message_sizes else 0
                            ),
                            max_message_size_bytes=(
                                max(message_sizes) if message_sizes else 0
                            ),
                            min_message_size_bytes=(
                                min(message_sizes) if message_sizes else 0
                            ),
                        )
                else:
                    error_msg = (
                        f"Giving up after {max_retries} retries for message {idx}"
                    )
                    logger.error(error_msg)
                    return SQSSendMessageResult(
                        success=False,
                        error_message=error_msg,
                        messages_sent=successful_writes,
                        total_message_bytes=(
                            sum(message_sizes) if message_sizes else 0
                        ),
                        max_message_size_bytes=(
                            max(message_sizes) if message_sizes else 0
                        ),
                        min_message_size_bytes=(
                            min(message_sizes) if message_sizes else 0
                        ),
                    )

                time.sleep(message_delay)

            if message_sizes:
                total_bytes = sum(message_sizes)
                max_size = max(message_sizes)
                min_size = min(message_sizes)
                avg_size = total_bytes / len(message_sizes)
                logger.info(
                    f"Successfully sent {successful_writes} messages to SQS. "
                    f"Message sizes (bytes): avg={avg_size:.0f}, max={max_size}, min={min_size}"
                )
            else:
                total_bytes = 0
                max_size = 0
                min_size = 0
                logger.info(f"Successfully sent {successful_writes} messages to SQS")

            return SQSSendMessageResult(
                success=True,
                error_message="",
                messages_sent=successful_writes,
                total_message_bytes=total_bytes,
                max_message_size_bytes=max_size,
                min_message_size_bytes=min_size,
            )

        except Exception as e:
            error_msg = f"Unexpected error in SQS send operation: {e}"
            logger.error(error_msg)
            return SQSSendMessageResult(
                success=False,
                error_message=error_msg,
                messages_sent=successful_writes,
                total_message_bytes=sum(message_sizes) if message_sizes else 0,
                max_message_size_bytes=max(message_sizes) if message_sizes else 0,
                min_message_size_bytes=min(message_sizes) if message_sizes else 0,
            )

    def send_message_batches_to_sqs(
        messages: list[SQSMessage],
        sqs_client,
        sqs_url: str,
        logger: WorkerLogger,
    ) -> Optional[str]:
        """
        Sends a batch of messages to the SQS queue using SendMessageBatch API.
        SendMessageBatch API message size limit is 1MB in total and maximum 1MB per message.
        API supports maximum 10 messages per batch, so message size is limited to 100KB.

        Args:
            messages: List of messages to send
            sqs_client: SQS client
            sqs_url: URL of the SQS queue
            logger: Logger for logging
        Returns:
            None if successful, error message string if failed
        Raises:
            Exception: If an unexpected error occurs
        """
        import json
        import time

        from botocore.exceptions import ClientError

        try:
            successful_writes = 0

            batch_size = 10  # Send in batches of 10 (SQS batch send limit).
            max_retries = 8  # Exponential backoff up to 2 mins max
            batch_delay = 0.1  # 100ms of delay between batches

            for i in range(0, len(messages), batch_size):
                batch = messages[i : i + batch_size]
                entries = [
                    {"Id": str(i + j), "MessageBody": json.dumps(m)}
                    for j, m in enumerate(batch)
                ]

                retry_count = 0
                backoff = 1  # seconds

                while retry_count < max_retries:
                    try:
                        response = sqs_client.send_message_batch(
                            QueueUrl=sqs_url, Entries=entries
                        )

                        failed = response.get("Failed", [])

                        if not failed:
                            successful_writes += len(batch)
                            break

                        logger.warning(
                            f"{len(failed)} messages failed; retrying (attempt {retry_count+1})"
                        )
                        backoff, retry_count = apply_backoff_with_jitter(
                            backoff, retry_count, logger
                        )
                        entries = [
                            {"Id": f["Id"], "MessageBody": f["MessageBody"]}
                            for f in failed
                        ]

                    except ClientError as e:
                        error_code = e.response["Error"]["Code"]
                        if error_code in [
                            "ThrottlingException",
                            "RequestLimitExceeded",
                        ]:
                            logger.warning(
                                f"SQS throttled ({error_code}); retrying (attempt {retry_count+1})"
                            )
                            backoff, retry_count = apply_backoff_with_jitter(
                                backoff, retry_count, logger
                            )
                            continue
                        else:
                            error_msg = f"SQS batch failed: {error_code} -> {str(e)}"
                            logger.error(error_msg)
                            # Fail immediately on non-retriable error
                            return error_msg

                    except Exception as e:
                        error_msg = f"Unexpected error sending to SQS: {e}"
                        logger.error(error_msg)
                        # Fail immediately on unexpected error
                        return error_msg
                else:
                    error_msg = f"Giving up after {max_retries} retries for {len(entries)} messages"
                    logger.error(error_msg)
                    # Fail immediately after exhausting retries
                    return error_msg

                time.sleep(batch_delay)  # Delay between batches in seconds

            logger.info(
                f"Completed sending messages to SQS. Successfully sent {successful_writes} messages"
            )

            return None
        except Exception as e:
            error_msg = f"Unexpected error in SQS batch send operation: {e}"
            logger.error(error_msg)
            return error_msg

    return process_partition
