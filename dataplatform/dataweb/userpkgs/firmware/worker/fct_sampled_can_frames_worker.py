"""
Worker functions for CAN frames processing that run on Spark worker nodes.
These functions are isolated from dataweb imports to avoid serialization issues.
"""
from typing import List, Optional, Tuple, NamedTuple

from dagster import AssetExecutionContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, BinaryType

SPARK_SCHEMA = StructType(
    [
        StructField("date", StringType(), False),
        StructField("org_id", LongType(), False),
        StructField("device_id", LongType(), False),
        StructField("timestamp_unix_us", LongType(), False),
        StructField("source_interface", StringType(), False),
        StructField("id", LongType(), False),
        StructField("payload", BinaryType(), False),
        StructField("direction", LongType(), False),
        StructField("trace_uuid", StringType(), False),
        StructField("_source", StringType(), True),  # Temporary column for metrics: "telemetry", "messagebus", or None
    ]
)

# Final output schema without the temporary _source column
FINAL_SCHEMA = StructType(
    [
        StructField("date", StringType(), False),
        StructField("org_id", LongType(), False),
        StructField("device_id", LongType(), False),
        StructField("timestamp_unix_us", LongType(), False),
        StructField("source_interface", StringType(), False),
        StructField("id", LongType(), False),
        StructField("payload", BinaryType(), False),
        StructField("direction", LongType(), False),
        StructField("trace_uuid", StringType(), False),
    ]
)

# Metrics structure to track worker processing statistics
class WorkerMetrics(NamedTuple):
    """Metrics collected from worker partitions."""
    frames_from_messagebus: int
    frames_from_telemetry: int
    traces_with_messagebus: int
    traces_with_telemetry: int
    traces_with_none: int



# Worker functions moved to worker_functions.py to avoid dataweb import serialization issues
def wrap_worker_processing(
    context: AssetExecutionContext, traces_df: DataFrame
) -> Tuple[DataFrame, WorkerMetrics]:
    """
    Worker nodes need all functions to be defined locally to avoid import serialization issues.
    This function wraps the worker processing in a way that avoids the need to define the worker
    functions in the main module.
    """

    # Define worker function locally to completely avoid import serialization issues
    def process_trace_partition(iterator):
        import pandas as pd
        import os
        import gzip

        WORKER_PRIMARY_KEYS = [
            "date",
            "org_id",
            "device_id",
            "timestamp_unix_us",
            "source_interface",
            "id",
            "payload",
            "direction",
            "trace_uuid",
            "_source",  # Temporary column for metrics tracking
        ]

        # Define helper functions locally to avoid import serialization issues

        def load_metadata_from_retrieval_worker(
            trace_uuid: str,
        ) -> Tuple[Optional[dict], Optional[str]]:
            """
            Load metadata.json file from sensor-replay directory.

            Args:
                trace_uuid: Trace UUID

            Returns:
                Tuple of (metadata_dict, error_message)
                On success: (metadata_dict, None)
                On failure: (None, error_message)
            """
            import json

            base_path = f"/Volumes/s3/sensor-replay/root/{trace_uuid}"

            try:
                # Look for files ending with -metadata.json (e.g., "5AQaalHw3e-metadata.json")
                if not os.path.exists(base_path):
                    error_msg = f"Trace directory does not exist: {base_path}"
                    return None, error_msg

                # List files and find the one ending with -metadata.json
                all_files = os.listdir(base_path)
                metadata_files = [f for f in all_files if f.endswith("-metadata.json")]

                if not metadata_files:
                    error_msg = f"No -metadata.json file found in {base_path}"
                    return None, error_msg

                if len(metadata_files) > 1:
                    error_msg = f"Multiple -metadata.json files found in {base_path}: {metadata_files}"
                    return None, error_msg

                metadata_path = os.path.join(base_path, metadata_files[0])

                with open(metadata_path, "r") as f:
                    metadata = json.load(f)

                return metadata, None

            except json.JSONDecodeError as e:
                error_msg = f"Error parsing JSON metadata from {metadata_path}: {e}"
                return None, error_msg
            except Exception as e:
                error_msg = f"Error loading metadata from {base_path}: {e}"
                return None, error_msg

        def load_telemetry_metadata_from_retrieval_worker(
            trace_uuid: str,
        ) -> Tuple[Optional[dict], Optional[str]]:
            """
            Load telemetry metadata from retrieval worker metadata.json file.
            Extracts telemetry file info and computes monotonic to unix offset.

            Args:
                trace_uuid: Trace UUID

            Returns:
                Tuple of (telemetry_info_dict, error_message)
                On success: (telemetry_info_dict with computed offset, None)
                On failure: (None, error_message)
            """
            from samsaradev.io.hubproto.camera_pb2 import DataStream

            # Load full metadata
            metadata, error = load_metadata_from_retrieval_worker(trace_uuid)
            if error or metadata is None:
                return None, error

            # Search FileInfos array for telemetry entry
            file_infos = metadata.get("FileInfos")
            if file_infos is None:
                error_msg = f"No file infos found in metadata for trace_uuid={trace_uuid}"
                return None, error_msg

            telemetry_file_info = None

            for file_info in file_infos:
                # Check if this is a telemetry stream
                data_stream_id = file_info.get("data_stream_id")
                if data_stream_id == DataStream.DATA_STREAM_TELEMETRY:
                    telemetry_file_info = file_info
                    break

            if telemetry_file_info is None:
                error_msg = f"No telemetry file info found in metadata for trace_uuid={trace_uuid}"
                return None, error_msg

            # Extract required fields for monotonic offset calculation
            # dashcam_report_start_time is nested in DashcamReport.start_time
            dashcam_report = metadata.get("DashcamReport", {})
            dashcam_report_start_time = dashcam_report.get("start_time")
            start_ago_ms = telemetry_file_info.get("start_ago_ms", 0)
            start_time_mono_ms = telemetry_file_info.get("start_time_mono_ms")

            if dashcam_report_start_time is None:
                error_msg = f"Missing DashcamReport.start_time in metadata"
                return None, error_msg

            if start_time_mono_ms is None:
                error_msg = f"Missing start_time_mono_ms in telemetry file info"
                return None, error_msg

            # Compute monotonic to unix offset
            monotonic_to_unix_offset_ms = (
                dashcam_report_start_time
                - start_ago_ms
                - start_time_mono_ms
            )

            # Return telemetry file info with computed offset
            telemetry_info = {
                **telemetry_file_info,
                "monotonic_to_unix_offset_ms": monotonic_to_unix_offset_ms,
            }

            return telemetry_info, None

        def find_telemetry_file_from_metadata_worker(
            trace_uuid: str,
            telemetry_metadata: dict,
        ) -> Tuple[Optional[str], Optional[str]]:
            """
            Find telemetry file path from telemetry metadata.
            Constructs S3 path to telemetry file and verifies it exists.

            Args:
                trace_uuid: Trace UUID
                telemetry_metadata: Telemetry file info dict from load_telemetry_metadata_from_retrieval_worker

            Returns:
                Tuple of (file_path, error_message)
                On success: (file_path, None)
                On failure: (None, error_message)
            """
            base_path = f"/Volumes/s3/sensor-replay/root/{trace_uuid}"

            # Get filename from telemetry metadata
            filename = telemetry_metadata.get("filename")
            if not filename:
                error_msg = f"No filename found in telemetry metadata for trace_uuid={trace_uuid}"
                return None, error_msg

            try:
                # Look for files ending with the filename (may have a prefix, e.g., "HiCHFw0scw-1764429436595.telemetry.gz")
                if not os.path.exists(base_path):
                    error_msg = f"Trace directory does not exist: {base_path}"
                    return None, error_msg

                # List files and find the one ending with the filename
                all_files = os.listdir(base_path)
                matching_files = [f for f in all_files if f.endswith(filename)]

                if not matching_files:
                    error_msg = f"No file ending with '{filename}' found in {base_path}"
                    return None, error_msg

                if len(matching_files) > 1:
                    error_msg = f"Multiple files ending with '{filename}' found in {base_path}: {matching_files}"
                    return None, error_msg

                file_path = os.path.join(base_path, matching_files[0])
                return file_path, None

            except Exception as e:
                error_msg = f"Error finding telemetry file for trace {trace_uuid}: {e}"
                return None, error_msg

        def load_telemetry_file_from_s3_worker(
            file_path: str,
        ) -> Tuple[Optional[bytes], Optional[str]]:
            """
            Load telemetry file from S3 path.
            Handles both gzipped and uncompressed files.

            Args:
                file_path: Full path to telemetry file

            Returns:
                Tuple of (raw_protobuf_bytes, error_message)
                On success: (bytes, None)
                On failure: (None, error_message)
            """
            try:
                if not os.path.exists(file_path):
                    error_msg = f"Telemetry file does not exist: {file_path}"
                    return None, error_msg

                # Check if file is gzipped based on extension
                if file_path.endswith(".gz"):
                    # Load and decompress gzipped file
                    with gzip.open(file_path, "rb") as f:
                        data = f.read()
                else:
                    # Load uncompressed file
                    with open(file_path, "rb") as f:
                        data = f.read()

                return data, None

            except gzip.BadGzipFile as e:
                error_msg = f"Error decompressing gzipped telemetry file {file_path}: {e}"
                return None, error_msg
            except Exception as e:
                error_msg = f"Error loading telemetry file from {file_path}: {e}"
                return None, error_msg

        def get_source_interface_name_worker(
            can_batch,
            index: int,
        ) -> str:
            """
            Get source interface name from CanFrameBatch using index.

            Args:
                can_batch: CanFrameBatch protobuf message
                index: Index into source_interface_names array

            Returns:
                Source interface name string, or "invalid index" if index is out of bounds
            """
            try:
                if index < 0 or index >= len(can_batch.source_interface_names):
                    return "invalid index"
                return can_batch.source_interface_names[index]
            except Exception:
                return "invalid index"

        def convert_monotonic_to_unix_us(monotonic_us: int, offset_ms: int) -> int:
            """
            Convert monotonic timestamp (microseconds) to Unix timestamp (microseconds).

            Truncates to millisecond precision by design:
            - The monotonic_to_unix_offset_ms is provided in milliseconds from firmware metadata
            - Preserving sub-millisecond precision would be misleading since the offset itself
              is only accurate to milliseconds
            - This ensures consistency: all timestamps are aligned to millisecond boundaries

            Args:
                monotonic_us: Monotonic timestamp in microseconds
                offset_ms: Offset from monotonic to Unix time in milliseconds

            Returns:
                Unix timestamp in microseconds, truncated to millisecond precision
            """
            # Truncate monotonic timestamp to milliseconds: int(us / 1000) * 1000
            # Then add offset converted to microseconds: offset_ms * 1000
            return int(monotonic_us / 1000) * 1000 + (offset_ms * 1000)

        def extract_can_frames_from_telemetry_worker(
            telemetry_data: bytes,
            trace_uuid: str,
            org_id: int,
            device_id: int,
            partition_date: str,
            monotonic_to_unix_offset_ms: int,
        ) -> Tuple[List[tuple], Optional[str]]:
            """
            Extract CAN frames from telemetry protobuf format.

            Returns list of frame tuples matching schema:
            (date, org_id, device_id, timestamp_unix_us, source_interface, id, payload, direction, trace_uuid)

            Args:
                telemetry_data: Raw telemetry protobuf data bytes
                trace_uuid: Trace UUID
                org_id: Organization ID
                device_id: Device ID
                partition_date: Partition date string
                monotonic_to_unix_offset_ms: Offset to convert monotonic time to unix time

            Returns:
                Tuple of (frame_tuples_list, error_message)
                On success: (tuples_list, None)
                On failure: ([], error_message)
            """
            CAN_FRAME_TELEMETRY_TOPIC = "vg/can_frame_batch/v1"

            try:
                from samsaradev.io.hubproto.telemetryproto.telemetry_pb2 import TelemetryStream
                from samsaradev.io.hubproto.telemetryproto.can_frame_batch_pb2 import CanFrameBatch
            except ImportError as e:
                error_msg = f"Failed to import telemetry proto modules: {e}"
                return [], error_msg

            try:
                # Parse TelemetryStream
                stream = TelemetryStream()
                stream.ParseFromString(telemetry_data)

                all_frames = []

                for msg in stream.telemetry:
                    # Filter for CAN frame batch topic
                    if msg.topic != CAN_FRAME_TELEMETRY_TOPIC:
                        continue

                    if len(msg.payload) == 0:
                        continue  # Skip empty payloads

                    # Parse CanFrameBatch
                    can_batch = CanFrameBatch()
                    can_batch.ParseFromString(msg.payload)

                    # Process base frame
                    base_monotonic_us = msg.timestamp.monotonic_us
                    base_unix_us = convert_monotonic_to_unix_us(base_monotonic_us, monotonic_to_unix_offset_ms)
                    base_source_interface = get_source_interface_name_worker(can_batch, can_batch.source_interface_index)

                    # Process batch frames (in reverse order)
                    batch_length = min(
                        len(can_batch.batch_ids),
                        len(can_batch.batch_payloads),
                        len(can_batch.batch_directions),
                        len(can_batch.batch_source_interface_indices),
                        len(can_batch.batch_delta_timestamp_us)
                    )

                    batch_frames = []
                    previous_monotonic_us = base_monotonic_us

                    # Process batch in reverse (will reverse again later)
                    for batch_index in range(batch_length - 1, -1, -1):
                        frame_monotonic_us = previous_monotonic_us - can_batch.batch_delta_timestamp_us[batch_index]
                        previous_monotonic_us = frame_monotonic_us
                        frame_unix_us = convert_monotonic_to_unix_us(frame_monotonic_us, monotonic_to_unix_offset_ms)

                        source_interface = get_source_interface_name_worker(
                            can_batch, can_batch.batch_source_interface_indices[batch_index]
                        )

                        frame_tuple = (
                            partition_date,
                            org_id,
                            device_id,
                            frame_unix_us,
                            source_interface,
                            int(can_batch.batch_ids[batch_index]) & 0x1FFFFFFF,
                            can_batch.batch_payloads[batch_index],
                            int(can_batch.batch_directions[batch_index]),
                            trace_uuid,
                        )
                        batch_frames.append(frame_tuple)

                    # Reverse to chronological order
                    batch_frames.reverse()

                    # Add base frame (most recent)
                    base_frame_tuple = (
                        partition_date,
                        org_id,
                        device_id,
                        base_unix_us,
                        base_source_interface,
                        int(can_batch.id) & 0x1FFFFFFF,
                        can_batch.payload,
                        int(can_batch.direction),
                        trace_uuid,
                    )
                    batch_frames.append(base_frame_tuple)

                    all_frames.extend(batch_frames)

                return all_frames, None

            except Exception as e:
                error_msg = f"Error extracting CAN frames from telemetry: {e}"
                return [], error_msg

        def find_sensor_replay_messagebus_archives_worker(
            trace_uuid: str,
        ) -> Tuple[List[str], Optional[str]]:
            """
            Find all sensor replay messagebus archives for a given trace UUID.
            Worker version that doesn't depend on dataweb imports.
            """
            base_path = f"/Volumes/s3/sensor-replay/root/{trace_uuid}"

            try:
                if not os.path.exists(base_path):
                    error_msg = f"Trace directory does not exist: {base_path}"
                    return [], error_msg

                # List all files in the trace directory
                all_files = os.listdir(base_path)
                messagebus_archives = [
                    os.path.join(base_path, f)
                    for f in all_files
                    if f.endswith(".messagebus.gz")
                ]

                return messagebus_archives, None

            except Exception as e:
                error_msg = (
                    f"Error finding messagebus files for trace {trace_uuid}: {e}"
                )
                return [], error_msg

        def load_messagebus_archive_from_s3_worker(
            file_path: str,
        ) -> Tuple[Optional[bytes], Optional[str]]:
            """
            Load and decompress a gzipped protobuf file.
            Worker version that doesn't depend on dataweb imports.
            """
            try:
                if not os.path.exists(file_path):
                    error_msg = f"File does not exist: {file_path}"
                    return None, error_msg

                # Load and decompress the file
                with gzip.open(file_path, "rb") as f:
                    data = f.read()

                return data, None

            except Exception as e:
                error_msg = f"Error loading compressed protobuf from {file_path}: {e}"
                return None, error_msg

        def extract_can_frames_from_bus_message_stream(
            protobuf_data: bytes,
            trace_uuid: str,
            org_id: int,
            device_id: int,
            partition_date: str,
        ) -> Tuple[List[tuple], Optional[str]]:
            """
            Extract CAN frames as tuples from a protobuf messagebus file.

            This function extracts CAN frames from a protobuf messagebus file and returns a list
            of tuples. Tuples are ordered as follows:
            (date, org_id, device_id, timestamp_unix_us, source_interface, id, payload, direction, trace_uuid)

            Args:
                protobuf_data: Raw protobuf data bytes
                trace_uuid: The trace UUID for linking frames to their source trace
                org_id: Organization ID
                device_id: Device ID
                partition_date: Partition date string

            Returns:
                Tuple of (frame_tuples_list, error_message)
                On success: (tuples_list, None)
                On failure: ([], error_message)
            """

            try:
                # Parse the protobuf data
                stream = BusMessageStream()
                stream.ParseFromString(protobuf_data)

                # Collect frame data as tuples for incremental extension
                frame_tuples = []

                # Extract CAN frames from the stream
                for message in stream.bus_message:
                    if (
                        message.type == BusMessage.Type.CAN_FRAMES
                        and len(message.payload_encoded) > 0
                    ):
                        # Parse the payload
                        payload = BusMessage.Payload()
                        payload.ParseFromString(message.payload_encoded)

                        # Process CAN frames from this message
                        if hasattr(payload, "can_frames") and payload.can_frames:
                            for can_frame in payload.can_frames:
                                # Create row tuple matching the schema order
                                row = (
                                    partition_date,  # date
                                    org_id,  # org_id
                                    device_id,  # device_id
                                    int(
                                        can_frame.timestamp.unix_us
                                    ),  # timestamp_unix_us
                                    can_frame.source_interface,  # source_interface
                                    int(can_frame.id)
                                    & 0x1FFFFFFF,  # id - mask out top 3 bits set by linux and not part of the 29-bit CAN ID
                                    can_frame.payload,  # payload (bytes)
                                    int(can_frame.direction),  # direction
                                    trace_uuid,  # trace_uuid
                                )
                                frame_tuples.append(row)

                return frame_tuples, None

            except Exception as e:
                error_msg = f"Error extracting CAN frames from protobuf: {e}"
                return [], error_msg

        def process_telemetry_for_trace_worker(
            trace_uuid: str,
            org_id: int,
            device_id: int,
            partition_date: str,
        ) -> Tuple[List[tuple], bool]:
            """
            Process telemetry files for a trace and extract CAN frames.

            Args:
                trace_uuid: Trace UUID
                org_id: Organization ID
                device_id: Device ID
                partition_date: Partition date string

            Returns:
                Tuple of (telemetry_tuples_list, success_flag)
                On success: (tuples_list, True)
                On failure or no telemetry: ([], False)
            """
            # Load telemetry metadata
            telemetry_metadata, error = load_telemetry_metadata_from_retrieval_worker(trace_uuid)
            if error or telemetry_metadata is None:
                return [], False

            # Find telemetry file
            telemetry_file_path, error = find_telemetry_file_from_metadata_worker(
                trace_uuid, telemetry_metadata
            )
            if error or telemetry_file_path is None:
                return [], False

            # Load telemetry file
            telemetry_data, error = load_telemetry_file_from_s3_worker(telemetry_file_path)
            if error or telemetry_data is None:
                return [], False

            # Extract CAN frames from telemetry
            monotonic_to_unix_offset_ms = telemetry_metadata.get("monotonic_to_unix_offset_ms")
            if monotonic_to_unix_offset_ms is None:
                return [], False

            telemetry_tuples, error = extract_can_frames_from_telemetry_worker(
                telemetry_data,
                trace_uuid,
                org_id,
                device_id,
                partition_date,
                monotonic_to_unix_offset_ms,
            )
            if error or not telemetry_tuples:
                return [], False

            return telemetry_tuples, True

        # message imports
        from samsaradev.io.hubproto.bus_message_pb2 import BusMessageStream

        # This import requires the following wheel to be installed that is updated by firmware buildkite
        # /Volumes/s3/databricks-workspace/firmware/python_proto_latest/wheels/fwproto-0.0.1-py3-none-any.whl
        from firmware.samsaradev.io.fwproto.bus_message_pb2 import BusMessage

        # Main processing loop for traces assigned to this worker
        all_frame_tuples = []

        for pdf in iterator:
            for _, row in pdf.iterrows():
                trace_uuid = row["TraceUuid"]
                org_id = int(row["SourceOrgId"])
                device_id = int(row["SourceDeviceId"])
                partition_date = row["conversion_date"]

                # Try telemetry first (prefer telemetry over messagebus)
                telemetry_tuples, telemetry_processed = process_telemetry_for_trace_worker(
                    trace_uuid, org_id, device_id, partition_date
                )
                if telemetry_processed:
                    # Add source column to each tuple
                    telemetry_tuples_with_source = [
                        tuple(list(t) + ["telemetry"]) for t in telemetry_tuples
                    ]
                    all_frame_tuples.extend(telemetry_tuples_with_source)
                else:
                    # Fallback to messagebus if telemetry not processed
                    messagebus_files, error = find_sensor_replay_messagebus_archives_worker(
                        trace_uuid
                    )
                    if not error and messagebus_files:
                        for file_path in messagebus_files:
                            protobuf_data, error = load_messagebus_archive_from_s3_worker(
                                file_path
                            )
                            if error:
                                continue

                            file_tuples, error = extract_can_frames_from_bus_message_stream(
                                protobuf_data, trace_uuid, org_id, device_id, partition_date
                            )
                            if error:
                                continue
                            # Add source column to each tuple
                            file_tuples_with_source = [
                                tuple(list(t) + ["messagebus"]) for t in file_tuples
                            ]
                            all_frame_tuples.extend(file_tuples_with_source)

        # Combine all frames and deduplicate
        result_pdf = pd.DataFrame(
            all_frame_tuples, columns=WORKER_PRIMARY_KEYS
        ).drop_duplicates()

        yield result_pdf

    results_df = traces_df.mapInPandas(process_trace_partition, schema=SPARK_SCHEMA)

    # Cache the DataFrame before metrics collection so that when evaluation happens,
    # the data is cached and can be reused for downstream operations. This avoids
    # re-executing the expensive mapInPandas pipeline.
    results_df.cache()

    from pyspark.sql import functions as F

    # Aggregate metrics in one pass - this will trigger one evaluation and populate cache
    metrics_agg = results_df.agg(
        F.count(F.when(F.col("_source") == "messagebus", 1)).alias("frames_from_messagebus"),
        F.count(F.when(F.col("_source") == "telemetry", 1)).alias("frames_from_telemetry"),
        F.countDistinct(F.when(F.col("_source") == "messagebus", F.col("trace_uuid"))).alias("traces_with_messagebus"),
        F.countDistinct(F.when(F.col("_source") == "telemetry", F.col("trace_uuid"))).alias("traces_with_telemetry"),
    ).collect()[0]

    frames_from_messagebus = metrics_agg["frames_from_messagebus"]
    frames_from_telemetry = metrics_agg["frames_from_telemetry"]
    traces_with_messagebus = metrics_agg["traces_with_messagebus"]
    traces_with_telemetry = metrics_agg["traces_with_telemetry"]

    # Count total traces processed
    total_traces = traces_df.select("TraceUuid").distinct().count()
    traces_with_none = total_traces - traces_with_messagebus - traces_with_telemetry

    # Select only the columns we need (excluding the temporary _source column)
    results_df = results_df.select(*[
        "date", "org_id", "device_id", "timestamp_unix_us",
        "source_interface", "id", "payload", "direction", "trace_uuid"
    ])

    # Create aggregated metrics
    metrics = WorkerMetrics(
        traces_with_messagebus=traces_with_messagebus,
        traces_with_telemetry=traces_with_telemetry,
        traces_with_none=traces_with_none,
        frames_from_messagebus=frames_from_messagebus,
        frames_from_telemetry=frames_from_telemetry,
    )

    # Return both DataFrame and metrics
    return results_df, metrics
