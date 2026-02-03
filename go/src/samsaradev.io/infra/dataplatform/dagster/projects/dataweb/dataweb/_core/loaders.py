import json
import os
from types import ModuleType
from typing import Any, Dict, List, Tuple, Union

from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    JobDefinition,
    MultiPartitionsDefinition,
    PartitionKeyRange,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SourceAsset,
)
from dagster import _check as check
from dagster import define_asset_job, schedule
from dagster._core.definitions.load_assets_from_modules import find_modules_in_package
from dagster._core.definitions.target import ExecutableDefinition
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from .utils import split_to_groups

DATAWEB_JOBS_DIR = os.path.abspath(os.path.join(__file__, "../../jobs"))


def load_assets_defs_by_key(module: ModuleType) -> Dict[AssetKey, AssetsDefinition]:

    modules = find_modules_in_package(module)

    assets = {}
    for module in modules:
        for attr in dir(module):
            value = getattr(module, attr)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, (AssetsDefinition, SourceAsset)):
                        assets[item.key] = item
    return assets


def load_job_configs_by_name() -> Dict[str, Any]:

    configs = {}
    for root, _, files in os.walk(DATAWEB_JOBS_DIR, followlinks=True):
        for fname in files:
            abs_fname = os.path.join(root, fname)

            if not fname.endswith(".json"):
                continue

            with open(f"{abs_fname}", "r") as f:
                config = json.load(f)

            job_name = config["name"]
            check.invariant(
                os.path.splitext(fname)[0] == job_name,
                f"""The job name must match the name of the file that defines it.
                'name': {config['name']} does not match file name: {os.path.splitext(fname)[0]}""",
            )
            configs[job_name] = config

    return configs


def load_job_def_from_config(
    config: Dict[str, Any]
) -> Union[JobDefinition, UnresolvedAssetJobDefinition]:

    metadata = {"owner": config["owner"]}

    assets_selection = []
    for tag in config["assets"]:
        tag_parts = tag.split(":")
        check.invariant(
            len(tag_parts) == 2, "Asset tag must be prefixed with a region specifier."
        )
        regions = tag_parts[0].split("|")
        for region in regions:
            database, entity = tag_parts[1].split(".")
            key = AssetKey([region, database, entity])
            assets_selection.append(key)

    # Kubernetes resource settings for Dagster job worker
    tags = {}
    if "container_config" in config:
        tags["dagster-k8s/config"] = {"container_config": config["container_config"]}

    # Dagster job configuration
    job_config_params = {}
    if "max_concurrent" in config:
        job_config_params["execution"] = {
            "config": {"multiprocess": {"max_concurrent": config["max_concurrent"]}}
        }

    job_def = define_asset_job(
        name=config["name"],
        selection=AssetSelection.assets(*assets_selection),
        metadata=metadata,
        tags=tags,
        config=job_config_params,
    )

    return job_def


def load_schedule_def_from_config(
    job: ExecutableDefinition, config: Dict[str, Any]
) -> ScheduleDefinition:

    schedule_name = f"{job.name}_{config['name']}"

    # Use backfill_window set explicitly, otherwise default to 1.
    backfill_window = config.get("backfill_window", 1)

    @schedule(name=schedule_name, job=job, cron_schedule=config["cron"])
    def _schedule(context: ScheduleEvaluationContext):
        scheduled_execution_time = (
            context.scheduled_execution_time.replace(tzinfo=None)
            .isoformat()
            .replace(":", ".")[:63]
        )

        job_def = job.resolve(asset_graph=context.repository_def.asset_graph)
        backfill_batch_size = config.get("backfill_batch_size", 1)

        partitions_def = job_def.partitions_def
        if partitions_def is None:
            context.log.info("Evaluating schedule for unpartitioned run.")
            return RunRequest(
                run_key=f"{scheduled_execution_time}",
                tags={"dagster/backfill": scheduled_execution_time},
            )

        # When partitions_def is a MultiPartitionsDefinition, the elements of partitions_defs
        # are instances of the PartitionDimensionDefinition class, as such their partitions_def
        # needs to be extracted.
        if isinstance(partitions_def, MultiPartitionsDefinition):
            partitions_defs = partitions_def.partitions_defs
            primary_partitions_def = partitions_defs[0].partitions_def
            secondary_partitions_def = partitions_defs[1].partitions_def
        else:
            primary_partitions_def = partitions_def
            secondary_partitions_def = None

        primary_partition_keys = primary_partitions_def.get_partition_keys()

        if len(primary_partition_keys) > backfill_window:
            primary_partition_keys = primary_partition_keys[-backfill_window:]

        # If backfill_window is 0, use the scheduled execution time as the partition key range start.
        if backfill_window == 0:
            scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
            partition_key_range_start = scheduled_date
            partition_key_range_end = scheduled_date
        else:
            partition_key_range_start = primary_partition_keys[0]
            partition_key_range_end = primary_partition_keys[-1]

        if secondary_partitions_def:
            # When use_scheduled_hour_partition is true, use the scheduled execution hour
            # as the secondary partition key. The cron schedule should align with partition keys.
            # Example: cron "0 */3 * * *" with partitions ["00", "03", ...] -> schedule at 09:00 uses "09"
            if config.get("use_scheduled_hour_partition"):
                secondary_key = f"{context.scheduled_execution_time.hour:02d}"
                context.log.info(
                    f"Using scheduled hour as secondary partition: {secondary_key}"
                )
                partition_key_range_start = (
                    f"{partition_key_range_start}|{secondary_key}"
                )
                partition_key_range_end = f"{partition_key_range_end}|{secondary_key}"
            else:
                # Default behavior: use all secondary partitions
                partition_key_range_start = f"{partition_key_range_start}|{secondary_partitions_def.get_first_partition_key()}"
                partition_key_range_end = f"{partition_key_range_end}|{secondary_partitions_def.get_last_partition_key()}"

        partition_keys = partitions_def.get_partition_keys_in_range(
            partition_key_range=PartitionKeyRange(
                start=partition_key_range_start,
                end=partition_key_range_end,
            )
        )

        context.log.info(
            f"Evaluating schedule run for partition keys: {partition_keys}"
        )
        if backfill_batch_size == 1:
            run_requests = [
                RunRequest(
                    run_key=f"{scheduled_execution_time}-{key}",
                    tags={
                        "dagster/backfill": scheduled_execution_time,
                        "dagster/asset_partition_range_start": key,
                        "dagster/asset_partition_range_end": key,
                    },
                )
                for key in partition_keys or []
            ]
        elif backfill_batch_size == -1:
            # -1 indicates a single run backfill
            run_requests = [
                RunRequest(
                    run_key=f"{scheduled_execution_time}",
                    tags={
                        "dagster/backfill": scheduled_execution_time,
                        "dagster/asset_partition_range_start": partition_keys[0],
                        "dagster/asset_partition_range_end": partition_keys[-1],
                    },
                )
            ]
        elif backfill_batch_size > 1:
            run_requests = [
                RunRequest(
                    run_key=f"{scheduled_execution_time}-{batch[0]}-{batch[-1]}",
                    tags={
                        "dagster/backfill": scheduled_execution_time,
                        "dagster/asset_partition_range_start": batch[0],
                        "dagster/asset_partition_range_end": batch[-1],
                    },
                )
                for batch in split_to_groups(partition_keys, backfill_batch_size)
            ]
        else:
            raise ValueError("no run requests found...")
        return run_requests

    return _schedule


def load_job_defs_schedules() -> Tuple[
    List[Union[JobDefinition, UnresolvedAssetJobDefinition]], List[ScheduleDefinition]
]:
    job_configs = load_job_configs_by_name()

    job_defs_by_name = {}
    for job_name, job_config in job_configs.items():
        job_def = load_job_def_from_config(job_config)
        job_defs_by_name[job_name] = job_def

    schedules_defs_by_name = {}
    for job_name, job_config in job_configs.items():
        for schedule_config in job_config["schedules"] or []:
            job_def = job_defs_by_name[job_name]
            schedule_def = load_schedule_def_from_config(job_def, schedule_config)
            schedules_defs_by_name[schedule_def.name] = schedule_def

    return list(job_defs_by_name.values()), list(schedules_defs_by_name.values())
