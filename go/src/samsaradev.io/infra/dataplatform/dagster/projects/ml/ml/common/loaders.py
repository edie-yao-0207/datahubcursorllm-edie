import json
import logging
import os
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, Union

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
)
from dagster import _check as check
from dagster import define_asset_job, schedule
from dagster._core.definitions.load_assets_from_modules import find_modules_in_package
from dagster._core.definitions.target import ExecutableDefinition
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from .utils import split_to_groups

ML_JOBS_DIR = os.path.abspath(os.path.join(__file__, "../../jobs"))
logger = logging.getLogger(__name__)


def validate_job_config(config: Dict[str, Any], filename: str) -> None:
    """Validate a job config against the JSON schema.

    Args:
        config: The job configuration dictionary
        filename: Name of the config file (for error messages)

    Raises:
        ValueError: If the config is invalid
    """
    try:
        import jsonschema
    except ImportError:
        logger.warning(
            "jsonschema not installed, skipping config validation. "
            "Install with: pip install jsonschema"
        )
        return

    schema_path = os.path.join(ML_JOBS_DIR, "job_config_schema.json")
    if not os.path.exists(schema_path):
        logger.warning(f"Schema file not found at {schema_path}, skipping validation")
        return

    try:
        with open(schema_path, "r") as f:
            schema = json.load(f)

        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.ValidationError as e:
        raise ValueError(
            f"Job config {filename} failed schema validation:\n"
            f"  Error: {e.message}\n"
            f"  Path: {'.'.join(str(p) for p in e.path)}"
        ) from e
    except Exception as e:
        logger.warning(f"Could not validate config {filename}: {e}")


def create_op_based_job_for_region(
    job_name: str,
    op_callable: Union[callable, List[callable]],
    region_resource: Optional[Any],
    config: Dict[str, Any],
) -> JobDefinition:
    """Create an op-based job for a specific region.

    Args:
        job_name: Name for the job (e.g., "anomaly_detection_eu")
        op_callable: The op function(s) to execute. Can be a single op
                    or a list of ops to chain together.
        region_resource: Optional Databricks resource for this region
        config: Job configuration from JSON (tags, container_config, etc.)

    Returns:
        JobDefinition configured for the specified region
    """
    from dagster import in_process_executor, job

    # Normalize op_callable to a list
    op_callables = op_callable if isinstance(op_callable, list) else [op_callable]

    # Extract tags from config
    tags = {}
    if "container_config" in config:
        tags["dagster-k8s/config"] = {"container_config": config["container_config"]}
    # Add default k8s config if not specified
    if "dagster-k8s/config" not in tags:
        tags["dagster-k8s/config"] = {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "512Mi"},
                }
            },
        }

    # Build resource_defs if region_resource is provided
    resource_defs = {}
    if region_resource is not None:
        resource_defs["databricks_pyspark_step_launcher"] = region_resource

    # Build op config from JSON (pass through fields like orgs, etc.)
    # Exclude job-level fields that shouldn't be passed to ops
    exclude_keys = {
        "name",
        "owner",
        "regions",
        "schedules",
        "container_config",
        "assets",
        "max_concurrent",
        "description",
        "op_name",
        "ops",
    }
    op_config_params = {k: v for k, v in config.items() if k not in exclude_keys}

    # Build job config with op config if any op params exist
    # Apply config to first op only (typically the main op)
    job_config = None
    if op_config_params and len(op_callables) > 0:
        first_op = op_callables[0]
        op_name = first_op.name if hasattr(first_op, "name") else first_op.__name__
        job_config = {"ops": {op_name: {"config": op_config_params}}}

    @job(
        name=job_name,
        executor_def=in_process_executor,
        resource_defs=resource_defs if resource_defs else None,
        tags=tags,
        config=job_config,
    )
    def _regional_job():
        # Chain ops together by passing output to next op's input
        # This creates a dependency graph: op1 -> op2 -> op3...
        result = op_callables[0]()
        for op in op_callables[1:]:
            result = op(result)

    return _regional_job


def load_assets_defs_by_key(
    module: ModuleType,
) -> Dict[AssetKey, AssetsDefinition]:

    modules = find_modules_in_package(module)

    assets = {}
    for module in modules:
        for attr in dir(module):
            value = getattr(module, attr)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, AssetsDefinition):
                        assets[item.key] = item
    return assets


def load_job_configs_by_name() -> Dict[str, Any]:

    configs = {}
    for root, _, files in os.walk(ML_JOBS_DIR, followlinks=True):
        for fname in files:
            abs_fname = os.path.join(root, fname)

            if not fname.endswith(".json"):
                continue

            # Skip the schema file itself
            if fname == "job_config_schema.json":
                continue

            with open(f"{abs_fname}", "r") as f:
                config = json.load(f)

            # Validate config against schema
            validate_job_config(config, fname)

            job_name = config["name"]
            check.invariant(
                os.path.splitext(fname)[0] == job_name,
                f"The job name must match the name of the file "
                f"that defines it. 'name': {config['name']} does not "
                f"match file name: {os.path.splitext(fname)[0]}",
            )
            configs[job_name] = config

    return configs


def auto_discover_ops(job_configs: Dict[str, Any]) -> Dict[str, callable]:
    """Auto-discover ops from the ops module based on JSON configs.

    For each job config, tries to import an op named {job_name}_op
    from the ops module. Only discovers ops for jobs that don't have
    "assets" defined (those are asset-based jobs, not op-based).

    Args:
        job_configs: Dict of job configurations loaded from JSON files

    Returns:
        Dict mapping job name -> op callable
    """
    import importlib
    import logging

    logger = logging.getLogger(__name__)

    # Import the ops module
    # From ml.common.loaders, we need to go up to ml.ops
    # Use absolute import to avoid confusion
    try:
        ops_module = importlib.import_module("ml.ops")
    except ImportError as e:
        logger.warning(f"Could not import ops module: {e}")
        return {}

    op_registry = {}
    for job_name, job_config in job_configs.items():
        # Skip asset-based jobs
        if "assets" in job_config:
            continue

        # Check if multiple ops are specified
        if "ops" in job_config:
            # Handle array of ops
            op_list = []
            missing_ops = []
            for base_op_name in job_config["ops"]:
                op_name = f"{base_op_name}_op"
                if hasattr(ops_module, op_name):
                    op_callable = getattr(ops_module, op_name)
                    op_list.append(op_callable)
                    logger.info(
                        f"[auto_discover_ops] Discovered op '{op_name}' "
                        f"for job '{job_name}'"
                    )
                else:
                    missing_ops.append(op_name)
                    logger.error(
                        f"[auto_discover_ops] No op found '{op_name}' "
                        f"for job '{job_name}'"
                    )

            # Fail if any ops are missing - silently building partial
            # chains could lead to data corruption or incorrect results
            if missing_ops:
                raise ValueError(
                    f"Job '{job_name}' specifies ops "
                    f"{job_config['ops']} but the following ops are "
                    f"missing: {missing_ops}. All specified ops must "
                    f"exist to prevent creating partial chains that "
                    f"skip processing steps."
                )

            if op_list:
                op_registry[job_name] = op_list
        else:
            # Single op: check if op_name is explicitly specified
            # Otherwise, derive from job_name
            if "op_name" in job_config:
                base_op_name = job_config["op_name"]
                op_name = f"{base_op_name}_op"
            else:
                op_name = f"{job_name}_op"

            if hasattr(ops_module, op_name):
                op_callable = getattr(ops_module, op_name)
                op_registry[job_name] = op_callable
                logger.info(
                    f"[auto_discover_ops] Discovered op '{op_name}' "
                    f"for job '{job_name}'"
                )
            else:
                logger.warning(
                    f"[auto_discover_ops] No op found for job '{job_name}' "
                    f"(expected '{op_name}' in ops module)"
                )

    return op_registry


def load_job_def_from_config(
    config: Dict[str, Any],
    job_name: Optional[str] = None,
    base_job_template: Optional[JobDefinition] = None,
    region_resource: Optional[Any] = None,
) -> Union[JobDefinition, UnresolvedAssetJobDefinition]:
    """Create a job definition from JSON config.

    For asset-based jobs: Creates job from assets list in config.
    For op-based jobs: Clones base_job_template with region-specific resource.
    """

    # Check if this is an op-based job (has a base template provided)
    if base_job_template is not None and region_resource is not None:
        # Op-based job: Clone the base job with region-specific resource
        from dagster import in_process_executor, job

        final_job_name = job_name if job_name is not None else config["name"]

        # Extract tags from config
        tags = {}
        if "container_config" in config:
            tags["dagster-k8s/config"] = {
                "container_config": config["container_config"]
            }

        # Get the op graph from base template
        base_graph = base_job_template.graph

        # Create new job with region-specific resource binding
        @job(
            name=final_job_name,
            executor_def=in_process_executor,
            resource_defs={"databricks_pyspark_step_launcher": region_resource},
            tags=tags,
        )
        def _regional_job():
            # Re-use the same op graph from base template
            return base_graph.invoke()

        return _regional_job

    # Asset-based job: Original logic
    # For ML project, assets may not have region prefix like dataweb
    # Support both formats: "database.table" and "region:database.table"
    assets_selection = []
    for tag in config.get("assets", []):
        if ":" in tag:
            # Format: "region:database.table"
            tag_parts = tag.split(":")
            check.invariant(
                len(tag_parts) == 2,
                "Asset tag must be prefixed with a region specifier.",
            )
            regions = tag_parts[0].split("|")
            for region in regions:
                table_parts = tag_parts[1].split(".")
                check.invariant(
                    len(table_parts) == 2,
                    f"Asset tag '{tag}' must have exactly one dot separator "
                    f"after the region prefix. Expected format: "
                    f"'region:database.table', but got '{tag_parts[1]}' "
                    f"which split into {len(table_parts)} parts.",
                )
                database, entity = table_parts
                key = AssetKey([region, database, entity])
                assets_selection.append(key)
        else:
            # Format: "database.table" (no region prefix)
            table_parts = tag.split(".")
            check.invariant(
                len(table_parts) == 2,
                f"Asset tag '{tag}' must have exactly one dot separator. "
                f"Expected format: 'database.table', but got "
                f"{len(table_parts)} parts.",
            )
            database, entity = table_parts
            key = AssetKey([database, entity])
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

    # Job metadata
    metadata = {"owner": config.get("owner", "DataEngineering")}

    # Use provided job_name (for region-specific jobs) or fall back
    # to config name
    final_job_name = job_name if job_name is not None else config["name"]

    job_def = define_asset_job(
        name=final_job_name,
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

    backfill_window = 1
    if config.get("backfill_window", 0) > 1:
        backfill_window = config.get("backfill_window")

    @schedule(name=schedule_name, job=job, cron_schedule=config["cron"])
    def _schedule(context: ScheduleEvaluationContext):
        scheduled_execution_time = (
            context.scheduled_execution_time.replace(tzinfo=None)
            .isoformat()
            .replace(":", ".")[:63]
        )

        # Resolve the job if it's unresolved, otherwise use it directly
        if isinstance(job, UnresolvedAssetJobDefinition):
            job_def = job.resolve(asset_graph=context.repository_def.asset_graph)
        else:
            job_def = job
        backfill_batch_size = config.get("backfill_batch_size", 1)

        partitions_def = job_def.partitions_def
        if partitions_def is None:
            context.log.info("Evaluating schedule for unpartitioned run.")
            return RunRequest(
                run_key=f"{scheduled_execution_time}",
                tags={"dagster/backfill": scheduled_execution_time},
            )

        # When partitions_def is a MultiPartitionsDefinition, the
        # elements of partitions_defs are instances of the
        # PartitionDimensionDefinition class, as such their
        # partitions_def needs to be extracted.
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

        partition_key_range_start = primary_partition_keys[0]
        partition_key_range_end = primary_partition_keys[-1]

        if secondary_partitions_def is None:
            # Single partitions definition
            batch_groups = split_to_groups(primary_partition_keys, backfill_batch_size)
            run_requests = []
            for batch in batch_groups:
                run_key = f"{scheduled_execution_time}_{batch[0]}_{batch[-1]}"
                run_requests.append(
                    RunRequest(
                        run_key=run_key,
                        partition_key=PartitionKeyRange(batch[0], batch[-1]),
                        tags={"dagster/backfill": scheduled_execution_time},
                    )
                )
        else:
            # Multi partitions definition
            secondary_partition_keys = secondary_partitions_def.get_partition_keys()

            all_partition_keys = []
            for primary_key in primary_partition_keys:
                for secondary_key in secondary_partition_keys:
                    partition_key = f"{primary_key}|{secondary_key}"
                    all_partition_keys.append(partition_key)

            batch_groups = split_to_groups(all_partition_keys, backfill_batch_size)
            run_requests = []
            for batch in batch_groups:
                run_key = f"{scheduled_execution_time}_{batch[0]}_{batch[-1]}".replace(
                    "|", "_"
                )
                run_requests.append(
                    RunRequest(
                        run_key=run_key,
                        partition_key=PartitionKeyRange(batch[0], batch[-1]),
                        tags={"dagster/backfill": scheduled_execution_time},
                    )
                )

        context.log.info(
            f"Evaluating schedule for {job.name}:\n"
            f"  - {len(primary_partition_keys)} partition keys\n"
            f"  - {len(run_requests)} run requests\n"
            f"  - partition key range: {partition_key_range_start} - "
            f"{partition_key_range_end}"
        )

        return run_requests

    return _schedule


def _get_region_suffix(region: str) -> str:
    """Get suffix for region-specific job/resource names."""
    region_map = {
        "us-west-2": "",  # US is default, no suffix
        "eu-west-1": "_eu",
        "ca-central-1": "_ca",
    }
    return region_map.get(region, "")


def load_job_defs_schedules(
    manual_jobs: Optional[Dict[str, JobDefinition]] = None,
    op_registry: Optional[Dict[str, callable]] = None,
    resource_registry: Optional[Dict[str, Any]] = None,
) -> Tuple[
    List[Union[JobDefinition, UnresolvedAssetJobDefinition]],
    List[ScheduleDefinition],
]:
    """Load job definitions and schedules from JSON configs.

    Args:
        manual_jobs: Dict mapping job names (with region suffixes) to
                    manually-defined JobDefinition objects. For
                    single-region or pre-defined jobs.
        op_registry: Optional dict mapping base job names to op callables.
                    If not provided, ops will be auto-discovered from the
                    ops module based on JSON configs.
        resource_registry: Dict mapping region names to Databricks
                    resources. Required for op-based jobs to bind
                    correct region resources.

    Returns:
        Tuple of (job_defs, schedule_defs)
    """
    import logging

    logger = logging.getLogger(__name__)

    job_configs = load_job_configs_by_name()
    manual_jobs = manual_jobs or {}
    resource_registry = resource_registry or {}

    # Auto-discover ops if no registry provided
    if op_registry is None or len(op_registry) == 0:
        logger.info(
            "[load_job_defs_schedules] No op_registry provided, "
            "auto-discovering ops from JSON configs"
        )
        op_registry = auto_discover_ops(job_configs)
    else:
        logger.info(
            f"[load_job_defs_schedules] Op registry provided: "
            f"{list(op_registry.keys())}"
        )

    logger.info(
        f"[load_job_defs_schedules] Manual jobs provided: "
        f"{list(manual_jobs.keys())}"
    )

    job_defs_by_name = {}
    for job_name, job_config in job_configs.items():
        # Check if regions are specified in the config
        # Default to US only
        regions = job_config.get("regions", ["us-west-2"])

        # Check if this is an op-based job (in op_registry)
        is_op_based = job_name in op_registry

        # For each region, create or look up the corresponding job
        for region in regions:
            suffix = _get_region_suffix(region)
            # Only add suffix if job_name doesn't already end with it
            if suffix and job_name.endswith(suffix):
                region_job_name = job_name
            else:
                region_job_name = f"{job_name}{suffix}"

            if region_job_name in manual_jobs:
                # Use the manually-defined region-specific job
                logger.info(
                    f"[load_job_defs_schedules] Using manual job "
                    f"'{region_job_name}' for region {region}"
                )
                job_defs_by_name[region_job_name] = manual_jobs[region_job_name]
            elif job_name in manual_jobs and len(regions) == 1:
                # Single region, use base job name
                logger.info(
                    f"[load_job_defs_schedules] Using manual job "
                    f"'{job_name}' (single region)"
                )
                job_defs_by_name[job_name] = manual_jobs[job_name]
            elif is_op_based:
                # Create op-based job from op_registry
                # Region resource is optional (not all ops need Databricks)
                region_resource = resource_registry.get(region, None)
                logger.info(
                    f"[load_job_defs_schedules] Creating op-based job "
                    f"'{region_job_name}' from op registry "
                    f"for region {region}"
                )
                op_callable = op_registry[job_name]
                job_def = create_op_based_job_for_region(
                    region_job_name, op_callable, region_resource, job_config
                )
                job_defs_by_name[region_job_name] = job_def
            elif "assets" in job_config:
                # Create asset-based job from config
                logger.info(
                    f"[load_job_defs_schedules] Creating asset-based job "
                    f"'{region_job_name}' from config for region {region}"
                )
                job_def = load_job_def_from_config(job_config, job_name=region_job_name)
                job_defs_by_name[region_job_name] = job_def
            else:
                # Cannot create job - missing information
                logger.warning(
                    f"[load_job_defs_schedules] Cannot create job "
                    f"'{region_job_name}': not in manual_jobs, "
                    f"op_registry, or assets config. Skipping."
                )

    schedules_defs_by_name = {}
    for job_name, job_config in job_configs.items():
        regions = job_config.get("regions", ["us-west-2"])

        for schedule_config in job_config.get("schedules") or []:
            # Create schedules for each region-specific job
            for region in regions:
                suffix = _get_region_suffix(region)
                # Only add suffix if job_name doesn't already end with it
                if suffix and job_name.endswith(suffix):
                    region_job_name = job_name
                else:
                    region_job_name = f"{job_name}{suffix}"

                if region_job_name in job_defs_by_name:
                    job_def = job_defs_by_name[region_job_name]
                    schedule_def = load_schedule_def_from_config(
                        job_def, schedule_config
                    )
                    schedules_defs_by_name[schedule_def.name] = schedule_def

    return (
        list(job_defs_by_name.values()),
        list(schedules_defs_by_name.values()),
    )
