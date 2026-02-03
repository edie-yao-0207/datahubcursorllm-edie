import inspect
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    BackfillPolicy,
    Backoff,
    DailyPartitionsDefinition,
    Jitter,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    RetryPolicy,
    WeeklyPartitionsDefinition,
    asset,
    build_output_context,
)
from dagster._core.definitions.asset_key import CoercibleToAssetKey
from pyspark.sql import DataFrame, SparkSession

from .._core import _checks as checks
from .._core.constants import GlossaryTerm
from .._core.resources.databricks.constants import DEFAULT_TIMEOUT_SECONDS
from ..userpkgs.constants import (
    GENERAL_PURPOSE_INSTANCE_POOL_KEY,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    DQCheckMode,
    Owner,
    WarehouseWriteMode,
)
from ..userpkgs.utils import (
    partition_key_ranges_from_context,
    partition_keys_from_context,
)
from .constants import RunConfigOverrides
from .dq_utils import DQCheck, log_dq_checks
from .resources.databricks.constants import DEFAULT_MAX_WORKERS, DEFAULT_SPARK_VERSION
from .resources.databricks.databricks_step_launchers import (
    ConfigurableDatabricksStepLauncher,
)
from .resources.io_managers.deltalake_io_managers import DeltaTableIOManager
from .utils import (
    adjust_partition_def_for_canada,
    get_code_location,
    get_region_bucket_prefix,
    get_run_env,
    range_partitions_from_execution_context,
)


def _coerce_dependencies(
    region: str, upstreams: Union[List[str], List[AssetKey], List[AssetDep]]
) -> List[AssetDep]:

    if upstreams is None:
        return upstreams

    deps = []
    for upstream in upstreams:
        partition_mapping = None
        if isinstance(upstream, AssetDep):
            deps.append(upstream)
        elif isinstance(upstream, AssetKey):
            deps.append(AssetDep(asset=upstream, partition_mapping=partition_mapping))
        elif isinstance(upstream, str):
            region_upstream_parts = upstream.split(":")
            if len(region_upstream_parts) > 1:
                regions = region_upstream_parts[0].split("|")
                for r in regions:
                    _key = AssetKey([r] + region_upstream_parts[1].split("."))
                    deps.append(
                        AssetDep(asset=_key, partition_mapping=partition_mapping)
                    )
            else:
                _key = AssetKey(
                    [
                        region,
                    ]
                    + region_upstream_parts[0].split(".")
                )
                deps.append(AssetDep(asset=_key, partition_mapping=partition_mapping))

    for idx, dep in enumerate(deps):
        if dep.asset_key.path[0] not in (
            AWSRegion.US_WEST_2,
            AWSRegion.EU_WEST_1,
            AWSRegion.CA_CENTRAL_1,
        ):
            deps[idx] = AssetDep(
                asset=dep.asset_key.with_prefix(region),
                partition_mapping=dep.partition_mapping,
            )
    return deps


def _coerce_partitions_def(
    partitioning: Union[List[str], PartitionsDefinition], partitioning_start: str
) -> PartitionsDefinition:

    if isinstance(partitioning, (PartitionsDefinition, type(None))):
        return partitioning

    partitions_defs = {}
    for partition in partitioning:

        if partition == "date":
            offset = partition.split(":")[1] if ":" in partition else 0
            partition_def = DailyPartitionsDefinition(
                start_date=partitioning_start, end_offset=offset, timezone="UTC"
            )
            partitions_defs[partition] = partition_def
        elif partition == "date_month":
            offset = partition.split(":")[1] if ":" in partition else 0
            partition_def = MonthlyPartitionsDefinition(
                start_date=partitioning_start,
                end_offset=offset,
                timezone="UTC",
                fmt="%Y-%m",
            )
            partitions_defs[partition] = partition_def

    if len(partitions_defs) == 1:
        partition_def = list(partitions_defs.values())[0]
    elif len(partitions_defs) == 2:
        partition_def = MultiPartitionsDefinition(partitions_defs=partitions_defs)
    else:
        raise ValueError("Cannot have more than 2 partition dimensions.")

    return partition_def


def table(
    # asset related arguments
    database: str,
    description: str,
    regions: List[str],
    owners: List[Owner],
    schema: List[Dict[str, Any]],
    primary_keys: List[str],
    partitioning: Optional[Union[List[str], PartitionsDefinition]] = None,
    upstreams: Optional[Union[List[str], List[AssetKey], List[AssetDep]]] = None,
    backfill_start_date: Optional[str] = None,
    # execution related arguments
    dq_checks: Optional[List[DQCheck]] = None,
    max_retries: Optional[int] = None,
    notifications: Optional[List[str]] = None,
    pypi_libraries: Optional[List[str]] = [],
    run_config_overrides: Optional[RunConfigOverrides] = None,
    write_mode: Optional[WarehouseWriteMode] = WarehouseWriteMode.OVERWRITE,
    backfill_batch_size: Optional[int] = None,
    dq_check_mode: DQCheckMode = DQCheckMode.RUN_PER_PARTITION,
    single_run_backfill: Optional[bool] = None,
    glossary_terms: Optional[List[GlossaryTerm]] = None,
    concurrency_key: Optional[str] = None,
    priority: Optional[int] = None,
) -> List[AssetsDefinition]:
    def _wrapper(asset_fn: Callable):

        asset_name = asset_fn.__name__
        signature = inspect.signature(asset_fn)
        return_type = signature.return_annotation
        _glossary_terms = (
            [f"{term.value}" for term in glossary_terms] if glossary_terms else []
        )

        # check definitions
        fields = [field["name"] for field in schema]
        checks._check_asset_name(asset_fn)
        checks._check_asset_parameters(asset_fn)
        checks._check_asset_upstreams_input(asset_name, upstreams)
        checks._check_dataset_location(asset_fn, database)
        checks._check_dataset_return_type(asset_fn)
        checks._check_dataset_primary_keys(asset_name, fields, primary_keys)
        checks._check_dataset_dq_checks(asset_name, fields, dq_checks)
        checks._check_dataset_partitioning_input(asset_name, partitioning, fields)
        checks._check_regions_input(asset_name, regions)
        checks._check_asset_notifications_input(asset_name, notifications)
        checks._check_glossary_terms(asset_fn, _glossary_terms)
        checks._check_asset_owners(asset_name, owners)
        checks._check_asset_description(asset_name, description)
        checks._check_max_retries(asset_name, max_retries)
        checks._check_backfill_batch_size(asset_name, backfill_batch_size)
        checks._check_run_config_overrides(asset_name, run_config_overrides)
        checks._check_dq_check_mode_configuration(
            asset_name, dq_check_mode, single_run_backfill
        )

        _owners = [f"team:{owner.team}" for owner in owners]
        _notifications = notifications or []
        metadata = {}
        metadata["code_location"] = get_code_location()
        metadata["description"] = description
        metadata["owners"] = _owners
        metadata["primary_keys"] = primary_keys
        metadata["schema"] = schema
        metadata["write_mode"] = write_mode
        metadata["notifications"] = _notifications
        metadata["run_config_overrides"] = run_config_overrides
        metadata["glossary_terms"] = _glossary_terms
        metadata["dq_checks"] = []
        for dq_check in dq_checks or []:
            metadata["dq_checks"].append(
                {
                    "type": dq_check.__class__.__name__,
                    "name": dq_check.name,
                    "blocking": dq_check.block_before_write
                    if dq_check.block_before_write is not None
                    else False,
                    "details": dq_check.get_details(),
                }
            )

        partition_def = _coerce_partitions_def(
            partitioning=partitioning, partitioning_start=backfill_start_date
        )

        backfill_policy = None
        if single_run_backfill is True:
            backfill_policy = BackfillPolicy.single_run()
        elif backfill_batch_size:
            backfill_policy = BackfillPolicy(max_partitions_per_run=backfill_batch_size)

        retry_policy = None
        if isinstance(max_retries, int):
            retry_policy = RetryPolicy(
                max_retries=max_retries,
                delay=600,  # 10 minutes
                backoff=Backoff.LINEAR,
                jitter=Jitter.FULL,
            )

        assets = []
        for region in regions:

            region_prefix = region[:2]
            required_resource_keys = {f"dbx_step_launcher_gp_{region_prefix}"}
            resource_defs = None
            if run_config_overrides:
                if len(run_config_overrides) == 1 and run_config_overrides.get(
                    "instance_pool_type"
                ):
                    if (
                        run_config_overrides.get("instance_pool_type")
                        == GENERAL_PURPOSE_INSTANCE_POOL_KEY
                    ):
                        required_resource_keys = (
                            {f"dbx_step_launcher_gp_{region_prefix}"},
                        )
                        resource_defs = None
                    elif (
                        run_config_overrides.get("instance_pool_type")
                        == MEMORY_OPTIMIZED_INSTANCE_POOL_KEY
                    ):
                        required_resource_keys = (
                            {f"dbx_step_launcher_mo_{region_prefix}"},
                        )
                        resource_defs = None
                else:
                    resource_key = f"dbx_{database}_{asset_name}_{region_prefix}"
                    required_resource_keys = {resource_key}
                    resource_defs = {
                        resource_key: ConfigurableDatabricksStepLauncher(
                            region=region,
                            instance_pool_type=run_config_overrides.get(
                                "instance_pool_type", ""
                            ),
                            driver_instance_type=run_config_overrides.get(
                                "driver_instance_type", ""
                            ),
                            worker_instance_type=run_config_overrides.get(
                                "worker_instance_type", ""
                            ),
                            max_workers=run_config_overrides.get(
                                "max_workers", DEFAULT_MAX_WORKERS
                            ),
                            spark_version=run_config_overrides.get(
                                "spark_version", DEFAULT_SPARK_VERSION
                            ),
                            timeout_seconds=run_config_overrides.get(
                                "timeout_seconds", DEFAULT_TIMEOUT_SECONDS
                            ),
                            spark_conf_overrides=run_config_overrides.get(
                                "spark_conf_overrides"
                            ),
                        )
                    }

            deps = _coerce_dependencies(region=region, upstreams=upstreams)
            op_tags = {
                "dagster/compute_kind": "spark",
                "notifications": json.dumps(_notifications),
                "pypi_libraries": json.dumps(pypi_libraries),
            }

            if concurrency_key:
                op_tags["dagster/concurrency_key"] = concurrency_key

            if priority:
                op_tags["dagster/priority"] = priority

            # Adjust partition definition for Canada region
            region_partition_def = partition_def
            if region == AWSRegion.CA_CENTRAL_1:
                region_partition_def = adjust_partition_def_for_canada(partition_def)

            @asset(
                name=asset_name,
                key_prefix=[region, database],
                description=description,
                owners=_owners,
                metadata=metadata,
                deps=deps,
                partitions_def=region_partition_def,
                compute_kind=f"{region_prefix}_table",
                group_name=f"{region_prefix}_{database}",
                output_required=False,
                backfill_policy=backfill_policy,
                retry_policy=retry_policy,
                required_resource_keys=required_resource_keys,
                resource_defs=resource_defs,
                op_tags=op_tags,
            )
            def _asset(context: AssetExecutionContext) -> MaterializeResult:

                macros = {}
                if context.assets_def.partitions_def:
                    partition_key_range = partition_key_ranges_from_context(context)
                    macros["FIRST_PARTITION_START"] = partition_key_range[0][0]
                    macros["FIRST_PARTITION_END"] = partition_key_range[0][1]
                    if len(partition_key_range) == 2:
                        macros["SECOND_PARTITION_START"] = partition_key_range[1][0]
                        macros["SECOND_PARTITION_END"] = partition_key_range[1][1]

                    partition_dim_keys = partition_keys_from_context(context)
                    macros["FIRST_PARTITION_KEYS"] = partition_dim_keys[0]
                    if len(partition_dim_keys) == 2:
                        macros["SECOND_PARTITION_KEYS"] = partition_dim_keys[1]

                context.log.info(f"Available Macros: \n{macros}")
                context.log.info(f"op_tags: {op_tags}")

                spark = SparkSession.builder.enableHiveSupport().getOrCreate()

                if return_type == type(str()):
                    query = asset_fn(context)
                    formatted = query.format(**macros)
                    context.log.info("Query to be executed:")
                    context.log.info(formatted)
                    df_result = spark.sql(formatted)
                elif return_type == DataFrame:
                    df_result = asset_fn(context)

                context.log.info(f"Executing data transformations...")
                region_from_context = context.asset_key.path[0]
                bucket_prefix = get_region_bucket_prefix(region_from_context)
                suffix = ""
                if get_run_env() != "prod":
                    suffix = "-dev"
                path = f"s3://{bucket_prefix}datamodel-warehouse{suffix}/tmp/{context.run_id}/{context.op_execution_context.node_handle.name}"
                df_result.write.mode("overwrite").parquet(path)
                context.log.info(f"Data transformations complete...")
                time.sleep(
                    60
                )  # S3 writes are eventually consistent, so we sleep for 60 seconds to ensure the data is available in the bucket

                df = spark.read.parquet(path)
                #############################################################
                # Iterate through the list of DQCheck instances and run
                # their evaluation
                #############################################################
                block_on_fail = False
                block_before_write = None
                result_messages_by_day = defaultdict(list)

                try:
                    if single_run_backfill:
                        partition_keys = range_partitions_from_execution_context(
                            context
                        )
                    else:
                        partition_keys = context.partition_keys
                except Exception as e:
                    partition_keys = [
                        datetime.today().strftime("%Y-%m-%d")
                        + "|"
                        + "UNPARTITIONED_RUN"
                    ]

                context.log.info(f"partition_keys: {partition_keys}")

                blocking_dq_checks_passed = True

                if dq_check_mode == DQCheckMode.WHOLE_RESULT:
                    # Note: Configuration validation is done at asset initialization time
                    context.log.info("Running DQ checks on entire result")
                    for dq_check in dq_checks or []:
                        # if any dq check fails, overall dq status is considered to fail
                        result_code, result_message = dq_check.run(
                            context=context,
                            df=df,
                            table=asset_name,
                            database=database,
                            partition_key=None,
                        )

                        partition_range_str = (
                            f"{partition_keys[0]}:{partition_keys[-1]}"
                        )
                        result_messages_by_day[partition_range_str].append(
                            (result_code, result_message)
                        )

                        if result_code == 1 and dq_check.block_before_write is True:
                            blocking_dq_checks_passed = False
                else:
                    context.log.info("Running DQ checks on individual partitions")
                    for partition_key in partition_keys:
                        for dq_check in dq_checks or []:
                            if single_run_backfill:
                                temp_df = df
                            elif isinstance(partition_def, MonthlyPartitionsDefinition):
                                temp_df = df.filter(df.date_month == partition_key)
                            elif isinstance(partition_def, (MultiPartitionsDefinition)):
                                where_conditions = []

                                partition_keys_split = [
                                    key.split("|") for key in partition_keys
                                ]

                                for idx, partition_dimension_def in enumerate(
                                    partition_def.partitions_defs
                                ):
                                    values_string = ",".join(
                                        [
                                            f"'{item}'"
                                            if isinstance(item, str)
                                            else f"{item}"
                                            for item in tuple(
                                                zip(*partition_keys_split)
                                            )[idx]
                                        ]
                                    )
                                    where_conditions.append(
                                        f"{partition_dimension_def.name} IN ({values_string})"
                                    )

                                where_clause = " AND ".join(where_conditions)
                                temp_df = df.filter(where_clause)

                            elif "UNPARTITIONED_RUN" not in partition_key:
                                temp_df = df.filter(df.date == partition_key)
                            else:
                                temp_df = df

                            if (
                                dq_check.check_before_write is False
                                and metadata["write_mode"] == "merge"
                            ):
                                # TODO: implement 'merge_source_with_target'
                                # df_check = merge_source_with_target(context, source=df)
                                df_check = temp_df
                                df_check.cache()
                            else:
                                df_check = temp_df
                            # if any dq check fails, overall dq status is considered to fail
                            result_code, result_message = dq_check.run(
                                context=context,
                                df=df_check,
                                table=asset_name,
                                database=database,
                                partition_key=partition_key,
                            )
                            result_messages_by_day[partition_key].append(
                                (result_code, result_message)
                            )
                            if result_code == 1 and dq_check.block_before_write is True:
                                blocking_dq_checks_passed = False

                result_messages_by_day = dict(result_messages_by_day)

                for dq_check in dq_checks or []:
                    # if any dq checks are blocking, 'block_before_write=True' takes precedence
                    # over any conflicts.
                    if dq_check.block_before_write in (True, False):
                        block_on_fail = True
                        if dq_check.block_before_write is True:
                            block_before_write = True

                if dq_checks:
                    log_dq_checks(context, result_messages_by_day, database)
                io_manager = DeltaTableIOManager()
                if block_on_fail:
                    if block_before_write is True:
                        if not blocking_dq_checks_passed:
                            raise Exception("DQ checks failed. Execution blocked.")
                        output_metadata = io_manager.handle_output(context, df)
                    else:
                        output_metadata = io_manager.handle_output(context, df)
                        if not blocking_dq_checks_passed:
                            raise Exception("DQ checks failed. Execution blocked.")
                else:
                    output_metadata = io_manager.handle_output(context, df)

                return MaterializeResult(
                    asset_key=context.asset_key, metadata=output_metadata
                )

            assets.append(_asset)
        return assets

    return _wrapper
