import inspect
import os
import re
from dataclasses import asdict
from types import ModuleType
from typing import Callable, List, Union

from dagster import (
    AssetDep,
    AssetKey,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
from dagster import _check as check
from pyspark.sql import DataFrame

from ..userpkgs.constants import (
    DATAANALYTICS,
    DATAENGINEERING,
    DATATOOLS,
    FIRMWAREVDP,
    SUSTAINABILITY,
    AWSRegion,
    DQCheckMode,
    MetricExpressionType,
    Owner,
)
from .constants import GlossaryTerm, RunConfigOverrides
from .dq_utils import (
    DQCheck,
    JoinableDQCheck,
    NonNegativeDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    ValueRangeDQCheck,
)
from .loaders import load_assets_defs_by_key
from .metric import DimensionDefinition


def _check_regions_input(name: str, regions: List[str]):
    valid_regions = list(asdict(AWSRegion()).values())
    check.invariant(
        not any(set(regions).difference(set(valid_regions))) and len(regions) > 0,
        f"""Invalid definition: 'regions' must be any of {valid_regions}. You gave {regions}.
        asset name: {name}
        """,
    )
    return


def _check_asset_parameters(asset_fn: Callable):
    signature = inspect.signature(asset_fn)
    parameters = signature.parameters
    check.invariant(
        "context" in parameters,
        f"""Invalid definition: asset function must contain a 'context' argument.
        asset name: {asset_fn.__name__}
        """,
    )
    return


def _check_asset_name(asset_fn: Callable):
    check.invariant(
        os.path.splitext(os.path.basename(asset_fn.__code__.co_filename))[0]
        == asset_fn.__name__,
        f"""Invalid definition: asset name must match its file name.
        asset name: {asset_fn.__name__}
        file name: {os.path.basename(asset_fn.__code__.co_filename)}
        """,
    )
    return


def _check_asset_upstreams_input(
    name: str, upstreams: Union[List[str], List[AssetKey], List[AssetDep]]
):
    if upstreams is None:
        return

    check.invariant(
        all(map(lambda x: isinstance(x, str), upstreams))
        or all(map(lambda x: isinstance(x, AssetKey), upstreams))
        or all(map(lambda x: isinstance(x, AssetDep), upstreams)),
        f"""Invalid definition: 'upstreams' cannot have mixed argument types.
        asset name: {name}
        """,
    )

    for upstream in upstreams:
        if isinstance(upstream, str):
            check.invariant(
                len(upstream.split(":")[-1].split(".")) == 2,
                f"""Invalid definition: implicit 'upstream' references must be in format '[<region>[|<region>]:]<database>.<table>'.
        asset name: {name}
        upstream: {upstream}
        """,
            )
        elif isinstance(upstream, AssetKey):
            check.invariant(
                len(upstream.path) in (2, 3),
                f"""Invalid definition: explicit AssetKey 'upstream' references must contain at least 2 or at most 3 parts in the order ([<region>,] <database>, <table>)'.
        asset name: {name}
        upstream: {upstream}
        """,
            )
        elif isinstance(upstream, AssetDep):
            check.invariant(
                len(upstream.asset_key.path) in (2, 3),
                f"""Invalid definition: explicit AssetDep 'upstream' references must pass an asset key with at least 2 or at most 3 parts in the order ([<region>,] <database>, <table>)'.
        asset name: {name}
        upstream: {upstream}
        """,
            )


def _check_asset_notifications_input(asset_name: str, notifications: List[str]):
    _notifications = notifications or []
    check.invariant(
        all(map(lambda x: len(x.split(":")) == 2, _notifications)),
        f"""Invalid definition: asset notifications must be in the format '<medium>:<destination>' (e.g., 'slack:alerts-data-engineering').
        asset name: {asset_name}
        notifications: {notifications}
        """,
    )

    check.invariant(
        all(
            map(
                lambda x: not x.split(":")[1].startswith("#"),
                filter(lambda x: x.startswith("slack:"), _notifications),
            )
        ),
        f"""Invalid definition: asset 'notifications' incorrect. slack channels cannot begin with '#'.
        asset name: {asset_name}
        notifications: {notifications}
        """,
    )

    check.invariant(
        all(
            map(
                lambda x: x.split(":")[0]
                in [
                    "slack",
                ],
                _notifications,
            )
        ),
        f"""Invalid definition: only 'slack' notification medium is currently supported.
        asset name: {asset_name}
        notifications: {notifications}
        """,
    )

    return


def _check_dataset_location(asset_fn: Callable, database: str):
    check.invariant(
        os.path.basename(os.path.dirname(asset_fn.__code__.co_filename)) == database,
        f"""Invalid definition: asset must be defined in a directory whose name matches its database.
        asset name: {asset_fn.__name__}
        asset directory: {os.path.basename(os.path.dirname(asset_fn.__code__.co_filename))}
        database: {database}
        """,
    )

    base_path = re.sub(
        r"\.\./\.\./tmp/tmp[a-zA-z0-9]*/",
        "",
        os.path.relpath(asset_fn.__code__.co_filename),
    ).lstrip("../")
    check.invariant(
        len(base_path.replace("dataweb/dataweb", "dataweb").split("/")) == 5,
        f"""
        Invalid definition: datasets cannot exist in subdirectories of a database folder.
        asset name: {asset_fn.__name__}
        {os.path.relpath(asset_fn.__code__.co_filename)}
        {base_path.replace("dataweb/dataweb", "dataweb")}
        """,
    )

    return


def _check_dataset_return_type(asset_fn: Callable):
    signature = inspect.signature(asset_fn)
    return_type = signature.return_annotation
    check.invariant(
        return_type in (type(str()), DataFrame),
        f"""Invalid definition: dataset function must specify return type 'str' or 'DataFrame'.
        asset name: {asset_fn.__name__}
        """,
    )
    return


def _check_asset_multipartitions_def(
    name: str, partitions_def: MultiPartitionsDefinition, fields: List[str]
):
    for dim, definition in partitions_def.partitions_defs:
        check.invariant(
            dim in fields,
            """Invalid definition: partition dimension must be in the asset schema.
            asset name: {name}
            asset fields: {fields}
            partition dimension: {dim}
            """,
        )
        if isinstance(definition, DailyPartitionsDefinition):
            _check_asset_daily_partitions_def(name, definition, fields)
        elif isinstance(definition, MonthlyPartitionsDefinition):
            _check_asset_monthly_partitions_def(name, definition, fields)
        elif isinstance(definition, StaticPartitionsDefinition):
            _check_asset_static_partitions_def(name, definition)
    return


def _check_asset_daily_partitions_def(
    name: str, partitions_def: DailyPartitionsDefinition, fields: List[str]
):
    _check_timewindow_partition_def_timezone(name, partitions_def)
    check.invariant(
        "date" in fields,
        f"""Invalid definition: 'date' field missing from asset schema. All daily partitioned assets must be
        partitioned by 'date'.
        asset name: {name}
        asset columns: {fields}
        """,
    )
    check.invariant(
        partitions_def.fmt in (None, "%Y-%m-%d"),
        f"""Invalid definition: asset's daily time partition must be in '%Y-%m-%d' format.
        asset name: {name}
        fmt: {partitions_def.fmt}
        """,
    )


def _check_timewindow_partition_def_timezone(
    name: str, partitions_def: TimeWindowPartitionsDefinition
):
    check.invariant(
        partitions_def.timezone.lower() in (None, "utc"),
        f"""Invalid definition: asset's time partition must be based on UTC timezone.
        asset name: {name}
        timezone: {partitions_def.timezone}
        """,
    )
    return


def _check_asset_static_partitions_def(
    name: str, partitions_def: StaticPartitionsDefinition
):
    check.invariant(
        partitions_def.get_partition_keys()
        == sorted(partitions_def.get_partition_keys()),
        f"""Invalid definition: partition_keys for assets with StaticPartitionsDefinition must be sorted.
        asset name: {name}
        """,
    )
    return


def _check_asset_monthly_partitions_def(
    name: str, partitions_def: MonthlyPartitionsDefinition, fields: List[str]
):
    _check_timewindow_partition_def_timezone(name, partitions_def)
    check.invariant(
        "date_month" in fields,
        f"""Invalid definition: 'date_month' field missing from asset schema. All monthly partitioned assets must be
        partitioned by 'date_month'.
        asset name: {name}
        asset columns: {fields}
        """,
    )


def _check_dataset_partitioning_input(
    name, partitioning: Union[List[str], PartitionsDefinition], fields: List[str]
):
    if not partitioning:
        return

    if isinstance(partitioning, PartitionsDefinition):
        # check explicit partitioning input

        if isinstance(partitioning, MultiPartitionsDefinition):
            _check_asset_multipartitions_def(name, partitioning, fields)
        if isinstance(partitioning, DailyPartitionsDefinition):
            _check_asset_daily_partitions_def(name, partitioning, fields)
        if isinstance(partitioning, MonthlyPartitionsDefinition):
            _check_asset_monthly_partitions_def(name, partitioning, fields)
        if isinstance(partitioning, StaticPartitionsDefinition):
            _check_asset_static_partitions_def(name, partitioning)
    else:
        # check implicit partitioning input
        check.invariant(
            len(set(partitioning).difference(["date"])) == 0 and len(partitioning) == 1,
            f"""Invalid partitioning definition: Currently only 'date' (for daily) or 'date_month' (for monthly) time-based partition dimensions are supported
            for implicit partition definition. Please specify one. Other single-/multi-partitioning schemes must be explicitly defined via a
            PartitionsDefinition object.
            asset name: {name}
            """,
        )
    return


def _check_metric_return_type(asset_fn: Callable):
    signature = inspect.signature(asset_fn)
    return_type = signature.return_annotation
    check.invariant(
        return_type in (type(str()),),
        f"""Invalid definition: asset function must specify return type 'str'.
        asset name: {asset_fn.__name__}
        """,
    )
    return


def _check_expression_type(asset_fn: Callable, expression_type: str):
    valid_expression_types = list(asdict(MetricExpressionType()).values())
    check.invariant(
        expression_type in valid_expression_types,
        f"""Invalid expression type: metric expression type is not an approved type.
        asset name: {asset_fn.__name__}
        expression_type: {expression_type}
        approved types: {valid_expression_types}
        """,
    )
    return


def _check_dataset_primary_keys(name: str, fields: List[str], primary_keys: List[str]):
    if len(primary_keys) > 0:
        for key in primary_keys:
            check.invariant(
                key in fields,
                f"""Invalid definition: primary keys must be in the asset schema.
                asset name: {name}
                asset fields: {fields}
                primary keys: {primary_keys}
                """,
            )
    return


def _check_dataset_dq_checks(name: str, fields: List[str], dq_checks: List[DQCheck]):
    if dq_checks is not None:
        for dq_check in dq_checks:
            if isinstance(dq_check, PrimaryKeyDQCheck):
                _check_dataset_primary_keys(name, fields, dq_check.primary_keys)
            elif isinstance(dq_check, NonNullDQCheck):
                for col in dq_check.non_null_columns:
                    check.invariant(
                        col in fields,
                        f"""Invalid definition: non-null columns must be in the asset schema.
                        asset name: {name}
                        asset fields: {fields}
                        non-null columns: {dq_check.non_null_columns}
                        """,
                    )
            elif isinstance(dq_check, NonNegativeDQCheck):
                for col in dq_check.columns:
                    check.invariant(
                        col in fields,
                        f"""Invalid definition: non-negative columns must be in the asset schema.
                        asset name: {name}
                        asset fields: {fields}
                        non-negative columns: {dq_check.columns}
                        """,
                    )
            elif isinstance(dq_check, TrendDQCheck):
                check.invariant(
                    dq_check.tolerance <= 1.0 and dq_check.tolerance >= 0.0,
                    f"""Invalid definition: tolerance must be within bounds.
                    asset name: {name}
                    tolerance: {dq_check.tolerance}
                    acceptable bounds: 0 to 1
                    """,
                )
                if dq_check.dimension is not None:
                    check.invariant(
                        dq_check.dimension in fields,
                        f"""Invalid definition: dimension must exist within schema.
                        asset name: {name}
                        dimension: {dq_check.dimension}
                        schema: {fields}
                        """,
                    )
                if dq_check.lookback_days is not None:
                    check.invariant(
                        dq_check.lookback_days > 0,
                        f"""Invalid definition: lookback days must be positive.
                        asset name: {name}
                        lookback days: {dq_check.lookback_days}
                        acceptable value: > 0
                        """,
                    )
                if dq_check.min_percent_share is not None:
                    check.invariant(
                        dq_check.min_percent_share > 0,
                        f"""Invalid definition: min_percent_share must be positive.
                        asset name: {name}
                        lookback days: {dq_check.min_percent_share}
                        acceptable value: > 0
                        """,
                    )
            elif isinstance(dq_check, JoinableDQCheck):
                check.invariant(
                    dq_check.null_right_table_rows_ratio <= 1.0
                    and dq_check.null_right_table_rows_ratio >= 0.0,
                    f"""Invalid definition: null_right_table_rows_ratio must be within bounds.
                    asset name: {name}
                    null_right_table_rows_ratio: {dq_check.null_right_table_rows_ratio}
                    acceptable bounds: 0 to 1
                    """,
                )
                check.invariant(
                    all(join_key[0] in fields for join_key in dq_check.join_keys),
                    f"""Invalid definition: All join keys must exist within the schema.
                    asset name: {name}
                    join keys: {dq_check.join_keys}
                    schema: {fields}
                    """,
                )
            elif isinstance(dq_check, ValueRangeDQCheck):
                for key in dq_check.column_range_map.keys():
                    check.invariant(
                        key in fields,
                        f"""Invalid definition: value range column must exist within schema.
                        asset name: {name}
                        column: {key}
                        schema: {fields}
                        """,
                    )
    return


def _check_unique_dq_names(module: ModuleType):
    assets_defs_by_key = load_assets_defs_by_key(module)
    dq_check_names = []
    for asset in assets_defs_by_key.values():
        asset_spec = asset.get_asset_spec()
        region, database, table = tuple(asset_spec.key.path)
        # only want to run this check against one version of the asset
        if (
            asset_spec.metadata.get("dq_checks") is not None
            and region == AWSRegion.US_WEST_2
        ):
            dq_checks = asset_spec.metadata.get("dq_checks", [])
            for dq_check in dq_checks:
                check.invariant(
                    dq_check.get("name") not in dq_check_names,
                    f"""Invalid definition: DQ check names must be unique across all assets.
                    asset name: {asset_spec.key}
                    dq check name: {dq_check.get("name")}
                    """,
                )
                dq_check_names.append(dq_check.get("name"))
    return


def _check_glossary_terms(asset_fn: Callable, glossary_terms: List[GlossaryTerm]):
    acceptable_glossary_terms = [g.value for g in GlossaryTerm]
    if glossary_terms is not None:
        for glossary_term in glossary_terms:
            check.invariant(
                glossary_term in acceptable_glossary_terms,
                f"""Invalid definition: glossary terms must be in the list of acceptable terms.
                asset name: {asset_fn.__name__}
                glossary term: {glossary_term}
                acceptable terms: {acceptable_glossary_terms}
                """,
            )
    return


def _check_asset_owners(asset_name: str, owners: List[Owner]):
    acceptable_owners = [
        DATAANALYTICS,
        DATAENGINEERING,
        DATATOOLS,
        FIRMWAREVDP,
        SUSTAINABILITY,
    ]
    for owner in owners:
        check.invariant(
            owner in acceptable_owners,
            f"""Invalid definition: asset owner must be one of {acceptable_owners}.
            asset name: {asset_name}
            owner: {owner.team}
            """,
        )
    return


def _check_metric_dimensions(asset_fn: Callable, dimensions: List[DimensionDefinition]):
    mandatory_dimensions = ["date"]
    for dimension in mandatory_dimensions:
        check.invariant(
            dimension in [d["name"] for d in dimensions],
            f"""Invalid definition: metric is missing at least one of the mandatory dimensions {mandatory_dimensions}.
                metric name: {asset_fn.__name__}
                dimension: {dimension}
                """,
        )
    return


def _check_asset_description(asset_name: str, description: str):
    check.invariant(
        description is not None and len(description) > 0,
        f"""Invalid definition: asset description is empty.
            metric name: {asset_name}
            description: {description}
            """,
    )
    return


def _check_max_retries(name: str, max_retries: int):
    if max_retries is not None:
        check.invariant(
            max_retries >= 0,
            f"""Invalid definition: max_retries cannot be negative.
            asset name: {name}
            max retires: {max_retries}
            """,
        )
    return


def _check_backfill_batch_size(name: str, backfill_batch_size: int):
    if backfill_batch_size is not None:
        check.invariant(
            backfill_batch_size >= 1,
            f"""Invalid definition: backfill_batch_size cannot be less than 1.
            asset name: {name}
            backfill_batch_size: {backfill_batch_size}
            """,
        )
    return


def _check_run_config_overrides(name: str, run_config_overrides: RunConfigOverrides):
    if run_config_overrides is not None:
        has_instance_pool = (
            "instance_pool_type" in run_config_overrides
            and run_config_overrides["instance_pool_type"] is not None
        )
        has_worker_type = (
            "worker_instance_type" in run_config_overrides
            and run_config_overrides["worker_instance_type"] is not None
        )
        has_driver_type = (
            "driver_instance_type" in run_config_overrides
            and run_config_overrides["driver_instance_type"] is not None
        )
        valid_config = has_instance_pool != (has_worker_type and has_driver_type)
        check.invariant(
            valid_config,
            f"""Invalid definition: both instance_pool_type and worker_instance_type/driver_instance_type can't be provided for a config override. Use one or the other.
            asset name: {name}
            run_config_overrides: {run_config_overrides}
            """,
        )
        check.invariant(
            not has_worker_type or has_driver_type,
            f"""Invalid definition: both worker_instance_type and driver_instance_type must be set if you use one.
            asset name: {name}
            run_config_overrides: {run_config_overrides}
            """,
        )
    return


def _check_dq_check_mode_configuration(
    name: str, dq_check_mode: DQCheckMode, single_run_backfill: bool
):
    """
    Validate DQ check mode configuration at asset initialization time.

    WHOLE_RESULT mode requires single_run_backfill=True because it assumes
    we've run a query on a range of partitions.
    """
    if dq_check_mode == DQCheckMode.WHOLE_RESULT:
        check.invariant(
            single_run_backfill is True,
            f"""Invalid definition: DQCheckMode.WHOLE_RESULT requires single_run_backfill=True.

            Whole result DQ checks are only supported for single run backfills because they
            assume the query has been run on a range of partitions.

            Fix by adding: single_run_backfill=True

            asset name: {name}
            dq_check_mode: {dq_check_mode}
            single_run_backfill: {single_run_backfill}
            """,
        )
    return
