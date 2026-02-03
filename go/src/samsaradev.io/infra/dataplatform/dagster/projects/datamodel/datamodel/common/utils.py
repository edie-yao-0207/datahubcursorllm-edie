import inspect
import json
import os
import random
import re
import subprocess
import textwrap
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum, unique
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

import boto3
import botocore
import datadog
import delta
import pytz
from botocore.exceptions import ClientError, NoCredentialsError
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    Config,
    DailyPartitionsDefinition,
    DataVersionsByPartition,
    ExpectationResult,
    Failure,
    HookContext,
    MetadataValue,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    OpExecutionContext,
    OutputContext,
    PartitionsDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SourceAsset,
    TimeWindowPartitionMapping,
    TimeWindowPartitionsDefinition,
    asset,
    failure_hook,
    schedule,
    success_hook,
)
from dagster._core.definitions.asset_dep import AssetDep as AssetDep
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from dagster._core.definitions.target import ExecutableDefinition
from dagster._core.errors import DagsterInvariantViolationError
from databricks import sql
from databricks.sdk.core import Config as dbxconfig
from databricks.sdk.core import oauth_service_principal
from pdpyras import EventsAPISession
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from slack import WebClient

from .constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    UPDATED_PARTITIONS_QUERY,
    DagsterLinks,
    GlossaryTerm,
)
from .data_diff import compare_dataframes, compare_dataframes_for_vdp
from .stats import *

E2_DATABRICKS_HOST_SSM_PARAMETER_KEY = "E2_DATABRICKS_HOST"

DELTALAKE_DATE_PARTITION_NAME = "date"

CANADA_PARTITION_START_DATE = "2025-11-03"


def adjust_partition_def_for_canada(
    partitions_def: Optional[PartitionsDefinition],
) -> Optional[PartitionsDefinition]:
    """
    Adjusts partition definition for Canada region by setting start_date
    to max(existing start_date, CANADA_PARTITION_START_DATE).

    Returns the adjusted partition definition if it's a DailyPartitionsDefinition,
    otherwise returns the original unchanged.
    """
    if not isinstance(partitions_def, DailyPartitionsDefinition):
        return partitions_def

    existing_start = partitions_def.start
    # Compare dates directly to avoid timezone issues
    # Convert both to date objects for comparison
    existing_date = existing_start.date()
    canada_date = datetime.strptime(CANADA_PARTITION_START_DATE, "%Y-%m-%d").date()
    new_date = max(existing_date, canada_date)

    # Preserve all other parameters from the original partition def
    new_start_date_str = new_date.strftime("%Y-%m-%d")

    # Build kwargs for DailyPartitionsDefinition constructor
    constructor_kwargs = {"start_date": new_start_date_str}

    # Preserve end_offset if it exists (most common case)
    if hasattr(partitions_def, "end_offset"):
        constructor_kwargs["end_offset"] = partitions_def.end_offset

    # Preserve end_date if it exists and end_offset doesn't
    elif hasattr(partitions_def, "end") and partitions_def.end:
        constructor_kwargs["end_date"] = partitions_def.end

    # Preserve timezone if it exists
    if hasattr(partitions_def, "timezone") and partitions_def.timezone:
        constructor_kwargs["timezone"] = partitions_def.timezone

    return DailyPartitionsDefinition(**constructor_kwargs)


class DQConfig(Config):
    unblock: bool = False


class Operator(Enum):
    eq = "eq"
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    ne = "ne"
    between = "between"
    all_between = "all_between"


class WarehouseWriteMode(Enum):
    merge = "merge"
    overwrite = "overwrite"


@dataclass
class RunEnvironment:
    DEV: str = field(default="dev")
    PROD: str = field(default="prod")


@dataclass
class Database:
    PRODUCT_ANALYTICS: str = field(default="product_analytics")
    PRODUCT_ANALYTICS_STAGING: str = field(default="product_analytics_staging")
    DATAENGINEERING: str = field(default="dataengineering")
    DATAMODEL_PLATFORM: str = field(default="datamodel_platform")
    DATAMODEL_PLATFORM_BRONZE: str = field(default="datamodel_platform_bronze")
    DATAMODEL_PLATFORM_SILVER: str = field(default="datamodel_platform_silver")
    DATAMODEL_TELEMATICS: str = field(default="datamodel_telematics")
    DATAMODEL_TELEMATICS_BRONZE: str = field(default="datamodel_telematics_bronze")
    DATAMODEL_TELEMATICS_SILVER: str = field(default="datamodel_telematics_silver")
    DATAMODEL_CORE: str = field(default="datamodel_core")
    DATAMODEL_CORE_BRONZE: str = field(default="datamodel_core_bronze")
    DATAMODEL_CORE_SILVER: str = field(default="datamodel_core_silver")
    DATAMODEL_SAFETY: str = field(default="datamodel_safety")
    DATAMODEL_SAFETY_BRONZE: str = field(default="datamodel_safety_bronze")
    DATAMODEL_SAFETY_SILVER: str = field(default="datamodel_safety_silver")
    DATAMODEL_DEV: str = field(default="datamodel_dev")
    DATAPLATFORM_DEV: str = field(default="dataplatform_dev")
    FIRMWARE_DEV: str = field(default="firmware_dev")
    MIXPANEL_SAMSARA: str = field(default="mixpanel_samsara")


class AWSRegions(Enum):
    US_WEST_2 = "us-west-2"
    EU_WEST_1 = "eu-west-1"
    CA_CENTRAL_1 = "ca-central-1"


def get_all_regions():
    return [region.value for region in AWSRegions]


@dataclass
class Owner:
    team: str


DATAENGINEERING = Owner("DataEngineering")
DATAPLATFORM = Owner("DataPlatform")


class DatadogMetric(NamedTuple):
    """Represents a single Datadog metric object."""

    metric: str
    points: List[Tuple[int, Union[int, float]]]
    type: str
    tags: List[str]

    @classmethod
    def from_dict(cls, obj):
        return cls(
            metric=obj.get("metric", str),
            points=obj.get("points", []),
            type=obj.get("type", str),
            tags=obj.get("tags", []),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric": self.metric,
            "points": self.points,
            "type": self.type,
            "tags": self.tags,
        }


class TableType(Enum):
    DAILY_DIMENSION = "daily_dimension"
    SLOWLY_CHANGING_DIMENSION = "slowly_changing_dimension"
    TRANSACTIONAL_FACT = "transactional_fact"
    ACCUMULATING_SNAPSHOT_FACT = "accumulating_snapshot_fact"
    LOOKUP = "lookup"
    LIFETIME = "lifetime"
    STAGING = "staging"


DQResult = Tuple[Literal[1, 0], str]


@dataclass
class DQCheck:
    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        raise NotImplementedError(
            "You must impelement the run method for your DQ Check"
        )


class DQGroup:
    def __init__(
        self,
        group_name: str,
        partition_def: PartitionsDefinition,
        slack_alerts_channel: str,
        step_launcher_resource: str = "databricks_pyspark_step_launcher",
        backfill_policy: BackfillPolicy = None,
        regions: Sequence[str] = ["us-west-2"],
    ):
        self.data = {}
        self.default_factory = list
        self.group_name = group_name
        self.partition_def = partition_def
        self.slack_alerts_channel = slack_alerts_channel
        self.step_launcher_resource = step_launcher_resource
        self.backfill_policy = backfill_policy
        self.regions = regions

    def __getitem__(self, key):
        if key not in self.data:
            if hasattr(self, "default_factory") and self.default_factory is not None:
                self.data[key] = self.default_factory()
            else:
                raise KeyError(f"'{key}'")
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def items(self):
        return self.data.items()

    def _get_db_map(self):
        result = {}
        for dq_name, dq_values in self.items():
            result[dq_name] = dq_values[0].database

        return result

    def generate(self) -> List[AssetsDefinition]:
        result = []

        for region in self.regions:
            if region == "eu-west-1":
                group_name = self.group_name + "_eu"
            elif region == "ca-central-1":
                group_name = self.group_name + "_ca"
            else:
                group_name = self.group_name

            # For Canada region, use a partition definition with start_date
            # set to max(existing start_date, CANADA_PARTITION_START_DATE)
            region_partitions_def = self.partition_def
            if region == AWSRegions.CA_CENTRAL_1.value:
                region_partitions_def = adjust_partition_def_for_canada(
                    self.partition_def
                )

            new_records = []

            for dq_name, dq_values in self.items():
                new_record = build_dq_asset(
                    dq_name=dq_name,
                    dq_checks=dq_values,
                    group_name=group_name,
                    partitions_def=region_partitions_def,
                    slack_alerts_channel=self.slack_alerts_channel,
                    step_launcher_resource=self.step_launcher_resource,
                    region=region,
                    database=self._get_db_map()[dq_name],
                    backfill_policy=self.backfill_policy,
                )

                new_records.append(new_record)

            result.extend(new_records)

        return result


DQList = Sequence[DQCheck]


@dataclass
class SQLDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    sql_query: str
    expected_value: Union[int, Tuple[Any]]
    blocking: bool = field(default=False)
    multi_date: bool = field(default=False)
    operator: Operator = field(default=Operator.eq)
    use_macros: bool = field(default=False)
    manual_upstreams: Union[Set[AssetKey], Set[str], Set[Tuple[str]]] = field(
        default=None
    )

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _sql_dq(
            context,
            self.name,
            self.sql_query,
            self.expected_value,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
            self.multi_date,
            self.operator,
            self.use_macros,
        )

    def format_sql_query(self, sql_query):
        sql_query = re.sub(r" +", " ", sql_query)
        sql_query = re.sub(r" *\n *", "\n", sql_query)
        sql_query = sql_query.strip()
        sql_query = sql_query.replace("\n", " ")

        return sql_query

    def get_details(self):
        return {
            "sql_query": self.format_sql_query(self.sql_query),
            "expected_value": self.expected_value,
            "operator": self.operator.value,
        }

    def get_manual_upstreams(self):
        return self.manual_upstreams


@dataclass
class NonEmptyDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    table: str
    blocking: bool = field(default=False)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _non_empty_dq(
            context,
            self.name,
            self.database,
            self.table,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
        )

    def get_details(self):
        return {"table": self.table}

    def get_manual_upstreams(self):
        return None


@dataclass
class TrendDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    table: str
    blocking: bool = field(default=False)
    tolerance: float = field(default=0.05)
    lookback_days: int = field(default=1)
    dimension: Optional[str] = field(default=None)
    min_percent_share: Optional[float] = field(default=0.1)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: float,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _trend_dq(
            context=context,
            dq_check_name=self.name,
            database=self.database,
            table=self.table,
            blocking=blocking,
            partition_where_clause=partition_where_clause,
            slack_alerts_channel=slack_alerts_channel,
            tolerance=self.tolerance,
            lookback_days=self.lookback_days,
            dimension=self.dimension,
            min_percent_share=self.min_percent_share,
            partition_key=partition_key,
        )

    def get_details(self):
        return {
            "table": self.table,
            "lookback_days": self.lookback_days,
            "dimension": self.dimension,
            "tolerance": self.tolerance,
        }

    def get_manual_upstreams(self):
        return None


@dataclass
class EquivalenceDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    asset1: AssetsDefinition
    asset2: AssetsDefinition
    threshold: float
    extra_where_clause: Optional[None] = field(default=None)
    blocking: bool = field(default=False)
    extra_params: Mapping[str, Any] = field(default_factory=dict)
    comparison_function: Callable = field(default=compare_dataframes)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _equivalence_dq(
            context=context,
            dq_check_name=self.name,
            asset1=self.asset1,
            asset2=self.asset2,
            threshold=self.threshold,
            blocking=blocking,
            extra_where_clause=self.extra_where_clause,
            slack_alerts_channel=slack_alerts_channel,
            extra_params=self.extra_params,
            comparison_function=self.comparison_function,
        )

    def get_details(self):
        return {}

    def get_manual_upstreams(self):
        return None


@dataclass
class JoinableDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    database_2: str
    input_asset_1: str
    input_asset_2: str
    join_keys: Sequence[Tuple[str, str]]
    blocking: bool = field(default=False)
    null_right_table_rows_ratio: float = field(default=0.0)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _joinable_dq(
            context,
            self.name,
            self.database,
            self.database_2,
            self.input_asset_1,
            self.input_asset_2,
            self.join_keys,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
            self.null_right_table_rows_ratio,
            partition_key,
        )

    def get_details(self):
        return {
            "input_asset_1": self.input_asset_1,
            "input_asset_2": self.input_asset_2,
        }

    def get_manual_upstreams(self):
        return None


@dataclass
class NonNullDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    table: str
    non_null_columns: Sequence[str]
    blocking: bool = field(default=False)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _non_null_dq(
            context,
            self.name,
            self.database,
            self.table,
            self.non_null_columns,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
        )

    def get_details(self):
        return {"non_null_columns": self.non_null_columns}

    def get_manual_upstreams(self):
        return None


@dataclass
class ValueRangeDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    table: str
    column_range_map: Mapping[str, Tuple[float, float]]
    blocking: bool = field(default=False)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _value_range_dq(
            context,
            self.name,
            self.database,
            self.table,
            self.column_range_map,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
        )

    def get_details(self):
        return {"column_range_map": self.column_range_map}

    def get_manual_upstreams(self):
        return None


@dataclass
class PrimaryKeyDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    table: str
    primary_keys: Sequence[str]
    blocking: bool = field(default=False)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _primary_key_dq(
            context,
            self.name,
            self.database,
            self.table,
            self.primary_keys,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
        )

    def get_details(self):
        return {"primary_keys": self.primary_keys}

    def get_manual_upstreams(self):
        return None


@dataclass
class NonNegativeDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database: str
    table: str
    columns: Sequence[str]
    blocking: bool = field(default=False)

    def run(
        self,
        context: OpExecutionContext,
        blocking: bool,
        partition_where_clause: Optional[str],
        slack_alerts_channel: str,
        tolerance: Optional[float] = None,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _non_negative_dq(
            context,
            self.name,
            self.database,
            self.table,
            self.columns,
            blocking,
            partition_where_clause,
            slack_alerts_channel,
        )

    def get_details(self):
        return {"columns": self.columns}

    def get_manual_upstreams(self):
        return None


def date_range_from_current_date(
    days: int = 0, offset: int = 0, start_date: str = None
):

    if start_date:
        _date = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        _date = datetime.utcnow()

    idx = 0
    keys = []
    while idx < days:
        key = (_date - timedelta(days=(idx + offset))).date()
        keys.append(f"{key}")
        idx += 1
    return keys


def date_range_from_current_date_multi_partitions(
    days: int = 0, offset: int = 0, static_partitions: list = []
):
    today = datetime.utcnow().date()
    idx = 0
    keys = []
    while idx < days:
        for static_partition in static_partitions:
            keys.append(f"{today - timedelta(days=(idx + offset))}|{static_partition}")
        idx += 1
    return keys


def get_run_env() -> Literal["prod", "dev"]:
    env = os.getenv("ENV", "dev")
    assert env in list(
        asdict(RunEnvironment()).values()
    ), "environment variable 'ENV' must be one of ('prod', 'dev')"
    return asdict(RunEnvironment()).get(env.upper())


def get_datahub_env() -> Literal["prod", "dev"]:
    if os.getenv("MODE") in ("LOCAL_PROD_CLUSTER_RUN", "LOCAL_DATAHUB_RUN", "DRY_RUN"):
        return "dev"
    elif get_run_env() == "prod":
        return "prod"
    else:
        return "dev"


def get_ssm_parameter(ssm_client: boto3.Session, name: str) -> str:
    res = ssm_client.get_parameter(Name=name, WithDecryption=True)
    return res["Parameter"]["Value"]


def get_region() -> str:
    """
    Get EC2 region from AWS instance metadata.

    Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    """
    session = boto3.session.Session()
    return session.region_name


def initialize_datadog() -> None:
    """
    Initialize and configure Datadog.api module
    Collects app key and api key from AWS SSM parameter store to use for initialize
    """

    api_key, app_key = get_datadog_token_and_app()

    datadog.initialize(api_key=api_key, app_key=app_key)


def send_datadog_metrics(context, metrics: List[DatadogMetric]) -> None:
    metrics = [m.to_dict() for m in metrics]
    try:
        datadog.api.Metric.send(metrics=metrics)
        context.log.info("successfully logged metrics to Datadog")
    except Exception as e:
        context.log.info(f"Error sending metrics to Datadog: {e}")
    return


def send_datadog_metric(
    metric_type: str, value: float, metric: str, tags: Sequence[str], context=None
) -> None:
    try:
        datadog.api.Metric.send(
            metrics=[
                {
                    "metric": metric,
                    "points": [(time.time(), value)],
                    "type": metric_type,
                    "tags": tags,
                }
            ],
        )
        print(f"successfully logged {metric} for tags: {tags}")
    except Exception as e:
        if context:
            context.log.info(f"Error sending metrics to Datadog: {e}")
        else:
            print(f"Error sending metrics to Datadog: {e}")
    return


def send_datadog_count_metric(metric: str, tags: Sequence[str]) -> None:
    send_datadog_metric("count", 1, metric, tags)


def send_datadog_gauge_metric(metric: str, value: float, tags: Sequence[str]) -> None:
    send_datadog_metric("gauge", value, metric, tags)


def get_ssm_token(ssm_parameter: str) -> str:
    region_name = "us-west-2"  # default to us-west-2 in case region can't be found

    try:
        region_name = get_region()
        if not region_name:
            region_name = "us-west-2"
    except botocore.exceptions.NoRegionError:
        # from a Dagster failure hook, generating the session sometimes fails since it can't derive a default region
        print("Unable to retrieve region, defaulting to us-west-2")
        region_name = "us-west-2"

    ssm = boto3.client("ssm", region_name=region_name)
    response = ssm.get_parameter(Name=ssm_parameter, WithDecryption=True)
    return response["Parameter"]["Value"]


def _get_partition_keys_from_hook_context(context: HookContext) -> List[str]:

    partition_keys = []

    if context._step_execution_context.has_asset_partitions_for_output("result"):
        partition_def = context._step_execution_context.partitions_def_for_output(
            "result"
        )
        partition_key_range = (
            context._step_execution_context.asset_partition_key_range_for_output(
                "result"
            )
        )
        partition_keys = partition_def.get_partition_keys_in_range(
            partition_key_range=partition_key_range
        )

    return partition_keys


def _get_op_path(context: HookContext) -> Tuple[str, str, str]:
    op_key_parts = context.op.name.split("__")
    if len(op_key_parts) == 3:
        region = op_key_parts[-3].replace("_", "-")
        database = op_key_parts[-2]
        table = op_key_parts[-1]
    else:
        region = "None"
        database = "None"
        table = op_key_parts[-1]

    return [region, database, table]


def send_pagerduty_events(
    context: HookContext, action: Literal["trigger", "resolve"]
) -> None:

    if _get_job_type(context) not in ["asset_materialization"]:
        return

    context.log.info(f"Sending PagerDuty '{action}' Event for run: {context.run_id}.")
    if get_run_env() != "prod":
        context.log.info("Skipping for non-prod run...")
        return

    routing_key = get_ssm_token("PAGERDUTY_DE_LOW_URGENCY_KEY")
    events_session = EventsAPISession(routing_key)

    now = datetime.utcnow().timestamp()
    region, database, table = _get_op_path(context)
    partition_keys = _get_partition_keys_from_hook_context(context) or ["unpartitioned"]
    for partition_key in partition_keys:

        event_key = f"{region}__{database}__{table}__{partition_key}"
        if action == "trigger":
            title = f"""[{region}] [{database} {table}] Failing to materialize partition [{partition_key}]"""
            dedup_key = events_session.trigger(
                dedup_key=event_key,
                summary=title,
                custom_details={
                    "body": f"""{title} \n\nPlease consult the Data Engineering Oncall Guide below for debugging techniques and/or notify the Data Engineering (#ask-data-engineering) team if persists or need help on debugging. \n@slack-alerts-data-engineering @pagerduty-DataEngineeringLowUrgency \n\n\nDagster Run Link: {DagsterLinks.RunPage.format(run_id=context.run_id)} \n\nDagster Overview Link: {DagsterLinks.RunsOverviewPage} \n\nOncall Runbook: {DagsterLinks.OncallRunBook}""",
                    "run_id": context.run_id,
                    "event_type": "dagster_hook",
                    "timestamp": now,
                },
                source="dagster.internal.samsara.com",
                severity="error",
            )

        elif action == "resolve":
            events_session.resolve(event_key)

    return


def get_dbx_warehouse_id(region: str) -> str:
    # aianddata warehouse
    if region == AWSRegions.CA_CENTRAL_1.value:
        return "1a85cdd2cf692db6"
    elif region == AWSRegions.EU_WEST_1.value:
        return "ff24022501efda35"
    else:
        return "351c51ed0e7809be"


def get_slack_token() -> str:
    try:
        return get_ssm_token("DAGSTER_SLACK_BOT_TOKEN")
    except (ClientError, NoCredentialsError) as e:
        print(e)
        print("Unable to retrieve slack token, cannot write to Slack")


def get_dbx_token(region: str) -> str:
    if region == AWSRegions.CA_CENTRAL_1.value:
        return get_dbx_ca_token()
    elif region == AWSRegions.EU_WEST_1.value:
        return get_dbx_eu_token()
    else:
        return get_dbx_us_token()


def get_datahub_dbt_token() -> str:
    return get_ssm_token("DATAHUB_DBT_API_TOKEN")


def get_datahub_tableau_token() -> str:
    return get_ssm_token("DATAHUB_TABLEAU_TOKEN")


def get_dbx_us_token() -> str:
    return os.getenv("DATABRICKS_TOKEN")


def get_dbx_eu_token() -> str:
    return os.getenv("DATABRICKS_EU_TOKEN")


def get_dbx_ca_token() -> str:
    return os.getenv("DATABRICKS_CA_TOKEN")


def get_dbx_oauth_credentials(region: str) -> Dict[str, str]:

    ssm_client = boto3.client("ssm", region_name=get_region())

    if region == AWSRegions.US_WEST_2.value:
        clientid_key = "DAGSTER_DATABRICKS_CLIENTID"
        client_secret_key = "DAGSTER_DATABRICKS_SECRET"
    elif region == AWSRegions.EU_WEST_1.value:
        clientid_key = "DAGSTER_DATABRICKS_EU_CLIENTID"
        client_secret_key = "DAGSTER_DATABRICKS_EU_SECRET"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        clientid_key = "DAGSTER_DATABRICKS_CA_CLIENTID"
        client_secret_key = "DAGSTER_DATABRICKS_CA_SECRET"
    else:
        raise ValueError(
            f"region  must be one of {[region.value for region in AWSRegions]}. got {region}"
        )

    client_id = ssm_client.get_parameter(Name=clientid_key, WithDecryption=True)[
        "Parameter"
    ]["Value"]
    client_secret = ssm_client.get_parameter(
        Name=client_secret_key, WithDecryption=True
    )["Parameter"]["Value"]

    return {"client_id": client_id, "client_secret": client_secret}


def get_datadog_token_and_app() -> str:
    try:
        if "DATADOG_API_KEY" in os.environ:
            api_key = os.getenv("DATADOG_API_KEY")
        else:
            api_key = get_ssm_token("DATADOG_API_KEY")
    except (ClientError, NoCredentialsError) as e:
        print(e)
        print("Unable to retrieve Datadog token, cannot write to Datadog")
        api_key = "API_KEY_NOT_FOUND"

    try:
        if "DATADOG_APP_KEY" in os.environ:
            app_key = os.getenv("DATADOG_APP_KEY")
        else:
            app_key = get_ssm_token("DATADOG_APP_KEY")
    except (ClientError, NoCredentialsError) as e:
        print(e)
        print("Unable to retrieve Datadog token, cannot write to Datadog")
        app_key = "APP_KEY_NOT_FOUND"

    return api_key, app_key


def get_datahub_token() -> str:
    if "DATAHUB_GMS_TOKEN" in os.environ:
        return os.environ["DATAHUB_GMS_TOKEN"]
    try:
        return get_ssm_token("DATAHUB_GMS_TOKEN")
    except (ClientError, NoCredentialsError) as e:
        print(e)
        print("Unable to retrieve DataHub token")
        return "NO_TOKEN_SET"


def get_gms_server() -> str:
    if "DATAHUB_GMS_SERVER" in os.environ:
        return os.environ["DATAHUB_GMS_SERVER"]
    try:
        return get_ssm_token("DATAHUB_GMS_SERVER")
    except (ClientError, NoCredentialsError) as e:
        print(e)
        print("Unable to retrieve DataHub GMS server, defaulting to prod VPC endpoint")
        return "http://datahub-gms.internal.samsara.com:8080"


def get_aws_sso_login(
    client_app: Literal["datahub", "dagster"] = "datahub",
    role_name="datahub-eks-nodes",
    duration=3600,
) -> Dict[str, str]:
    assert client_app in [
        "datahub",
        "dagster",
    ], f"client_app must be one of ['datahub', 'dagster'], got {client_app}"

    if get_run_env() == "prod" and os.getenv("MODE") not in (
        "LOCAL_PROD_CLUSTER_RUN",
        "LOCAL_DATAHUB_RUN",
        "DRY_RUN",
    ):
        import boto3

        sts_client = boto3.client("sts")
        role_arn = f"arn:aws:iam::492164655156:role/{role_name}"

        assumed_role = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=f"{client_app}-session",
            DurationSeconds=duration,
        )

        credentials = assumed_role["Credentials"]

        return credentials

    # for local runs, use awsssologin
    try:
        profile = os.getenv("AWS_DEFAULT_PROFILE")

        output = subprocess.run(
            ["awsssologin", "-verbose", "--profile", profile],
            capture_output=True,
            text=True,
        )
        credentials = json.loads(output.stdout)
    except Exception as e:
        print(e)
        print("Unable to retrieve AWS SSO credentials")
        credentials = {
            "AccessKeyId": "NO_ACCESS_KEY_ID",
            "SecretAccessKey": "NO_SECRET_ACCESS_KEY",
            "SessionToken": "NO_SESSION_TOKEN",
        }

    return credentials


table_type_map = {
    TableType.DAILY_DIMENSION: """This is a daily snapshot dimension table - a daily snapshot dimension table is a table partitioned by the ‘date’ column where each date partition represents the entity’s attributes on a given date

For example - A daily dimension snapshot table of organizations will provide one row per day per organization id.

To avoid duplications, you should filter on a specific date, or join on the primary keys and date to a fact table. For example:

dim.date = [fact_date_value]""",
    TableType.SLOWLY_CHANGING_DIMENSION: """
This is a slowly changing dimension table - a slowly changing dimension table is a table that shows the information of an entity on a date range

For example - A slowly changing dimension table for an organization will show that the organization was linked to account ‘abc‘ between the dates [start_date] and [end_date] and linked to a different account between [start_date] and [end_date]

To query the state of an entity on a given date you will want to use the condition

[date_value] BETWEEN start_date and end_date""",
    TableType.TRANSACTIONAL_FACT: """This is a transactional fact table - a transactional fact table is a

table where each date partition stores the transactions that happened on a given date.

To query activity on a time period, filter on the date column

For example - A fact trips table will show all the trips taken on a date or a range of dates""",
    TableType.LOOKUP: """This is a lookup table - a lookup table has no history or partition columns.

It is meant to give you a way to look up the association between various entities.

To use this table join or filter on the primary key to get its most recent values""",
    TableType.ACCUMULATING_SNAPSHOT_FACT: """This is an accumulating snapshot fact table -
an accumulating snapshot fact table tracks the lifecycle of an entity and updates rows as new data is introduced.
The date tracks the original date that the event or activity occurred.

Example - A safety event will reside in the date partition when the event originally happened.
If actions were taken on that safety event on a later date (e.g. coaching actions),
those actions will still reside in the original date partition.
""",
    TableType.LIFETIME: """This is a lifetime table partitioned by date.

A row for a given date, stores the cumulative historical activity for the entity including when was their first action,
last action and all dates when an action occurred.""",
    TableType.STAGING: """This is a staging dataset. Users who do not have a technical understanding of the source data for this table should not use it, but rather the downstream table(s) that are built on top of this table.""",
}


def build_table_description(
    table_desc: str,
    row_meaning: str,
    table_type: TableType,
    freshness_slo_hours: Optional[int] = None,
    freshness_slo_updated_by: str = "",
    related_table_info: Dict[str, str] = {},
    caveats: Optional[str] = None,
) -> str:
    """
    Builds a description for a table based on the provided parameters.

    Args:
        table_desc (str): The table description to give more context on what the table means. This should give general context to users not familiar with the table on what type of data is in this table. Add business context, link to more documentation, etc.
        row_meaning (str): The meaning of each row in the table.
        table_type (TableType): The type of the table.
        freshness_slo_hours (Optional[int]): The maximum age of the table in hours.
        freshness_slo_updated_by (Optional[str]): The time of day the table will be updated by.
        related_table_info (Dict[str, str], optional): The information about related tables. Defaults to {}.
        caveats (str, optional): Any additional caveats or notes about the table. Defaults to None.

    Returns:
        str: The formatted table description.
    """

    assert (
        not freshness_slo_hours or not freshness_slo_updated_by
    ), "Either freshness_slo_hours or freshness_slo_updated_by must be provided, not both"

    assert (
        freshness_slo_hours or freshness_slo_updated_by
    ), "Either freshness_slo_hours or freshness_slo_updated_by must be provided"

    if freshness_slo_updated_by:
        SLO_CLAUSE = f"This table will be updated with yesterday`s data by {freshness_slo_updated_by} each day."
    elif freshness_slo_hours:
        SLO_CLAUSE = f"This table will be updated at least once every {freshness_slo_hours} hours."

    table_desc = textwrap.dedent(table_desc)
    row_meaning = textwrap.dedent(row_meaning)

    related_table_clause = ""

    for tbl, desc in related_table_info.items():
        desc = textwrap.dedent(desc)
        related_table_clause += f"   - {tbl} => {desc.strip()}.\n"

    related_table_clause = textwrap.dedent(related_table_clause)

    table_type_clause = table_type_map.get(table_type, "no value found")

    table_type_clause = textwrap.dedent(table_type_clause)

    result_str = f"""**Table Description**:
{table_desc}

**Meaning of each row**:
{row_meaning}

**Table Type**:
{table_type_clause}

**SLO**:
{SLO_CLAUSE}
Breaches to this guarantee will be monitored and responded to during business days (9am-5pm PT).

{"**Related Tables**:" if related_table_info else ""}
{related_table_clause}
{caveats if caveats else ""}"""

    formatted_str = textwrap.dedent(result_str)

    return formatted_str


def slack_custom_dq_failure_alert(
    context: OpExecutionContext, channel: str, blocking: bool, result_str: str
) -> None:
    """Create a Slack alert when a data quality (DQ) check fails

    Parameters:
    context: always = "context". It's supplied by env but needs to be passed to asset - must always be first parameter
    channel: Slack channel to post message to
    blocking: True if the DQ check will block downstream execution on failure

    Returns:
    Nothing
    """
    slack_token = get_slack_token()
    op_name = context.op_handle.name
    run_id = context.run_id
    client = WebClient(token=slack_token)
    try:
        partition_key = context.partition_key
    except Exception as e:
        context.log.error(f"Error getting partition key: {e}")
        partition_key = "unknown"

    dagster_path = (
        "http://127.0.0.1:3000"
        if get_run_env() == "dev"
        else "https://dagster.internal.samsara.com"
    )
    dagster_url = f"{dagster_path}/runs/{run_id}"

    if dagster_path == "https://dagster.internal.samsara.com":
        message = f"""
                    {'Non-' if not blocking else ''}Blocking Dagster DQ check failed for op {op_name} on partition {partition_key} \
                        \nurl: {dagster_url}
                        \n{result_str}
                    """
        client.chat_postMessage(channel=channel, text=message)
    return


def slack_custom_alert(channel: str, message: str) -> None:
    """Create a Slack alert when a data quality (DQ) check fails

    Parameters:
    channel: Slack channel to post message to
    message: message to send to channel

    Returns:
    Nothing
    """
    slack_token = get_slack_token()
    client = WebClient(token=slack_token)
    client.chat_postMessage(channel=channel, text=message)
    return


# TODO - harden implementation using sqlparse library, adding improved support for subqueries
def add_sql_partition_filter(sql_query: str, filter_to_prepend: str):
    """Add partition filter to SQL query

    Parameters:
    sql_query: SQL query string
    filter_to_prepend: additional partition-based filter used to prune query set

    Returns:
    Updated SQL query string filtering first by partition
    """
    where_index = sql_query.lower().find("where")

    # Check if "where" keyword is present in the query
    if where_index != -1:
        # Find the index where the "where" clause ends (i.e., where the next keyword starts)
        where_end_index = where_index + len("where ")
        # Insert the additional filter just after "where "
        updated_query = (
            sql_query[:where_end_index]
            + f"{filter_to_prepend} AND "
            + sql_query[where_end_index:]
        )
    else:
        # No "where" clause in the query; add one at the end
        updated_query = sql_query.strip()
        if updated_query[-1] == ";":
            # Remove the ending semicolon if present
            updated_query = updated_query[:-1]
        updated_query += f" WHERE {filter_to_prepend}"

        if sql_query[-1] == ";":
            updated_query += ";"  # Add back the semicolon if it was originally there

    return updated_query


def _handle_dq_result(
    context: OpExecutionContext,
    dq_check_name: str,
    expected_value: Union[int, float, Tuple[int, float]],
    observed_value: Union[int, float, Tuple[int, float]],
    blocking: bool,
    slack_alerts_channel: str,
    operator: Operator = Operator.eq,
    dq_check_type: str = "Unknown",
) -> Literal[1, 0]:
    if operator == Operator.eq:
        success = True if observed_value == expected_value else False
    elif operator == Operator.gt:
        success = True if observed_value > expected_value else False
    elif operator == Operator.lt:
        success = True if observed_value < expected_value else False
    elif operator == Operator.gte:
        success = True if observed_value >= expected_value else False
    elif operator == Operator.lte:
        success = True if observed_value <= expected_value else False
    elif operator == Operator.ne:
        success = True if observed_value != expected_value else False
    elif operator == Operator.between:
        success = (
            True if expected_value[0] <= observed_value <= expected_value[1] else False
        )
    elif operator == Operator.all_between:
        success = (
            True
            if all(
                expected_value[0] <= v <= expected_value[1]
                for v in observed_value.values()
            )
            else False
        )

    status = "success" if success else "failure"

    if isinstance(observed_value, float) or isinstance(observed_value, Decimal):
        observed_value = round(float(observed_value), 8)

    result = ExpectationResult(
        success=success,
        label="dq_check_result",
        metadata={
            "DQ Result": MetadataValue.json(
                {
                    "Check Name": dq_check_name,
                    "Status": status,
                    "Operator": str(operator.value),
                    "Comparison Value": expected_value,
                    "Observed Value": observed_value,
                    "DQ Check Type": dq_check_type,
                }
            )
        },
    )

    context.log_event(result)

    op_name = context.op_handle.name

    initialize_datadog()
    send_datadog_count_metric(
        "dagster.job.run",
        [
            "job_type:dq",
            f"status:{status}",
            f"name:{op_name}",
            f"region:{get_region()}",
            f"env:{get_run_env()}",
        ],
    )

    slack_alerts_channel = "#" + slack_alerts_channel

    context.log.info(f"logging to {slack_alerts_channel} channel")

    result_str = json.dumps(result.metadata["DQ Result"].value)

    if blocking and not status == "success":
        slack_custom_dq_failure_alert(
            context,
            channel=slack_alerts_channel,
            blocking=blocking,
            result_str=result_str,
        )
        return 1, result_str
    elif not status == "success":
        slack_custom_dq_failure_alert(
            context,
            channel=slack_alerts_channel,
            blocking=blocking,
            result_str=result_str,
        )
        context.log.info("DQ Failed")
    else:
        context.log.info("DQ succeeded")

    return 0, result_str


def get_partition_clause_from_keys(
    partition_keys: Sequence[str], partitions_def: PartitionsDefinition
):

    where_conditions = []

    if isinstance(
        partitions_def, (DailyPartitionsDefinition, MonthlyPartitionsDefinition)
    ):
        values_string = ",".join(
            [
                f"'{item}'" if isinstance(item, str) else f"{item}"
                for item in partition_keys
            ]
        )
        where_conditions.append(f"date IN ({values_string})")

    elif isinstance(partitions_def, (MultiPartitionsDefinition)):

        partition_keys = [key.split("|") for key in partition_keys]

        for idx, partition_dimension_def in enumerate(partitions_def.partitions_defs):
            values_string = ",".join(
                [
                    f"'{item}'" if isinstance(item, str) else f"{item}"
                    for item in tuple(zip(*partition_keys))[idx]
                ]
            )
            where_conditions.append(
                f"{partition_dimension_def.name} IN ({values_string})"
            )

    return " AND ".join(where_conditions)


def get_partition_value_map_from_context(context):
    if hasattr(context.partition_key, "keys_by_dimension"):
        partition_keys_by_dimension = context.partition_key.keys_by_dimension
    elif hasattr(context, "partition_key"):
        partition_keys_by_dimension = {"date": context.partition_key}
    else:
        partition_keys_by_dimension = {}

    return partition_keys_by_dimension


def _add_query_partition_filters(sql_query, partition_where_clause, context):
    if partition_where_clause:
        where_clause = partition_where_clause
        sql_query = add_sql_partition_filter(sql_query, where_clause)
        sql_query = sql_query.replace("1=1 AND ", "")

    context.log.info(f"Running DQ query:\n{sql_query}")
    return sql_query


def _add_query_macros(sql_query, context):
    partition_date_str = context.asset_partition_key_for_output()
    sql_query = sql_query.format(DATEID=partition_date_str)
    context.log.info(f"Running DQ query:\n{sql_query}")
    return sql_query


def _sql_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    sql_query: str,
    expected_value: int,
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    multi_date: bool = False,
    operator: Operator = Operator.eq,
    use_macros: bool = field(default=False),
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    if not multi_date:
        sql_query = _add_query_partition_filters(
            sql_query, partition_where_clause, context
        )

    if use_macros:
        sql_query = _add_query_macros(sql_query, context)

    context.log.info(f"Running DQ query:\n{sql_query}")

    try:
        observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]
    except Exception as e:
        context.log.info(e)
        context.log.error(
            """SQL DQ checks must name
                          their return column 'observed_value'
                          """
        )

    return _handle_dq_result(
        context,
        dq_check_name,
        expected_value,
        observed_value,
        blocking,
        slack_alerts_channel,
        operator,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _non_empty_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    table: str,
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    sql_query = f"select count(1) as observed_value from {database}.{table}"

    sql_query = _add_query_partition_filters(sql_query, partition_where_clause, context)

    observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

    return _handle_dq_result(
        context,
        dq_check_name,
        0,
        observed_value,
        blocking,
        slack_alerts_channel,
        Operator.gt,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _trend_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    table: str,
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    tolerance: float = 0.01,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    lookback_days: int = 1,
    dimension: Optional[str] = None,
    min_percent_share: float = 0.1,
    partition_key: Optional[str] = None,
) -> Literal[1, 0]:
    def _find_previous_date(input_string, lookback_days):
        # Attempt to find a date in the format 'YYYY-MM-DD' within the string
        try:
            date_str = input_string.split("'")[
                1
            ]  # Extracts the date assuming it's between single quotes
            date_obj = datetime.strptime(
                date_str, "%Y-%m-%d"
            )  # Converts string to date object
            new_date_obj = date_obj - timedelta(
                days=lookback_days
            )  # Subtracts specified number of days
            new_date_str = new_date_obj.strftime("%Y-%m-%d")  # Converts back to string
            return input_string.replace(
                date_str, new_date_str
            )  # Replaces old date with new date in the string
        except (IndexError, ValueError):
            return "Error: Date format or placement is incorrect."

    if partition_key == datetime.today().strftime("%Y-%m-%d"):
        tolerance = 0.98

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    context.log.info(partition_where_clause)

    partition_where_clause_previous_day = _find_previous_date(
        partition_where_clause, lookback_days
    )

    sql_query = f"""
        SELECT SUM(IF({partition_where_clause},1,0)) / GREATEST(SUM(IF({partition_where_clause_previous_day},1,0)),0.001) AS observed_value
        FROM {database}.{table}
        WHERE ({partition_where_clause_previous_day}) or ({partition_where_clause})
    """

    if dimension:
        # find unique slices for the dimension
        slice_sql_query = f"""
            WITH t as (
                SELECT {dimension} as slice_value, count(1) as slice_count,
                (COUNT(1) / SUM(COUNT(1)) OVER ()) AS percent_share
                FROM {database}.{table}
                WHERE {dimension} is not null
                AND {partition_where_clause_previous_day}
                GROUP BY {dimension}
                ORDER BY 2 DESC LIMIT 10
                ),
            t2 as (
                SELECT {dimension} as slice_value, count(1) as slice_count,
                (COUNT(1) / SUM(COUNT(1)) OVER ()) AS percent_share
                FROM {database}.{table}
                WHERE {dimension} is not null
                AND {partition_where_clause}
                GROUP BY {dimension}
                ORDER BY 2 DESC LIMIT 10
                )
                SELECT slice_value FROM t WHERE percent_share >= {min_percent_share}
                UNION
                SELECT slice_value FROM t2 WHERE percent_share >= {min_percent_share}
        """

        context.log.info(f"Slice SQL Query: {slice_sql_query}")

        slice_values = list(spark.sql(slice_sql_query).toPandas()["slice_value"])

        context.log.info(f"Slice values: {slice_values}")

        observed_values = {}

        for slice_value in slice_values:
            sql_query = f"""
                SELECT SUM(IF({partition_where_clause},1,0)) / GREATEST(SUM(IF({partition_where_clause_previous_day},1,0)),0.001) AS observed_value
                FROM {database}.{table}
                WHERE ({partition_where_clause_previous_day} or {partition_where_clause})
                AND {dimension} = '{slice_value}'
            """

            context.log.info(sql_query)

            observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

            context.log.info(f"Observed value for {slice_value}: {observed_value}")

            if observed_value is None:
                context.log.info(
                    f"Observed value for {slice_value} is None, setting to 0"
                )
                observed_value = 0  # default to 0 if no result is returned

            observed_values[slice_value] = float(observed_value)

    else:
        context.log.info(sql_query)
        result = list(spark.sql(sql_query).toPandas()["observed_value"])[0]
        if result is None:
            context.log.info("Trend DQ check returned no result, setting to 0")
            result = 0  # default to 0 if no result is returned
        observed_values = {"ALL": float(result)}

    context.log.info(f"Observed values: {observed_values}")

    return _handle_dq_result(
        context,
        dq_check_name,
        (1 - tolerance, 1 + tolerance),
        observed_values,
        blocking,
        slack_alerts_channel,
        Operator.all_between,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _equivalence_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    asset1: AssetsDefinition,
    asset2: AssetsDefinition,
    threshold: float,
    blocking: bool = False,
    extra_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    extra_params: Mapping[str, Any] = {},
    comparison_function: Callable = compare_dataframes,
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DATEID = context.asset_partition_key_for_output()

    key1 = asset1.key.to_user_string().split("/")
    key2 = asset2.key.to_user_string().split("/")

    db1, table1 = key1[-2], key1[-1]
    db2, table2 = key2[-2], key2[-1]

    query1 = f"""select * FROM {db1}.{table1}
        where date = '{DATEID}'
        and {extra_where_clause}
        """

    context.log.info(query1)
    df1 = spark.sql(query1)

    query2 = f"""
        select * from {db2}.{table2}
        where date = '{DATEID}'
        and {extra_where_clause}
        """

    context.log.info(query2)
    df2 = spark.sql(query2)

    result = compare_dataframes_for_vdp(df1, df2, **extra_params)
    observed_value = 100 - 100.0 * round(result, 5)
    context.log.info(f"% match for values = {str(observed_value)}%")

    return _handle_dq_result(
        context,
        dq_check_name,
        threshold,
        observed_value,
        blocking,
        slack_alerts_channel,
        Operator.gt,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _joinable_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    database_2: str,
    input_asset_1: str,
    input_asset_2: str,
    join_keys: Sequence[Tuple[str, str]],
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    null_right_table_rows_ratio: float = 0.0,
    partition_key: Optional[str] = None,
) -> Literal[1, 0]:
    context.log.info(
        f"Running joinable DQ check for {input_asset_1} and {input_asset_2} for {partition_key}"
    )

    if partition_key == datetime.today().strftime("%Y-%m-%d"):
        # For checks on the current date, pass this DQ check as long as the right table
        # has populated some data for that day.
        comparison_value = 0.99
    else:
        comparison_value = null_right_table_rows_ratio

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    join_statement = " ON " + " AND ".join(
        [
            f"left_table.{left_key} = right_table.{right_key}"
            for left_key, right_key in join_keys
        ]
    )

    # if the left table is empty, now rows are returned, so we coalesce to a very high value that will always fail the check
    sql_query = f"""WITH t AS (
            SELECT left_table.*, right_table.{join_keys[0][0]} AS right_key
            FROM {database}.{input_asset_1} left_table
            LEFT JOIN {database_2}.{input_asset_2} right_table
            {join_statement}
        )
        SELECT COALESCE(1.0*COUNT_IF(t.right_key IS NULL)/NULLIF(COUNT(*), 0), 9999) AS null_right_table_rows_ratio
        FROM t
    """
    # TODO - allow differently named join keys and convert from USING to ON syntax

    sql_query = _add_query_partition_filters(sql_query, partition_where_clause, context)

    result_df = spark.sql(sql_query).toPandas()

    null_right_table_rows_ratio = list(result_df["null_right_table_rows_ratio"])[0]

    observed_value = null_right_table_rows_ratio

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_value,
        observed_value,
        blocking,
        slack_alerts_channel,
        Operator.lte,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _non_null_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    table: str,
    non_null_columns: Sequence[str],
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
) -> Literal[1, 0]:
    observed_values = {}

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    for non_null_column in non_null_columns:
        sql_query = f"select count(1) as observed_value from {database}.{table} where {non_null_column} is null"

        sql_query = _add_query_partition_filters(
            sql_query, partition_where_clause, context
        )

        observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

        observed_values[non_null_column] = observed_value

    comparison_values = {k: 0 for k in non_null_columns}

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_values,
        observed_values,
        blocking,
        slack_alerts_channel,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _value_range_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    table: str,
    column_range_map: Mapping[str, Tuple[int, int]],
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
) -> Literal[1, 0]:
    observed_values = {}

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    for col, (min_val, max_val) in column_range_map.items():
        sql_query = f"""
        select count(1) as observed_value FROM {database}.{table}
        WHERE {col} < {min_val} OR {col} > {max_val}"""

        sql_query = _add_query_partition_filters(
            sql_query, partition_where_clause, context
        )

        observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

        observed_values[col] = observed_value

    comparison_values = {k: 0 for k in column_range_map}

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_values,
        observed_values,
        blocking,
        slack_alerts_channel,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _non_negative_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    table: str,
    columns: Sequence[str],
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
) -> Literal[1, 0]:
    observed_values = {}

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    for col in columns:
        sql_query = f"""
        select count(1) as observed_value FROM {database}.{table}
        WHERE {col} < 0 """

        sql_query = _add_query_partition_filters(
            sql_query, partition_where_clause, context
        )

        observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]
        observed_values[col] = observed_value

    comparison_values = {k: 0 for k in columns}

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_values,
        observed_values,
        blocking,
        slack_alerts_channel,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _primary_key_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    database: str,
    table: str,
    primary_keys: Sequence[str],
    blocking: bool = False,
    partition_where_clause: Optional[str] = None,
    slack_alerts_channel: str = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    primary_key_str = ",".join(primary_keys)

    if partition_where_clause:
        partition_clause = "WHERE " + partition_where_clause
    else:
        partition_clause = " "

    sql_query = f"""with t as
                        (
                        SELECT {primary_key_str},
                        count(1) as num_instances
                        FROM {database}.{table}
                        {partition_clause}
                        GROUP BY {primary_key_str}
                        )
                        select count(1) as observed_value from t where num_instances > 1
                        """

    context.log.info(f"Running Primary Key DQ query:\n{sql_query}")

    observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

    return _handle_dq_result(
        context,
        dq_check_name,
        0,
        observed_value,
        blocking,
        slack_alerts_channel,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def get_timetravel_str(partition_date_str):
    return "@" + partition_date_str.replace("-", "") + "23595909999"


def get_code_location():
    stack = inspect.stack()
    for caller in stack:
        if (
            caller.filename.find("datamodel/assets") != -1
            or caller.filename.find("datamodel/ops") != -1
        ) and (
            caller.filename.find("utils.py") == -1
            and caller.filename.find("datamodel/assets/metrics/core.py") == -1
        ):
            break

    backend_root = os.getenv("BACKEND_ROOT", "/code")

    code_location = caller.filename.replace(backend_root, "").replace(
        "/go/src/samsaradev.io/infra/dataplatform/", ""
    )
    code_line = caller.lineno

    return f"{code_location}, line {code_line}"


def epoch_to_human_readable_gmt(epoch_time: int) -> str:
    """
    Convert an epoch time (milliseconds since the Unix epoch) to a human-readable datetime string in GMT.

    Args:
    epoch_time (int): Epoch time in milliseconds.

    Returns:
    str: Human-readable datetime in GMT.
    """
    # Convert milliseconds to seconds
    epoch_seconds = epoch_time / 1000.0
    # Create a datetime object in UTC timezone
    dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    # Format the datetime object to a string with minute
    return dt.strftime("%b %d, %Y %I:%M%p UTC")


def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""
    if not field_path.startswith("[version=2.0]"):
        # not a v2, we assume this is a simple path
        return field_path
        # this is a v2 field path
    tokens = [
        t for t in field_path.split(".") if not (t.startswith("[") or t.endswith("]"))
    ]

    return ".".join(tokens)


def replace_timetravel_string(sql_query: str) -> str:
    """
    Replaces the timetravel string in the given SQL query with an empty string.

    Args:
        sql_query (str): The SQL query to be processed.

    Returns:
        str: The SQL query with the timetravel string replaced.

    """
    tokens = sql_query.split()

    updated_tokens = []

    for token in tokens:
        if "@202" in token:
            token = token.split("@202")[0]
        updated_tokens.append(token)

    return " ".join(updated_tokens)


region_to_step_launcher = {
    "us-west-2": "databricks_pyspark_step_launcher",
    "eu-west-1": "databricks_pyspark_step_launcher_eu",
    "ca-central-1": "databricks_pyspark_step_launcher_ca",
}


dq_audit_log_schema = [
    {"name": "partition_str", "type": "long", "nullable": True, "metadata": {}},
    {"name": "op_name", "type": "string", "nullable": False, "metadata": {}},
    {"name": "status", "type": "string", "nullable": False, "metadata": {}},
    {"name": "dagster_run_id", "type": "string", "nullable": False, "metadata": {}},
    {
        "name": "results",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": {
                "type": "map",
                "keyType": "string",
                "valueType": "string",
                "valueContainsNull": True,
            },
            "valueContainsNull": True,
        },
        "nullable": False,
        "metadata": {},
    },
]


def build_dq_asset(
    dq_name: Union[str, Tuple[str]],
    dq_checks: DQList,
    group_name: str,
    partitions_def: Optional[DailyPartitionsDefinition] = None,
    slack_alerts_channel: Optional[str] = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    step_launcher_resource: str = "databricks_pyspark_step_launcher",
    region: str = "us-west-2",
    database: str = "no_db",
    backfill_policy: BackfillPolicy = None,
    tolerance: float = 0.01,
) -> AssetsDefinition:
    op_name = f"dq_{'__'.join(list(dq_name) if type(dq_name) is tuple else [dq_name])}"

    # tables that should NOT be joined to their downstream DQs
    tables_to_bypass_direct_dependency = ["dim_licenses"]

    input_deps = list(dq_name) if type(dq_name) is tuple else [dq_name]
    input_deps = [x for x in input_deps if x not in tables_to_bypass_direct_dependency]
    input_deps = [AssetKey([region, database, x]) for x in input_deps]

    metadata = {"dq_checks": []}

    for dq_check in dq_checks:
        manual_upstreams = dq_check.get_manual_upstreams()
        if manual_upstreams:
            input_deps = set()
            for upstream in manual_upstreams or []:
                if isinstance(upstream, AssetKey):
                    asset_key = upstream.with_prefix(region)
                    input_deps.add(asset_key)
                elif isinstance(upstream, (Sequence, str)):
                    asset_key = AssetKey(upstream)
                    asset_key = asset_key.with_prefix(region)
                    input_deps.add(asset_key)
                else:
                    raise TypeError("invalid type for 'dq upstreams'")

        metadata["dq_checks"].append(
            {
                "type": dq_check.__class__.__name__,
                "name": dq_check.name,
                "blocking": dq_check.blocking,
                "details": dq_check.get_details(),
            }
        )

    assets_with_auto_materialization_policies = [
        "dq_stg_device_activity_daily",
        "dq_lifetime_device_activity",
        "dq_lifetime_device_online",
    ]

    if op_name in assets_with_auto_materialization_policies:
        auto_materialize_policy = AutoMaterializePolicy.eager(
            max_materializations_per_minute=None
        ).with_rules(AutoMaterializeRule.skip_on_not_all_parents_updated())
    else:
        auto_materialize_policy = None

    @asset(
        io_manager_key="metadata_io_manager",
        owners=["team:DataEngineering"],
        key_prefix=[region, database],
        name=op_name,
        compute_kind="dq",
        op_tags={"asset_type": "dq"},
        metadata=metadata,
        required_resource_keys={region_to_step_launcher[region]},
        group_name=group_name,
        partitions_def=partitions_def,
        non_argument_deps=set(input_deps),
        auto_materialize_policy=auto_materialize_policy,
        retry_policy=RetryPolicy(max_retries=0),
        backfill_policy=backfill_policy,
    )
    def _asset(context: AssetExecutionContext, config: DQConfig):
        if os.getenv("MODE") == "DRY_RUN":
            context.log.info("dry run - exiting without materialization")
            return {"dry_run_early_exit": MetadataValue.int(0)}

        def to_sql_map(x: Dict[Any, Any]) -> str:
            def value_to_str(value: Any) -> str:
                if isinstance(value, dict):
                    # Convert nested dictionaries to string representations and remove single quotes
                    dict_str = str(value)
                    dict_str_clean = dict_str.replace("'", "")
                    return f"'{dict_str_clean}'"
                else:
                    return f"'{str(value)}'"

            def first_level_to_map(value: Dict[Any, Any]) -> str:
                return ", ".join(
                    f"'{key}', {value_to_str(val)}" for key, val in value.items()
                )

            map_sql = ", ".join(
                (
                    f"'{key}', map({first_level_to_map(value)})"
                    if isinstance(value, dict)
                    else f"'{key}', {value_to_str(value)}"
                )
                for key, value in x.items()
            )
            return f"map({map_sql})"

        result = 0
        result_messages_by_day = defaultdict(list)
        for dq_check in dq_checks:
            if config.unblock:
                context.log.info("manually skipping pipeline blocking settings")
                blocking = False
            else:
                # Temporarily make trend DQ checks non-blocking for Canada region
                # due to volatile row counts during rollout
                if region == AWSRegions.CA_CENTRAL_1.value and isinstance(
                    dq_check, TrendDQCheck
                ):
                    context.log.info(
                        f"Making trend DQ check {dq_check.name} "
                        f"non-blocking for Canada region"
                    )
                    blocking = False
                else:
                    blocking = True if dq_check.blocking else False

            if partitions_def:
                partition_keys = context.partition_keys
                partition_str = f"{context.partition_key_range.start}:{context.partition_key_range.end}"

                for key in partition_keys:

                    partition_where_clause = get_partition_clause_from_keys(
                        [
                            key,
                        ],
                        partitions_def,
                    )
                    result_code, result_message = dq_check.run(
                        context,
                        blocking,
                        partition_where_clause,
                        slack_alerts_channel,
                        tolerance,
                        key,
                    )

                    result += result_code
                    result_messages_by_day[key].append(result_message)

            else:
                key = datetime.today().strftime("%Y-%m-%d")
                partition_str = None
                partition_where_clause = None

                result_code, result_message = dq_check.run(
                    context,
                    blocking,
                    partition_where_clause,
                    slack_alerts_channel,
                    tolerance,
                )

                result += result_code
                result_messages_by_day[key].append(result_message)

        result_messages_by_day = dict(result_messages_by_day)

        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        status = "success" if result == 0 else "failure"
        dagster_run_id = context.run_id

        context.log.info(f"running in ENV: {get_run_env()}")

        dq_check_result_db = (
            "auditlog" if get_run_env() == "prod" else "dataplatform_dev"
        )

        context.log.info(f"result_messages_by_day: {result_messages_by_day}")

        for partition_str, result_messages in result_messages_by_day.items():
            result_message_map = {}
            for x in result_messages:
                val = json.loads(x)
                check_name = val["Check Name"]
                del val["Check Name"]
                result_message_map[check_name] = val
                print(partition_str)
                print(result_message_map)

            results_expr = to_sql_map(result_message_map)

            context.log.info(f"results_expr: {results_expr}")
            context.log.info(f"result_message_map: {result_message_map}")

            # for unpartitioned runs, we record the date in the partition_str
            # so that it can be parsed by the DataHub ingestion job (splitting on pipe char)
            if partition_str == "None" or not partition_str:
                partition_str = (
                    datetime.today().strftime("%Y-%m-%d") + "|" + "UNPARTITIONED_RUN"
                )

            dq_audit_log_fields = {
                "partition_str": partition_str,
                "op_name": op_name,
                "status": status,
                "dagster_run_id": dagster_run_id,
                "results": results_expr,
            }

            merge_query = f"""
                MERGE INTO {dq_check_result_db}.dq_check_log as target

                USING (SELECT '{partition_str}' as partition_str,
                '{op_name}' as op_name,
                '{status}' as status,
                '{dagster_run_id}' as dagster_run_id,
                {results_expr} as results,
                '{database}' as database
                ) as updates

                ON target.partition_str = updates.partition_str
                and target.op_name = updates.op_name

                when matched then update set *
                when not matched then insert *
                """

            context.log.info(f"merge_query:\n {merge_query}")

            actual_fields = list(dq_audit_log_fields.keys())
            expected_fields = [x["name"] for x in dq_audit_log_schema]

            assert (
                actual_fields == expected_fields
            ), f"""audit log fields must match schema
            expected: {expected_fields}
            actual: {actual_fields}
            """

            success = False
            retry_count = 1

            while not success and retry_count < 5:
                try:
                    spark.sql(merge_query)
                    success = True
                except delta.exceptions.ConcurrentAppendException:
                    retry_count += 1
                    delay = (3 * retry_count) ** 2
                    context.log.warning(
                        f"merge failed for ConcurrentAppendException, retrying in {delay} seconds"
                    )
                    time.sleep(delay)

            result_messages = "\n".join(result_messages)

            context.log.info(f"DQ Results:\n{result_messages}")

            if result == 0:
                context.log.info(
                    f"this DQ asset does not produce an output but logs result to {dq_check_result_db}.dq_check_log"
                )

        if result > 0:
            raise Failure(description="DQ check failed")

    return _asset


def build_merge_stg_asset(
    merge_table: str,
    merge_into_table: str,
    primary_keys: Sequence[str],
    upstreams: Sequence[str],
    group_name: str,
    database: str,
    region: str,
) -> AssetsDefinition:
    @asset(
        name=merge_into_table,
        owners=["team:DataEngineering"],
        non_argument_deps=set(upstreams),
        key_prefix=[region, database],
        compute_kind="merge_df",
        op_tags={"asset_type": "merge"},
        metadata={
            "database": database,
            "write_mode": "merge",
            "primary_keys": primary_keys,
        },
        required_resource_keys={"databricks_pyspark_step_launcher"},
        group_name=group_name,
    )
    def _asset():
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        df = spark.table(f"{database}.{merge_table}")
        return df

    return _asset


def build_ddl_asset(
    name: str,
    description: str,
    schema: Any,
    sql_statement: str,
    upstreams: Sequence[str],
    group_name: str,
    database: str,
    region: str,
) -> AssetsDefinition:
    @asset(
        name=name,
        owners=["team:DataEngineering"],
        description=description,
        non_argument_deps=set(upstreams),
        io_manager_key="in_memory_io_manager",
        key_prefix=[region, database],
        compute_kind="ddl",
        op_tags={"asset_type": "ddl"},
        metadata={
            "description": "this is a view",
            "schema": schema,
        },
        required_resource_keys={region_to_step_launcher[region]},
        group_name=group_name,
    )
    def _asset(context) -> int:
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        context.log.info(f"running query as part of DDL: {sql_statement}")
        try:
            spark.sql(sql_statement)
        except Exception as e:
            context.log.error(e)
            return 1
        return 0

    return _asset


def log_query_data(
    context: OutputContext,
    sql_query: Optional[str],
):

    if not sql_query:
        context.log.info(
            """
        No query provided for logging.\n
        To log your SQL query, you can add it to the metadata of your asset like this\n
        @asset(\n
                metadata=\n
                        {"database": DATABASE,\n
                         "sql_query": ADD YOUR QUERY HERE\n,
              },\n
            )
        """
        )
        return

    context.log.info(f"Running SQL query in Databricks: \n{sql_query}")


def get_partition_value_from_context(context, k):
    if hasattr(context.partition_key, "keys_by_dimension"):
        partition_keys_by_dimension = context.partition_key.keys_by_dimension
    else:
        partition_keys_by_dimension = {"date": context.partition_key}

    context.log.info(partition_keys_by_dimension)

    return partition_keys_by_dimension.get(k)


SQLFunc = Callable[[Dict[str, Union[str, int]]], str]


def _build_asset(
    name: str,
    sql_query: Union[str, SQLFunc],
    primary_keys: Sequence[str],
    upstreams: Union[Set[AssetKey], Set[str], Set[Tuple[str]]],
    group_name: str,
    database: str,
    databases: Sequence[str],
    schema: Sequence[Mapping[str, Any]],
    description: Optional[str] = None,
    write_mode: WarehouseWriteMode = WarehouseWriteMode.overwrite,
    region: str = "us-west-2",
    partitions_def: Optional[PartitionsDefinition] = None,
    auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
    query_type: str = None,
    custom_query_params: Optional[Mapping[str, Union[str, Mapping[str, str]]]] = None,
    depends_on_past: bool = False,
    depends_on_all: bool = False,
    custom_macros: Dict[str, Union[str, int]] = {},
    step_launcher: str = "databricks_pyspark_step_launcher",
    retry_policy: RetryPolicy = RetryPolicy(max_retries=0),
    backfill_policy: BackfillPolicy = None,
    op_tags: Optional[Mapping[str, Any]] = None,
    glossary_terms: Optional[List[GlossaryTerm]] = None,
) -> AssetsDefinition:
    deps = []
    table_name = f"{database}.{name}"

    for upstream in list(upstreams):
        if depends_on_all:
            deps.append(
                AssetDep(
                    upstream,
                    partition_mapping=MultiToSingleDimensionPartitionMapping("date"),
                )
            )
        else:
            deps.append(upstream)

    if depends_on_past:
        depend_on_past_mapping = TimeWindowPartitionMapping(
            start_offset=-1, end_offset=-1
        )
        deps.append(
            AssetDep(
                AssetKey([region, database, name]),
                partition_mapping=depend_on_past_mapping,
            )
        )

    code_location = get_code_location()

    metadata = {
        "database": database,
        "write_mode": write_mode.value,
        "primary_keys": primary_keys,
        "sql_query": "dynamic",
        "schema": schema,
        "region": region,
        "code_location": code_location,
        "description": description,
        "owners": ["team:DataEngineering"],
        "glossary_terms": [f"{term.value}" for term in glossary_terms]
        if glossary_terms
        else [],
    }
    if description:
        metadata["description"] = description

    if step_launcher == "databricks_pyspark_step_launcher_firmware":
        if region == "us-west-2":
            print("using databricks_pyspark_step_launcher_firmware")
        elif region == "eu-west-1":
            step_launcher = "databricks_pyspark_step_launcher_firmware_eu"
            print("using databricks_pyspark_step_launcher_firmware_eu")
        elif region == "ca-central-1":
            step_launcher = "databricks_pyspark_step_launcher_firmware_ca"
            print("using databricks_pyspark_step_launcher_firmware_ca")
    elif step_launcher == "databricks_pyspark_step_launcher_dagster_cost_eval":
        if region == "us-west-2":
            print("using databricks_pyspark_step_launcher_dagster_cost_eval")
        elif region == "eu-west-1":
            step_launcher = "databricks_pyspark_step_launcher_dagster_cost_eval_eu"
            print("using databricks_pyspark_step_launcher_dagster_cost_eval_eu")
        elif region == "ca-central-1":
            step_launcher = "databricks_pyspark_step_launcher_dagster_cost_eval_ca"
            print("using databricks_pyspark_step_launcher_dagster_cost_eval_ca")
    elif step_launcher == "databricks_pyspark_step_launcher_devices_settings":
        if region == "us-west-2":
            print("using databricks_pyspark_step_launcher_devices_settings")
        elif region == "eu-west-1":
            step_launcher = "databricks_pyspark_step_launcher_devices_settings_eu"
            print("using databricks_pyspark_step_launcher_devices_settings_eu")
        elif region == "ca-central-1":
            step_launcher = "databricks_pyspark_step_launcher_devices_settings_ca"
            print("using databricks_pyspark_step_launcher_devices_settings_ca")
    elif step_launcher == "databricks_pyspark_step_launcher_firmware_events":
        if region == "us-west-2":
            print("using databricks_pyspark_step_launcher_firmware_events")
        elif region == "eu-west-1":
            step_launcher = "databricks_pyspark_step_launcher_firmware_events_eu"
            print("using databricks_pyspark_step_launcher_firmware_events_eu")
        elif region == "ca-central-1":
            step_launcher = "databricks_pyspark_step_launcher_firmware_events_ca"
            print("using databricks_pyspark_step_launcher_firmware_events_ca")
    else:
        step_launcher = region_to_step_launcher[region]

    _op_tags = {"asset_type": write_mode.value}
    if op_tags:
        _op_tags.update(op_tags)

    @asset(
        name=name,
        owners=["team:DataEngineering"],
        description=description,
        deps=deps,
        key_prefix=[region, database],
        compute_kind="sql",
        op_tags=_op_tags,
        metadata=metadata,
        required_resource_keys={step_launcher},
        group_name=group_name,
        partitions_def=partitions_def,
        auto_materialize_policy=auto_materialize_policy,
        retry_policy=retry_policy,
        backfill_policy=backfill_policy,
    )
    def _asset(context: AssetExecutionContext):
        os.environ["AWS_REGION"] = region

        context.log.info(f"table description: {metadata.get('description')}")

        if partitions_def:

            if isinstance(partitions_def, TimeWindowPartitionsDefinition):

                partition_date_str = context.partition_key_range.start
                first_day = partition_date_str == partitions_def.start.strftime(
                    "%Y-%m-%d"
                )
            elif isinstance(partitions_def, MultiPartitionsDefinition):
                partition_date_str = context.partition_key_range.start
                first_day = (
                    partition_date_str
                    == partitions_def.primary_dimension.partitions_def.start.strftime(
                        "%Y-%m-%d"
                    )
                )

            partition_where_clause = get_partition_clause_from_keys(
                context.partition_keys, partitions_def
            )
        else:
            partition_date_str = "NO DATE AVAILABLE"
            partition_where_clause = "NO PARTITIONS AVAILABLE"
            first_day = False

        context.log.info(partition_date_str)

        timetravel_date_str = get_timetravel_str(partition_date_str)

        if isinstance(sql_query, Callable):
            partition_value_map = get_partition_value_map_from_context(context)
            context.log.info(partition_value_map)
            sql_query_derived = sql_query(partition_value_map, **custom_query_params)
        else:
            sql_query_derived = sql_query

        final_query = (
            custom_query_params["alt_query"]
            if (query_type in ("lifetime", "cumulative") and first_day)
            else sql_query_derived
        )

        custom_macros_with_values = {
            k: get_partition_value_from_context(context, v)
            for k, v in custom_macros.items()
        }

        kw_args = databases
        kw_args.update(custom_macros_with_values)

        if partitions_def:
            kw_args["PARTITION_KEY_RANGE_START"] = context.partition_key_range.start
            kw_args["PARTITION_KEY_RANGE_END"] = context.partition_key_range.end

        context.log.info("kw_args = " + str(kw_args))

        # below are "macros" we can use inside query format strings - more can be added here
        formatted_sql_query = final_query.format(
            DATEID=partition_date_str,
            PARTITION_FILTERS=partition_where_clause,
            TIMETRAVEL_DATE=timetravel_date_str,
            **kw_args,
        )

        log_query_data(context, formatted_sql_query)

        if region == "us-west-2":
            # TODO (https://samsara.atlassian-us-gov-mod.net/browse/DATAPLAT-1655) this is a temporary solution
            # to help build the POC of lineage in new metadata catalog (DataHub)
            # it should be moved into its own standalone job or replaced with Unity Catalog lineage
            try:

                def trim_comment(sql: str) -> str:
                    import sqlparse

                    return str(sqlparse.format(sql, strip_comments=True))

                from sqllineage.runner import LineageRunner

                sql_to_parse = f"""insert into {table_name}
                            {trim_comment(formatted_sql_query)}
                            """
                sql_to_parse = sql_to_parse.replace("'", "`")
                sql_to_parse = sql_to_parse.replace("--sql", "")
                sql_to_parse = sql_to_parse.replace("--endsql", "")
                sql_to_parse = replace_timetravel_string(sql_to_parse)
                context.log.info(f"sql to parse: {sql_to_parse}")
                lineage_result = LineageRunner(sql_to_parse)
                context.log.info(f"lineage result: {lineage_result.source_tables}")
                result_deps = [x.__str__() for x in lineage_result.source_tables]
                context.log.info(f"result deps: {result_deps}")

                raw_json = json.dumps(result_deps)

                partition = context.asset_partition_key_for_output()

                s3_client = boto3.client("s3")
                s3_client.put_object(
                    Body=raw_json,
                    Bucket="samsara-amundsen-metadata",
                    Key=f"staging/lineage/{database}/{name}/{partition}.json",
                )
            except Exception as e:
                context.log.info(e)
                context.log.info(f"failed to get lineage for {formatted_sql_query}")

        if os.getenv("MODE") == "DRY_RUN":
            context.log.info("dry run - exiting without materialization")
            return {"dry_run_early_exit": MetadataValue.int(0)}

        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        df = spark.sql(formatted_sql_query)

        return df

    return _asset


def build_asset_from_sql_in_region(
    name: str,
    sql_query: str,
    primary_keys: Sequence[str],
    upstreams: Union[Set[AssetKey], Set[str], Set[Tuple[str]]],
    group_name: str,
    database: str,
    databases: Sequence[str],
    schema: Sequence[Mapping[str, Any]],
    region: str,
    description: Optional[str] = None,
    write_mode: WarehouseWriteMode = WarehouseWriteMode.overwrite,
    partitions_def: Optional[PartitionsDefinition] = None,
    auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
    query_type: str = None,
    custom_query_params: Optional[Mapping[str, Union[str, Mapping[str, str]]]] = None,
    depends_on_past: bool = False,
    depends_on_all: bool = False,
    custom_macros: Dict[str, Union[str, int]] = {},
    step_launcher: str = "databricks_pyspark_step_launcher",
    retry_policy: RetryPolicy = RetryPolicy(max_retries=0),
    backfill_policy: BackfillPolicy = None,
    op_tags: Optional[Mapping[str, Any]] = None,
    glossary_terms: Optional[List[GlossaryTerm]] = None,
) -> Tuple[AssetsDefinition, AssetsDefinition]:
    if region == "eu-west-1":
        group_name += "_eu"
    elif region == "ca-central-1":
        group_name += "_ca"

    # Adjust partition definition for Canada region
    if region == AWSRegions.CA_CENTRAL_1.value:
        partitions_def = adjust_partition_def_for_canada(partitions_def)

    asset = _build_asset(
        name=name,
        sql_query=sql_query,
        primary_keys=primary_keys,
        upstreams=upstreams,
        group_name=group_name,
        database=database,
        databases=databases,
        schema=schema,
        description=description,
        write_mode=write_mode,
        region=region,
        partitions_def=partitions_def,
        auto_materialize_policy=auto_materialize_policy,
        query_type=query_type,
        custom_query_params=custom_query_params,
        depends_on_past=depends_on_past,
        depends_on_all=depends_on_all,
        custom_macros=custom_macros,
        step_launcher=step_launcher,
        retry_policy=retry_policy,
        backfill_policy=backfill_policy,
        op_tags=op_tags,
        glossary_terms=glossary_terms,
    )

    return asset


def build_assets_from_sql(
    name: str,
    sql_query: str,
    primary_keys: Sequence[str],
    upstreams: Union[Set[AssetKey], Set[str], Set[Tuple[str]]],
    group_name: str,
    database: str,
    databases: Sequence[str],
    schema: Sequence[Mapping[str, Any]],
    regions: Sequence[str],
    description: Optional[str] = None,
    write_mode: WarehouseWriteMode = WarehouseWriteMode.overwrite,
    partitions_def: Optional[PartitionsDefinition] = None,
    query_type: str = None,
    custom_query_params: Optional[Mapping[str, Union[str, Mapping[str, str]]]] = {},
    depends_on_past: bool = False,
    depends_on_all: bool = False,
    custom_macros: Dict[str, Union[str, int]] = {},
    step_launcher: str = "databricks_pyspark_step_launcher",
    auto_materialize_policy: AutoMaterializePolicy = None,
    retry_policy: RetryPolicy = RetryPolicy(max_retries=0),
    backfill_policy: BackfillPolicy = None,
    op_tags: Optional[Mapping[str, Any]] = None,
    glossary_terms: Optional[List[GlossaryTerm]] = None,
) -> Mapping[str, AssetsDefinition]:
    assets_defs = {}

    for region in regions:
        upstream_assets = set()
        for upstream in upstreams or []:
            if isinstance(upstream, AssetKey):
                asset_key = upstream.with_prefix(region)
                upstream_assets.add(asset_key)
            elif isinstance(upstream, (Sequence, str)):
                asset_key = AssetKey(upstream)
                asset_key = asset_key.with_prefix(region)
                upstream_assets.add(asset_key)
            else:
                raise TypeError("invalid type for 'upstreams'")

        asset = build_asset_from_sql_in_region(
            name,
            sql_query,
            primary_keys,
            upstream_assets,
            group_name,
            database,
            databases,
            schema,
            region=region,
            description=description,
            write_mode=write_mode,
            partitions_def=partitions_def,
            auto_materialize_policy=auto_materialize_policy,
            query_type=query_type,
            custom_query_params=custom_query_params,
            depends_on_past=depends_on_past,
            depends_on_all=depends_on_all,
            custom_macros=custom_macros,
            step_launcher=step_launcher,
            retry_policy=retry_policy,
            backfill_policy=backfill_policy,
            op_tags=op_tags,
            glossary_terms=glossary_terms,
        )
        assets_defs[region] = asset

    return assets_defs


def apply_db_overrides(databases, database_dev_overrides):
    # when running unit tests via pytest, we want to resolve to prod database
    # settings so that we can simulate the asset keys of the prod environment
    if get_run_env() == "prod" or os.getenv("MODE") in [
        "DRY_RUN",
        "PROD_BACKFILL",
        "LOCAL_PROD_CLUSTER_RUN",
        "LOCAL_DATAHUB_RUN",
        "PYTEST_RUN",
    ]:
        databases_dev = {f"{x}_dev": y for x, y in databases.items()}
    else:
        databases_dev = database_dev_overrides

    databases.update(databases_dev)

    return databases


def is_development_database(database: str) -> bool:
    return database.endswith("_dev")


def get_aws_account_id(region: str) -> str:
    if region == AWSRegions.US_WEST_2.value:
        account_id = "492164655156"
    elif region == AWSRegions.EU_WEST_1.value:
        account_id = "353964698255"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        account_id = "211125295193"
    else:
        raise ValueError(
            f"Region {region} is not one of ({[e.value for e in AWSRegions]})"
        )
    return account_id


def get_region_from_databricks_host(host: str):
    if host.startswith(
        f"https://samsara-dev-{AWSRegions.US_WEST_2.value}.cloud.databricks.com"
    ):
        region = AWSRegions.US_WEST_2.value
    elif host.startswith(
        f"https://samsara-dev-{AWSRegions.EU_WEST_1.value}.cloud.databricks.com"
    ):
        region = AWSRegions.EU_WEST_1.value
    elif host.startswith(
        f"https://samsara-dev-{AWSRegions.CA_CENTRAL_1.value}.cloud.databricks.com"
    ):
        region = AWSRegions.CA_CENTRAL_1.value
    else:
        raise ValueError(
            f"Host {host} is not from one of supported regions ({[e.value for e in AWSRegions]})"
        )
    return region


def get_databricks_workspace_id(region: str):
    if region == AWSRegions.US_WEST_2.value:
        workspace_id = "5924096274798303"
    elif region == AWSRegions.EU_WEST_1.value:
        workspace_id = "6992178240159315"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        workspace_id = "1976292512529253"
    else:
        raise ValueError(
            f"Region {region} is not one of ({[e.value for e in AWSRegions]})"
        )
    return workspace_id


def get_region_bucket_prefix(region: str) -> str:
    if region == AWSRegions.US_WEST_2.value:
        bucket_prefix = "samsara-"
    elif region == AWSRegions.EU_WEST_1.value:
        bucket_prefix = "samsara-eu-"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        bucket_prefix = "samsara-ca-"
    else:
        raise ValueError(
            f"Region {region} is not one of ({[e.value for e in AWSRegions]})"
        )
    return bucket_prefix


def get_databricks_workspace_prefix(region: str):
    if region == AWSRegions.US_WEST_2.value:
        key_prefix = "oregon-prod"
    elif region == AWSRegions.EU_WEST_1.value:
        key_prefix = "ireland-prod"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        key_prefix = "canada-prod"
    else:
        raise ValueError(
            f"Region {region} is not one of ({[e.value for e in AWSRegions]})"
        )
    workspace_id = get_databricks_workspace_id(region)
    return f"{key_prefix}/{workspace_id}"


def get_databricks_workspace_bucket(region: str) -> str:
    bucket_prefix = get_region_bucket_prefix(region)
    return f"{bucket_prefix}databricks-dev-{region}-root"


def get_region_bucket_name(region: str, database: str):
    bucket_prefix = get_region_bucket_prefix(region)
    bucket_suffix = "-dev" if is_development_database(database) else ""
    return f"{bucket_prefix}datamodel-warehouse{bucket_suffix}"


def _get_replication_sql_query(table: str, database: str, region: str) -> str:
    bucket_name = get_region_bucket_name(region, database)
    query = f"SELECT * FROM delta.`s3://{bucket_name}/{database}.db/{table}`"

    return query


def build_asset_replica_in_region(
    asset: AssetsDefinition,
    upstreams: Union[Set[AssetKey], Set[str]],
    databases: Sequence[str],
    replication_region: str,
    group_name: str,
    description: Optional[str] = None,
    auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
) -> AssetsDefinition:
    name = asset.key.path[-1]
    partitions_def = asset.partitions_def
    origin_region = asset.metadata_by_key[asset.key].get("region")
    database = asset.metadata_by_key[asset.key].get("database")
    primary_keys = asset.metadata_by_key[asset.key].get("primary_keys")
    schema = asset.metadata_by_key[asset.key].get("schema")
    backfill_policy = asset.backfill_policy

    assert (
        origin_region != replication_region
    ), f"Origin region '{origin_region}' cannot be same as {replication_region}"
    replica_name = f"{name}_{origin_region[:2]}"
    replication_query = _get_replication_sql_query(
        table=name, database=database, region=origin_region
    )

    asset = _build_asset(
        name=replica_name,
        sql_query=replication_query,
        primary_keys=primary_keys,
        upstreams=upstreams,
        group_name=group_name,
        database=database,
        databases=databases,
        schema=schema,
        description=description,
        write_mode=WarehouseWriteMode.overwrite,
        region=replication_region,
        partitions_def=partitions_def,
        auto_materialize_policy=auto_materialize_policy,
        backfill_policy=backfill_policy,
    )
    return asset


def build_source_assets(
    name: str,
    database: str,
    group_name: str,
    regions: Sequence[str],
    description: str = "",
    partitions_def: Optional[PartitionsDefinition] = None,
    partition_keys: Sequence[str] = [],
    observe_fn: Optional[Callable] = None,
    auto_observe_interval_minutes: int = None,
) -> Mapping[str, AssetsDefinition]:
    source_asset_defs = {}
    for region in regions:
        metadata = {
            "database": database,
            "region": region,
            "partition_keys": partition_keys,
        }

        asset = SourceAsset(
            key=[region, database, name],
            group_name=f"source_{group_name}_{region[:2]}",
            description=description,
            metadata=metadata,
            partitions_def=partitions_def,
            observe_fn=observe_fn,
            auto_observe_interval_minutes=auto_observe_interval_minutes,
        )

        source_asset_defs[region] = asset
    return source_asset_defs


def _get_run_type(context: HookContext):
    backfill_key = context._step_execution_context.run_tags.get("dagster/backfill")
    automaterialize_key = context._step_execution_context.run_tags.get(
        "dagster/asset_evaluation_id"
    )
    schedule_key = context._step_execution_context.run_tags.get("dagster/schedule_name")
    if backfill_key:
        run_type = "backfill"
    elif schedule_key:
        run_type = "schedule"
    elif automaterialize_key:
        run_type = "automaterialization"
    else:
        run_type = "unclassified"

    return run_type


def _get_job_type(context):
    compute_kind = context.op.tags.get("dagster/compute_kind")
    if compute_kind == "sql":
        job_type = "asset_materialization"
    elif compute_kind == "dq":
        job_type = "dq"
    elif compute_kind == "ddl":
        job_type = "dq"
    else:
        job_type = "unclassified"
    return job_type


def gen_step_completion_hook(context: HookContext, status: str):

    run_type = _get_run_type(context)
    job_type = _get_job_type(context)

    asset_key_parts = context.op.name.split("__")
    if len(asset_key_parts) == 3:
        region = asset_key_parts[-3].replace("_", "-")
        database = asset_key_parts[-2]
        table = asset_key_parts[-1]
    else:
        region = "None"
        database = "None"
        table = asset_key_parts[-1]

    from datamodel import defs

    asset_specs = {spec.key: spec for spec in defs.get_all_asset_specs()}

    owner_map = {
        ".".join(key.path[1:]): value.owners
        for key, value in asset_specs.items()
        if value.owners
    }

    owner_map = {key: value[0] for key, value in owner_map.items()}

    table_owner = owner_map.get(f"{database}.{table}")

    if table_owner:
        table_owner = table_owner.replace("team:", "")

    context.log.info(f"table_owner: {table_owner}")

    tags = [
        f"job_type:{job_type}",
        f"run_type:{run_type}",
        f"status:{status}",
        f"name:{context.op.name}",
        f"region:{region}",
        f"database:{database}",
        f"table:{table}",
        f"owner:{table_owner}",
    ]

    if context._step_execution_context.has_asset_partitions_for_output("result"):
        partition_def = context._step_execution_context.partitions_def_for_output(
            "result"
        )
        partition_key_range = (
            context._step_execution_context.asset_partition_key_range_for_output(
                "result"
            )
        )
        partition_keys = partition_def.get_partition_keys_in_range(
            partition_key_range=partition_key_range
        )

        if partition_key_range.start == partition_key_range.end:
            partition_key_tag = f"{partition_key_range.start}"
        else:
            partition_key_tag = f"{partition_key_range.start}_{partition_key_range.end}"

    else:
        partition_keys = ["unpartitioned"]
        partition_key_tag = "unpartitioned"

    now = datetime.utcnow().timestamp()
    metrics = []
    for partition_key in partition_keys:
        dagster_run_tags = tags + [
            f"partition:{partition_key}",
        ]
        metrics.append(
            DatadogMetric(
                metric="dagster.job.run",
                type="count",
                points=[
                    (now, 1.0),
                ],
                tags=dagster_run_tags,
            )
        )

    databricks_run_id = getattr(
        context._step_execution_context.step_launcher, "_databricks_run_id", ""
    )

    try:
        databricks_run = context._step_execution_context.step_launcher.databricks_runner.client.workspace_client.jobs.get_run(
            databricks_run_id
        ).as_dict()
    except Exception:
        databricks_run = {}

    # Emit metrics for associated Databricks run, if one exists.
    if databricks_run:
        run_duration = databricks_run.get("end_time", 0) - databricks_run.get(
            "start_time", 0
        )
        databricks_run_tags = tags + [
            f"partition:{partition_key_tag}",
        ]
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.duration",
                type="count",
                points=[
                    (now, run_duration),
                ],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.setup_duration",
                type="count",
                points=[
                    (now, databricks_run.get("setup_duration", 0)),
                ],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.cleanup_duration",
                type="count",
                points=[
                    (now, databricks_run.get("cleanup_duration", 0)),
                ],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.execution_duration",
                type="count",
                points=[
                    (now, databricks_run.get("execution_duration", 0)),
                ],
                tags=databricks_run_tags,
            )
        )

    if get_run_env() == "prod":
        initialize_datadog()
        send_datadog_metrics(context, metrics=metrics)
    else:
        context.log.info(
            f"printing {status} hook metrics that would be logged on prod run"
        )
        context.log.info(f"Dagster run tags: {dagster_run_tags}")
        context.log.info(f"Databricks run tags: {databricks_run_tags}")
    return


def split_to_groups(items: Sequence[Any], size: int):
    batches = []
    for i in range(0, len(items), size):
        batches.append(items[i : i + size])
    return batches


def build_backfill_schedule(
    job: ExecutableDefinition,
    cron: str,
    partitions_fn: Union[Iterable[str], Callable[[], Sequence[str]]],
    backfill_policy: BackfillPolicy = None,
    name: str = None,
    options: Dict[str, str] = {},
) -> ScheduleDefinition:

    schedule_name = name if name else f"{job.name}_backfill_schedule"

    @schedule(job=job, name=schedule_name, cron_schedule=cron)
    def _schedule(context: ScheduleEvaluationContext):

        _options = options.copy()
        if "start_date" not in options:
            _options["start_date"] = context.scheduled_execution_time.date().isoformat()

        partition_keys = sorted(
            partitions_fn
            if isinstance(partitions_fn, Iterable)
            else partitions_fn(**_options),
            reverse=True,
        )

        context.log.info(f"Evaluating schedule for partition keys: ({partition_keys})")
        scheduled_execution_time = (
            context.scheduled_execution_time.replace(tzinfo=None)
            .isoformat()
            .replace(":", ".")[:63]
        )
        if backfill_policy is None or backfill_policy.max_partitions_per_run == 1:
            run_requests = [
                RunRequest(
                    run_key=f"{scheduled_execution_time}-{key}",
                    tags={"dagster/backfill": scheduled_execution_time},
                    partition_key=key,
                )
                for key in partition_keys or []
            ]
        elif backfill_policy and backfill_policy.max_partitions_per_run > 1:
            run_requests = [
                RunRequest(
                    run_key=f"{scheduled_execution_time}-{batch[-1]}-{batch[0]}",
                    tags={
                        "dagster/backfill": scheduled_execution_time,
                        "dagster/asset_partition_range_start": batch[-1],
                        "dagster/asset_partition_range_end": batch[0],
                    },
                )
                for batch in split_to_groups(
                    partition_keys, backfill_policy.max_partitions_per_run
                )
            ]
        else:
            raise ValueError("Error: no run requests found")
        return run_requests

    return _schedule


@failure_hook
def datamodel_job_failure_hook(context: HookContext):
    gen_step_completion_hook(context, "failure")
    # send_pagerduty_events(context, "trigger") # TODO: Uncomment this once we fix the pagerduty integration


@success_hook
def datamodel_job_success_hook(context: HookContext):
    gen_step_completion_hook(context, "success")
    # send_pagerduty_events(context, "resolve") # TODO: Uncomment this once we fix the pagerduty integration


def _map_observed_database(database: str) -> str:
    db = database
    if database.endswith("_history"):
        db = database.split("_history")[0]

    if database.endswith("db_shards"):
        db = f"{database.split('db_shards')[0]}\_shard\_%db"

    return db


def build_observation_function(observation_window_days: int = 7):
    def _observe_partitioned_source(context) -> DataVersionsByPartition:
        context.log.info(f"Observing Source Asset: {context.node_handle}")
        region, database, table = str(context.node_handle).split("__")
        region = region.replace("_", "-")

        # We only log updated partitions for physical entities but many source assets are actually
        # logical entities. For example, all entities in rds replicated databases ending with "db_shards"
        # are views that union all of the physical shards. To observe logical Dagster source assets we
        # need to map the multiple physical entities to the single logical one.
        mapped_database = _map_observed_database(database)

        query = UPDATED_PARTITIONS_QUERY.format(
            database=mapped_database,
            table=table,
            observation_window_days=observation_window_days,
        )
        context.log.info(query)
        DOMAIN = f"samsara-dev-{region}.cloud.databricks.com"

        databricks_token = get_dbx_token(region)
        if databricks_token:
            credentials_provider = None
        else:
            credentials_provider = lambda: oauth_service_principal(
                dbxconfig(host=f"https://{DOMAIN}", **get_dbx_oauth_credentials(region))
            )

        with sql.connect(
            server_hostname=DOMAIN,
            http_path=f"/sql/1.0/warehouses/{get_dbx_warehouse_id(region)}",
            access_token=databricks_token,
            credentials_provider=credentials_provider,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

        versions = {row.partition_key: row.created_at for row in result}
        context.log.info(f"Updated versions: ({versions})")
        return DataVersionsByPartition(data_versions_by_partition=versions)

    return _observe_partitioned_source


def query_dagster_asset_landing_logs_run_ids(asset_keys):

    region = "us-west-2"
    DOMAIN = f"samsara-dev-{region}.cloud.databricks.com"
    warehouse_id = get_dbx_warehouse_id(region)

    databricks_token = get_dbx_token(region)
    if databricks_token:
        credentials_provider = None
    else:
        credentials_provider = lambda: oauth_service_principal(
            dbxconfig(host=f"https://{DOMAIN}", **get_dbx_oauth_credentials(region))
        )

    query = f"""
        --sql
        SELECT DISTINCT asset_partition, dagster_run_id
        FROM auditlog.dagster_asset_landing_logs
        WHERE run_environment = '{get_run_env()}'
        AND asset = "{asset_keys}"
        AND pst_timestamp >= current_date() - 5
        --endsql
        """

    with sql.connect(
        server_hostname=DOMAIN,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=databricks_token,
        credentials_provider=credentials_provider,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

    return result


def populate_dagster_asset_landing_logs(
    asset_keys, partition, utc_time, dagster_run_id, dq_metadata_output
):

    # TODO: Add link to databricks notebook with table create statement
    pst_zone = pytz.timezone("America/Los_Angeles")
    pst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(pst_zone)

    region = "us-west-2"
    DOMAIN = f"samsara-dev-{region}.cloud.databricks.com"
    warehouse_id = get_dbx_warehouse_id(region)

    databricks_token = get_dbx_token(region)
    if databricks_token:
        credentials_provider = None
    else:
        credentials_provider = lambda: oauth_service_principal(
            dbxconfig(host=f"https://{DOMAIN}", **get_dbx_oauth_credentials(region))
        )

    regex_date_extractor = "^(\\\d{4}-\\\d{2}-\\\d{2})"

    query = f"""
        --sql
        MERGE INTO auditlog.dagster_asset_landing_logs AS target
        USING (SELECT '{get_run_env()}' AS run_environment,
                "{asset_keys}" AS asset,
                '{partition}' AS asset_partition,
                regexp_extract('{partition}', '{regex_date_extractor}', 1) AS date_partition,
                regexp_replace(REPLACE('{partition}', regexp_extract('{partition}', '{regex_date_extractor}', 1), ''), '^\\\|', '') AS sub_partitions,
                '{str(pst_time)}' AS pst_timestamp,
                '{dagster_run_id}' AS dagster_run_id,
                '{dq_metadata_output}' AS dq_metadata) AS source
        ON target.run_environment = source.run_environment
            AND target.asset = source.asset
            AND target.asset_partition = source.asset_partition
            AND target.dagster_run_id = source.dagster_run_id
        WHEN NOT MATCHED THEN
            INSERT (run_environment,
                    asset,
                    asset_partition,
                    date_partition,
                    sub_partitions,
                    pst_timestamp,
                    dagster_run_id,
                    dq_metadata)
            VALUES (source.run_environment,
                    source.asset,
                    source.asset_partition,
                    source.date_partition,
                    source.sub_partitions,
                    source.pst_timestamp,
                    source.dagster_run_id,
                    source.dq_metadata);
        --endsql
        """
    print(query)

    with sql.connect(
        server_hostname=DOMAIN,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=databricks_token,
        credentials_provider=credentials_provider,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)


def clean_up_metadata_dict(data):
    if isinstance(data, dict):
        # Create a new dictionary excluding the "details" key
        return {k: clean_up_metadata_dict(v) for k, v in data.items() if k != "details"}
    elif isinstance(data, list):
        # Recursively apply to each item in the list
        return [clean_up_metadata_dict(item) for item in data]
    else:
        # Base case: return the data itself if it's neither a dict nor a list
        return data


def get_non_empty(d: Dict[str, Any], key: str, default: Any) -> Any:
    """
    Return the value for key if key is in the dictionary, else default.
    If the key is in the dictionary but has a value of None, return default.
    """
    return d.get(key) if d.get(key) else default


def safe_get(d: dict, keys: list, default=None) -> any:
    """
    Attempts to get a value from a nested dictionary using a list of keys.
    safe_get is syntactic sugar for cases where you are accessing a nested property
    and it could be None at any stage.
    Trying this out to approximate safe calls in Kotlin.

    Args:
        d (dict): The nested dictionary to retrieve the value from.
        keys (list): A list of keys representing the path to the desired value.
        default (any, optional): The default value to return if the value is not found. Defaults to None.

    Returns:
        any: The value if found, otherwise returns `default`.
    """
    assert isinstance(keys, list), "keys must be provided as a list"
    for key in keys:
        try:
            d = d.get(key, default)
        except AttributeError:
            # In case the intermediary is not a dictionary and does not have .get
            return default
    return d


def find_line_number_in_code(go_file_path: str, search_term: str) -> Optional[int]:
    """
    Reads a Go file and returns the line number where the search term is first found.

    Parameters:
    - go_file_path: str, the path to the Go file.
    - search_term: str, the term to search for in the file.

    Returns:
    - int, the line number where the search term is first found, or None if not found.
    """
    try:
        with open(go_file_path, "r", encoding="utf-8") as file:
            for i, line in enumerate(file, 1):
                if search_term.lower() in line.lower():
                    return i
    except FileNotFoundError:
        print(f"File not found: {go_file_path}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    # Return None if the search term is not found
    return None
