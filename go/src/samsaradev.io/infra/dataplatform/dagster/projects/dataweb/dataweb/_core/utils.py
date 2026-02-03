import inspect
import os
from collections.abc import Sequence
from dataclasses import asdict, fields
from datetime import datetime
from typing import Any, Dict, List, Literal, NamedTuple, Optional, Tuple, Union

import boto3
import botocore
import datadog
from botocore.exceptions import ClientError, NoCredentialsError
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    DataVersionsByPartition,
    PartitionsDefinition,
)
from databricks import sql
from databricks.sdk.core import Config as dbxconfig
from databricks.sdk.core import oauth_service_principal
from pyspark.sql.types import *
from slack import WebClient

from ..userpkgs.constants import AWSRegion, Database, RunEnvironment
from .constants import (
    CADatabricksSQLWarehouseId,
    DagsterTag,
    DatabricksAWSAccountId,
    DatabricksWorkspaceId,
    EUDatabricksSQLWarehouseId,
    USDatabricksSQLWarehouseId,
)


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


def get_all_regions() -> List[str]:
    return list(asdict(AWSRegion).values())


def get_run_env() -> Literal["prod", "dev"]:
    env = os.getenv("ENV", "dev")
    assert env in list(
        asdict(RunEnvironment()).values()
    ), "environment variable 'ENV' must be one of ('prod', 'dev')"
    return asdict(RunEnvironment()).get(env.upper())


def get_ssm_parameter(ssm_client: boto3.Session, name: str) -> str:
    res = ssm_client.get_parameter(Name=name, WithDecryption=True)
    return res["Parameter"]["Value"]


def get_databases() -> Dict[Database, Database]:
    dbs = {
        table: table
        for table in [getattr(Database, field.name) for field in fields(Database)]
    }

    if get_run_env() == "prod" or os.getenv("MODE") in [
        "DRY_RUN",
        "PROD_BACKFILL",
        "LOCAL_PROD_CLUSTER_RUN",
        "LOCAL_DATAHUB_RUN",
        "PYTEST_RUN",
    ]:
        return dbs

    dev_tables = {table: Database.DATAMODEL_DEV for table in dbs}
    dev_tables[Database.KINESISSTATS] = Database.KINESISSTATS
    dev_tables[Database.KINESISSTATS_HISTORY] = Database.KINESISSTATS_HISTORY
    return dev_tables


def get_region() -> str:
    """
    Get EC2 region from AWS instance metadata.

    Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    """
    session = boto3.session.Session()
    return session.region_name


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


def get_ssm_token(ssm_parameter: str) -> str:
    region_name = (
        AWSRegion.US_WEST_2
    )  # default to us-west-2 in case region can't be found

    try:
        region_name = get_region()
        if not region_name:
            region_name = AWSRegion.US_WEST_2
    except botocore.exceptions.NoRegionError:
        # from a Dagster failure hook, generating the session sometimes fails since it can't derive a default region
        print("Unable to retrieve region, defaulting to us-west-2")
        region_name = AWSRegion.US_WEST_2

    ssm = boto3.client("ssm", region_name=region_name)
    response = ssm.get_parameter(Name=ssm_parameter, WithDecryption=True)
    return response["Parameter"]["Value"]


def get_slack_token() -> str:
    try:
        return get_ssm_token("DAGSTER_SLACK_BOT_TOKEN")
    except (ClientError, NoCredentialsError) as e:
        print(e)
        print("Unable to retrieve slack token, cannot write to Slack")


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


def get_code_location() -> str:
    stack = inspect.stack()
    for caller in stack:
        if (caller.filename.find("dataweb/assets") != -1) and (
            caller.filename.find("utils.py") == -1
            and caller.filename.find("datamodel/assets/metrics/core.py") == -1
        ):
            break

    backend_root = os.getenv("BACKEND_ROOT", "/code")
    code_location = (
        caller.filename.replace(backend_root, "")
        .replace("/go/src/samsaradev.io/infra/dataplatform/dagster/projects/", "")
        .replace("dataweb/dataweb/", "dataweb/")
    )
    code_line = caller.lineno

    return f"{code_location}, line {code_line}"


def apply_db_overrides(
    databases: Dict[str, str], database_dev_overrides: Dict[str, str]
) -> Dict[str, str]:
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
    if region == AWSRegion.US_WEST_2:
        account_id = DatabricksAWSAccountId.US_WEST_2
    elif region == AWSRegion.EU_WEST_1:
        account_id = DatabricksAWSAccountId.EU_WEST_1
    elif region == AWSRegion.CA_CENTRAL_1:
        account_id = DatabricksAWSAccountId.CA_CENTRAL_1
    else:
        raise ValueError(
            f"Region {region} is not one of {list(asdict(AWSRegion).values())}"
        )
    return account_id


def get_dbx_warehouse_id(region: str) -> str:
    if region == AWSRegion.US_WEST_2:
        warehouse_id = USDatabricksSQLWarehouseId.AIANDDATA
    elif region == AWSRegion.EU_WEST_1:
        warehouse_id = EUDatabricksSQLWarehouseId.AIANDDATA
    elif region == AWSRegion.CA_CENTRAL_1:
        warehouse_id = CADatabricksSQLWarehouseId.AIANDDATA
    else:
        raise ValueError("Invalid region '{region}'")
    return warehouse_id


def get_dbx_token(region: str) -> str:
    if region == AWSRegion.CA_CENTRAL_1:
        token = get_dbx_ca_token()
    elif region == AWSRegion.EU_WEST_1:
        token = get_dbx_eu_token()
    elif region == AWSRegion.US_WEST_2:
        token = get_dbx_us_token()
    else:
        raise ValueError(f"Invalid region '{region}'")
    return token


def get_dbx_oauth_credentials(region: str) -> Dict[str, str]:

    ssm_client = boto3.client("ssm", region_name=get_region())

    if region == AWSRegion.US_WEST_2:
        clientid_key = "DAGSTER_DATABRICKS_CLIENTID"
        client_secret_key = "DAGSTER_DATABRICKS_SECRET"
    elif region == AWSRegion.EU_WEST_1:
        clientid_key = "DAGSTER_DATABRICKS_EU_CLIENTID"
        client_secret_key = "DAGSTER_DATABRICKS_EU_SECRET"
    elif region == AWSRegion.CA_CENTRAL_1:
        clientid_key = "DAGSTER_DATABRICKS_CA_CLIENTID"
        client_secret_key = "DAGSTER_DATABRICKS_CA_SECRET"
    else:
        raise ValueError(
            f"region  must be one of {list(asdict(AWSRegion).values())}. got {region}"
        )

    client_id = ssm_client.get_parameter(Name=clientid_key, WithDecryption=True)[
        "Parameter"
    ]["Value"]
    client_secret = ssm_client.get_parameter(
        Name=client_secret_key, WithDecryption=True
    )["Parameter"]["Value"]

    return {"client_id": client_id, "client_secret": client_secret}


def get_dbx_us_token() -> str:
    return os.getenv("DATABRICKS_TOKEN")


def get_dbx_eu_token() -> str:
    return os.getenv("DATABRICKS_EU_TOKEN")


def get_dbx_ca_token() -> str:
    return os.getenv("DATABRICKS_CA_TOKEN")


def get_region_from_databricks_host(host: str) -> str:
    if host.startswith(
        f"https://samsara-dev-{AWSRegion.US_WEST_2}.cloud.databricks.com"
    ):
        region = AWSRegion.US_WEST_2
    elif host.startswith(
        f"https://samsara-dev-{AWSRegion.EU_WEST_1}.cloud.databricks.com"
    ):
        region = AWSRegion.EU_WEST_1
    elif host.startswith(
        f"https://samsara-dev-{AWSRegion.CA_CENTRAL_1}.cloud.databricks.com"
    ):
        region = AWSRegion.CA_CENTRAL_1
    else:
        raise ValueError(
            f"Host {host} is not from one of supported regions {list(asdict(AWSRegion).values())}"
        )
    return region


def get_databricks_workspace_id(region: str) -> str:
    if region == AWSRegion.US_WEST_2:
        workspace_id = DatabricksWorkspaceId.US_WEST_2
    elif region == AWSRegion.EU_WEST_1:
        workspace_id = DatabricksWorkspaceId.EU_WEST_1
    elif region == AWSRegion.CA_CENTRAL_1:
        workspace_id = DatabricksWorkspaceId.CA_CENTRAL_1
    else:
        raise ValueError(
            f"Region {region} is not one of {list(asdict(AWSRegion).values())}"
        )
    return workspace_id


def get_region_bucket_prefix(region: str) -> str:
    if region == AWSRegion.US_WEST_2:
        bucket_prefix = "samsara-"
    elif region == AWSRegion.EU_WEST_1:
        bucket_prefix = "samsara-eu-"
    elif region == AWSRegion.CA_CENTRAL_1:
        bucket_prefix = "samsara-ca-"
    else:
        raise ValueError(
            f"Region {region} is not one of {list(asdict(AWSRegion).values())}"
        )
    return bucket_prefix


def get_databricks_workspace_prefix(region: str) -> str:
    if region == AWSRegion.US_WEST_2:
        key_prefix = "oregon-prod"
    elif region == AWSRegion.EU_WEST_1:
        key_prefix = "ireland-prod"
    elif region == AWSRegion.CA_CENTRAL_1:
        key_prefix = "canada-prod"
    else:
        raise ValueError(
            f"Region {region} is not one of {list(asdict(AWSRegion).values())}"
        )
    workspace_id = get_databricks_workspace_id(region)
    return f"{key_prefix}/{workspace_id}"


def get_databricks_workspace_bucket(region: str) -> str:
    bucket_prefix = get_region_bucket_prefix(region)
    return f"{bucket_prefix}databricks-dev-{region}-root"


def get_region_bucket_name(region: str, database: str) -> str:
    bucket_prefix = get_region_bucket_prefix(region)
    bucket_suffix = "-dev" if is_development_database(database) else ""
    return f"{bucket_prefix}datamodel-warehouse{bucket_suffix}"


def split_to_groups(items: Sequence[Any], size: int) -> List[List[Any]]:
    batches = []
    for i in range(0, len(items), size):
        batches.append(items[i : i + size])
    return batches


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

        query = f"""
            WITH latest_partition_updates AS (
            SELECT
            created_at,
            concat_ws("|", partition_values) as partition_key,
            ROW_NUMBER() OVER (PARTITION BY partition_columns, partition_values ORDER BY created_at DESC) as rnk
            FROM auditlog.deltatable_partition_updates_log
            WHERE database LIKE '{mapped_database}'
            AND LOWER(table) = '{table}'
            AND CASE WHEN (partition_columns[0] = 'date' AND partition_values[0] >= DATE_SUB(CURRENT_DATE(), {observation_window_days})) THEN TRUE ELSE FALSE END
            )
            SELECT
                partition_key,
                created_at
            FROM latest_partition_updates
            WHERE rnk = 1
        """

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


def range_partitions_from_execution_context(
    context: AssetExecutionContext,
) -> List[str]:
    """Gets a list of partitions between the dagster/asset_partition_range_start and
    dagster/asset_partition_range_end.

    :returns List of partition strings ["2024-11-11|a", "2024-11-11|b", ...]"""
    if DagsterTag.PARTITION.value in context.run.tags:
        # In cases where the partition range only contains one partition, Dagster will
        # only pass the partition tag instead of the range_start/range_end tags.
        range_start = context.run.tags.get(DagsterTag.PARTITION.value)
        range_end = context.run.tags.get(DagsterTag.PARTITION.value)
    else:
        range_start = context.run.tags.get(DagsterTag.ASSET_PARTITION_RANGE_START.value)
        range_end = context.run.tags.get(DagsterTag.ASSET_PARTITION_RANGE_END.value)

    assert (
        range_start is not None and range_end is not None,
        "Missing asset_partition_range_start and asset_partition_range_end tags in execution context",
    )

    partitions_def = context.assets_def.partitions_def
    assert (
        partitions_def is not None,
        "Missing partitions_def in asset execution context",
    )

    all_partition_keys = partitions_def.get_partition_keys()
    partition_idx_start, partition_idx_end = all_partition_keys.index(
        range_start
    ), all_partition_keys.index(range_end)

    return all_partition_keys[partition_idx_start : partition_idx_end + 1]
