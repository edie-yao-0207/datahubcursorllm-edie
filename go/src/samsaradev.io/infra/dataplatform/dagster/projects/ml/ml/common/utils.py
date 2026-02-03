# ML project utilities - includes job monitoring hooks for Datadog
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, NamedTuple, Sequence, Tuple, Union

import boto3
import botocore.exceptions
import datadog
from dagster import HookContext, failure_hook, success_hook


@dataclass
class Owner:
    team: str


DATAENGINEERING = Owner("DataEngineering")
DATAPLATFORM = Owner("DataPlatform")
DATAPRODUCTS = Owner("DataProducts")
DATATOOLS = Owner("DataTools")


class AWSRegions(Enum):
    US_WEST_2 = "us-west-2"
    EU_WEST_1 = "eu-west-1"
    CA_CENTRAL_1 = "ca-central-1"


class WarehouseWriteMode(Enum):
    OVERWRITE = "overwrite"
    MERGE = "merge"


class DatadogMetric(NamedTuple):
    """Represents a single Datadog metric object."""

    metric: str
    points: List[Tuple[float, Union[int, float]]]
    type: str
    tags: List[str]

    def to_dict(self):
        return {
            "metric": self.metric,
            "points": self.points,
            "type": self.type,
            "tags": self.tags,
        }


def get_ssm_token(ssm_parameter: str) -> str:
    """Get a token from AWS SSM Parameter Store."""
    region_name = "us-west-2"

    try:
        region_name = get_region()
        if not region_name:
            region_name = "us-west-2"
    except botocore.exceptions.NoRegionError:
        print("Unable to retrieve region, defaulting to us-west-2")
        region_name = "us-west-2"

    ssm_client = boto3.client("ssm", region_name=region_name)
    return ssm_client.get_parameter(Name=ssm_parameter, WithDecryption=True)[
        "Parameter"
    ]["Value"]


def get_datadog_token_and_app():
    """Get Datadog API and APP keys from environment or SSM."""
    try:
        if "DATADOG_API_KEY" in os.environ:
            api_key = os.getenv("DATADOG_API_KEY")
        else:
            api_key = get_ssm_token("DATADOG_API_KEY")
    except Exception as e:
        print(f"Unable to retrieve Datadog API key: {e}")
        api_key = "API_KEY_NOT_FOUND"

    try:
        if "DATADOG_APP_KEY" in os.environ:
            app_key = os.getenv("DATADOG_APP_KEY")
        else:
            app_key = get_ssm_token("DATADOG_APP_KEY")
    except Exception as e:
        print(f"Unable to retrieve Datadog APP key: {e}")
        app_key = "APP_KEY_NOT_FOUND"

    return api_key, app_key


def initialize_datadog() -> None:
    """Initialize Datadog API with keys from SSM or environment."""
    api_key, app_key = get_datadog_token_and_app()
    datadog.initialize(api_key=api_key, app_key=app_key)


def send_datadog_metrics(context, metrics: List[DatadogMetric]) -> None:
    """Send metrics to Datadog."""
    metric_dicts = [m.to_dict() for m in metrics]
    try:
        datadog.api.Metric.send(metrics=metric_dicts)
        context.log.info("Successfully logged metrics to Datadog")
    except Exception as e:
        context.log.info(f"Error sending metrics to Datadog: {e}")


def _get_run_type(context: HookContext) -> str:
    """Determine the run type from Dagster run tags."""
    backfill_key = context._step_execution_context.run_tags.get("dagster/backfill")
    automaterialize_key = context._step_execution_context.run_tags.get(
        "dagster/asset_evaluation_id"
    )
    schedule_key = context._step_execution_context.run_tags.get("dagster/schedule_name")

    if backfill_key:
        return "backfill"
    elif schedule_key:
        return "schedule"
    elif automaterialize_key:
        return "automaterialization"
    else:
        return "unclassified"


def _get_job_type(context: HookContext) -> str:
    """Determine the job type from op tags."""
    compute_kind = context.op.tags.get("dagster/compute_kind")
    kind = context.op.tags.get("kind")

    if compute_kind == "sql":
        return "asset_materialization"
    elif kind == "databricks":
        return "databricks"
    else:
        return "ml"


def _get_owner(context: HookContext) -> str:
    """Get owner from op tags."""
    return context.op.tags.get("owner", "DataEngineering")


def _get_database(context: HookContext) -> str:
    """Get database from op tags."""
    return context.op.tags.get("database", "")


def _get_table(context: HookContext) -> str:
    """Get table from op tags."""
    return context.op.tags.get("table", "")


def gen_step_completion_hook(context: HookContext, status: str) -> None:
    """Generate metrics for job completion and send to Datadog.

    Emits:
    - dagster.job.run: Count metric with status (success/failure)
    - dagster.databricks.job.run.*: Duration metrics for Databricks runs
    """
    run_type = _get_run_type(context)
    job_type = _get_job_type(context)
    region = os.getenv("AWS_REGION", "us-west-2")
    owner = _get_owner(context)
    database = _get_database(context)
    table = _get_table(context)

    tags = [
        f"job_type:{job_type}",
        f"run_type:{run_type}",
        f"status:{status}",
        f"name:{context.op.name}",
        f"region:{region}",
        f"project:ml",
        f"owner:{owner}",
    ]

    # Add database and table tags if they exist in op tags
    if database:
        tags.append(f"database:{database}")
    if table:
        tags.append(f"table:{table}")

    now = datetime.utcnow().timestamp()
    metrics = []

    # Main job run metric
    dagster_run_tags = tags + ["partition:unpartitioned"]
    metrics.append(
        DatadogMetric(
            metric="dagster.job.run",
            type="count",
            points=[(now, 1.0)],
            tags=dagster_run_tags,
        )
    )

    # Try to get Databricks run metrics
    databricks_run_id = getattr(
        context._step_execution_context.step_launcher, "_databricks_run_id", ""
    )

    databricks_run = {}
    try:
        if databricks_run_id:
            databricks_run = context._step_execution_context.step_launcher.databricks_runner.client.workspace_client.jobs.get_run(
                databricks_run_id
            ).as_dict()
    except Exception:
        pass

    # Emit Databricks-specific metrics if available
    if databricks_run:
        run_duration = databricks_run.get("end_time", 0) - databricks_run.get(
            "start_time", 0
        )
        databricks_run_tags = tags + ["partition:unpartitioned"]

        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.duration",
                type="gauge",
                points=[(now, run_duration)],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.setup_duration",
                type="gauge",
                points=[(now, databricks_run.get("setup_duration", 0))],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.cleanup_duration",
                type="gauge",
                points=[(now, databricks_run.get("cleanup_duration", 0))],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.execution_duration",
                type="gauge",
                points=[(now, databricks_run.get("execution_duration", 0))],
                tags=databricks_run_tags,
            )
        )

    # Send to Datadog only in prod
    if get_run_env() == "prod":
        initialize_datadog()
        send_datadog_metrics(context, metrics=metrics)
    else:
        context.log.info(
            f"[{get_run_env()}] Would send {status} metrics: {dagster_run_tags}"
        )
        if databricks_run:
            context.log.info(f"Databricks run duration: {run_duration}ms")


@failure_hook
def datamodel_job_failure_hook(context: HookContext):
    """Hook triggered on job failure - sends metrics to Datadog."""
    gen_step_completion_hook(context, "failure")


@success_hook
def datamodel_job_success_hook(context: HookContext):
    """Hook triggered on job success - sends metrics to Datadog."""
    gen_step_completion_hook(context, "success")


def get_aws_account_id(region):
    """Get AWS account ID for the given region."""
    if region == AWSRegions.US_WEST_2.value:
        return "492164655156"
    elif region == AWSRegions.EU_WEST_1.value:
        return "353964698255"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        return "211125295193"
    return "492164655156"  # default to us-west-2


def get_databricks_workspace_id(region: str) -> str:
    """Get Databricks workspace ID for the given region."""
    if region == AWSRegions.US_WEST_2.value:
        return "5924096274798303"
    elif region == AWSRegions.EU_WEST_1.value:
        return "6992178240159315"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        return "1976292512529253"
    return "5924096274798303"  # default to us-west-2


def get_databricks_workspace_prefix(region: str):
    """Get Databricks workspace prefix for S3 paths."""
    if region == AWSRegions.US_WEST_2.value:
        key_prefix = "oregon-prod"
    elif region == AWSRegions.EU_WEST_1.value:
        key_prefix = "ireland-prod"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        key_prefix = "canada-prod"
    else:
        key_prefix = "oregon-prod"
    workspace_id = get_databricks_workspace_id(region)
    return f"{key_prefix}/{workspace_id}"


def get_databricks_workspace_bucket(region: str) -> str:
    """Get Databricks workspace S3 bucket name."""
    bucket_prefix = get_region_bucket_prefix(region)
    return f"{bucket_prefix}databricks-dev-{region}-root"


def get_region() -> str:
    """Get EC2 region from AWS instance metadata.

    Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    """
    import boto3

    session = boto3.session.Session()
    return session.region_name


def get_dbx_oauth_credentials(region: str) -> dict:
    """Get Databricks OAuth credentials from AWS SSM Parameter Store.

    Args:
        region: Databricks region to get credentials for (e.g., 'us-west-2', 'eu-west-1', 'ca-central-1')

    Returns:
        Dictionary with 'client_id' and 'client_secret'

    Note:
        SSM parameters are stored in the local/instance region (get_region()),
        but they contain credentials for all Databricks regions.
    """
    import boto3

    # SSM client uses local region where parameters are stored
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
            f"region must be one of {[r.value for r in AWSRegions]}. got {region}"
        )

    client_id = ssm_client.get_parameter(Name=clientid_key, WithDecryption=True)[
        "Parameter"
    ]["Value"]
    client_secret = ssm_client.get_parameter(
        Name=client_secret_key, WithDecryption=True
    )["Parameter"]["Value"]

    return {"client_id": client_id, "client_secret": client_secret}


def get_dbx_token(region):
    """Get Databricks token from environment variable based on region.

    Returns region-specific token:
    - us-west-2: DATABRICKS_TOKEN
    - eu-west-1: DATABRICKS_EU_TOKEN
    - ca-central-1: DATABRICKS_CA_TOKEN
    """
    if region == AWSRegions.CA_CENTRAL_1.value:
        return os.getenv("DATABRICKS_CA_TOKEN")
    elif region == AWSRegions.EU_WEST_1.value:
        return os.getenv("DATABRICKS_EU_TOKEN")
    elif region == AWSRegions.US_WEST_2.value:
        return os.getenv("DATABRICKS_TOKEN")
    else:
        # Default to US token for unknown regions
        return os.getenv("DATABRICKS_TOKEN")


def get_region_bucket_prefix(region: str) -> str:
    """Get S3 bucket prefix for the given region."""
    if region == AWSRegions.US_WEST_2.value:
        bucket_prefix = "samsara-"
    elif region == AWSRegions.EU_WEST_1.value:
        bucket_prefix = "samsara-eu-"
    elif region == AWSRegions.CA_CENTRAL_1.value:
        bucket_prefix = "samsara-ca-"
    else:
        bucket_prefix = "samsara-"
    return bucket_prefix


def get_region_from_databricks_host(host: str) -> str:
    """Extract AWS region from Databricks host URL.

    Args:
        host: Databricks host URL like 'https://samsara-dev-eu-west-1.cloud.databricks.com/'

    Returns:
        AWS region string like 'eu-west-1', 'us-west-2', or 'ca-central-1'
    """
    if f"samsara-dev-{AWSRegions.US_WEST_2.value}" in host:
        return AWSRegions.US_WEST_2.value
    elif f"samsara-dev-{AWSRegions.EU_WEST_1.value}" in host:
        return AWSRegions.EU_WEST_1.value
    elif f"samsara-dev-{AWSRegions.CA_CENTRAL_1.value}" in host:
        return AWSRegions.CA_CENTRAL_1.value
    else:
        # Default to us-west-2 for unknown/legacy hosts
        return AWSRegions.US_WEST_2.value


def get_run_env():
    """Get the current runtime environment (dev, staging, or prod).

    Returns 'dev' by default for local development, or reads from ENV environment variable.
    """
    return os.getenv("ENV", "dev")


def log_query_data(context, sql_query):
    """Stub function"""
    pass


def split_to_groups(items, size: int):
    """Split a sequence of items into groups of specified size.

    Args:
        items: Sequence of items to split
        size: Size of each group

    Returns:
        List of lists, where each inner list has at most `size` items
    """
    batches = []
    for i in range(0, len(items), size):
        batches.append(items[i : i + size])
    return batches
