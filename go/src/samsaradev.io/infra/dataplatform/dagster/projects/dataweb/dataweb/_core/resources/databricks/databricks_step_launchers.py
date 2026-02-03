import gzip
import io
import json
import os
import pickle
import random
import re
import sys
import tempfile
import time
import zipfile
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Union,
    cast,
)

import boto3
from dagster import HookDefinition, InitResourceContext, RetryRequested
from dagster import _check as check
from dagster._config.pythonic_config.resource import ConfigurableResource
from dagster._core.definitions.step_launcher import StepRunRef
from dagster._core.events import DagsterEvent
from dagster._core.events.log import EventLogEntry
from dagster._core.execution.context.system import StepExecutionContext
from dagster._core.execution.plan.external_step import (
    PICKLED_EVENTS_FILE_NAME,
    PICKLED_STEP_RUN_REF_FILE_NAME,
)
from dagster._core.log_manager import DagsterLogManager
from dagster._serdes import deserialize_value
from dagster._utils.backoff import backoff
from dagster_databricks import DatabricksPySparkStepLauncher
from dagster_databricks.databricks_pyspark_step_launcher import (
    CODE_ZIP_NAME,
    PICKLED_CONFIG_FILE_NAME,
)
from dagster_pyspark.utils import DEFAULT_EXCLUDE
from databricks.sdk.core import DatabricksError

from ....userpkgs.constants import (
    GENERAL_PURPOSE_INSTANCE_POOL_KEY,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
)
from ...hooks import step_failure_hook, step_success_hook
from ...utils import (
    get_aws_account_id,
    get_databricks_workspace_bucket,
    get_databricks_workspace_prefix,
    get_datadog_token_and_app,
    get_dbx_oauth_credentials,
    get_dbx_token,
    get_region_bucket_prefix,
    get_region_from_databricks_host,
    get_run_env,
)
from . import constants as c


@dataclass
class S3StorageInfo:
    destination: str = field(default="")
    region: str = field(default="")
    endpoint: str = field(default="")
    enable_encryption: bool = field(default=True)
    encryption_type: str = field(default="sse-s3")
    canned_acl: str = field(default="bucket-owner-full-control")


@dataclass
class DbfsStorageInfo:
    destination: str = field(default=None)


@dataclass
class ClusterAutoScale:
    min_workers: int = field(default=1)
    max_workers: int = field(default=c.DEFAULT_MAX_WORKERS)


@dataclass
class ClusterSize:
    num_workers: int = field(default=2)
    autoscale: ClusterAutoScale = field(default_factory=ClusterAutoScale)


@dataclass
class ClusterNodeTypes:
    node_type_id: str = field(default=c.DEFAULT_WORKER_INSTANCE_TYPE)
    driver_node_type_id: str = field(default=c.DEFAULT_DRIVER_INSTANCE_TYPE)


@dataclass
class ClusterNodes:
    node_types: ClusterNodeTypes = field(default_factory=ClusterNodeTypes)
    driver_instance_pool_id: str = field(default=None)
    instance_pool_id: str = field(default=None)


@dataclass
class ClusterLogConfs:
    s3: S3StorageInfo = field(default_factory=S3StorageInfo)
    dbfs: DbfsStorageInfo = field(default_factory=DbfsStorageInfo)


@dataclass
class ClusterAwsAttributes:
    first_on_demand: int = field(default=1)
    availability: str = field(default="SPOT_WITH_FALLBACK")
    zone_id: str = field(default="auto")
    instance_profile_arn: str = field(default="")
    ebs_volume_type: str = field(default="GENERAL_PURPOSE_SSD")
    ebs_volume_count: int = field(default=0)


@dataclass
class NewCluster:
    spark_version: str = field(default=c.DEFAULT_SPARK_VERSION)
    size: ClusterSize = field(default_factory=ClusterSize)
    nodes: ClusterNodes = field(default_factory=ClusterNodes)
    aws_attributes: ClusterAwsAttributes = field(default_factory=ClusterAwsAttributes)
    cluster_log_conf: ClusterLogConfs = field(default_factory=ClusterLogConfs)
    ssh_public_keys: List[str] = field(default_factory=list)
    enable_elastic_disk: bool = field(default=True)
    spark_conf: Dict[str, str] = field(default_factory=dict)
    custom_tags: List[Dict[str, str]] = field(default_factory=dict)
    init_scripts: List[Dict[str, Any]] = field(default_factory=dict)
    spark_env_vars: Dict[str, str] = field(default_factory=dict)


@dataclass
class Cluster:
    new: NewCluster = field(default_factory=NewCluster)
    existing: str = field(default="")


@dataclass
class RunConfig:
    run_name: str = field(default="Dagster Run")
    cluster: Cluster = field(default_factory=Cluster)
    libraries: List[Dict[str, str]] = field(default_factory=list)
    install_default_libraries: bool = field(default=False)
    timeout_seconds: int = field(default=86400)
    idempotency_token: str = field(default=None)


@dataclass
class DatabricksToken:
    env: str = field(default="DATABRICKS_TOKEN")


@dataclass
class OauthCredentials:
    client_id: str = field(default="")
    client_secret: str = field(default="")


@dataclass
class StepConfig:
    run_config: RunConfig = field(
        default_factory=lambda: RunConfig(
            libraries=c.DEFAULT_CLUSTER_LIBRARIES,
            timeout_seconds=c.DEFAULT_TIMEOUT_SECONDS,
        ),
    )
    databricks_host: str = field(default="")
    databricks_token: Union[str, DatabricksToken, None] = field(
        default_factory=(DatabricksToken)
    )
    oauth_credentials: Union[OauthCredentials, None] = field(default=None)
    env_variables: Dict[str, str] = field(default_factory=dict)
    secrets_to_env_variables: List[Dict[str, str]] = field(
        default_factory=list
    )  # example: [{"key": "", "name":"", "scope":""}]
    local_dagster_job_package_path: str = field(
        default=""
    )  # this must point to root project directory
    staging_prefix: str = field(default="/dagster_steps")
    wait_for_logs: bool = field(default=False)
    max_completion_wait_time_seconds: int = field(default=86400)
    poll_interval_sec: float = field(default=30.0)
    verbose_logs: bool = field(default=False)
    add_dagster_env_variables: bool = field(default=False)
    permissions: Dict[str, str] = field(default_factory=dict)

    def to_dict(self):
        config = asdict(self)
        # Remove properties that conflict in the Databricks Jobs API
        if self.run_config.idempotency_token is None:
            del config["run_config"]["idempotency_token"]

        if self.run_config.cluster.existing:
            del config["run_config"]["cluster"]["new"]
            return config
        else:
            del config["run_config"]["cluster"]["existing"]

        if self.run_config.cluster.new.size.autoscale is not None:
            del config["run_config"]["cluster"]["new"]["size"]["num_workers"]
        else:
            del config["run_config"]["cluster"]["new"]["size"]["autoscale"]

        if self.run_config.cluster.new.nodes.instance_pool_id:
            del config["run_config"]["cluster"]["new"]["nodes"]["node_types"]
            del config["run_config"]["cluster"]["new"]["enable_elastic_disk"]
            del config["run_config"]["cluster"]["new"]["aws_attributes"][
                "ebs_volume_type"
            ]
            del config["run_config"]["cluster"]["new"]["aws_attributes"][
                "ebs_volume_count"
            ]
            del config["run_config"]["cluster"]["new"]["aws_attributes"]["zone_id"]
            del config["run_config"]["cluster"]["new"]["aws_attributes"]["availability"]
            del config["run_config"]["cluster"]["new"]["aws_attributes"][
                "first_on_demand"
            ]
        else:
            del config["run_config"]["cluster"]["new"]["nodes"]["instance_pool_id"]
            del config["run_config"]["cluster"]["new"]["nodes"][
                "driver_instance_pool_id"
            ]

        if self.run_config.cluster.new.cluster_log_conf.s3 is not None:
            del config["run_config"]["cluster"]["new"]["cluster_log_conf"]["dbfs"]
        else:
            del config["run_config"]["cluster"]["new"]["cluster_log_conf"]["s3"]

        return config


def build_step_config(
    region: str,
    spark_version: Optional[str] = "",
    instance_pool_type: Optional[str] = "",
    driver_instance_type: Optional[str] = "",
    worker_instance_type: Optional[str] = "",
    max_workers: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    spark_conf_overrides: Optional[Dict[str, str]] = None,
) -> StepConfig:

    prefix = get_region_bucket_prefix(region)
    account_id = get_aws_account_id(region)
    env = get_run_env()
    write_db = os.getenv("WRITE_DB", "datamodel_dev")
    mode = os.getenv("MODE", "")

    datadog_api_key = ""
    datadog_app_key = ""
    if env == "prod":
        datadog_api_key, datadog_app_key = get_datadog_token_and_app()

    # Build Cluster settings for run
    new_cluster = NewCluster(
        spark_version=spark_version or c.DEFAULT_SPARK_VERSION,
        aws_attributes=ClusterAwsAttributes(
            instance_profile_arn=f"arn:aws:iam::{account_id}:instance-profile/dataplatform-dagster-prod-cluster"
        ),
        custom_tags=c.DEFAULT_CLUSTER_TAGS,
    )

    if max_workers:
        new_cluster.size.autoscale.max_workers = max_workers

    cluster_log_conf = ClusterLogConfs(
        s3=S3StorageInfo(
            destination=f"s3://{prefix}databricks-cluster-logs/dataplatform/dagster",
            region=f"{region}",
            enable_encryption=True,
            encryption_type="sse-s3",
            canned_acl="bucket-owner-full-control",
        )
    )
    new_cluster.cluster_log_conf = cluster_log_conf

    spark_conf = c.DEFAULT_SPARK_CONF
    if spark_conf_overrides:
        spark_conf.update(spark_conf_overrides)
    new_cluster.spark_conf = spark_conf

    env_vars = {
        "GOOGLE_APPLICATION_CREDENTIALS": "/databricks/samsara-data-5142c7cd3ba2.json",
        "GOOGLE_CLOUD_PROJECT": "samsara-data",
        "AWS_DEFAULT_REGION": f"{region}",
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
        "region": f"{region}",
        "ENV": env,
    }

    if worker_instance_type:
        new_cluster.nodes = ClusterNodes(
            instance_pool_id=None,
            driver_instance_pool_id=None,
            node_types=ClusterNodeTypes(
                driver_node_type_id=driver_instance_type,
                node_type_id=worker_instance_type,
            ),
        )
    else:
        if instance_pool_type in ("", GENERAL_PURPOSE_INSTANCE_POOL_KEY):
            instance_pools = random.choice(c.GENERAL_PURPOSE_INSTANCE_POOLS[region])
        elif instance_pool_type == MEMORY_OPTIMIZED_INSTANCE_POOL_KEY:
            instance_pools = random.choice(c.MEMORY_OPTIMIZED_INSTANCE_POOLS[region])

        new_cluster.nodes = ClusterNodes(
            node_types=None,
            driver_instance_pool_id=instance_pools["driver_instance_pool"]["pool_id"],
            instance_pool_id=instance_pools["worker_instance_pool"]["pool_id"],
        )
        env_vars["INSTANCE_POOL_TYPE"] = instance_pool_type

    new_cluster.spark_env_vars = env_vars

    databricks_cluster_id = None
    if env == "dev" and mode != "LOCAL_PROD_CLUSTER_RUN":

        if region == c.AWSRegion.US_WEST_2:
            databricks_cluster_id = (
                os.getenv("DATABRICKS_CLUSTER_ID") or c.DagsterDevClusterId.US_WEST_2
            )
        elif region == c.AWSRegion.EU_WEST_1:
            databricks_cluster_id = (
                os.getenv("DATABRICKS_EU_CLUSTER_ID") or c.DagsterDevClusterId.EU_WEST_1
            )
        elif region == c.AWSRegion.CA_CENTRAL_1:
            databricks_cluster_id = (
                os.getenv("DATABRICKS_CA_CLUSTER_ID")
                or c.DagsterDevClusterId.CA_CENTRAL_1
            )

    if databricks_cluster_id:
        cluster = Cluster(new=None, existing=databricks_cluster_id)
        databricks_token = get_dbx_token(region=region)
        oauth_credentials = None
    else:
        cluster = Cluster(new=new_cluster, existing=None)
        databricks_token = None  # Use oauth authorization for runs using job clusters
        oauth_credentials = OauthCredentials(**get_dbx_oauth_credentials(region=region))

    libraries = c.DEFAULT_CLUSTER_LIBRARIES + c.REGION_SPECIFIC_LIBRARIES.get(
        region, []
    )

    run_config = RunConfig(
        cluster=cluster,
        libraries=libraries,
        timeout_seconds=timeout_seconds or c.DEFAULT_TIMEOUT_SECONDS,
    )

    return StepConfig(
        databricks_host=f"https://samsara-dev-{region}.cloud.databricks.com/",
        databricks_token=databricks_token,
        oauth_credentials=oauth_credentials,
        run_config=run_config,
        wait_for_logs=True,
        env_variables={
            "WRITE_DB": write_db,
            "MODE": mode,
            "DATADOG_API_KEY": datadog_api_key,
            "DATADOG_APP_KEY": datadog_app_key,
        },
        local_dagster_job_package_path=f"{'/' if (env == 'prod' and mode not in ['LOCAL_PROD_CLUSTER_RUN', 'LOCAL_DATAHUB_RUN']) else ''}"
        + f"{Path(__file__).parent.parent.parent.parent.parent}",  # this must point to root project directory
        permissions={
            "job_permissions": {
                "CAN_MANAGE_RUN": [
                    {"group_name": "data-engineering-group"},
                    {"group_name": "data-analytics-group"},
                    {"group_name": "firmware-vdp-group"},
                    {"group_name": "sustainability-group"},
                ]
            }
        },
    )


class SamsaraDatabricksStepLauncher(DatabricksPySparkStepLauncher):
    def __init__(self, config: StepConfig, hooks: Optional[Set[HookDefinition]] = None):
        super().__init__(**config)
        self._hooks = hooks or set()

    def _with_dataplatform_tags(self, context: StepExecutionContext):
        if not self.run_config["cluster"].get("new"):
            return

        # Use asset spec to get owner
        node_handle = context.step.node_handle
        asset_def = context.job_def.asset_layer.assets_defs_by_node_handle[node_handle]
        owners = asset_def.owners_by_key[asset_def.key]
        team_name = owners[0].split(":")[1].lower()  # Team tags are lowercase

        node_handle_parts = node_handle.name.split("__")
        service_tag = (
            f"{node_handle_parts[-2]}-{node_handle_parts[-1]}"
            if len(node_handle_parts) > 1
            else f"{node_handle_parts[-1]}"
        )
        tags = {
            tag["key"]: tag["value"]
            for tag in self.run_config["cluster"]["new"].get("custom_tags", {})
        }
        tags.update(
            {
                "samsara:service": f"databricksjob-dagster-{service_tag}",
                "samsara:product-group": "aianddata",
                "samsara:rnd-allocation": "1",
                "samsara:team": team_name,
            }
        )

        # Update tags for runs using instance pools
        if self.run_config["cluster"]["new"]["nodes"].get("instance_pool_id"):
            instance_pool_id = self.run_config["cluster"]["new"]["nodes"].get(
                "instance_pool_id"
            )
            driver_pool_id = self.run_config["cluster"]["new"]["nodes"].get(
                "driver_instance_pool_id"
            )
            tags.update(
                {
                    "samsara:pooled-job:service": f"databricksjob-dagster-{service_tag}",
                    "samsara:pooled-job:product-group": "aianddata",
                    "samsara:pooled-job:rnd-allocation": "1",
                    "samsara:pooled-job:team": team_name,
                    "samsara:pooled-job:driver-pool-id": driver_pool_id,
                    "samsara:pooled-job:pool-id": instance_pool_id,
                }
            )
            del tags["samsara:service"]
            del tags["samsara:product-group"]
            del tags["samsara:rnd-allocation"]
            del tags["samsara:team"]

        custom_tags = [{"key": k, "value": v} for k, v in tags.items()]
        self.run_config["cluster"]["new"]["custom_tags"] = custom_tags

    def _with_hooks(self, context: StepExecutionContext) -> None:
        self._hooks.add(step_success_hook)
        self._hooks.add(step_failure_hook)

        for idx, _ in enumerate(context.job_def.nodes):
            context.job_def.nodes[idx]._hook_defs = self._hooks

    def _with_libraries(self, context: StepExecutionContext) -> None:
        pypi_libraries = context.op.tags.get("pypi_libraries", "[]")
        for lib in json.loads(pypi_libraries):
            self.run_config["libraries"].append({"pypi": {"package": lib}})

    def _with_step_run_name(self, context: StepExecutionContext):
        self.run_config[
            "run_name"
        ] = f"{context.step.node_handle.name} ({context.plan_data.dagster_run.run_id.split('-')[0]})"

    def _handle_pool_assignment(self, context: StepExecutionContext):
        region = self.run_config["cluster"]["new"]["spark_env_vars"].get(
            "AWS_DEFAULT_REGION"
        )
        instance_pool_type = self.run_config["cluster"]["new"]["spark_env_vars"].get(
            "INSTANCE_POOL_TYPE", ""
        )

        if self.run_config["cluster"]["new"]["nodes"].get("instance_pool_id"):

            if instance_pool_type == GENERAL_PURPOSE_INSTANCE_POOL_KEY:
                pools = random.choice(c.GENERAL_PURPOSE_INSTANCE_POOLS[region])
            elif instance_pool_type == MEMORY_OPTIMIZED_INSTANCE_POOL_KEY:
                pools = random.choice(c.MEMORY_OPTIMIZED_INSTANCE_POOLS[region])

            self.run_config["cluster"]["new"]["nodes"]["instance_pool_id"] = pools[
                "worker_instance_pool"
            ]["pool_id"]
            self.run_config["cluster"]["new"]["nodes"][
                "driver_instance_pool_id"
            ] = pools["driver_instance_pool"]["pool_id"]

    def _handle_exception(self, exception: Exception) -> None:
        # Attempt to retry failed runs from known transient errors
        if c.AWS_INSUFFICIENT_INSTANCE_CAPACITY_EXCEPTION in str(exception):
            raise RetryRequested(
                max_retries=5, seconds_to_wait=1200 + (60 * random.randint(-5, 20))
            ) from exception
        if c.CLOUD_PROVIDER_LAUNCH_EXCEPTION in str(exception):
            raise RetryRequested(
                max_retries=3, seconds_to_wait=1200 + (60 * random.randint(-5, 20))
            ) from exception
        if c.LIBRARY_EXCEPTION in str(exception):
            raise RetryRequested(
                max_retries=3, seconds_to_wait=1200 + (60 * random.randint(-5, 20))
            ) from exception
        if c.SHUFFLE_EXCEPTION in str(exception):
            raise RetryRequested(
                max_retries=3, seconds_to_wait=1200 + (60 * random.randint(-5, 20))
            ) from exception
        if c.FAILED_READ_EXCEPTION in str(exception):
            raise RetryRequested(
                max_retries=3, seconds_to_wait=1200 + (60 * random.randint(-5, 20))
            ) from exception
        return

        #### DatabricksPySparkStepLauncher method overrides below

    def _log_logs_from_cluster(self, log: DagsterLogManager, run_id: int) -> None:
        self._databricks_run_id = run_id
        super()._log_logs_from_cluster(log, run_id)

    def get_step_events(
        self, run_id: str, step_key: str, retry_number: int
    ) -> Sequence[EventLogEntry]:
        path = self._dbfs_path(
            run_id, step_key, f"{retry_number}_{PICKLED_EVENTS_FILE_NAME}"
        )

        def _get_step_records() -> Sequence[EventLogEntry]:
            time.sleep(5)

            is_local_run = bool(self.run_config["cluster"].get("existing"))
            if is_local_run:
                workspace_client = self.databricks_runner.client.workspace_client
                serialized_records = workspace_client.files.download(
                    path
                ).contents.read()
            else:
                serialized_records = self.databricks_runner.client.read_file(path)

            if not serialized_records:
                return []
            return cast(
                Sequence[EventLogEntry],
                deserialize_value(pickle.loads(gzip.decompress(serialized_records))),
            )

        try:
            # reading from dbfs while it writes can be flaky
            # allow for retry if we get malformed data
            return backoff(
                fn=_get_step_records,
                retry_on=(pickle.UnpicklingError, OSError, zlib.error, EOFError),
                max_retries=4,
            )
        # if you poll before the Databricks process has had a chance to create the file,
        # we expect to get this error
        except DatabricksError as e:
            if e.error_code == "RESOURCE_DOES_NOT_EXIST":
                return []
            raise

    def log_compute_logs(
        self, log: DagsterLogManager, run_id: str, step_key: str
    ) -> None:
        is_local_run = bool(self.run_config["cluster"].get("existing"))
        try:
            if is_local_run:
                stdout = (
                    self.databricks_runner.client.workspace_client.files.download(
                        self._dbfs_path(run_id, step_key, "stdout")
                    )
                    .contents.read()
                    .decode()
                )
            else:
                stdout = self.databricks_runner.client.read_file(
                    self._dbfs_path(run_id, step_key, "stdout")
                ).decode()
            log.info(f"Captured stdout for step {step_key}:")
            log.info(stdout)
            sys.stdout.write(stdout)
        except Exception as e:
            log.error(
                f"Encountered exception {e} when attempting to load stdout logs for step"
                f" {step_key}. Check the databricks console for more info."
            )
        try:
            if is_local_run:
                stderr = (
                    self.databricks_runner.client.workspace_client.files.download(
                        self._dbfs_path(run_id, step_key, "stderr")
                    )
                    .contents.read()
                    .decode()
                )
            else:
                stderr = self.databricks_runner.client.read_file(
                    self._dbfs_path(run_id, step_key, "stderr")
                ).decode()
            log.info(f"Captured stderr for step {step_key}:")
            log.info(stderr)
            sys.stderr.write(stderr)
        except Exception as e:
            log.error(
                f"Encountered exception {e} when attempting to load stderr logs for step"
                f" {step_key}. Check the databricks console for more info."
            )

    def _get_databricks_task(self, run_id: str, step_key: str) -> Mapping[str, Any]:
        """Construct the 'task' parameter to  be submitted to the Databricks API.

        This will create a 'spark_python_task' dict where `python_file` is a path on DBFS
        pointing to the 'databricks_step_main.py' file, and `parameters` is an array with a single
        element, a path on DBFS pointing to the picked `step_run_ref` data.

        See https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobssparkpythontask.
        """

        region = get_region_from_databricks_host(self.databricks_host)
        bucket_prefix = get_region_bucket_prefix(region)
        python_file = self._dbfs_path(run_id, step_key, self._main_file_name())

        is_local_run = bool(self.run_config["cluster"].get("existing"))
        if is_local_run:
            python_file = python_file.replace("/Volumes/s3/", f"s3://{bucket_prefix}")

        parameters = [
            self._internal_dbfs_path(run_id, step_key, PICKLED_STEP_RUN_REF_FILE_NAME),
            self._internal_dbfs_path(run_id, step_key, PICKLED_CONFIG_FILE_NAME),
            self._internal_dbfs_path(run_id, step_key, CODE_ZIP_NAME),
        ]
        return {
            "spark_python_task": {"python_file": python_file, "parameters": parameters}
        }

    def _dbfs_path(self, run_id: str, step_key: str, filename: str) -> str:
        path = "/".join(
            [
                self.staging_prefix,
                run_id,
                self._sanitize_step_key(step_key),
                os.path.basename(filename),
            ]
        )

        is_local_run = bool(self.run_config["cluster"].get("existing"))
        if is_local_run:
            return f"{c.DATABRICKS_VOLUMES_PATH}{path}"
        else:
            return f"dbfs://{path}"

    def _internal_dbfs_path(self, run_id: str, step_key: str, filename: str) -> str:
        """Scripts running on Databricks should access Unity Catalog Volumes at /Volumes/."""
        path = "/".join(
            [
                self.staging_prefix,
                run_id,
                self._sanitize_step_key(step_key),
                os.path.basename(filename),
            ]
        )
        is_local_run = bool(self.run_config["cluster"].get("existing"))
        if is_local_run:
            return f"{c.DATABRICKS_VOLUMES_PATH}{path}"
        else:
            return f"/dbfs/{path}"

    # We override this method while still invoking the parent class' implementation
    # in order to perform some pre-/post-execution operations
    def launch_step(self, step_context: StepExecutionContext) -> Iterator[DagsterEvent]:

        self._with_hooks(step_context)
        self._with_libraries(step_context)

        if self.run_config["cluster"].get("existing"):
            yield from super().launch_step(step_context)

        else:
            # pre-execution operations
            self._handle_pool_assignment(step_context)
            self._with_step_run_name(step_context)
            self._with_dataplatform_tags(context=step_context)

            try:
                yield from super().launch_step(step_context)
            except Exception as e:
                step_context.log.info(e)
                self._handle_exception(e)

    def build_pyspark_zip(self, zip_file, path, exclude=DEFAULT_EXCLUDE) -> None:
        """Archives the current path into a file named `zip_file`.
        Note.
            - This method replaces the Dagster implementation to allow follow symbolic
              links when walking through directories.

        Args:
            zip_file (str): The name of the zip file to create.
            path (str): The path to archive.
            exclude (Optional[List[str]]): A list of regular expression patterns to exclude paths from
                the archive. Regular expressions will be matched against the absolute filepath with
                `re.search`.
        """
        check.str_param(zip_file, "zip_file")
        check.str_param(path, "path")

        with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(path, followlinks=True):
                for fname in files:
                    abs_fname = os.path.join(root, fname)
                    real_path = os.path.realpath(abs_fname)

                    # Skip various artifacts
                    if any([re.search(pattern, abs_fname) for pattern in exclude]):
                        continue

                    zf.write(
                        real_path, os.path.relpath(os.path.join(root, fname), path)
                    )

    # We override this method to use a different protocol for uploading the source code of a Dagster step
    # than the released implementation. Here we are directly uploading source code directly to DBFS's mounted
    # S3 location instead of via the Databricks' DBFS API.
    def _upload_artifacts(
        self,
        log: DagsterLogManager,
        step_run_ref: StepRunRef,
        run_id: str,
        step_key: str,
    ) -> None:
        """Upload the step run ref and pyspark code to DBFS's mounted S3 location to run as a job."""

        is_local_run = bool(self.run_config["cluster"].get("existing"))

        region = get_region_from_databricks_host(self.databricks_host)
        workspace_bucket = get_databricks_workspace_bucket(region)
        workspace_prefix = get_databricks_workspace_prefix(region)

        s3 = boto3.client("s3")

        log.info(
            f"Uploading main file to {'Volumes' if is_local_run else 'DBFS S3 Location'}"
        )
        main_local_path = self._main_file_local_path()
        if is_local_run:
            with open(main_local_path, "rb") as infile:
                self.databricks_runner.client.workspace_client.files.upload(
                    self._dbfs_path(run_id, step_key, self._main_file_name()),
                    infile,
                    overwrite=True,
                )
        else:
            s3.upload_file(
                Filename=main_local_path,
                Bucket=workspace_bucket,
                Key=self._dbfs_path(run_id, step_key, self._main_file_name()).replace(
                    "dbfs://", workspace_prefix
                ),
                ExtraArgs={"ACL": "bucket-owner-full-control"},
            )

        log.info(
            f"Uploading dagster job to {'Volumes' if is_local_run else 'DBFS S3 Location'}"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            # Zip and upload package containing dagster job
            zip_local_path = os.path.join(temp_dir, CODE_ZIP_NAME)
            self.build_pyspark_zip(zip_local_path, self.local_dagster_job_package_path)
            if is_local_run:
                with open(zip_local_path, "rb") as infile:
                    self.databricks_runner.client.workspace_client.files.upload(
                        self._dbfs_path(run_id, step_key, CODE_ZIP_NAME),
                        infile,
                        overwrite=True,
                    )
            else:
                s3.upload_file(
                    Filename=zip_local_path,
                    Bucket=workspace_bucket,
                    Key=self._dbfs_path(run_id, step_key, CODE_ZIP_NAME).replace(
                        "dbfs://", workspace_prefix
                    ),
                    ExtraArgs={"ACL": "bucket-owner-full-control"},
                )

        log.info(
            f"Uploading step run ref file to {'Volumes' if is_local_run else 'DBFS S3 Location'}"
        )
        step_pickle_file = io.BytesIO()
        pickle.dump(step_run_ref, step_pickle_file)
        step_pickle_file.seek(0)
        if is_local_run:
            self.databricks_runner.client.workspace_client.files.upload(
                self._dbfs_path(run_id, step_key, PICKLED_STEP_RUN_REF_FILE_NAME),
                step_pickle_file,
                overwrite=True,
            )
        else:
            s3.upload_fileobj(
                Fileobj=step_pickle_file,
                Bucket=workspace_bucket,
                Key=self._dbfs_path(
                    run_id, step_key, PICKLED_STEP_RUN_REF_FILE_NAME
                ).replace("dbfs://", workspace_prefix),
                ExtraArgs={"ACL": "bucket-owner-full-control"},
            )

        databricks_config = self.create_remote_config()
        log.info(
            f"Uploading Databricks configuration to {'Volumes' if is_local_run else 'DBFS S3 Location'}"
        )
        databricks_config_file = io.BytesIO()
        pickle.dump(databricks_config, databricks_config_file)
        databricks_config_file.seek(0)
        if is_local_run:
            self.databricks_runner.client.workspace_client.files.upload(
                self._dbfs_path(run_id, step_key, PICKLED_CONFIG_FILE_NAME),
                databricks_config_file,
                overwrite=True,
            )
        else:
            s3.upload_fileobj(
                Fileobj=databricks_config_file,
                Bucket=workspace_bucket,
                Key=self._dbfs_path(run_id, step_key, PICKLED_CONFIG_FILE_NAME).replace(
                    "dbfs://", workspace_prefix
                ),
                ExtraArgs={"ACL": "bucket-owner-full-control"},
            )

        return


class ConfigurableDatabricksStepLauncher(ConfigurableResource):
    region: str
    driver_instance_type: str = ""
    worker_instance_type: str = ""
    instance_pool_type: str = ""
    max_workers: int = c.DEFAULT_MAX_WORKERS
    spark_version: str = c.DEFAULT_SPARK_VERSION
    timeout_seconds: int = c.DEFAULT_TIMEOUT_SECONDS
    spark_conf_overrides: Dict[str, str] = None

    def create_resource(
        self, context: InitResourceContext
    ) -> SamsaraDatabricksStepLauncher:

        region = context.resource_config["region"]
        instance_pool_type = context.resource_config["instance_pool_type"]
        driver_instance_type = context.resource_config["driver_instance_type"]
        worker_instance_type = context.resource_config["worker_instance_type"]
        max_workers = context.resource_config["max_workers"]
        spark_version = context.resource_config["spark_version"]
        timeout_seconds = context.resource_config["timeout_seconds"]
        spark_conf_overrides = context.resource_config.get("spark_conf_overrides", {})

        # Perform static runtime checks on inputs
        check.invariant(
            bool(worker_instance_type) == bool(driver_instance_type),
            f"Must set both 'worker_instance_type' and 'driver_instance_type'",
        )

        check.invariant(
            bool(instance_pool_type)
            == (not (bool(driver_instance_type) or bool(worker_instance_type))),
            f"Cannot set 'instance_pool_type' along with 'worker_instance_type' and ''driver_instance_type'",
        )

        check.invariant(
            instance_pool_type
            in (
                "",
                GENERAL_PURPOSE_INSTANCE_POOL_KEY,
                MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            ),
            f"Invalid input for 'instance_pool_type'. Must be one of ('', '{GENERAL_PURPOSE_INSTANCE_POOL_KEY}', '{MEMORY_OPTIMIZED_INSTANCE_POOL_KEY}')",
        )

        config = build_step_config(
            region=region,
            instance_pool_type=instance_pool_type,
            driver_instance_type=driver_instance_type,
            worker_instance_type=worker_instance_type,
            max_workers=max_workers,
            spark_version=spark_version,
            timeout_seconds=timeout_seconds,
            spark_conf_overrides=spark_conf_overrides,
        ).to_dict()

        return SamsaraDatabricksStepLauncher(config)
