import gzip
import os
import re
import shlex
import subprocess
from typing import List

import boto3
from dagster import (
    AssetSelection,
    Config,
    Failure,
    In,
    InMemoryIOManager,
    Nothing,
    OpExecutionContext,
    Out,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    build_schedule_from_partitioned_job,
    define_asset_job,
    graph,
    job,
    op,
    sensor,
)
from dagster_aws.s3.sensor import get_s3_keys
from databricks.sdk import WorkspaceClient
from slack_sdk import WebClient

from ..common.datahub_utils import chain_functions, initialize_datadog
from ..common.llm.generate_datahub_schema_op import export_datahub_schemas
from ..common.utils import (
    AWSRegions,
    build_backfill_schedule,
    datamodel_job_failure_hook,
    datamodel_job_success_hook,
    date_range_from_current_date,
    get_aws_sso_login,
    get_datahub_dbt_token,
    get_datahub_env,
    get_datahub_tableau_token,
    get_datahub_token,
    get_dbx_oauth_credentials,
    get_dbx_token,
    get_gms_server,
    get_run_env,
    get_slack_token,
)
from ..ops.biztech_metadata import get_dbt_artifacts_op
from ..ops.metadata import (
    amundsen_metadata,
    create_tags_and_terms,
    delete_stale_entities,
    delete_unused_tables,
    emit_column_descriptions_to_datahub,
    emit_domains_and_views_to_datahub,
    emit_highlighted_queries_to_datahub,
    emit_keys_to_datahub,
    emit_non_dagster_urls_to_datahub,
    emit_partitions_to_datahub,
    emit_table_descriptions_to_datahub,
    emit_table_sizes_to_datahub,
    extract_lineage_to_datahub,
    extract_query_stats_to_datahub,
    scrape_datahub,
    scrape_source_files,
)
from ..ops.metadata_databricks import (
    audit_databricks_applications,
    setup_query_agents_logs_table_with_column_mask,
    upload_cursor_query_agent_files_to_s3,
)
from ..ops.metadata_hourly import (
    emit_users_and_groups_to_datahub,
    extract_dq_checks_to_datahub,
    extract_metadata_to_datahub_hourly_0,
    extract_metadata_to_datahub_hourly_1,
    extract_metadata_to_datahub_hourly_2,
)


@job(hooks={datamodel_job_failure_hook, datamodel_job_success_hook})
def metadata_job_amundsen():
    amundsen_metadata()


@job(hooks={datamodel_job_failure_hook, datamodel_job_success_hook})
def delete_unused_tables_ad_hoc_job():
    delete_unused_tables()


def set_aws_credentials(token_duration=3600):
    credentials = get_aws_sso_login(
        "datahub", "datahub-eks-nodes", duration=token_duration
    )
    os.environ["AWS_ACCESS_KEY_ID"] = credentials.get("AccessKeyId")
    os.environ["AWS_SECRET_KEY"] = credentials.get("SecretAccessKey")
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.get("SecretAccessKey")
    os.environ["AWS_SESSION_TOKEN"] = credentials.get("SessionToken")


if get_run_env() == "prod" and os.getenv("MODE") not in (
    "LOCAL_PROD_CLUSTER_RUN",
    "LOCAL_DATAHUB_RUN",
):
    path_to_recipe = "/datamodel/datamodel/resources/datahub"
else:
    backend_root = os.getenv("BACKEND_ROOT")
    path_to_recipe = (
        f"{backend_root}/go/src/samsaradev.io/infra/dataplatform/dagster/"
        f"projects/datamodel/datamodel/resources/datahub"
    )


def get_datahub_binary_path(use_unity_env=False):
    """
    Get the path to the datahub binary based on the execution environment.

    For local development (MODE=LOCAL_DATAHUB_RUN), uses the UV virtual
    environment. For containerized environments (prod/dev EKS), uses the
    container paths.

    Args:
        use_unity_env: If True, uses the unity-specific environment path
            (for Unity Catalog and DBT ingestion)

    Returns:
        str: Path to the datahub binary
    """
    if os.getenv("MODE") == "LOCAL_DATAHUB_RUN":
        # Local development with UV virtual environments
        backend_root = os.getenv("BACKEND_ROOT", os.path.expanduser("~/co/backend"))
        if use_unity_env:
            # Unity Catalog environment with great_expectations and extras
            venv_path = f"{backend_root}/../venvs/datamodel-unity/bin/datahub"
        else:
            # Core DataHub environment
            venv_path = f"{backend_root}/../venvs/datamodel/bin/datahub"
        return venv_path
    else:
        # Containerized environment (EKS)
        if use_unity_env:
            return "/datahub_env_unity/bin/datahub"
        else:
            return "/datahub_env/bin/datahub"


def get_op(db, ingestion_source="glue", custom_op_name=None, token_duration=3600):
    if custom_op_name == "initialize_structured_properties":
        config_file = (
            f"{path_to_recipe.replace('/resources/datahub', '/')}/"
            f"datahub_ingestion/structured_properties/"
            f"structured_properties.yaml"
        )
        datahub_bin = get_datahub_binary_path(use_unity_env=False)
        shell_cmd = f"{datahub_bin} properties upsert -f " f"{shlex.quote(config_file)}"
        op_name = "initialize_structured_properties"
    elif custom_op_name == "create_ownership_types":
        DATAHUB_CONFIG_PATH = path_to_recipe.replace("/resources/datahub", "/")
        os.environ["DATAHUB_CONFIG_PATH"] = DATAHUB_CONFIG_PATH
        config_file = (
            f"{DATAHUB_CONFIG_PATH}/datahub_ingestion/ownership_types/"
            f"ownership_recipe.yaml"
        )
        datahub_bin = get_datahub_binary_path(use_unity_env=False)
        shell_cmd = f"{datahub_bin} ingest -c {shlex.quote(config_file)}"
        op_name = "create_ownership_types"
    elif custom_op_name:
        config_file = f"{path_to_recipe}/{custom_op_name}_recipe.yaml"
        datahub_bin = get_datahub_binary_path(use_unity_env=False)
        shell_cmd = f"{datahub_bin} ingest -c {shlex.quote(config_file)}"
        op_name = f"sync_{custom_op_name}"
    elif ingestion_source == "unity":
        config_file = f"{path_to_recipe}/{ingestion_source}_recipe_{db}.yaml"
        datahub_bin = get_datahub_binary_path(use_unity_env=True)
        shell_cmd = f"{datahub_bin} ingest -c {shlex.quote(config_file)}"
        op_name = f"sync_{db}_{ingestion_source}_to_datahub"
    elif ingestion_source == "dbt":
        config_file = f"{path_to_recipe}/{ingestion_source}_recipe_{db}.yaml"
        datahub_bin = get_datahub_binary_path(use_unity_env=True)
        shell_cmd = f"{datahub_bin} ingest -c {shlex.quote(config_file)}"
        op_name = f"sync_{db}_{ingestion_source}_to_datahub"
    else:
        config_file = f"{path_to_recipe}/{ingestion_source}_recipe_{db}.yaml"
        datahub_bin = get_datahub_binary_path(use_unity_env=False)
        shell_cmd = f"{datahub_bin} ingest -c {shlex.quote(config_file)}"
        op_name = f"sync_{db}_{ingestion_source}_to_datahub"

    @op(
        name=op_name,
        description=None,
        ins={"start": In(Nothing)},
        out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
        tags=None,
    )
    def _op(context) -> int:

        set_aws_credentials(token_duration=token_duration)

        if ingestion_source == "unity":
            databricks_host = "https://samsara-dev-us-west-2.cloud.databricks.com"
            if get_datahub_env() == "dev":
                access_token = get_dbx_token(AWSRegions.US_WEST_2.value)
            else:
                # Skip setting DATABRICKS_UNITY_TOKEN if already exists to avoid creating multiple temporary tokens
                if not os.getenv("DATABRICKS_UNITY_TOKEN"):
                    oauth_credentials = get_dbx_oauth_credentials(
                        AWSRegions.US_WEST_2.value
                    )
                    dbx_client = WorkspaceClient(
                        host=databricks_host,
                        **oauth_credentials,
                    )
                    # WARNING: Avoid using 'create_obo_token' method.
                    # There is a limit of 600 tokens that can be generated.
                    # This is a temporary workaround until upgrading databricks-sdk to v0.29.0
                    # or above.
                    token = dbx_client.token_management.create_obo_token(
                        application_id=oauth_credentials["client_id"],
                        comment=f"dagster-metadata-ingest-{context.run_id}",
                        lifetime_seconds=3600,
                    )
                    access_token = token.token_value

            # Set environment variables required by Databricks SDK and DataHub Unity Catalog source
            os.environ[
                "DATABRICKS_UNITY_TOKEN"
            ] = access_token  # Used by DataHub Unity recipe
            os.environ["DATABRICKS_TOKEN"] = access_token  # Used by Databricks SDK
            os.environ["DATABRICKS_HOST"] = databricks_host

        elif ingestion_source == "dbt":
            os.environ["DATAHUB_DBT_API_TOKEN"] = get_datahub_dbt_token()
        elif ingestion_source == "tableau":
            os.environ["DATAHUB_TABLEAU_TOKEN"] = get_datahub_tableau_token()

        context.log.info(f"Executing shell command: {shell_cmd}")

        # Log environment variables for Unity source
        if ingestion_source == "unity":
            context.log.info(
                f"DATABRICKS_HOST: {os.environ.get('DATABRICKS_HOST', 'NOT SET')}"
            )
            context.log.info(
                f"DATABRICKS_TOKEN: {'SET' if os.environ.get('DATABRICKS_TOKEN') else 'NOT SET'}"
            )
            context.log.info(
                f"DATABRICKS_UNITY_TOKEN: {'SET' if os.environ.get('DATABRICKS_UNITY_TOKEN') else 'NOT SET'}"
            )

        # Use shlex.split() to handle quoted args and paths with spaces
        command_parts = shlex.split(shell_cmd)
        if not command_parts:
            raise Failure(description=f"Shell command is empty or invalid: {shell_cmd}")

        command = command_parts[0]
        args = command_parts[1:] if len(command_parts) > 1 else []

        try:
            result = subprocess.run(
                [command] + args,
                capture_output=True,
                text=True,
                check=False,
                timeout=3600,  # 1 hour timeout
                env=os.environ.copy(),  # Explicitly pass environment variables
            )

            # Log the output for debugging
            if result.stdout:
                context.log.info(f"Command stdout: {result.stdout}")
            if result.stderr:
                context.log.info(f"Command stderr: {result.stderr}")

            if result.returncode != 0:
                raise Failure(
                    description=(
                        f"Shell command execution failed with return code "
                        f"{result.returncode}. stdout: {result.stdout}, "
                        f"stderr: {result.stderr}"
                    )
                )

            return 0
        except subprocess.TimeoutExpired as e:
            raise Failure(
                description=f"Shell command execution timed out after 1 hour: {str(e)}"
            )
        except Exception as e:
            raise Failure(description=f"Shell command execution failed: {str(e)}")

    return _op


if os.getenv("MODE") == "LOCAL_DATAHUB_RUN":
    os.environ["DATAHUB_GMS_URL"] = "http://localhost:8080"
else:
    os.environ["DATAHUB_GMS_URL"] = get_gms_server()

os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@graph
def metadata_datahub_graph_glue():
    os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
    os.environ["SPARK_VERSION"] = "3.3"

    unity_ops = {}
    for db in [
        "datamodel",
        "datapipelines",
        "datastreams",
        "dynamo",
        "mapping_tables",
        "s3_big_stats",
        "rds",
        "ks",
        "non_domain",
        "non_govramp",
    ]:
        unity_ops[db] = get_op(db, ingestion_source="unity")

    # commented out ad we are now using dbt-core recipe which is in the biztech_metadata.py file
    # dbt_ops = {}
    # dbt_ops["biztech"] = get_op(db="biztech", ingestion_source="dbt")

    tableau_rd_op = get_op(db="rd", ingestion_source="tableau")

    initialize_structured_properties = get_op(
        db="datamodel",
        ingestion_source="structured_properties",
        custom_op_name="initialize_structured_properties",
    )

    create_ownership_types = get_op(
        db="datamodel",
        ingestion_source="ownership_types",
        custom_op_name="create_ownership_types",
    )

    func_list_1 = [
        unity_ops["s3_big_stats"],
        # dbt_ops["biztech"],
        unity_ops["datapipelines"],
        unity_ops["datastreams"],
        unity_ops["mapping_tables"],
        unity_ops["non_domain"],
        unity_ops["dynamo"],
        unity_ops["rds"],
        unity_ops["ks"],
        unity_ops["datamodel"],
        unity_ops["non_govramp"],
        tableau_rd_op,
        delete_stale_entities,
        initialize_structured_properties,
        create_ownership_types,
    ]

    return chain_functions(func_list_1)


@graph
def metadata_datahub_graph_1():
    os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
    os.environ["SPARK_VERSION"] = "3.3"

    func_list = [
        scrape_source_files,
        emit_domains_and_views_to_datahub,
        extract_lineage_to_datahub,
        create_tags_and_terms,
        emit_non_dagster_urls_to_datahub,
        emit_keys_to_datahub,
        emit_highlighted_queries_to_datahub,
        scrape_datahub,
        export_datahub_schemas,
    ]
    return chain_functions(func_list)


@graph
def metadata_datahub_graph_2():
    os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
    os.environ["SPARK_VERSION"] = "3.3"

    func_list = [
        emit_table_descriptions_to_datahub,
        emit_partitions_to_datahub,
        emit_column_descriptions_to_datahub,
        emit_table_sizes_to_datahub,
    ]

    return chain_functions(func_list)


@graph()
def metadata_datahub_graph_6h():
    result1 = metadata_datahub_graph_1()
    result2 = metadata_datahub_graph_2()
    return result1, result2


@job(
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
    resource_defs={
        "in_memory_io_manager": InMemoryIOManager(),
    },
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "1024Mi"},
                }
            },
        }
    },
)
def metadata_job_datahub_glue_ingestion():
    metadata_datahub_graph_glue()


@job(
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "1024Mi"},
                }
            },
        }
    },
)
def metadata_job_datahub_6h():
    metadata_datahub_graph_6h()


@graph
def metadata_graph_databricks():
    os.environ["SPARK_VERSION"] = "3.3"

    func_list = [
        audit_databricks_applications,
        upload_cursor_query_agent_files_to_s3,
        setup_query_agents_logs_table_with_column_mask,
    ]
    return chain_functions(func_list)


@job(
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "1024Mi"},
                }
            },
        }
    },
)
def metadata_job_databricks():
    metadata_graph_databricks()


@graph
def metadata_datahub_graph_stats():
    os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
    os.environ["SPARK_VERSION"] = "3.3"

    # run s3 stats scrape and query stats extractor first and in parallel,
    # since they take the longest and consume the most memory
    extract_query_stats_to_datahub()


@graph
def metadata_datahub_graph_biztech():
    os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
    os.environ["SPARK_VERSION"] = "3.3"

    # Extract metadata from dbt Cloud artifacts (models, tests, jobs) and ingest into DataHub for each team
    tableau_op = get_op(db="biztech", ingestion_source="tableau")
    biztech_op = get_dbt_artifacts_op(team="biztech")
    gtm_op = get_dbt_artifacts_op(team="gtm")

    # Execute ops for each team
    biztech_op()
    gtm_op()
    tableau_op()


@graph
def metadata_datahub_graph_hourly():
    extract_metadata_to_datahub_hourly_0()
    extract_metadata_to_datahub_hourly_1()
    extract_metadata_to_datahub_hourly_2()
    extract_dq_checks_to_datahub(emit_users_and_groups_to_datahub())


@job(
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "8000m", "memory": "16384Mi"},
                }
            },
        }
    },
)
def metadata_job_datahub_stats():
    metadata_datahub_graph_stats()


@job(
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
    resource_defs={
        "in_memory_io_manager": InMemoryIOManager(),
    },
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "8000m", "memory": "16384Mi"},
                }
            },
        }
    },
)
def metadata_job_datahub_biztech():
    metadata_datahub_graph_biztech()


@job(
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
    resource_defs={"in_memory_io_manager": InMemoryIOManager()},
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,
                },
            }
        }
    },
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {"requests": {"cpu": "1000m", "memory": "1024Mi"}}
            },
        }
    },
)
def metadata_job_datahub_hourly():
    metadata_datahub_graph_hourly()


if get_datahub_env() == "prod":
    DATABASE = "auditlog"
else:
    DATABASE = "dataplatform_dev"


class MaterializationConfig(Config):
    asset_key: List[str]


metadata_job_datahub_scrape = define_asset_job(
    "metadata_job_datahub_scrape",
    selection=AssetSelection.groups("datahub"),
    tags={"dagster/max_retries": 1},
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

metadata_job_datahub_15m = define_asset_job(
    "metadata_job_datahub_15m",
    selection=AssetSelection.groups("datahub_15m"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)


initialize_datadog()

if get_datahub_env() == "prod":
    METRIC_PREFIX = "dagster"
else:
    METRIC_PREFIX = "dagster.dev"


S3_BUCKET = "samsara-datahub-metadata"
BASE_PREFIX = "AWSLogs/492164655156/elasticloadbalancing/us-west-2/"


def is_valid_ip_address(ip):
    # Regular expression for matching valid IP addresses
    pattern = r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"

    if re.match(pattern, ip):
        return True
    else:
        return False


def download_and_get_file_contents(context, s3_bucket, s3_key):
    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Define a list to hold the lines
    observed_ips = []

    # Download the file
    local_file_name = s3_key.split("/")[-1]
    s3.download_file(s3_bucket, s3_key, local_file_name)

    # Check if the file needs to be decompressed
    if "datahub" in s3_key and "gz" in local_file_name:
        with gzip.open(local_file_name, "rt") as f:
            line1 = f.readline()
            line1_fields = line1.split(" ")
            ip_idx = 0
            for field in line1_fields:
                if is_valid_ip_address(field.split(":")[0]):
                    break
                ip_idx += 1

            for line in f:
                if not line:
                    break
                ip_addr = line.split(" ")[ip_idx].split(":")[0]
                observed_ips.append(ip_addr)
    elif "a77fe98f021bc410e8befa9c5f16eb98" in s3_key:
        with open(local_file_name, "r") as f:
            for line in f:
                if not line:
                    break
                ip_addr = line.strip().split(" ")[2].split(":")[0]
                observed_ips.append(ip_addr)
    else:
        return []

    # Clean up the downloaded file
    os.remove(local_file_name)

    allowed_ips = [
        "52.52.25.19",
        "100.20.103.225",
        "52.27.40.118",
        "52.89.143.16",
        "52.52.108.161",
        "3.111.155.138",
        "15.181.200.153",
        "54.70.184.89",
        "52.41.8.52",
        "54.151.86.198",
        "44.231.79.181",
        "44.240.177.131",
        "15.188.32.248",
        "18.156.115.174",
        "3.212.179.6",
        "52.16.26.70",
        "52.53.100.212",
        "52.10.232.176",
        "18.189.252.214",
        "44.230.106.30",
        "52.89.25.127",
        "50.18.205.11",
        "54.225.84.247",
        "54.194.75.90",
        "15.181.81.17",
        "18.236.13.56",
        "15.181.17.94",
        "44.226.6.35",
        "54.168.101.198",
        "18.132.101.7",
        "35.167.55.1",  # dagster-us-west-2a-eip-nat
        "34.216.88.56",  # dagster-us-west-2b-eip-nat
        "52.38.67.177",  # dagster-us-west-2c-eip-nat
    ]

    disallowed_ips = set()

    for ip in observed_ips:
        if ip not in allowed_ips:
            disallowed_ips.add(ip)

    context.log.info(f"observed_ips: {list(sorted(set(observed_ips)))}")

    return list(sorted(disallowed_ips))


@op
def s3_lb_ingest(context: OpExecutionContext) -> None:
    s3_key = context.run_tags.get("dagster/run_key")

    file_contents = download_and_get_file_contents(context, S3_BUCKET, s3_key)
    slack_token = get_slack_token()
    client = WebClient(token=slack_token)
    context.log.info(f"suspicious ips: {file_contents}")
    if file_contents:
        client.chat_postMessage(
            channel="alerts-data-tools",
            text=f"suspicious IP addresses making LB requests: {file_contents}",
        )

    return


@job(hooks={datamodel_job_failure_hook, datamodel_job_success_hook})
def s3_lb_ingest_job():
    s3_lb_ingest()


@sensor(
    name="datahub_lb_sensor",
    job=s3_lb_ingest_job,
    minimum_interval_seconds=60,
)
def s3_lb_sensor(context):

    since_key = context.cursor or None

    if get_datahub_env() == "dev":
        since_key = "AWSLogs/492164655156/elasticloadbalancing/us-west-2/2024/03/20/492164655156_elasticloadbalancing_us-west-2_a77fe98f021bc410e8befa9c5f16eb98_20240320T0055Z_52.12.58.158_1xgywjzd.log"

    new_s3_keys = get_s3_keys(
        S3_BUCKET,
        prefix=BASE_PREFIX,
        since_key=since_key,
    )
    if not new_s3_keys:
        return SkipReason("No new s3 files found for bucket my_s3_bucket.")
    last_key = new_s3_keys[-1]

    run_requests = [
        RunRequest(run_key=s3_key, tags={"s3_key": s3_key}) for s3_key in new_s3_keys
    ]

    context.update_cursor(last_key)

    return run_requests


metadata_amundsen_schedule = ScheduleDefinition(
    job=metadata_job_amundsen, cron_schedule="25 * * * *"  # At minute 25 each hour
)

metadata_datahub_glue_schedule = ScheduleDefinition(
    job=metadata_job_datahub_glue_ingestion,
    cron_schedule="26 5,13,21 * * *",  # 5:26 AM, 1:26 PM, and 9:26 PM UTC each day
)

metadata_datahub_6h_schedule = ScheduleDefinition(
    job=metadata_job_datahub_6h,
    cron_schedule="49 2,8,14,20 * * *",  # 2:49 AM, 8:49 AM, 2:49 PM, and 8:49 PM UTC each day
)

metadata_databricks_schedule = ScheduleDefinition(
    job=metadata_job_databricks,
    cron_schedule="20 9 * * *",  # once per day at 9:20am UTC
)

metadata_datahub_scrape_schedule = build_schedule_from_partitioned_job(
    job=metadata_job_datahub_scrape, minute_of_hour=45, hour_of_day=11
)

metadata_datahub_scrape_backfill_schedule = build_backfill_schedule(
    job=metadata_job_datahub_scrape,
    cron="25 0,6,12,18 * * *",  # every 6 hours starting at 12:25am
    partitions_fn=date_range_from_current_date,
    name="metadata_job_datahub_scrape_backfill",
)

metadata_datahub_stats_schedule = ScheduleDefinition(
    job=metadata_job_datahub_stats,
    cron_schedule="49 2 * * *",  # At 2:49am UTC each day
)

metadata_datahub_hourly_schedule = ScheduleDefinition(
    job=metadata_job_datahub_hourly,
    cron_schedule="33 * * * *",  # At minute 33 past every hour
)

metadata_datahub_15m_schedule = ScheduleDefinition(
    job=metadata_job_datahub_15m,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
)

metadata_datahub_biztech_schedule = ScheduleDefinition(
    job=metadata_job_datahub_biztech,
    cron_schedule="50 */2 * * *",  # At 50th minute of every 2nd hour
)
