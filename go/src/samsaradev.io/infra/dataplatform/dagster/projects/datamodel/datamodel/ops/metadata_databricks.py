import base64
import hashlib
import os
import re
import time
from typing import Dict

import requests
from dagster import In, Nothing, Out, op
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from ..common.utils import (
    AWSRegions,
    get_datahub_env,
    get_dbx_oauth_credentials,
    get_dbx_token,
    initialize_datadog,
    slack_custom_alert,
)

initialize_datadog()

load_dotenv()


# Constants
DAP_PATTERN = r"\bdap\w{10,}"
DEFAULT_DATABRICKS_INSTANCE = "samsara-dev-us-west-2"
DEFAULT_REGION = AWSRegions.US_WEST_2.value
OAUTH_TOKEN_EXPIRY_BUFFER = 300  # 5 minutes

# Cache for OAuth tokens (token string, expiry timestamp)
_oauth_token_cache: Dict[str, tuple[str, float]] = {}


def get_host(databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE) -> str:
    """Get Databricks host URL."""
    host = os.getenv("DATABRICKS_HOST", f"{databricks_instance}.cloud.databricks.com")
    if not host.startswith("https://"):
        host = f"https://{host}"
    return host


def get_oauth_access_token(host: str, client_id: str, client_secret: str) -> str:
    """Get OAuth access token using client credentials flow."""
    # Check cache first
    cache_key = f"{host}:{client_id}"
    if cache_key in _oauth_token_cache:
        token, expiry = _oauth_token_cache[cache_key]
        # Use cached token if it expires in more than buffer time
        if time.time() < expiry - OAUTH_TOKEN_EXPIRY_BUFFER:
            return token

    token_endpoint = f"{host}/oidc/v1/token"

    # Create Basic Auth header
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = base64.b64encode(auth_string.encode()).decode()

    # Request OAuth token
    data = {"grant_type": "client_credentials", "scope": "all-apis"}
    headers = {
        "Authorization": f"Basic {auth_bytes}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    resp = requests.post(token_endpoint, data=data, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to get OAuth token: {resp.status_code} - {resp.text}"
        )

    oauth_response = resp.json()
    access_token = oauth_response["access_token"]
    expires_in = oauth_response.get("expires_in", 3600)  # Default 1 hour

    # Cache the token (expires in expires_in seconds, minus buffer)
    expiry_time = time.time() + expires_in - OAUTH_TOKEN_EXPIRY_BUFFER
    _oauth_token_cache[cache_key] = (access_token, expiry_time)

    return access_token


def _get_auth_token_for_region(region: str, host: str) -> str:
    """Get authentication token for a region (PAT in dev, OAuth in prod)."""
    if get_datahub_env() == "dev":
        token = get_dbx_token(region)
        if not token:
            raise ValueError("DATABRICKS_TOKEN is not set for dev environment")
        return token
    else:
        oauth_credentials = get_dbx_oauth_credentials(region)
        return get_oauth_access_token(
            host,
            oauth_credentials["client_id"],
            oauth_credentials["client_secret"],
        )


def get_auth_headers(
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
) -> Dict[str, str]:
    """Get authentication headers for Databricks API calls.

    In dev: uses PAT token from environment/SSM
    In prod: uses OAuth client credentials flow
    """
    host = get_host(databricks_instance)
    token = _get_auth_token_for_region(DEFAULT_REGION, host)
    return {"Authorization": f"Bearer {token}"}


def _make_api_request(
    endpoint: str,
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
    method: str = "GET",
    params: Dict = None,
) -> requests.Response:
    """Make an authenticated API request to Databricks."""
    host = get_host(databricks_instance)
    headers = get_auth_headers(databricks_instance)
    url = f"{host}{endpoint}"
    return requests.request(method, url, headers=headers, params=params)


def list_all_apps(
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
):
    """List all Databricks apps."""
    resp = _make_api_request("/api/2.0/apps", databricks_instance)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to list apps: {resp.status_code} - {resp.text}")
    return resp.json().get("apps", [])


def get_app_details(
    app_name: str,
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
):
    """Get full details for a specific app including active deployment."""
    resp = _make_api_request(f"/api/2.0/apps/{app_name}", databricks_instance)
    if resp.status_code == 200:
        return resp.json()
    print(f"    Failed to get app details: {resp.status_code}")
    return None


def get_databricks_client(
    context, databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE
) -> WorkspaceClient:
    """Get a Databricks WorkspaceClient with appropriate authentication."""
    host = get_host(databricks_instance)
    config = {"host": host}

    if get_datahub_env() == "dev":
        config["token"] = get_dbx_token(DEFAULT_REGION)
    else:
        oauth_credentials = get_dbx_oauth_credentials(DEFAULT_REGION)
        config["client_id"] = oauth_credentials["client_id"]
        config["client_secret"] = oauth_credentials["client_secret"]

    try:
        return WorkspaceClient(**config)
    except Exception as e:
        context.log.error(f"Failed to get Databricks client: {e}")
        raise


def list_workspace_files(
    path: str,
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
):
    """List files in a workspace path recursively."""
    resp = _make_api_request(
        "/api/2.0/workspace/list",
        databricks_instance,
        params={"path": path},
    )
    if resp.status_code != 200:
        return []

    files = []
    objects = resp.json().get("objects", [])
    for obj in objects:
        obj_type = obj.get("object_type")
        obj_path = obj.get("path")
        if obj_type in ["FILE", "NOTEBOOK"]:
            files.append(obj_path)
        elif obj_type == "DIRECTORY":
            # Recursively list subdirectories
            files.extend(list_workspace_files(obj_path, databricks_instance))
    return files


def download_workspace_file(
    path: str,
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
) -> bytes:
    """Download a workspace file."""
    resp = _make_api_request(
        "/api/2.0/workspace/export",
        databricks_instance,
        params={"path": path, "format": "SOURCE"},
    )
    if resp.status_code != 200:
        return None

    content_b64 = resp.json().get("content", "")
    return base64.b64decode(content_b64)


def is_text_file(path):
    """Check if file is a text file based on extension."""
    binary_extensions = {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".bmp",
        ".ico",
        ".pdf",
        ".zip",
        ".tar",
        ".gz",
        ".pyc",
        ".so",
        ".dll",
        ".exe",
        ".bin",
        ".dat",
        ".db",
        ".sqlite",
        ".pem",
        ".key",
        ".crt",
        ".der",
    }
    return not any(path.lower().endswith(ext) for ext in binary_extensions)


def file_contains_sensitive_strings(content):
    """Check if file contains TOKEN AND a dapXXX token string."""
    if not content:
        return False
    try:
        text_content = content.decode("utf-8", errors="ignore")
        # Must have TOKEN reference
        if "TOKEN" not in text_content:
            return False

        return bool(re.search(DAP_PATTERN, text_content))
    except Exception:
        return False


def _extract_source_code_path(app_details: Dict) -> str:
    """Extract source code path from app details."""
    active_deployment = app_details.get("active_deployment")
    if active_deployment:
        return active_deployment.get("source_code_path")

    pending = app_details.get("pending_deployment")
    if pending:
        return pending.get("source_code_path")

    return app_details.get("default_source_code_path")


def _create_line_snippet(line: str, search_pos: int, search_len: int = 0) -> str:
    """Create a snippet around a search position with context."""
    start = max(0, search_pos - 30)
    end = min(len(line), search_pos + (search_len or 0) + 30)
    snippet = line[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(line):
        snippet = snippet + "..."
    return snippet


def scan_workspace_apps(
    context,
    download_root="/tmp/databricks_apps",
    databricks_instance: str = DEFAULT_DATABRICKS_INSTANCE,
):
    apps = list_all_apps(databricks_instance)
    context.log.info(f"\nFound {len(apps)} apps\n")

    num_files_with_leaks = 0

    for app in apps:
        app_name = app.get("name")

        # Get full app details
        app_details = get_app_details(app_name, databricks_instance)
        if not app_details:
            continue

        src = _extract_source_code_path(app_details)
        if not src:
            continue

        files = list_workspace_files(src, databricks_instance)

        matching_files_found = False
        for fpath in files:
            # Skip binary files
            if not is_text_file(fpath):
                continue

            content = download_workspace_file(fpath, databricks_instance)

            if content and file_contains_sensitive_strings(content):
                num_files_with_leaks += 1
                result_str = ""
                if not matching_files_found:
                    app_label = f"Databricks App: {app_name}"
                    result_str += f"\n⚠️  FOUND LEAKED TOKEN in {app_label}"
                    matching_files_found = True

                # Construct Databricks workspace URL (remove /Workspace prefix)
                # Databricks URLs use /#workspace/<path>
                host = get_host(databricks_instance)
                file_path = fpath.replace("/Workspace", "")
                workspace_url = f"{host}/#workspace{file_path}"
                result_str += f"\n    File: {fpath}"
                result_str += f"\n    URL:  {workspace_url}"
                # Decode content and find matching lines
                try:
                    text_content = content.decode("utf-8")
                    lines = text_content.splitlines()

                    for i, line in enumerate(lines, 1):
                        # Check for TOKEN reference
                        if "TOKEN" in line:
                            pos = line.find("TOKEN")
                            snippet = _create_line_snippet(line, pos, len("TOKEN"))
                            # Redact any token values in the snippet
                            redacted = "dap************"
                            snippet = re.sub(DAP_PATTERN, redacted, snippet)
                            result_str += f"\n      Line {i}: {snippet}"

                        # Check for actual dapXXXXXX token
                        match = re.search(DAP_PATTERN, line)
                        if match:
                            token = match.group(0)
                            snippet = _create_line_snippet(
                                line, match.start(), len(token)
                            )
                            # Strip out actual token value from snippet
                            snippet = snippet.replace(token, "dap************")
                            result_str += f"\n      Line {i} [LEAKED TOKEN]: {snippet}"
                except UnicodeDecodeError:
                    # Binary file, show preview
                    preview = content.decode("utf-8", errors="ignore")[:50]
                    context.log.info(f"      Preview (binary): {preview}")

                context.log.info(result_str)

                # publish to #alert-data-tools Slack channel
                if get_datahub_env() == "prod":
                    slack_custom_alert("alerts-data-tools", result_str)

    if num_files_with_leaks == 0:
        if get_datahub_env() == "prod":
            msg = "No leaked tokens found in Databricks applications! ✅"
            slack_custom_alert("alerts-data-tools", msg)

        else:
            context.log.info("No leaked tokens found in Databricks applications! ✅")


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def audit_databricks_applications(context) -> int:
    """
    Audit Databricks applications and emit token leaks to Slack.
    """
    context.log.info("Auditing Databricks applications")
    scan_workspace_apps(context)
    context.log.info("Databricks applications audited")
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def upload_cursor_query_agent_files_to_s3(context) -> int:
    """
    Upload cursor query agent files to S3.
    """
    context.log.info("Uploading cursor query agent files to S3")

    export_base_path = "/Volumes/s3/datahub-metadata/root/cqa/"
    files_volume_path = f"{export_base_path}files/"
    bootstrap_volume_path = export_base_path  # Keep bootstrap script in cqa/ directory

    is_prod = get_datahub_env() == "prod" and os.getenv("MODE") not in (
        "LOCAL_PROD_CLUSTER_RUN",
        "LOCAL_DATAHUB_RUN",
    )

    if is_prod:
        # In K8s container, paths are flattened as per Dockerfile
        llm_common_base = "/datamodel/datamodel/common/llm/"
        datahub_llm_base = "/dataplatform/datahub/llm/"
        file_mapping = {
            f"{llm_common_base}sql_runner.py": (f"{files_volume_path}sql_runner.py"),
            f"{llm_common_base}.init_agent.py": (f"{files_volume_path}.init_agent.py"),
            f"{llm_common_base}.close_agent.py": (
                f"{files_volume_path}.close_agent.py"
            ),
            f"{llm_common_base}lineage.py": (f"{files_volume_path}lineage.py"),
            f"{llm_common_base}_db_logger.py": (f"{files_volume_path}_db_logger.py"),
            f"{llm_common_base}gen_datahub_schema.py": (
                f"{files_volume_path}gen_datahub_schema.py"
            ),
            f"{datahub_llm_base}mcp_app.py": (f"{files_volume_path}mcp_app.py"),
            f"{datahub_llm_base}.python_startup.py": (
                f"{files_volume_path}.python_startup.py"
            ),
            f"{llm_common_base}requirements.cursordatahub.txt": (
                f"{files_volume_path}requirements.cursordatahub.txt"
            ),
            f"{llm_common_base}requirements.cursordatahub.mcp.txt": (
                f"{files_volume_path}requirements.cursordatahub.mcp.txt"
            ),
            f"{llm_common_base}mcp_config.json": (
                f"{files_volume_path}mcp_config.json"
            ),
            "/datamodel/query_agent.mdc": (f"{files_volume_path}query_agent.mdc"),
            "/datamodel/bin/cursor-query-agent-setup.sh": (
                f"{bootstrap_volume_path}cqa.sh"
            ),
        }

        def get_full_path(p):
            return p

    else:
        # Local development using backend root
        backend_root = os.getenv("BACKEND_ROOT") or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../" * 10)
        )
        llm_common_rel = (
            "go/src/samsaradev.io/infra/dataplatform/dagster/"
            "projects/datamodel/datamodel/common/llm/"
        )
        datahub_llm_rel = "go/src/samsaradev.io/infra/dataplatform/datahub/llm/"

        file_mapping = {
            f"{llm_common_rel}sql_runner.py": (f"{files_volume_path}sql_runner.py"),
            f"{llm_common_rel}.init_agent.py": (f"{files_volume_path}.init_agent.py"),
            f"{llm_common_rel}.close_agent.py": (f"{files_volume_path}.close_agent.py"),
            f"{llm_common_rel}lineage.py": (f"{files_volume_path}lineage.py"),
            f"{llm_common_rel}_db_logger.py": (f"{files_volume_path}_db_logger.py"),
            f"{llm_common_rel}gen_datahub_schema.py": (
                f"{files_volume_path}gen_datahub_schema.py"
            ),
            f"{datahub_llm_rel}mcp_app.py": (f"{files_volume_path}mcp_app.py"),
            f"{datahub_llm_rel}.python_startup.py": (
                f"{files_volume_path}.python_startup.py"
            ),
            f"{llm_common_rel}requirements.cursordatahub.txt": (
                f"{files_volume_path}requirements.cursordatahub.txt"
            ),
            f"{llm_common_rel}requirements.cursordatahub.mcp.txt": (
                f"{files_volume_path}requirements.cursordatahub.mcp.txt"
            ),
            f"{llm_common_rel}mcp_config.json": (f"{files_volume_path}mcp_config.json"),
            ".cursor/rules/query_agent.mdc": (f"{files_volume_path}query_agent.mdc"),
            "bin/cursor-query-agent-setup.sh": (f"{bootstrap_volume_path}cqa.sh"),
        }

        def get_full_path(p):
            return os.path.join(backend_root, p)

    dbx_client = get_databricks_client(context)

    # make sure export_base_path is never the root dire
    if export_base_path == "/":
        raise RuntimeError("export_base_path cannot be the root directory")

    # Purge existing files in the prefix if possible
    context.log.info(f"Purging existing files in {export_base_path}")
    # Note: Databricks SDK FilesAPI.delete does not support 'recursive'.
    # We rely on overwrite=True for the files we upload.

    # Upload files
    success_count = 0
    fail_count = 0
    for local_rel_path, target_path in file_mapping.items():
        local_path = get_full_path(local_rel_path)

        if not os.path.exists(local_path):
            context.log.warning(
                f"Skipping: {target_path} (file not found: {local_path})"
            )
            fail_count += 1
            continue

        context.log.info(f"Uploading: {local_path} to {target_path}")
        try:
            # Special handling for bootstrap script: enable --lite by default
            if (
                "cursor-query-agent-setup.sh" in local_rel_path
                or "cqa.sh" in target_path
            ):
                with open(local_path, "r", encoding="utf-8") as f:
                    content = f.read()
                # Enable lite mode by default
                content = content.replace("LITE_FLAG=false", "LITE_FLAG=true")
                content_bytes = content.encode("utf-8")
                dbx_client.files.upload(target_path, content_bytes, overwrite=True)
                context.log.info("Uploaded cqa.sh with --lite mode enabled by default")

                # Calculate and log hash for security verification
                cqa_hash = hashlib.sha256(content_bytes).hexdigest()
                context.log.info(f"cqa.sh hash: {cqa_hash}")

                # Upload hash to Databricks Secrets
                try:
                    dbx_client.secrets.put_secret(
                        scope="cqa",
                        key="hash",
                        string_value=cqa_hash,
                    )
                    context.log.info(
                        "✅ Uploaded hash to Databricks secret: " "cqa/hash"
                    )
                except Exception as e:
                    context.log.warning(
                        f"Failed to upload hash to Databricks secret: {e}"
                    )
                    context.log.info(
                        "Manual update required - run:\n"
                        f"  databricks secrets put-secret cqa "
                        f'hash --string-value "{cqa_hash}" '
                        "--profile samsara-dev-us-west-2"
                    )
            else:
                with open(local_path, "rb") as f:
                    dbx_client.files.upload(target_path, f, overwrite=True)
            success_count += 1
        except Exception as e:
            context.log.error(f"Failed to upload {target_path}: {e}")
            fail_count += 1

    context.log.info(f"Successfully uploaded: {success_count} files")
    if fail_count > 0:
        context.log.error(f"Failed to upload: {fail_count} files")
        # Fail the op if critical files are missing
        raise RuntimeError(f"Failed to upload {fail_count} files to S3")

    context.log.info("Cursor query agent files uploaded to S3")

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def setup_query_agents_logs_table_with_column_mask(context) -> int:
    """
    Create query_agents.logs table, masking function, and apply column mask.

    This op creates:
    1. The logs table (if not exists)
    2. The mask_payload function
    3. Applies the mask to the payload column

    All operations are idempotent and can be run multiple times.
    Uses Databricks SQL Statement API since Unity Catalog DDL requires it.
    """
    context.log.info("Setting up query_agents.logs table, function, and mask")

    # Use the cursor-query-agent SQL warehouse for DDL operations
    warehouse_id = "9fb6a34db2b0bbde"
    host = get_host()
    token = _get_auth_token_for_region(DEFAULT_REGION, host)

    def execute_sql(sql: str, description: str) -> None:
        """Execute SQL via Databricks SQL Statement API.

        Raises RuntimeError if the statement fails so the Dagster op fails and
        the pipeline can detect issues (matching
        upload_cursor_query_agent_files_to_s3).
        """
        context.log.info(f"Executing: {description}")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        def get_state_and_error(payload: dict) -> tuple[str, str]:
            status_obj = payload.get("status", {}) or {}
            state = status_obj.get("state", "") or ""
            error_obj = status_obj.get("error", {}) or {}
            error_msg = error_obj.get("message", "") or ""
            return state, error_msg

        try:
            resp = requests.post(
                f"{host}/api/2.0/sql/statements",
                headers=headers,
                json={
                    "warehouse_id": warehouse_id,
                    "catalog": "default",
                    "statement": sql,
                    "wait_timeout": "30s",
                },
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Request failed for {description}: {e}") from e

        statement_id = result.get("statement_id", "") or ""
        state, error_msg = get_state_and_error(result)

        # The statement API may return RUNNING/PENDING if it doesn't finish
        # within wait_timeout.
        # Poll until a terminal state so we don't treat "still running"
        # as failure.
        terminal_states = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
        start_time = time.time()
        max_wait_seconds = 180

        while (
            state not in terminal_states and time.time() - start_time < max_wait_seconds
        ):
            time.sleep(2)
            try:
                status_resp = requests.get(
                    f"{host}/api/2.0/sql/statements/{statement_id}",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30,
                )
                status_resp.raise_for_status()
                status_payload = status_resp.json()
                state, error_msg = get_state_and_error(status_payload)
            except requests.exceptions.RequestException as e:
                raise RuntimeError(
                    "Request failed while polling "
                    f"{description} (statement_id={statement_id}): {e}"
                ) from e

        if state == "SUCCEEDED":
            context.log.info(f"✅ {description} succeeded")
            return

        if state == "CLOSED" and not error_msg:
            # Treat as success if the statement is closed without an error.
            context.log.info(f"✅ {description} completed (state=CLOSED)")
            return

        if state not in terminal_states:
            raise RuntimeError(
                f"{description} did not reach a terminal state within "
                f"{max_wait_seconds}s "
                f"(statement_id={statement_id}, state={state})"
            )

        raise RuntimeError(
            f"{description} failed "
            f"(statement_id={statement_id}, state={state}): "
            f"{error_msg or 'Unknown error'}"
        )

    # Create the table
    create_table_sql = """
CREATE TABLE IF NOT EXISTS default.query_agents.logs (
    log_timestamp TIMESTAMP COMMENT 'When the log entry was created (UTC)',
    log_type STRING COMMENT 'Type of log: search, sql, etc.',
    agent_name STRING DEFAULT 'cursor-query-agent'
        COMMENT 'Name of the query agent (e.g., cursor-query-agent)',
    user_email STRING COMMENT 'Email of user who triggered the log',
    payload STRING COMMENT 'JSON payload with log details',
    date DATE GENERATED ALWAYS AS (DATE(log_timestamp))
        COMMENT 'Date partition derived from log_timestamp'
)
USING DELTA
PARTITIONED BY (log_type, agent_name, date)
COMMENT 'Query agent activity logs. Write-only for most teams.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
    """

    # Create the masking function
    mask_function_sql = """
CREATE OR REPLACE FUNCTION default.query_agents.mask_payload(
    payload STRING
)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('data-platform-group') THEN payload
    WHEN is_account_group_member('data-tools-group') THEN payload
    WHEN is_account_group_member('data-engineering-group') THEN payload
    ELSE '***MASKED***'
END
    """

    # Apply the mask to the payload column
    apply_mask_sql = """
ALTER TABLE default.query_agents.logs
ALTER COLUMN payload SET MASK default.query_agents.mask_payload
    """

    # Execute DDL statements. Fail fast on any errors so the pipeline can
    # detect issues.
    execute_sql(create_table_sql, "Creating query_agents.logs table")
    execute_sql(mask_function_sql, "Creating mask_payload function")
    execute_sql(apply_mask_sql, "Applying mask to payload column")
    context.log.info("✅ query_agents.logs setup completed successfully")

    return 0
