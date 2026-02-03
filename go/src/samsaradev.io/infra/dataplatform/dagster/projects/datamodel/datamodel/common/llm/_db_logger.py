#! /usr/bin/env python3
"""
Shared utility for logging query agent activity to Delta table.

Permissions are controlled by Unity Catalog schema grants defined in:
  go/src/samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries/

The query_agents.logs table has:
  - CanReadWriteGroups: All CursorQueryAgentTeams (includes
    DataPlatform, DataEngineering, DataTools)
  - Column masking: payload column is masked for all teams except
    DataPlatform/DataTools/DataEngineering

This allows ~40 teams to log their query agent activity without being
able to read other users' sensitive data. Column masking protects the
payload column:
- All granted teams can INSERT (which requires SELECT for Delta Lake
  metadata checks)
- The payload column is masked for most teams - they see '***MASKED***'
  instead
- Only DataPlatform, DataTools, and DataEngineering can read the actual
  payload data

If writes fail with permission errors, check:
1. Terraform has been applied to sync Unity Catalog grants
2. User is member of Databricks account group corresponding to their team
3. Databricks token has necessary scopes
"""
import json
import os
import re
import subprocess
import time

# Stdlib fallback for HTTP requests (used when `requests` isn't available)
import urllib.error
import urllib.request
from datetime import datetime, timezone

# Gracefully handle missing requests library
try:
    import requests
except ImportError:
    requests = None

# SQL warehouse ID for query agent logging
SQL_WAREHOUSE_ID = "9fb6a34db2b0bbde"

# Databricks workspace URL
DATABRICKS_HOST = "https://samsara-dev-us-west-2.cloud.databricks.com"

# Table for logging
LOG_TABLE = "default.query_agents.logs"

# Cache OAuth token to avoid repeated CLI calls
_oauth_token_cache = None
_oauth_token_cache_time = None
_oauth_token_cache_ttl = 300  # Cache for 5 minutes


def _extract_log_type(log_path: str) -> str:
    """Extract log type from path.

    Example: 'search' from '.../logs/search/...'
    """
    match = re.search(r"/logs/([^/]+)/", log_path)
    return match.group(1) if match else "unknown"


def _get_user_email_from_payload(payload: dict) -> str:
    """Extract user_email from payload, or return unknown.

    The calling code (.init_agent.py, .close_agent.py) computes user_email
    with the correct fallback logic (git_user_email -> USER_EMAIL env var ->
    USER env var + @samsara.com). We extract it from the payload to ensure
    the SQL column matches the payload's user_email field.
    """
    return payload.get("user_email", "unknown")


def _get_agent_name_from_payload(payload: dict) -> str:
    """Extract agent_name from payload, or return default.

    The agent_name identifies which query agent generated the log entry.
    Falls back to 'cursor-query-agent' if not specified in payload.
    """
    return payload.get("agent_name", "cursor-query-agent")


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def get_git_user_email() -> str | None:
    """Get git user email, handling errors gracefully."""
    try:
        return os.popen("git config user.email").read().strip()
    except Exception:
        return None


def get_oauth_token(profile: str = "samsara-dev-us-west-2") -> str | None:
    """
    Get OAuth token from Databricks CLI.
    Tries multiple profiles and methods to get an authentication token.
    Uses caching to avoid repeated CLI calls.

    Args:
        profile: Databricks profile name to use
            (default: samsara-dev-us-west-2)

    Returns:
        OAuth token string if successful, None otherwise
    """
    global _oauth_token_cache, _oauth_token_cache_time

    # Check cache first
    if (
        _oauth_token_cache is not None
        and _oauth_token_cache_time is not None
        and time.time() - _oauth_token_cache_time < _oauth_token_cache_ttl
    ):
        return _oauth_token_cache

    # Check if .databrickscfg exists and if the profile is available
    databrickscfg_path = os.path.expanduser("~/.databrickscfg")
    profiles_to_try = [profile, "DEFAULT", ""]

    if os.path.exists(databrickscfg_path):
        # Check which profiles exist in the config file
        with open(databrickscfg_path, "r") as f:
            config_content = f.read()
            # Filter to only profiles that exist in config
            available_profiles = []
            for p in profiles_to_try:
                if p == "" or f"[{p}]" in config_content:
                    available_profiles.append(p)
            if available_profiles:
                profiles_to_try = available_profiles

    # Try to find databricks CLI
    databricks_cli = None
    for cli_name in ["databricks", "databricks-cli"]:
        try:
            result = subprocess.run(
                ["which", cli_name],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0:
                databricks_cli = result.stdout.strip()
                break
        except (subprocess.TimeoutExpired, FileNotFoundError):
            continue

    if not databricks_cli:
        return None

    # Try each profile to get a token
    for profile_to_try in profiles_to_try:
        try:
            # Unset all DATABRICKS_* env vars to force CLI to use config
            env = os.environ.copy()
            env.pop("DATABRICKS_TOKEN", None)
            env.pop("DATABRICKS_HOST", None)
            env.pop("DATABRICKS_ACCESS_TOKEN", None)
            env.pop("DATABRICKS_AUTH_TYPE", None)
            env.pop("DATABRICKS_CONFIG_PROFILE", None)

            cmd = [databricks_cli, "auth", "token"]
            if profile_to_try:
                cmd.extend(["--profile", profile_to_try])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10,  # Increased timeout for OAuth
                env=env,
            )

            if result.returncode == 0:
                token = result.stdout.strip().split("\n")[0].strip()
                if token:
                    # Cache the token
                    _oauth_token_cache = token
                    _oauth_token_cache_time = time.time()
                    return token
        except (
            subprocess.TimeoutExpired,
            FileNotFoundError,
            subprocess.SubprocessError,
        ):
            continue

    return None


def get_databricks_token() -> str | None:
    """
    Get Databricks authentication token.
    Tries DATABRICKS_TOKEN env var first, then falls back to OAuth via:
    - Databricks CLI (`databricks auth token`) if available
    - Databricks SDK cached OAuth token (via
      `WorkspaceClient().config.authenticate()`)

    Returns:
        Token string if successful, None otherwise
    """
    # First try environment variable (only if it looks valid)
    token = os.getenv("DATABRICKS_TOKEN")
    if token and validate_token(token):
        return token

    # Fallback to OAuth (Databricks CLI)
    token = get_oauth_token()
    if token and validate_token(token):
        return token

    # Fallback to OAuth (Databricks SDK cached token)
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore

        # Mirror sql_runner.py OAuth behavior: ensure host is set and remove
        # env vars so the SDK uses cached OAuth U2M when available.
        original_token = os.environ.pop("DATABRICKS_TOKEN", None)
        original_access_token = os.environ.pop("DATABRICKS_ACCESS_TOKEN", None)
        original_auth_type = os.environ.pop("DATABRICKS_AUTH_TYPE", None)
        original_host = os.environ.get("DATABRICKS_HOST")

        try:
            os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST
            workspace_client = WorkspaceClient()

            # Force auth resolution (uses cached token if present)
            try:
                workspace_client.current_user.me()
            except Exception:
                return None

            auth_headers = workspace_client.config.authenticate()
        finally:
            # Restore environment variables
            if original_token is not None:
                os.environ["DATABRICKS_TOKEN"] = original_token
            if original_access_token is not None:
                os.environ["DATABRICKS_ACCESS_TOKEN"] = original_access_token
            if original_auth_type is not None:
                os.environ["DATABRICKS_AUTH_TYPE"] = original_auth_type
            if original_host is not None:
                os.environ["DATABRICKS_HOST"] = original_host
            elif "DATABRICKS_HOST" in os.environ:
                del os.environ["DATABRICKS_HOST"]

        if isinstance(auth_headers, dict):
            auth_header = auth_headers.get("Authorization", "")
            if isinstance(auth_header, str) and auth_header.startswith("Bearer "):
                token = auth_header.replace("Bearer ", "").strip()
                if validate_token(token):
                    return token
    except Exception:
        return None

    return None


def validate_token(token: str) -> bool:
    """
    Quick validation that token is not empty and looks valid.
    Does not make an API call to avoid overhead.
    """
    if not token or not token.strip():
        return False
    # Basic sanity check: tokens are usually base64-like strings
    if len(token) < 10:
        return False
    return True


def log_to_local_fallback(payload: dict) -> None:
    """
    Fallback: log to local file if DB logging fails completely.
    This helps with debugging and provides a backup record.
    """
    try:
        log_dir = os.path.expanduser("~/.cursor/query-agent-logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(
            log_dir,
            f"query_log_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl",
        )
        with open(log_file, "a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        # Silently fail - this is just a fallback
        pass


def log_to_table(payload: dict, log_path: str, databricks_token: str) -> bool:
    """
    Log a payload to the query_agents.logs Delta table via SQL Statement API.

    All granted teams have READWRITE access. Column masking on the
    payload column prevents most teams from reading sensitive query data.

    Args:
        payload: Dictionary to log as JSON
        log_path: Path used to extract log_type (e.g., '.../logs/search/...')
        databricks_token: Databricks authentication token

    Returns:
        True if successful, False otherwise
    """
    log_type = _extract_log_type(log_path)
    agent_name = _get_agent_name_from_payload(payload)
    user_email = _get_user_email_from_payload(payload)
    timestamp = datetime.now(timezone.utc).isoformat()

    # Escape all string values for SQL to prevent injection
    log_type_escaped = _escape_sql_string(log_type)
    agent_name_escaped = _escape_sql_string(agent_name)
    user_email_escaped = _escape_sql_string(user_email)
    payload_json_escaped = _escape_sql_string(json.dumps(payload))

    # Build INSERT statement
    # Note: Delta Lake requires SELECT permission on the table to perform
    # INSERT (for metadata and constraint checks). This is a known
    # limitation. Users have SELECT + MODIFY permissions, but the intent is
    # write-only access. Column masking on the payload column prevents
    # reading sensitive data.
    sql = f"""
        INSERT INTO {LOG_TABLE}
        (log_timestamp, log_type, agent_name, user_email, payload)
        VALUES (
            '{timestamp}',
            '{log_type_escaped}',
            '{agent_name_escaped}',
            '{user_email_escaped}',
            '{payload_json_escaped}'
        )
    """

    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }
    body_obj = {
        "warehouse_id": SQL_WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "10s",
    }

    if requests is not None:
        try:
            response = requests.post(
                url,
                headers=headers,
                json=body_obj,
                timeout=15,
            )
            response.raise_for_status()
            result = response.json()
            status = result.get("status", {}).get("state", "")
            return status == "SUCCEEDED"
        except requests.exceptions.Timeout:
            return False
        except requests.exceptions.HTTPError:
            return False
        except requests.exceptions.RequestException:
            return False
        except Exception:
            return False

    # --- Stdlib fallback (no `requests` dependency) ---
    try:
        body = json.dumps(body_obj).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp_body = resp.read().decode("utf-8")

        result = json.loads(resp_body) if resp_body else {}
        status = result.get("status", {}).get("state", "")
        return status == "SUCCEEDED"
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        ValueError,
    ):
        return False
    except Exception:
        return False
