#! /usr/bin/env python3

import json
import os
from typing import Literal

from databricks import sql


def run_dbx_query(query: str, env: Literal["dev", "prod"] = "dev"):
    DOMAIN = "samsara-dev-us-west-2.cloud.databricks.com"

    # Ensure CA bundle is set for SSL verification
    ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE")
    if ca_bundle and os.path.exists(ca_bundle):
        os.environ["SSL_CERT_FILE"] = ca_bundle

    # Try OAuth first if available, then fall back to PAT token
    auth_method = None
    credentials_provider = None
    databricks_token = None

    # Production mode: use service principal OAuth M2M authentication
    if env == "prod":
        from databricks.sdk.core import Config as dbxconfig
        from databricks.sdk.core import oauth_service_principal

        from ...common.utils import AWSRegions, get_dbx_oauth_credentials

        print("\t🔐 Using service principal OAuth M2M authentication (production mode)")

        def _get_credentials_provider():
            return oauth_service_principal(
                dbxconfig(
                    host=f"https://{DOMAIN}",
                    **get_dbx_oauth_credentials(AWSRegions.US_WEST_2.value),
                )
            )

        credentials_provider = _get_credentials_provider
        databricks_token = None  # OAuth M2M doesn't use a static token
        auth_method = "OAuth M2M (service principal)"
    else:
        # Dev mode: Try OAuth U2M first (user authentication), then fall back to PAT token
        # Check if we should skip OAuth and use token directly
        auth_type = os.environ.get("DATABRICKS_AUTH_TYPE", "").lower()
        skip_oauth = auth_type == "token"

        # Try OAuth U2M first (user authentication) - unless explicitly using token mode
        if skip_oauth:
            print("\t🔐 Using PAT token authentication (DATABRICKS_AUTH_TYPE=token)")
            databricks_token = os.environ.get("DATABRICKS_TOKEN")
            if not databricks_token:
                raise EnvironmentError(
                    "❌ Environment variable DATABRICKS_TOKEN is not set."
                )
            auth_method = "PAT token"
        else:
            try:
                from databricks.sdk import WorkspaceClient

                # Temporarily unset environment variables to force OAuth U2M
                # WorkspaceClient will use PAT token if DATABRICKS_TOKEN is in the environment
                # and will get confused if DATABRICKS_AUTH_TYPE is set
                original_token = os.environ.pop("DATABRICKS_TOKEN", None)
                original_host = os.environ.pop("DATABRICKS_HOST", None)
                original_auth_type = os.environ.pop("DATABRICKS_AUTH_TYPE", None)

                try:
                    # Set DATABRICKS_HOST for OAuth U2M
                    os.environ["DATABRICKS_HOST"] = f"https://{DOMAIN}"

                    # Use WorkspaceClient for OAuth U2M
                    # Don't specify auth_type - let it default to OAuth U2M when no token
                    # The SDK will automatically use cached tokens if available
                    workspace_client = WorkspaceClient()

                    print(
                        "\t🔐 Attempting OAuth U2M authentication "
                        "(browser will open for login if no cached token)..."
                    )

                    # Trigger authentication by making an API call
                    # This will open browser for OAuth U2M if no cached token exists
                    # The SDK caches tokens in ~/.databrickscfg or similar location
                    try:
                        workspace_client.current_user.me()
                        # If this succeeds without opening browser, it used a cached token
                        print(
                            "\tℹ️  Using cached OAuth U2M token (no browser login needed)"
                        )
                    except Exception:
                        # If authentication fails, it might need a fresh login
                        # The SDK will handle opening browser for OAuth flow
                        raise

                    # Get the token from the config after authentication
                    # The config should have the token after OAuth U2M completes
                    if (
                        hasattr(workspace_client.config, "token")
                        and workspace_client.config.token
                    ):
                        databricks_token = workspace_client.config.token
                    elif (
                        hasattr(workspace_client.config, "access_token")
                        and workspace_client.config.access_token
                    ):
                        databricks_token = workspace_client.config.access_token
                    else:
                        # Try to extract from auth headers
                        auth_headers = workspace_client.config.authenticate()
                        if isinstance(auth_headers, dict):
                            auth_header = auth_headers.get("Authorization", "")
                            if auth_header.startswith("Bearer "):
                                databricks_token = auth_header.replace("Bearer ", "")
                            else:
                                raise ValueError(
                                    "Could not extract token from WorkspaceClient"
                                )
                        else:
                            raise ValueError(
                                "Could not extract token from WorkspaceClient"
                            )

                    # Token successfully extracted from OAuth authentication

                    credentials_provider = None
                    auth_method = "OAuth U2M (user authentication)"
                    print("\t✅ OAuth U2M authentication successful")
                finally:
                    # Restore original environment variables
                    if original_token is not None:
                        os.environ["DATABRICKS_TOKEN"] = original_token
                    if original_host is not None:
                        os.environ["DATABRICKS_HOST"] = original_host
                    elif "DATABRICKS_HOST" in os.environ:
                        del os.environ["DATABRICKS_HOST"]
                    if original_auth_type is not None:
                        os.environ["DATABRICKS_AUTH_TYPE"] = original_auth_type
                    elif "DATABRICKS_AUTH_TYPE" in os.environ:
                        del os.environ["DATABRICKS_AUTH_TYPE"]
            except Exception as u2m_error:
                # Restore original environment variables on error
                # Only restore/delete variables that we actually modified
                if "original_token" in locals() and original_token is not None:
                    os.environ["DATABRICKS_TOKEN"] = original_token
                elif "original_token" in locals() and "DATABRICKS_TOKEN" in os.environ:
                    # We popped it but it was None, so delete what we set
                    del os.environ["DATABRICKS_TOKEN"]
                if "original_host" in locals():
                    if original_host is not None:
                        os.environ["DATABRICKS_HOST"] = original_host
                    elif "DATABRICKS_HOST" in os.environ:
                        # We popped it but it was None, so delete what we set
                        del os.environ["DATABRICKS_HOST"]
                # If original_host not in locals(), we never modified DATABRICKS_HOST
                if "original_auth_type" in locals():
                    if original_auth_type is not None:
                        os.environ["DATABRICKS_AUTH_TYPE"] = original_auth_type
                    elif "DATABRICKS_AUTH_TYPE" in os.environ:
                        # We popped it but it was None, so delete what we set
                        del os.environ["DATABRICKS_AUTH_TYPE"]
                # If original_auth_type not in locals(), we never modified DATABRICKS_AUTH_TYPE

                print(
                    f"\t⚠️ OAuth U2M authentication failed: "
                    f"{type(u2m_error).__name__}: {str(u2m_error)}"
                )
                print("\t🔄 Falling back to PAT token authentication...")
                credentials_provider = None
                databricks_token = None

        # Fall back to PAT token if OAuth U2M not available or failed
        # Only use PAT if we don't already have a token from OAuth U2M
        if not databricks_token:
            databricks_token = os.environ.get("DATABRICKS_TOKEN")
            if not databricks_token:
                raise EnvironmentError(
                    "❌ Environment variable DATABRICKS_TOKEN is not set "
                    "and OAuth credentials are not available."
                )
            auth_method = "PAT token"

    # Print which authentication method we're using
    print(f"\t🔐 Using Databricks authentication: {auth_method}")

    conn_kwargs = dict(
        server_hostname=DOMAIN,
        http_path="/sql/1.0/warehouses/9fb6a34db2b0bbde",
        access_token=databricks_token,
        credentials_provider=credentials_provider,
    )

    try:
        with sql.connect(**conn_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    except Exception as e:
        # If OAuth failed, try falling back to PAT token
        # Check if we're using OAuth (U2M or M2M) and connection failed
        is_oauth = auth_method and "OAuth" in auth_method
        if is_oauth and auth_method != "PAT token":
            print(f"\t⚠️ OAuth authentication failed: {str(e)}")
            print("\t🔄 Falling back to PAT token authentication...")
            databricks_token = os.environ.get("DATABRICKS_TOKEN")
            if not databricks_token:
                raise EnvironmentError(
                    "❌ OAuth failed and DATABRICKS_TOKEN is not set."
                )
            auth_method = "PAT token (fallback)"
            print(f"\t🔐 Using Databricks authentication: {auth_method}")
            conn_kwargs = dict(
                server_hostname=DOMAIN,
                http_path="/sql/1.0/warehouses/9fb6a34db2b0bbde",
                access_token=databricks_token,
                credentials_provider=None,
            )
            try:
                with sql.connect(**conn_kwargs) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        return cursor.fetchall()
            except Exception as fallback_error:
                raise fallback_error

        if "CERTIFICATE_VERIFY_FAILED" in str(e):
            print("\t⚠️ SSL verification failed. Retrying with REQUESTS_CA_BUNDLE...")
            ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE")
            if ca_bundle and os.path.exists(ca_bundle):
                os.environ["SSL_CERT_FILE"] = ca_bundle
                with sql.connect(**conn_kwargs) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        return cursor.fetchall()
        raise


def clean_schema_key(key: str) -> str:
    """Remove type annotations from schema keys."""
    type_patterns = [
        "[version=2.0].",
        "[type=struct].",
        "[type=array].",
        "[type=long].",
        "[type=double].",
        "[type=string].",
        "[type=boolean].",
        "[type=timestamp].",
        "[type=date].",
        "[type=float].",
        "[type=integer].",
        "[type=int].",
        "[type=null].",
        "[type=map].",
        "[type=object].",
        "[type=bytes].",
    ]

    for pattern in type_patterns:
        key = key.replace(pattern, "")

    return key


def clean_table_description(description: str) -> str:
    """Remove SLO and other metadata from table description."""
    updated_description = (
        description.split("**Meaning of each row**:")[0]
        .split("Data Freshness: This table will be at least")[0]
        .replace("**Table Description**:", "")
        .replace("no_table_description", "")
        .strip()
    )

    patterns = [
        "<code>",
        "</code>",
        "<p>",
        "</p>",
        "<em>",
        "</em>",
    ]
    for pattern in patterns:
        updated_description = updated_description.replace(pattern, "").strip()

    return updated_description


def clean_column_description(description: str, column_name: str) -> str:
    if column_name.startswith("is_"):
        return ""

    patterns = [
        "Queried from biztech_edw_silver.dim_sfdc_account. ",
        "Queried from edw.silver.dim_customer. ",
        "Queried from edw.silver.fct_orders. ",
        "the ",
        "The ",
        "<code>",
        "</code>",
        "<p>",
        "</p>",
        "<br>",
    ]
    for pattern in patterns:
        description = description.replace(pattern, "").strip()

    replacement_patterns = {
        "The Samsara cloud dashboard ID that the data belongs to": "Samsara Cloud Dashboard ID",
        "The ID of the customer device that the data belongs to": "Device ID",
        "The date the event was logged (yyyy-mm-dd).": "Date event was logged",
    }

    for pattern, replacement in replacement_patterns.items():
        description = description.replace(pattern, replacement)

    return description


DB_DENYLIST = [
    "firmware",
    "hardware",
    "hardware_analytics",
    "mdm",
    "cm_heath_report",
    "product_analytics_staging",
    "perf_infra",
    "ev_charging_report",
    "engine_state",
    "billing",
    "fuel_energy_efficiency_report",
    "engine_state_report",
    "data_analytics",
    "cm_health_report",
    "platops",
    "customer360",
    "dataprep",
]


def create_schema_file(
    DB_FORCE_INCLUDE=None, include_all=False, env: Literal["dev", "prod"] = None
) -> None:

    if include_all:
        DB_FORCE_INCLUDE_CLAUSE = "1=1"
    elif DB_FORCE_INCLUDE:
        DB_FORCE_INCLUDE_CLAUSE = "database in ({})".format(
            ", ".join(f"'{db}'" for db in DB_FORCE_INCLUDE)
        )
    else:
        DB_FORCE_INCLUDE_CLAUSE = "1=2"

    query = """select
      concat(IF(urn like '%urn:li:dataPlatform:dbt,edw.%', 'edw.', ''), database, '.', table) as table,
      table_description,
      datahub_schema
FROM
  default.auditlog.datahub_scrapes
where
   date = (
    select
      max(date)
    from
      default.auditlog.datahub_scrapes
  )
  and
  (
  (
    {}

    AND urn not like '%databricks,edw%'
  )
    OR
  (
  (
    num_queries >= 50
    or num_users >= 5
    or certified = True
  )
  and owner is not null
  and not endswith(database, 'bronze')
  and not endswith(database, 'silver')
  and not endswith(database, 'dev')
  and not (table like '%reporting_agg%' and database = 'datamodel_core')
  and not (table in ('dim_devices_fast', 'dim_devices_sensitive') and database = 'datamodel_core')
  and database not in ({})
  )
  )""".format(
        DB_FORCE_INCLUDE_CLAUSE,
        ", ".join(f"'{db}'" for db in DB_DENYLIST),
    )

    print("Generating DataHub schema file (This may take a few seconds)...")
    result_df = run_dbx_query(query, env=env)
    result_dict = {
        row["table"]: {
            "table_description": row["table_description"],
            "datahub_schema": row["datahub_schema"],
        }
        for row in result_df
    }

    # Fix: Handle datahub_schema as a list of tuples instead of a dictionary
    result_dict = {
        row["table"]: {
            "description": clean_table_description(row["table_description"]),
            "schema": {
                clean_schema_key(k): clean_column_description(
                    next((item[1] for item in v if item[0] == "mergedDescription"), ""),
                    k,
                )
                for k, v in row["datahub_schema"]
                if isinstance(v, list) and len(v) > 0
            },
        }
        for row in result_df
    }

    print(
        f"✅ Done generating DataHub schema file. Number of tables: {len(result_dict)}\n"
    )

    # count number of tables per db
    db_counts = {}
    for table, data in result_dict.items():
        table_unpacked = table.split(".")
        if len(table_unpacked) == 2:
            db = table_unpacked[0]
        elif len(table_unpacked) == 3:
            db = table_unpacked[1]
        else:
            raise ValueError(f"Unexpected table format: {table}")
        if db not in db_counts:
            db_counts[db] = 0
        db_counts[db] += 1

    # print("\n".join([f"{db}: {count}" for db, count in db_counts.items()]))

    # Format the JSON once with desired formatting options
    formatted_json = json.dumps(result_dict, indent=2, sort_keys=True)

    return formatted_json


if __name__ == "__main__":
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate DataHub schema file")
    parser.add_argument(
        "--suffix",
        "-s",
        type=str,
        default="",
        help="Suffix to append to the output filename (default: empty)",
    )
    parser.add_argument(
        "--force-include",
        "-f",
        type=str,
        default="",
        help="Comma-separated list of databases to force include",
    )
    parser.add_argument(
        "--cleanup",
        "-c",
        action="store_true",
        help="Remove cached datahub.json files",
    )
    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Include all tables in the output",
    )
    parser.add_argument(
        "--source",
        "-sc",
        action="store_true",
        help="Include source code map in the exported context",
    )
    args = parser.parse_args()

    if args.cleanup:
        # Remove all files in the cache directory
        cache_dir = os.path.expanduser(
            f"{os.environ.get('BACKEND_ROOT', os.environ.get('HOME') + '/co/backend')}/go/src/samsaradev.io/infra/dataplatform/datahub/llm/cache"
        )
        if os.path.exists(cache_dir):
            for file in os.listdir(cache_dir):
                file_path = os.path.join(cache_dir, file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    print(f"Error: {e}")
        print("Cleaned up cache directory")
        exit()

    file_suffix = args.suffix
    file_suffix = ("_" + file_suffix).replace("__", "_")
    DB_FORCE_INCLUDE = args.force_include.split(",")

    if file_suffix == "_":
        file_suffix = ""

    formatted_json = create_schema_file(DB_FORCE_INCLUDE, args.all)
    backend_root = os.environ.get(
        "BACKEND_ROOT", os.environ.get("HOME") + "/co/backend"
    )

    # create llm/cache directory if it doesn't exist
    os.makedirs(
        os.path.expanduser(
            f"{backend_root}/go/src/samsaradev.io/infra/dataplatform/datahub/llm/cache"
        ),
        exist_ok=True,
    )

    # Write to local file
    output_path = os.path.expanduser(
        f"{backend_root}/go/src/samsaradev.io/infra/dataplatform/datahub/llm/cache/datahub{file_suffix}.json"
    )

    print(f"Writing to {output_path}")

    with open(output_path, "w") as f:
        f.write(formatted_json)
        # print size of datahub.json

    # remove existing symlink
    if os.path.exists(
        os.path.expanduser(
            f"{backend_root}/../datahubcursorllm/datahub{file_suffix}.json"
        )
    ):
        os.remove(
            os.path.expanduser(
                f"{backend_root}/../datahubcursorllm/datahub{file_suffix}.json"
            )
        )
    os.symlink(
        output_path,
        os.path.expanduser(
            f"{backend_root}/../datahubcursorllm/datahub{file_suffix}.json"
        ),
    )
    print(
        f"Size of datahub{file_suffix}.json: {round(os.path.getsize(output_path) / 1024 / 1024, 4)} MB"
    )

    if args.source:

        query = """with t as (
                select
                    concat(database, '.', table) as tbl,
                    replace(
                    replace(
                        regexp_replace(github_url, '#L.*', ''),
                        'https://github.com/samsara-dev/backend/blob/master/',
                        ''
                    ),
                    '//',
                    '/'
                    ) as source_code_location
                from
                    auditlog.datahub_scrapes
                where
                    datahub_scrapes.github_url is not null
                    and date = (
                    select
                        max(date)
                    from
                        auditlog.datahub_scrapes
                    )
                )
                select
                *
                from
                t
                where
                source_code_location not like '%github.com%'
                and source_code_location like '%dataplatform%'
                order by 2
        """
        print("Generating source code map (This may take a few seconds)...")
        result_df = run_dbx_query(query)
        result_dict = {row["tbl"]: row["source_code_location"] for row in result_df}

        output_path = os.path.expanduser(
            f"{backend_root}/../datahubcursorllm/source_code_map.json"
        )
        with open(output_path, "w") as f:
            f.write(json.dumps(result_dict, indent=2, sort_keys=True))
        print("✅ Done generating source code map.\n")
