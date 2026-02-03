import json
import os
import subprocess
import time
from typing import Dict

import requests
from dagster import In, Nothing, Out, op

from ..common.datahub_utils import log_op_duration
from ..common.utils import (
    get_datahub_dbt_token,
    get_datahub_env,
    get_datahub_token,
    get_gms_server,
)
from .datahub.constants import (
    DBT_CLOUD_ACCOUNT_ID,
    DBT_CLOUD_BASE_URL,
    DBT_CLOUD_JOB_IDS,
)


def get_dbt_artifacts_op(team: str):
    """
    Get an op that extracts dbt artifacts for a specific team.

    Args:
        team: The team name to extract artifacts for.

    Returns:
        An op that extracts dbt artifacts.
    """

    @op(
        name=f"extract_dbt_artifacts_to_datahub_{team}",
        ins={"start": In(Nothing)},
        out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    )
    def extract_dbt_artifacts_to_datahub(context) -> int:
        """
        Extract and save dbt artifacts (catalog, manifest, run_results, sources) from dbt Cloud.

        Returns:
            int: 0 if successful, 1 if there was an error
        """
        op_start_time = time.time()

        if team not in DBT_CLOUD_JOB_IDS:
            context.log.error(f"Team {team} not found in DBT_CLOUD_JOB_IDS")
            return 1

        context.log.info(f"team: {team}")

        os.environ["DATAHUB_DBT_API_TOKEN"] = get_datahub_dbt_token()

        # Define job IDs for different artifacts based on team
        JOB_IDS = {
            "catalog.json": DBT_CLOUD_JOB_IDS[team][
                "docs"
            ],  # Special job ID for catalog
            "manifest.json": DBT_CLOUD_JOB_IDS[team]["model"],
            "run_results.json": DBT_CLOUD_JOB_IDS[team]["model"],
            "sources.json": DBT_CLOUD_JOB_IDS[team]["model"],
        }

        def get_api_headers() -> Dict[str, str]:
            """Get the headers required for dbt Cloud API calls."""
            return {"Authorization": f'Bearer {os.environ["DATAHUB_DBT_API_TOKEN"]}'}

        def get_latest_finished_run(job_id: int, success_only: bool = False) -> int:
            """Get the latest finished run ID for a given job."""
            url = f"{DBT_CLOUD_BASE_URL}/{DBT_CLOUD_ACCOUNT_ID}/runs"
            url = f"{url}?job_definition_id={job_id}&order_by=-finished_at&limit=10"

            response = requests.get(url, headers=get_api_headers())
            if response.ok:
                runs = response.json().get("data", [])
                if success_only:
                    finished_runs = [run for run in runs if run.get("status") == 10]
                    status_desc = "successful"
                else:
                    finished_runs = [
                        run for run in runs if run.get("status") in [10, 20, 30]
                    ]
                    status_desc = "finished"

                if finished_runs:
                    context.log.info(
                        f"Found {status_desc} run with ID: {finished_runs[0]['id']}"
                    )
                    return finished_runs[0]["id"]
                else:
                    raise ValueError(f"No {status_desc} runs found for job {job_id}")
            else:
                raise Exception(f"Failed to fetch runs: {response.status_code}")

        def build_artifact_urls() -> Dict[str, str]:
            """Build URLs for each artifact file based on their latest runs."""
            urls = {}
            for artifact_file in JOB_IDS.keys():
                job_id = JOB_IDS[artifact_file]
                try:
                    success_only = artifact_file == "catalog.json"
                    run_id = get_latest_finished_run(job_id, success_only=success_only)
                    urls[
                        artifact_file
                    ] = f"{DBT_CLOUD_BASE_URL}/{DBT_CLOUD_ACCOUNT_ID}/runs/{run_id}/artifacts/{artifact_file}"
                except Exception as e:
                    context.log.error(
                        f"Error getting latest run for {artifact_file}: {str(e)}"
                    )
                    continue
            return urls

        def download_and_save_artifacts(urls: Dict[str, str]) -> None:
            """Download and save artifacts locally."""
            headers = get_api_headers()
            for artifact_file, url in urls.items():
                context.log.info(f"Fetching {artifact_file} from {url}")
                response = requests.request("GET", url, headers=headers)
                if response.ok:
                    with open(f"{team}_{artifact_file}", "w") as f:
                        json.dump(response.json(), f, indent=2)
                    context.log.info(
                        f"Successfully downloaded and saved {artifact_file}"
                    )
                else:
                    context.log.error(
                        f"Failed to fetch {artifact_file}: {response.status_code}"
                    )

        try:
            if os.getenv("MODE") == "DRY_RUN":
                context.log.info("DRY_RUN")
                return 0

            if get_datahub_env() == "prod":
                os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
                os.environ["DATAHUB_GMS_URL"] = get_gms_server()
            else:
                os.environ["DATAHUB_GMS_URL"] = "http://localhost:8080"

            if get_datahub_env() == "prod":
                path_to_recipe = "/datamodel/datamodel/resources/datahub"
            else:
                backend_root = os.getenv("BACKEND_ROOT")
                path_to_recipe = f"{backend_root}/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub"

            # Download dbt artifacts from dbt Cloud and ingest them into DataHub
            # This includes manifest.json, catalog.json, run_results.json, and sources.json
            # which contain table metadata, lineage, and source information
            urls = build_artifact_urls()
            download_and_save_artifacts(urls)

            yaml_file = f"{path_to_recipe}/dbt_recipe_{team}.yaml"

            result = subprocess.getoutput(
                f"/datahub_env_unity/bin/datahub ingest -c {yaml_file}"
            )
            if "Pipeline finished successfully; produced" not in result:
                context.log.error(result)
                raise Exception("Error running datahub ingest")
            else:
                context.log.info(result)

            log_op_duration(op_start_time)
            return 0
        except Exception as e:
            context.log.error(f"Error in dbt artifacts extraction: {str(e)}")
            return 1
        finally:
            # Clean up downloaded artifact files regardless of success or failure
            for artifact_file in urls.keys():
                if os.path.exists(f"{team}_{artifact_file}"):
                    try:
                        os.remove(f"{team}_{artifact_file}")
                        context.log.info(f"Successfully deleted {team}_{artifact_file}")
                    except Exception as e:
                        context.log.warning(
                            f"Failed to delete {team}_{artifact_file}: {str(e)}"
                        )

    return extract_dbt_artifacts_to_datahub
