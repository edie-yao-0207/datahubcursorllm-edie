# just importing the module will fail if the DAG can't be built
import subprocess

import datamodel.assets.examples.example_dq_pipeline_assets
from dagster import (
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
from datamodel import defs
from datamodel.common.utils import AWSRegions
from datamodel.resources.deltalake_io_managers import SCHEMA_VALIDATION_SKIP_LIST

DELTALAKE_DATE_PARTITION_NAME = "date"


def test_assets_key():

    repo_def = defs.get_repository_def()
    for asset_key, _ in repo_def.assets_defs_by_key.items():
        assert (
            len(asset_key.path) == 3
        ), f"{asset_key} must have length of 3 in format (region, database, table)."

        region = asset_key.path[0]
        assert region in [
            e.value for e in AWSRegions
        ], f"Invalid AWS region ({region}). Must be one of {[e.value for e in AWSRegions]}."


def test_assets_partitions_def():

    repo_def = defs.get_repository_def()
    for asset_key, asset_def in repo_def.assets_defs_by_key.items():

        metadata = asset_def.metadata_by_key[asset_key] or {}

        # Temporary hack to skip over DQ Assets since their partitioning does not match the output schema of auditlog.dq_check_log.
        if asset_key.path[-1].startswith("dq_") and "dq_checks" in metadata:
            continue
        # Temporary hack to skip over assets that are known to not have a schema specified because their schemas change
        # rapidly or suddenly and we want to capture those changes.
        if f"{asset_key.path[-2]}.{asset_key.path[-1]}" in SCHEMA_VALIDATION_SKIP_LIST:
            continue

        partitions_def = asset_def.partitions_def
        schema = metadata.get("schema") or []

        columns = [field.get("name") for field in schema]

        if partitions_def:

            if isinstance(
                partitions_def, (DailyPartitionsDefinition, MonthlyPartitionsDefinition)
            ):
                assert (
                    DELTALAKE_DATE_PARTITION_NAME in columns or "dq_checks" in metadata
                ), f"Error ({asset_key}): Single dimension partitioned assets must have a daily or monthly partition definition. The column must be named '{DELTALAKE_DATE_PARTITION_NAME}'."

                assert (
                    partitions_def.timezone == "UTC"
                ), f"({asset_key}) - time partitioned definition must have timezone 'UTC'"

            elif isinstance(partitions_def, MultiPartitionsDefinition):
                assert (
                    partitions_def.partitions_defs[0].name
                    == DELTALAKE_DATE_PARTITION_NAME
                    and isinstance(
                        partitions_def.partitions_defs[0].partitions_def,
                        (DailyPartitionsDefinition, MonthlyPartitionsDefinition),
                    )
                    and DELTALAKE_DATE_PARTITION_NAME in columns
                ), f"({asset_key}) - Multi dimension partitioned assets must have a daily or monthly primary partition definition. The column must be named '{DELTALAKE_DATE_PARTITION_NAME}'."

                for partition_dimension_def in partitions_def.partitions_defs:
                    if isinstance(
                        partition_dimension_def.partitions_def,
                        TimeWindowPartitionsDefinition,
                    ):
                        assert (
                            partition_dimension_def.partitions_def.timezone == "UTC"
                        ), f"({asset_key}) - time partitioned definition must have timezone 'UTC'"

            else:
                assert (
                    False
                ), f"({asset_key}) - unsupported partitions definition ({partitions_def})"


def test_patched_subtypes_library():
    from datahub.ingestion.source.common.subtypes import DatasetSubTypes

    assert (
        DatasetSubTypes.METRIC == "Metric"
    ), "unable to import modified patched version of datahub subtypes library, check Dockerfile"


def test_patched_unity_library():
    # Run the UnityCatalogSource import and check in a shell with the virtualenv activated
    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "source /datahub_env_unity/bin/activate && "
                "python -c '"
                "from datahub.ingestion.source.unity.source import UnityCatalogSource; "
                'assert UnityCatalogSource.platform == "databricks", '
                '"UnityCatalogSource.platform != databricks"\''
            ),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Test failed in virtualenv with stderr:\n{result.stderr}\n"
            f"stdout:\n{result.stdout}"
        )


def test_databricks_sdk_execute_statement_response_import():
    """Test that ExecuteStatementResponse can be imported from databricks.sdk.service.sql"""
    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "source /datahub_env_unity/bin/activate && "
                "python -c '"
                "from databricks.sdk.service.sql import ExecuteStatementResponse; "
                'print("ExecuteStatementResponse imported successfully")\''
            ),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"ExecuteStatementResponse import failed in virtualenv with stderr:\n{result.stderr}\n"
            f"stdout:\n{result.stdout}"
        )


def test_dbt_node_constructor_compatibility():
    """Test that DBTNode constructor is compatible with expected parameters"""
    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "source /datahub_env_unity/bin/activate && "
                "python -c '"
                "from datahub.ingestion.source.dbt.dbt_common import DBTNode; "
                "import inspect; "
                "sig = inspect.signature(DBTNode.__init__); "
                "params = list(sig.parameters.keys()); "
                'assert "dbt_name" in params, f"dbt_name not found in DBTNode constructor parameters: {params}"; '
                'print("DBTNode constructor is compatible")\''
            ),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"DBTNode constructor compatibility test failed with stderr:\n{result.stderr}\n"
            f"stdout:\n{result.stdout}"
        )


def test_cluster_libraries():
    from datamodel.resources.databricks_cluster_specs import (
        DEFAULT_CLUSTER_LIBRARIES as l1,
    )
    from dataweb._core.resources.databricks.constants import (
        DEFAULT_CLUSTER_LIBRARIES as l2,
    )

    requirements_file_map = {
        "datamodel": "datamodel/requirements.txt",
        "dataweb": "dataweb/requirements.txt",
    }

    CLUSTER_LIBRARY_IGNORE_PACKAGES = ["databricks-sdk", "sqllineage"]
    REQUIREMENTS_IGNORE_PACKAGES = [
        "aiobotocore",
        "boto3",
        "delta-spark",
        "fsspec",
        "mock",
        "pandas",
        "pyarrow",
        "pyspark",
        "pytest",
        "s3fs",
        "scikit-learn",
        "scipy",
        "sqlalchemy",
        "sql-formatter",
        "dagster-postgres",
        "dagster-webserver",
        "acryl-datahub[sql-queries]",
        "acryl-datahub[unity-catalog]",
        "acryl-datahub[tableau]",
        "dagster-celery[flower,redis,kubernetes]",
        "dagster-k8s",
        "dagster-celery-k8s",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "dagster-pandas",
        "numpy",
        "sqlglot",
        "optuna",
    ]

    for project, cluster_libraries in [("datamodel", l1), ("dataweb", l2)]:
        with open(requirements_file_map[project], "r") as f:
            requirements = [x.replace("\n", "").split(" ")[0] for x in f.readlines()]
            requirements = [x for x in requirements if x != "" and len(x) > 3]

        for cluster_library in cluster_libraries:
            # ensure the package versions are the same for the cluster libraries and the requirements file after reading in the requirements file
            if cluster_library.get("whl"):
                continue

            assert (
                cluster_library.get("pypi", {}).get("package") in requirements
                or cluster_library.get("pypi", {}).get("package", "").split("==")[0]
                in requirements
                or cluster_library.get("pypi", {}).get("package", "").split("==")[0]
                in CLUSTER_LIBRARY_IGNORE_PACKAGES
            ), f"cluster library package version should match the requirements file {requirements}"

        for requirement in requirements:
            assert (
                requirement
                in [
                    cluster_library.get("pypi", {}).get("package")
                    for cluster_library in cluster_libraries
                ]
                or requirement
                in [
                    cluster_library.get("pypi", {}).get("package", "").split("==")[0]
                    for cluster_library in cluster_libraries
                ]
                or requirement.split("==")[0]
                in [
                    cluster_library.get("pypi", {}).get("package", "").split("==")[0]
                    for cluster_library in cluster_libraries
                ]
                or requirement.split("==")[0] in REQUIREMENTS_IGNORE_PACKAGES
            ), f"requirements file version should be in cluster libraries requirements:{requirements}; cluster_libraries:{cluster_libraries} run in {project} project"


def test_subprocess_migration():
    """
    Test that the subprocess migration works correctly.
    This test verifies that the source code has been properly migrated from
    PipesSubprocessClient to standard subprocess.run.
    """
    import os

    # Read the metadata_jobs.py file directly to avoid import issues
    metadata_jobs_path = os.path.join(
        os.path.dirname(__file__), "..", "datamodel", "jobs", "metadata_jobs.py"
    )

    with open(metadata_jobs_path, "r") as f:
        source_code = f.read()

    # Verify dagster-shell is not imported
    assert "dagster_shell" not in source_code, "dagster_shell should not be imported"
    assert "ShellOpConfig" not in source_code, "ShellOpConfig should not be used"
    assert (
        "from dagster_shell" not in source_code
    ), "dagster_shell imports should be removed"

    # Verify PipesSubprocessClient is NOT imported (we replaced it with subprocess)
    assert (
        "PipesSubprocessClient" not in source_code
    ), "PipesSubprocessClient should not be imported (replaced with subprocess)"

    # Verify subprocess is imported and used
    assert "import subprocess" in source_code, "subprocess should be imported"
    assert "subprocess.run(" in source_code, "subprocess.run should be called"

    # Verify old execute function is not used
    assert (
        "execute(" not in source_code or source_code.count("execute(") <= 1
    ), "Old execute function should not be used"

    # Verify command validation is present
    assert (
        "if not command_parts:" in source_code
    ), "Command validation should be present to prevent IndexError"


def test_pipes_subprocess_client_requirements_files():
    """
    Test that dagster-shell has been removed from requirements files.
    """
    import os

    # Check main requirements.txt
    requirements_path = os.path.join(
        os.path.dirname(__file__), "..", "requirements.txt"
    )

    if os.path.exists(requirements_path):
        with open(requirements_path, "r") as f:
            requirements_content = f.read()

        assert (
            "dagster-shell" not in requirements_content
        ), "dagster-shell should be removed from requirements.txt"

    # Check dataweb requirements.txt
    dataweb_requirements_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "dataweb", "requirements.txt"
    )

    if os.path.exists(dataweb_requirements_path):
        with open(dataweb_requirements_path, "r") as f:
            dataweb_requirements_content = f.read()

        assert (
            "dagster-shell" not in dataweb_requirements_content
        ), "dagster-shell should be removed from dataweb requirements.txt"


def test_job_definitions_after_subprocess_migration():
    """
    Test that job definitions are properly updated after subprocess migration.
    PipesSubprocessClient should be removed, but InMemoryIOManager should remain.
    """
    import os

    # Read the metadata_jobs.py file directly
    metadata_jobs_path = os.path.join(
        os.path.dirname(__file__), "..", "datamodel", "jobs", "metadata_jobs.py"
    )

    with open(metadata_jobs_path, "r") as f:
        source_code = f.read()

    # Verify that PipesSubprocessClient is NOT in job definitions
    assert (
        '"pipes_subprocess_client": PipesSubprocessClient()' not in source_code
    ), "Jobs should NOT include PipesSubprocessClient in resource_defs (replaced with subprocess)"

    # Verify that PipesSubprocessClient references are completely removed
    pipes_client_count = source_code.count("PipesSubprocessClient")
    assert (
        pipes_client_count == 0
    ), f"PipesSubprocessClient should be completely removed, found {pipes_client_count} references"

    # Verify that InMemoryIOManager is still included where needed
    assert (
        '"in_memory_io_manager": InMemoryIOManager()' in source_code
    ), "Jobs should still include InMemoryIOManager in resource_defs"


def test_no_dagster_shell_imports():
    """
    Test that dagster-shell is no longer imported anywhere in the metadata jobs.
    """
    # Get the module's source code
    import inspect

    import datamodel.jobs.metadata_jobs as metadata_jobs_module

    source = inspect.getsource(metadata_jobs_module)

    # Verify dagster-shell is not imported
    assert "dagster_shell" not in source, "dagster_shell should not be imported"
    assert "ShellOpConfig" not in source, "ShellOpConfig should not be used"
    assert "from dagster_shell" not in source, "dagster_shell imports should be removed"

    # Verify PipesSubprocessClient is NOT imported (replaced with subprocess)
    assert (
        "PipesSubprocessClient" not in source
    ), "PipesSubprocessClient should not be imported"

    # Verify subprocess is imported instead
    assert "import subprocess" in source, "subprocess should be imported"
