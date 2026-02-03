"""Basic tests for ml project structure and definitions."""

from ml import defs


def test_defs_loads():
    """Test that the Dagster definitions can be loaded without errors."""
    assert defs is not None
    repo_def = defs.get_repository_def()
    assert repo_def is not None


def test_jobs_defined():
    """Test that expected jobs are defined."""
    job_names = list(defs.get_repository_def().get_all_jobs())

    # Check that our jobs exist
    assert len(job_names) > 0
    assert any("anomaly_detection" in job.name for job in job_names)


def test_resources_defined():
    """Test that required resources are defined."""
    # Check for region-suffixed databricks resources (backwards compatibility)
    assert "databricks_pyspark_step_launcher_eu" in defs.resources
    assert "databricks_pyspark_step_launcher_ca" in defs.resources

    # Check that anomaly_detection jobs have job-level resource bindings
    repo_def = defs.get_repository_def()
    anomaly_jobs = ["anomaly_detection", "anomaly_detection_eu", "anomaly_detection_ca"]
    for job_name in anomaly_jobs:
        job = repo_def.get_job(job_name)
        assert "databricks_pyspark_step_launcher" in job.resource_defs


def test_cluster_libraries():
    """Test that cluster libraries are properly configured."""
    from ml.resources.databricks_cluster_specs import DEFAULT_CLUSTER_LIBRARIES

    # Check that libraries list is defined and not empty
    assert DEFAULT_CLUSTER_LIBRARIES is not None
    assert len(DEFAULT_CLUSTER_LIBRARIES) > 0

    # Check that each library has the expected structure
    for lib in DEFAULT_CLUSTER_LIBRARIES:
        lib_str = f"Library missing pypi or whl key: {lib}"
        assert "pypi" in lib or "whl" in lib, lib_str
        if "pypi" in lib:
            pkg_str = f"Library missing package key: {lib}"
            assert "package" in lib["pypi"], pkg_str


def test_requirements_file_exists():
    """Test that requirements.txt exists and is valid."""
    import os

    requirements_path = os.path.join(
        os.path.dirname(__file__), "..", "requirements.txt"
    )

    assert os.path.exists(requirements_path), "requirements.txt not found"

    with open(requirements_path, "r") as f:
        requirements = f.readlines()

    # Should have some requirements
    assert len(requirements) > 0

    # Check for key dependencies
    requirements_text = "".join(requirements)
    assert "dagster" in requirements_text
    assert "pyspark" in requirements_text


def test_no_import_errors():
    """Test that all ops modules can be imported without errors."""
    try:
        from ml.ops import anomaly_detection

        assert anomaly_detection is not None
    except ImportError as e:
        raise AssertionError(f"Failed to import ops.anomaly_detection: {e}")


def test_anomaly_detection_op_defined():
    """Test that the anomaly detection op is properly defined."""
    from ml.ops.anomaly_detection import anomaly_detection_op

    assert anomaly_detection_op is not None
    assert hasattr(anomaly_detection_op, "required_resource_keys")
    resource_keys = anomaly_detection_op.required_resource_keys
    assert "databricks_pyspark_step_launcher" in resource_keys


def test_databricks_jobs_use_in_process_executor():
    """Test that jobs using Databricks step launchers use in_process_executor.

    Jobs that use databricks_pyspark_step_launcher must use in_process_executor
    instead of the default multiprocess_executor. The multiprocess_executor
    loses communication with remote Databricks processes, causing errors like:
    'Execution exited with steps in an unknown state to this process.'
    """
    from dagster import in_process_executor

    repo_def = defs.get_repository_def()
    jobs = list(repo_def.get_all_jobs())

    for job in jobs:
        job_def = repo_def.get_job(job.name)

        # Get all ops in the job
        ops_in_job = list(job_def.graph.nodes)

        # Check if any op requires databricks_pyspark_step_launcher
        uses_databricks = False
        for node in ops_in_job:
            op_def = node.definition
            if hasattr(op_def, "required_resource_keys"):
                resource_keys = op_def.required_resource_keys
                if "databricks_pyspark_step_launcher" in resource_keys:
                    uses_databricks = True
                    break

        if uses_databricks:
            # Job must use in_process_executor
            executor = job_def.executor_def
            assert executor == in_process_executor, (
                f"Job '{job.name}' uses databricks_pyspark_step_launcher but "
                f"has executor '{executor}'. It must use in_process_executor "
                "to work properly with remote Databricks execution. "
                "Add 'executor_def=in_process_executor' to the @job decorator."
            )
