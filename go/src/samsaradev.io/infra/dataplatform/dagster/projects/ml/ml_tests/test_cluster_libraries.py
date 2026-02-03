"""Tests for cluster library configuration."""

import os


def test_cluster_libraries_match_requirements():
    """Test that cluster libraries match requirements.txt."""
    from ml.resources.databricks_cluster_specs import DEFAULT_CLUSTER_LIBRARIES

    requirements_file_path = os.path.join(
        os.path.dirname(__file__), "..", "requirements.txt"
    )

    CLUSTER_LIBRARY_IGNORE_PACKAGES = [
        "databricks-sdk",
        "sqllineage",
    ]

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
        "dagster-celery[flower,redis,kubernetes]",
        "dagster-k8s",
        "dagster-celery-k8s",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "dagster-pandas",
        "numpy",
        "sqlglot",
        "h3",
        "shapely",
        "jsonschema",
    ]

    with open(requirements_file_path, "r") as f:
        requirements = [x.replace("\n", "").split(" ")[0] for x in f.readlines()]
        requirements = [x for x in requirements if x != "" and len(x) > 3]

    # Check that cluster libraries are in requirements
    for cluster_library in DEFAULT_CLUSTER_LIBRARIES:
        if cluster_library.get("whl"):
            continue

        package = cluster_library.get("pypi", {}).get("package", "")
        package_name = package.split("==")[0]

        assert (
            package in requirements
            or package_name in requirements
            or package_name in CLUSTER_LIBRARY_IGNORE_PACKAGES
        ), f"Cluster library {package} not found in requirements.txt"

    # Check that requirements are in cluster libraries
    cluster_packages = [
        lib.get("pypi", {}).get("package", "").split("==")[0]
        for lib in DEFAULT_CLUSTER_LIBRARIES
        if "pypi" in lib
    ]

    for requirement in requirements:
        requirement_name = requirement.split("==")[0]
        assert (
            requirement in cluster_packages
            or requirement_name in cluster_packages
            or requirement_name in REQUIREMENTS_IGNORE_PACKAGES
        ), f"Requirement {requirement} not found in cluster libraries"
