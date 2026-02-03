"""Test configuration for dataplatform."""

import os.path
import time
from pathlib import Path
from typing import Dict, FrozenSet, Generator

import pytest
from _pytest.fixtures import FixtureRequest
from _pytest.monkeypatch import MonkeyPatch
from _pytest.tmpdir import TempPathFactory
from pyspark.sql import SparkSession

_EMPTY_SET: FrozenSet[str] = frozenset()


@pytest.fixture(scope="session", autouse=True)
def set_aws_default_region() -> Generator[None, None, None]:
    """Set AWS default region so moto tests run without AWS profiles."""
    patcher = MonkeyPatch()
    try:
        patcher.setenv("AWS_DEFAULT_REGION", "us-west-2")
        patcher.setenv("AWS_REGION", "us-west-2")
        patcher.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
        patcher.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/test.json")
        yield
    finally:
        patcher.undo()


@pytest.fixture(scope="session", autouse=True)
def forced_utc_everywhere() -> Generator[None, None, None]:
    """Force a given test to run inside of a UTC timezone context.

    Some internal Python functions default to localtime behavior, which can
    cause tests to give different results depending where they are run.
    Notably, the Spark representation of TimestampType. Here we override the
    system timezone to UTC for all test executions.

    This is, in part, to workaround https://issues.apache.org/jira/browse/SPARK-25244
    """
    patcher = MonkeyPatch()
    try:
        patcher.setenv("TZ", "UTC")
        time.tzset()
        yield
    finally:
        patcher.undo()
        time.tzset()


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory: TempPathFactory) -> SparkSession:
    metastore_dir = tmp_path_factory.mktemp("metastore")
    extra_java_options = ["-Duser.timezone=GMT"]
    driver_extra_java_options = extra_java_options + [
        f"-Dderby.system.home={metastore_dir}/derby"
    ]

    config: Dict[str, str] = {
        "spark.driver.extraJavaOptions": " ".join(driver_extra_java_options),
        "spark.executor.extraJavaOptions": " ".join(extra_java_options),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "1",
        "spark.sql.warehouse.dir": f"{metastore_dir}/spark-warehouse",
        "spark.sql.sources.default": "delta",
        "spark.jars.packages": "io.delta:delta-core_2.12:2.1.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Increase number of tries to acquire an open port because we run pytest in parallel in CI.
        # https://github.com/holdenk/spark-testing-base/issues/279
        "spark.port.maxRetries": "100",
        # disable spark ui / don't print to console,
        # inspired from https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.dagGraph.retainedRootRDDs": "1",
        "spark.ui.retainedJobs": "1",
        "spark.ui.retainedStages": "1",
        "spark.ui.retainedTasks": "1",
        "spark.sql.ui.retainedExecutions": "1",
        "spark.worker.ui.retainedExecutors": "1",
        "spark.worker.ui.retainedDrivers": "1",
        # reduce delta snapshot repartitioning,
        # copied from delta tests https://github.com/delta-io/delta/blob/master/build.sbt#L146-L155
        "spark.databricks.delta.snapshotPartitions": "2",
    }

    # build a spark session with only 1 executor, we don't need more for unit tests
    builder = SparkSession.builder.master("local[1]").appName("test suite")
    for key, value in config.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()

    return session
