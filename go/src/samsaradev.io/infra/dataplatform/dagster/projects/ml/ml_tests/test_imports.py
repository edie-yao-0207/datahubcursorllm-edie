"""Tests for module imports and dependencies."""


def test_dagster_imports():
    """Test that dagster modules can be imported."""
    try:
        from dagster import Config, Definitions, op

        assert Config is not None
        assert Definitions is not None
        assert op is not None
    except ImportError as e:
        raise AssertionError(f"Failed to import dagster modules: {e}")


def test_pyspark_imports():
    """Test that pyspark modules can be imported."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructField, StructType

        assert SparkSession is not None
        assert F is not None
        assert StructType is not None
        assert StructField is not None
    except ImportError as e:
        raise AssertionError(f"Failed to import pyspark modules: {e}")


def test_ml_package_imports():
    """Test that ml package modules can be imported."""
    try:
        from ml import defs
        from ml.common import constants, utils
        from ml.ops import anomaly_detection
        from ml.resources import databricks_cluster_specs

        assert defs is not None
        assert anomaly_detection is not None
        assert databricks_cluster_specs is not None
        assert constants is not None
        assert utils is not None
    except ImportError as e:
        raise AssertionError(f"Failed to import ml modules: {e}")


def test_scientific_libraries():
    """Test that scientific computing libraries can be imported."""
    try:
        import h3
        import numpy as np
        import pandas as pd
        import shapely
        from scipy.spatial import ConvexHull
        from sklearn.cluster import DBSCAN

        assert h3 is not None
        assert np is not None
        assert pd is not None
        assert shapely is not None
        assert ConvexHull is not None
        assert DBSCAN is not None
    except ImportError as e:
        raise AssertionError(f"Failed to import scientific libraries: {e}")
