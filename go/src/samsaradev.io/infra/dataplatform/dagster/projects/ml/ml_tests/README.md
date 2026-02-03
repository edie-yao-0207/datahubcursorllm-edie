# ML Project Tests

Comprehensive test suite for the ML project, modeled after the datamodel_tests structure.

## Test Files

### 1. `test_ml.py`
Basic tests for project structure and definitions:
- Tests that Dagster definitions load correctly
- Verifies jobs are properly defined
- Checks that resources (Databricks step launchers) are configured
- Validates cluster libraries configuration
- Tests that requirements.txt is valid
- Verifies all ops modules can be imported

### 2. `test_anomaly_detection.py`
Comprehensive tests for anomaly detection functionality:
- Schema validation tests (`_get_cluster_schema`)
- H3 polygon boundary retrieval tests
- DBSCAN clustering tests with various scenarios:
  - Basic clustering with two clusters
  - Single point (insufficient data)
  - All stops at same location
- Deterministic sorting validation
- Logging verification

### 3. `test_cluster_libraries.py`
Tests for cluster library configuration:
- Validates that cluster libraries match requirements.txt
- Ensures consistency between runtime and build-time dependencies
- Checks for ignored packages (like databricks-sdk, sqllineage)

### 4. `test_imports.py`
Tests for module imports and dependencies:
- Dagster imports (Config, Definitions, op)
- PySpark imports (SparkSession, functions, types)
- ML package imports (defs, ops, resources, common)
- Scientific libraries (h3, numpy, pandas, shapely, scipy, sklearn)

### 5. `test_deterministic_sorting.py`
Tests for deterministic sorting implementation:
- Verifies `cluster_stops_spark` has explicit sorting
- Ensures sorting is documented to prevent non-deterministic results

## Running Tests Locally

Tests require PySpark and other dependencies:

```bash
cd go/src/samsaradev.io/infra/dataplatform/dagster/projects/ml
pip install pytest pyspark==3.3.0
SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest ml_tests/
```

## Running Tests in Docker

Tests automatically run during Docker build (see `docker/ml/Dockerfile`):

```dockerfile
RUN pip install pytest pyspark==3.3.0
RUN SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /ml/ml_tests/
```

## Test Coverage

The test suite covers:
- ✅ Project structure and configuration
- ✅ Dagster definitions (jobs, resources)
- ✅ Anomaly detection algorithms (DBSCAN, H3)
- ✅ Deterministic clustering behavior
- ✅ Library and dependency management
- ✅ Module imports and API contracts

## CI/CD Integration

Tests run automatically during:
1. Docker image build (local and CI)
2. PR checks via Buildkite
3. Deployment pipeline

If tests fail, the Docker build will fail and the image won't be pushed to ECR.

