# ML Tests - How to Run

## Using dagster-setup.sh with -mltests flag (Recommended)

The easiest way to run the ML tests is using the `-mltests` flag with `dagster-setup.sh`:

```bash
cd /Users/doug.stone/co/backend
bin/dagster-setup.sh -mltests
```

When prompted, enter:
```
ml
```

### What happens:
1. ✅ Automatically uses `uv` for fast dependency management
2. ✅ Sets up Python virtual environment with correct version
3. ✅ Installs all ML project dependencies
4. ✅ Installs test dependencies (`pytest`, `pyspark==3.3.0`)
5. ✅ Runs all tests with `pytest ml_tests/ -v`
6. ✅ Reports test results and exits

### Expected output:
```
Welcome to dagster-setup.sh! This script is going to setup your local Dagster development
environment...

✅ Installing darwin tools.
✅ Installing uv.
🔑 Checking Databricks token configuration...
✅ All set! Go write some great Dagster pipelines!
ML tests will run in the next step.

🧪 Running ML project tests...
Installing test dependencies...
Running tests with pytest...

============================= test session starts ==============================
ml_tests/test_ml.py::test_defs_loads PASSED                             [ 10%]
ml_tests/test_ml.py::test_jobs_defined PASSED                           [ 20%]
ml_tests/test_ml.py::test_resources_defined PASSED                      [ 30%]
ml_tests/test_cluster_libraries.py::test_cluster_libraries_match_requirements PASSED [ 40%]
ml_tests/test_imports.py::test_dagster_imports PASSED                   [ 50%]
ml_tests/test_imports.py::test_pyspark_imports PASSED                   [ 60%]
ml_tests/test_imports.py::test_ml_package_imports PASSED                [ 70%]
ml_tests/test_imports.py::test_scientific_libraries PASSED              [ 80%]
ml_tests/test_deterministic_sorting.py::test_cluster_stops_spark_has_sorting PASSED [ 90%]
ml_tests/test_deterministic_sorting.py::test_sorting_prevents_non_determinism PASSED [100%]
============================= 10 passed in X.XXs ===============================

✅ All ML tests passed!
```

## Manual Testing (Alternative)

If you already have the environment set up:

```bash
cd /Users/doug.stone/co/backend/go/src/samsaradev.io/infra/dataplatform/dagster/projects/ml
source .venv/bin/activate

# Install test dependencies if needed
uv pip install pytest pyspark==3.3.0

# Run all tests
SPARK_VERSION=3.3 MODE=PYTEST_RUN PYTHONPATH=. pytest ml_tests/ -v

# Or run specific test files
SPARK_VERSION=3.3 MODE=PYTEST_RUN PYTHONPATH=. pytest ml_tests/test_ml.py -v
SPARK_VERSION=3.3 MODE=PYTEST_RUN PYTHONPATH=. pytest ml_tests/test_anomaly_detection.py -v
```

## In Docker (CI/CD)

Tests run automatically during Docker build:

```bash
# Build the ML Docker image (tests run as part of build)
cd /Users/doug.stone/co/backend
docker build -f go/src/samsaradev.io/infra/dataplatform/dagster/docker/ml/Dockerfile .
```

The Dockerfile contains:
```dockerfile
RUN pip install pytest pyspark==3.3.0
RUN SPARK_VERSION=3.3 MODE=PYTEST_RUN pytest /ml/ml_tests/
```

If tests fail, the Docker build will fail and the image won't be created.

## Test Coverage

The test suite includes:

- **test_ml.py** - Basic project structure and Dagster definitions
- **test_anomaly_detection.py** - Core DBSCAN clustering algorithm tests
- **test_cluster_libraries.py** - Dependency management validation
- **test_imports.py** - Module import verification
- **test_deterministic_sorting.py** - Sorting implementation validation

## Troubleshooting

### Tests fail to import modules
Make sure dependencies are installed:
```bash
source .venv/bin/activate
uv pip install -r requirements.txt
```

### Java errors (PySpark)
Install Java if not available:
```bash
brew install openjdk@17
```

### Permission errors with dagster-setup.sh
Fix Homebrew permissions:
```bash
sudo chown -R $(whoami) /opt/homebrew/Cellar
```

## Validation

The `-mltests` flag has been validated to:
- ✅ Correctly parse command line arguments
- ✅ Set USE_UV=true and RUN_ML_TESTS=true
- ✅ Skip Dagster launch when tests are requested
- ✅ Execute run_ml_tests function correctly
- ✅ Report test results and exit with appropriate code

