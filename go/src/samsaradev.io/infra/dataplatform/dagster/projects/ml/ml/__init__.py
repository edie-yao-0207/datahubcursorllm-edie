import logging

from dagster import Definitions

from .common.loaders import load_job_defs_schedules
from .common.utils import AWSRegions
from .resources import databricks_cluster_specs

logger = logging.getLogger(__name__)


# Define region-specific resources
databricks_us = databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
    region=AWSRegions.US_WEST_2.value,
    instance_pool_type=(databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY),
    spark_conf_overrides={
        "spark.databricks.sql.initial.catalog.name": "default",
    },
)
logger.info(
    f"[__init__] Created databricks_us resource with "
    f"region={AWSRegions.US_WEST_2.value}"
)

databricks_eu = databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
    region=AWSRegions.EU_WEST_1.value,
    max_workers=4,
    instance_pool_type=(databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY),
    spark_conf_overrides={
        "spark.databricks.sql.initial.catalog.name": "default",
    },
)
logger.info(
    f"[__init__] Created databricks_eu resource with "
    f"region={AWSRegions.EU_WEST_1.value}"
)

databricks_ca = databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
    region=AWSRegions.CA_CENTRAL_1.value,
    max_workers=4,
    instance_pool_type=(databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY),
    spark_conf_overrides={
        "spark.databricks.sql.initial.catalog.name": "default",
    },
)
logger.info(
    f"[__init__] Created databricks_ca resource with "
    f"region={AWSRegions.CA_CENTRAL_1.value}"
)

# Register region-specific Databricks resources
# Maps region name -> resource instance
resource_registry = {
    AWSRegions.US_WEST_2.value: databricks_us,
    AWSRegions.EU_WEST_1.value: databricks_eu,
    AWSRegions.CA_CENTRAL_1.value: databricks_ca,
}

# Load all jobs and schedules from JSON configs
# Ops are auto-discovered from the ops module based on JSON file names
# (e.g., "anomaly_detection.json" -> "anomaly_detection_op" from ops module)
logger.info("[__init__] Loading job definitions and schedules...")
job_defs, schedule_defs = load_job_defs_schedules(
    resource_registry=resource_registry,
)
logger.info(
    f"[__init__] Loaded {len(job_defs)} jobs and {len(schedule_defs)} schedules"
)
for job in job_defs:
    logger.info(f"[__init__]   - Job: {job.name}")

defs = Definitions(
    jobs=job_defs,
    schedules=schedule_defs,
    resources={
        # Region-suffixed resources for backwards compatibility
        # Note: anomaly_detection jobs use job-level resource_defs,
        # not global resources
        "databricks_pyspark_step_launcher_eu": databricks_eu,
        "databricks_pyspark_step_launcher_ca": databricks_ca,
    },
)
logger.info("[__init__] Definitions created successfully")
