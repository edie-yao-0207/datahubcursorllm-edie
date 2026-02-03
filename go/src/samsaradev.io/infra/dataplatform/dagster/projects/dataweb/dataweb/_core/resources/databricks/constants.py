from dataclasses import dataclass

from ....userpkgs.constants import AWSRegion


@dataclass
class DagsterDevClusterId:
    US_WEST_2 = "0809-193945-fon8dc5v"
    EU_WEST_1 = "0809-193953-z38tvth5"
    CA_CENTRAL_1 = "0916-202418-xl4399r"


DATABRICKS_VOLUMES_PATH = "/Volumes/s3/dataplatform-deployed-artifacts"

# Known Exceptions raised by Databricks
CLOUD_PROVIDER_LAUNCH_EXCEPTION = "CLOUD_PROVIDER_LAUNCH_FAILURE"
AWS_INSUFFICIENT_INSTANCE_CAPACITY_EXCEPTION = (
    "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE"
)
LIBRARY_EXCEPTION = "Library installation failed"
SHUFFLE_EXCEPTION = "Job aborted due to stage failure"
FAILED_READ_EXCEPTION = "FAILED_READ_FILE.NO_HINT"

DEFAULT_SPARK_VERSION = "15.4.x-scala2.12"
DEFAULT_DRIVER_INSTANCE_TYPE = "md-fleet.xlarge"
DEFAULT_WORKER_INSTANCE_TYPE = "rd-fleet.2xlarge"
DEFAULT_TIMEOUT_SECONDS = 28800
DEFAULT_MAX_WORKERS = 4
DEFAULT_SPARK_CONF = {
    "viewMaterializationDataset": "spark_view_materialization",
    "spark.databricks.delta.stalenessLimit": "1h",
    "spark.storage.decommission.enabled": "true",
    "spark.databricks.delta.properties.defaults.logRetentionDuration": "INTERVAL 30 DAYS",
    "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
    "spark.hadoop.aws.glue.max-error-retries": "10",
    "spark.databricks.hive.metastore.glueCatalog.enabled": "true",
    "spark.hadoop.fs.gs.project.id": "samsara-data",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.storage.decommission.rddBlocks.enabled": "true",
    "spark.driver.maxResultSize": "16g",
    "spark.hadoop.google.cloud.auth.service.account.enable": "true",
    "spark.hadoop.fs.s3a.canned.acl": "BucketOwnerFullControl",
    "spark.databricks.delta.history.metricsEnabled": "true",
    "spark.sql.shuffle.partitions": "auto",
    "spark.databricks.delta.autoCompact.enabled": "auto",
    "temporaryGcsBucket": "samsara-bigquery-spark-connector",
    "spark.decommission.enabled": "true",
    "spark.databricks.service.server.enabled": "true",
    "spark.sql.warehouse.dir": "s3://samsara-databricks-playground/warehouse/",
    "spark.databricks.delta.properties.defaults.checkpointRetentionDuration": "INTERVAL 30 DAYS",
    "viewsEnabled": "true",
    "spark.storage.decommission.shuffleBlocks.enabled": "true",
    "credentialsFile": "/databricks/samsara-data-5142c7cd3ba2.json",
    "spark.sql.sources.default": "delta",
    "spark.hadoop.aws.glue.partition.num.segments": "1",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.default.parallelism": "4096",
    "spark.databricks.hive.metastore.client.pool.size": "3",
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/databricks/samsara-data-5142c7cd3ba2.json",
    "databricks.loki.fileStatusCache.enabled": "false",
    "spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
    "spark.databricks.scan.modTimeCheck.enabled": "false",
}
DEFAULT_CLUSTER_LIBRARIES = [
    {"pypi": {"package": "acryl-datahub==0.13.2"}},
    {"pypi": {"package": "dagster==1.9.1"}},
    {"pypi": {"package": "dagster-aws==0.25.1"}},
    {"pypi": {"package": "dagster-databricks==0.25.1"}},
    {"pypi": {"package": "dagster-pyspark==0.25.1"}},
    {"pypi": {"package": "dagster-slack==0.25.1"}},
    {"pypi": {"package": "datadog==0.42.0"}},
    {"pypi": {"package": "slackclient==2.8.0"}},
    {"pypi": {"package": "sqllineage==1.3.8"}},
    {"pypi": {"package": "sql-formatter==0.6.2"}},
    {"pypi": {"package": "databricks-sql-connector==2.9.3"}},
    {"pypi": {"package": "requests==2.29.0"}},
    {"pypi": {"package": "databricks-sdk==0.29.0"}},
    {"pypi": {"package": "pdpyras==5.2.0"}},
    {"pypi": {"package": "protobuf==3.20.3"}},
    {"pypi": {"package": "gogo-python==0.0.1"}},
    {"pypi": {"package": "optuna==4.6.0"}},
]
REGION_SPECIFIC_LIBRARIES = {
    AWSRegion.US_WEST_2: [
        {
            "whl": "s3://samsara-databricks-workspace/firmware/python_proto_latest/wheels/fwproto-0.0.1-py3-none-any.whl"
        },
        {
            "whl": "s3://samsara-databricks-workspace/firmware/python_proto_latest/wheels/can_recompiler-1.0.0-py3-none-any.whl"
        },
    ],
    AWSRegion.EU_WEST_1: [
        {
            "whl": "s3://samsara-eu-databricks-workspace/firmware/python_proto_latest/wheels/fwproto-0.0.1-py3-none-any.whl"
        },
        {
            "whl": "s3://samsara-eu-databricks-workspace/firmware/python_proto_latest/wheels/can_recompiler-1.0.0-py3-none-any.whl"
        },
    ],
}
DEFAULT_CLUSTER_TAGS = [
    {"key": "samsara:product-group", "value": "aianddata"},
    {"key": "samsara:service", "value": "databricksjob-dagster-cluster"},
    {"key": "samsara:rnd-allocation", "value": "1"},
    {"key": "samsara:team", "value": "dataengineering"},
]

# These Databricks Instance Pools are managed by terraform. We bootstrapped the instance
# pools to obtain the pool ids which are required to launch Dagster runs on them.

# We need to ensure that the availability zones of the driver and worker pools match to avoid cross-az data transfer costs.
# So we bundle the compatible driver and worker pools together.
# Add a new dict item for each additional pool.
GENERAL_PURPOSE_INSTANCE_POOLS = {
    AWSRegion.US_WEST_2: [
        {
            "driver_instance_pool": {
                "pool_id": "0218-200532-pled670-pool-y6fwpqtu",
                "availability_zone": "us-west-2a",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200532-folly880-pool-ddvb16vm",
                "availability_zone": "us-west-2a",
            },
        },
        {
            "driver_instance_pool": {
                "pool_id": "0218-200531-calfs886-pool-0zlkiqly",
                "availability_zone": "us-west-2b",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200531-lags847-pool-svi0qvwy",
                "availability_zone": "us-west-2b",
            },
        },
    ],
    AWSRegion.EU_WEST_1: [
        {
            "driver_instance_pool": {
                "pool_id": "0218-200540-aye38-pool-zclqcd8t",
                "availability_zone": "eu-west-1a",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200540-shove28-pool-vl3kygb1",
                "availability_zone": "eu-west-1a",
            },
        },
        {
            "driver_instance_pool": {
                "pool_id": "0218-200540-clove30-pool-kryl1jbh",
                "availability_zone": "eu-west-1b",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200540-tun26-pool-o4l96zv1",
                "availability_zone": "eu-west-1b",
            },
        },
    ],
    AWSRegion.CA_CENTRAL_1: [
        {
            "driver_instance_pool": {
                "pool_id": "0916-202418-react25-pool-aruqeku3",
                "availability_zone": "ca-central-1a",
            },
            "worker_instance_pool": {
                "pool_id": "0916-202418-maim23-pool-45vi1urv",
                "availability_zone": "ca-central-1a",
            },
        },
        {
            "driver_instance_pool": {
                "pool_id": "0916-202418-seize24-pool-f0zs6k1n",
                "availability_zone": "ca-central-1b",
            },
            "worker_instance_pool": {
                "pool_id": "0916-202418-comma21-pool-e8e98idn",
                "availability_zone": "ca-central-1b",
            },
        },
    ],
}

MEMORY_OPTIMIZED_INSTANCE_POOLS = {
    AWSRegion.US_WEST_2: [
        {
            "driver_instance_pool": {
                "pool_id": "0218-200532-debar887-pool-6znegzyq",
                "availability_zone": "us-west-2a",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200531-wimp668-pool-o4kr6t5e",
                "availability_zone": "us-west-2a",
            },
        },
        {
            "driver_instance_pool": {
                "pool_id": "0218-200531-jock667-pool-7lkxzhg2",
                "availability_zone": "us-west-2b",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200532-bogey671-pool-arnycw7m",
                "availability_zone": "us-west-2b",
            },
        },
    ],
    AWSRegion.EU_WEST_1: [
        {
            "driver_instance_pool": {
                "pool_id": "0218-200541-penny33-pool-bgtlsv0t",
                "availability_zone": "eu-west-1a",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200540-mixer31-pool-24roagwd",
                "availability_zone": "eu-west-1a",
            },
        },
        {
            "driver_instance_pool": {
                "pool_id": "0218-200540-crimp30-pool-4xr6d2j1",
                "availability_zone": "eu-west-1b",
            },
            "worker_instance_pool": {
                "pool_id": "0218-200541-talon34-pool-qu81r6rx",
                "availability_zone": "eu-west-1b",
            },
        },
    ],
    AWSRegion.CA_CENTRAL_1: [
        {
            "driver_instance_pool": {
                "pool_id": "0916-202418-duvet16-pool-ybm4g8p7",
                "availability_zone": "ca-central-1a",
            },
            "worker_instance_pool": {
                "pool_id": "0916-202418-brake26-pool-3eei0557",
                "availability_zone": "ca-central-1a",
            },
        },
        {
            "driver_instance_pool": {
                "pool_id": "0916-202418-acorn18-pool-plq6xxxn",
                "availability_zone": "ca-central-1b",
            },
            "worker_instance_pool": {
                "pool_id": "0916-202418-cuff19-pool-vuzy69k7",
                "availability_zone": "ca-central-1b",
            },
        },
    ],
}
