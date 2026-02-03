from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Mapping, TypedDict


@dataclass
class DatabricksAWSAccountId:
    US_WEST_2 = "492164655156"
    EU_WEST_1 = "353964698255"
    CA_CENTRAL_1 = "211125295193"


@dataclass
class DatabricksWorkspaceId:
    US_WEST_2 = "5924096274798303"
    EU_WEST_1 = "6992178240159315"
    CA_CENTRAL_1 = "1976292512529253"


@dataclass
class USDatabricksSQLWarehouseId:
    AIANDDATA = "351c51ed0e7809be"


@dataclass
class EUDatabricksSQLWarehouseId:
    AIANDDATA = "ff24022501efda35"


@dataclass
class CADatabricksSQLWarehouseId:
    AIANDDATA = "1a85cdd2cf692db6"


@dataclass
class ReplicationGroup:
    RDS = "rds"
    KINESISSTATS = "kinesisstats"
    DATASTREAMS = "datastreams"
    S3BIGSTATS = "s3bigstats"
    DATAPIPELINES = "datapipelines"


@dataclass
class Operator:
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    NE = "ne"
    BETWEEN = "between"


@dataclass
class RunEnvironment:
    DEV: str = field(default="dev")
    PROD: str = field(default="prod")


class ClusterConfig(TypedDict):
    driver_instance_type: str
    worker_instance_type: str
    instance_pool_type: str
    max_workers: int
    spark_version: str
    spark_conf_overrides: Dict[str, str]


class RunConfigOverrides(ClusterConfig, TypedDict):
    partition_run_config_overrides: Mapping[str, ClusterConfig]


class DagsterLinks:
    OncallRunBook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5841416/Data+Engineering+-+Oncall+Guide"
    RunPage = "https://dagster.internal.samsara.com/runs/{run_id}"
    RunsOverviewPage = "https://dagster.internal.samsara.com/runs"


class GlossaryTerm(Enum):
    AI = "AI"
    BUSINESS_VALUE = "Business Value"
    MPR = "MPR"
    PLATFORM = "Platform"
    SAFETY = "Safety"
    TELEMATICS = "Telematics"
    TRAINING = "Training"
    FORMS = "Forms"


class DagsterTag(Enum):
    ASSET_PARTITION_RANGE_START = "dagster/asset_partition_range_start"
    ASSET_PARTITION_RANGE_END = "dagster/asset_partition_range_end"
    PARTITION = "dagster/partition"
