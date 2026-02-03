from dagster import Definitions, load_assets_from_modules

from . import assets, ecodriving_ml_assets
from ._core import _checks as checks
from ._core.constants import GlossaryTerm
from ._core.dq_utils import (
    JoinableDQCheck,
    NonEmptyDQCheck,
    NonNegativeDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TrendDQCheck,
    ValueRangeDQCheck,
    build_general_dq_checks,
)
from ._core.loaders import load_assets_defs_by_key, load_job_defs_schedules
from ._core.metric import metric
from ._core.resources.databricks import databricks_step_launchers
from ._core.resources.io_managers.deltalake_io_managers import DeltaTableIOManager
from ._core.table import table
from ._core.utils import get_databases
from .userpkgs.constants import (
    GENERAL_PURPOSE_INSTANCE_POOL_KEY,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    AWSRegion,
)

__all__ = ["table", "metric"]


assets_defs_by_key = load_assets_defs_by_key(assets)
# Load raw @asset functions from ecodriving_ml_assets using Dagster's standard loader
ecodriving_ml_assets_list = load_assets_from_modules([ecodriving_ml_assets])
for asset_def in ecodriving_ml_assets_list:
    assets_defs_by_key[asset_def.key] = asset_def

checks._check_unique_dq_names(assets)
checks._check_unique_dq_names(ecodriving_ml_assets)

job_defs, schedule_defs = load_job_defs_schedules()

defs = Definitions(
    assets=assets_defs_by_key.values(),
    jobs=job_defs,
    schedules=schedule_defs,
    resources={
        "dbx_step_launcher_gp_us": databricks_step_launchers.ConfigurableDatabricksStepLauncher(
            region=AWSRegion.US_WEST_2,
            instance_pool_type=GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            max_workers=4,
        ),
        "dbx_step_launcher_gp_eu": databricks_step_launchers.ConfigurableDatabricksStepLauncher(
            region=AWSRegion.EU_WEST_1,
            instance_pool_type=GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            max_workers=4,
        ),
        "dbx_step_launcher_gp_ca": databricks_step_launchers.ConfigurableDatabricksStepLauncher(
            region=AWSRegion.CA_CENTRAL_1,
            instance_pool_type=GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            max_workers=4,
        ),
        "dbx_step_launcher_mo_us": databricks_step_launchers.ConfigurableDatabricksStepLauncher(
            region=AWSRegion.US_WEST_2,
            instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            max_workers=4,
        ),
        "dbx_step_launcher_mo_eu": databricks_step_launchers.ConfigurableDatabricksStepLauncher(
            region=AWSRegion.EU_WEST_1,
            instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            max_workers=4,
        ),
        "dbx_step_launcher_mo_ca": databricks_step_launchers.ConfigurableDatabricksStepLauncher(
            region=AWSRegion.CA_CENTRAL_1,
            instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            max_workers=4,
        ),
        "deltalake_io_manager": DeltaTableIOManager(),
    },
)
